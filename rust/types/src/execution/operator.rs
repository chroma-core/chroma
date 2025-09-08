use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    hash::Hash,
};
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    chroma_proto, logical_size_of_metadata, parse_where, CollectionAndSegments, CollectionUuid,
    Metadata, ScalarEncoding, SparseVector, Where,
};

use super::error::QueryConversionError;

pub type InitialInput = ();

/// The `Scan` opeartor pins the data used by all downstream operators
///
/// # Parameters
/// - `collection_and_segments`: The consistent snapshot of collection
#[derive(Clone, Debug)]
pub struct Scan {
    pub collection_and_segments: CollectionAndSegments,
}

impl TryFrom<chroma_proto::ScanOperator> for Scan {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::ScanOperator) -> Result<Self, Self::Error> {
        Ok(Self {
            collection_and_segments: CollectionAndSegments {
                collection: value
                    .collection
                    .ok_or(QueryConversionError::field("collection"))?
                    .try_into()?,
                metadata_segment: value
                    .metadata
                    .ok_or(QueryConversionError::field("metadata segment"))?
                    .try_into()?,
                record_segment: value
                    .record
                    .ok_or(QueryConversionError::field("record segment"))?
                    .try_into()?,
                vector_segment: value
                    .knn
                    .ok_or(QueryConversionError::field("vector segment"))?
                    .try_into()?,
            },
        })
    }
}

#[derive(Debug, Error)]
pub enum ScanToProtoError {
    #[error("Could not convert collection to proto")]
    CollectionToProto(#[from] crate::CollectionToProtoError),
}

impl TryFrom<Scan> for chroma_proto::ScanOperator {
    type Error = ScanToProtoError;

    fn try_from(value: Scan) -> Result<Self, Self::Error> {
        Ok(Self {
            collection: Some(value.collection_and_segments.collection.try_into()?),
            knn: Some(value.collection_and_segments.vector_segment.into()),
            metadata: Some(value.collection_and_segments.metadata_segment.into()),
            record: Some(value.collection_and_segments.record_segment.into()),
        })
    }
}

#[derive(Clone, Debug)]
pub struct CountResult {
    pub count: u32,
    pub pulled_log_bytes: u64,
}

impl CountResult {
    pub fn size_bytes(&self) -> u64 {
        size_of_val(&self.count) as u64
    }
}

impl From<chroma_proto::CountResult> for CountResult {
    fn from(value: chroma_proto::CountResult) -> Self {
        Self {
            count: value.count,
            pulled_log_bytes: value.pulled_log_bytes,
        }
    }
}

impl From<CountResult> for chroma_proto::CountResult {
    fn from(value: CountResult) -> Self {
        Self {
            count: value.count,
            pulled_log_bytes: value.pulled_log_bytes,
        }
    }
}

/// The `FetchLog` operator fetches logs from the log service
///
/// # Parameters
/// - `start_log_offset_id`: The offset id of the first log to read
/// - `maximum_fetch_count`: The maximum number of logs to fetch in total
/// - `collection_uuid`: The uuid of the collection where the fetched logs should belong
#[derive(Clone, Debug)]
pub struct FetchLog {
    pub collection_uuid: CollectionUuid,
    pub maximum_fetch_count: Option<u32>,
    pub start_log_offset_id: u32,
}

/// The `Filter` operator filters the collection with specified criteria
///
/// # Parameters
/// - `query_ids`: The user provided ids, which specifies the domain of the filter if provided
/// - `where_clause`: The predicate on individual record
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Filter {
    #[serde(default)]
    pub query_ids: Option<Vec<String>>,
    #[serde(default, deserialize_with = "Filter::deserialize_where")]
    pub where_clause: Option<Where>,
}

impl Filter {
    fn deserialize_where<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Where>, D::Error> {
        let where_json = Value::deserialize(deserializer)?;
        if where_json.is_null() {
            Ok(None)
        } else {
            Ok(Some(
                parse_where(&where_json).map_err(|e| D::Error::custom(e.to_string()))?,
            ))
        }
    }
}

impl TryFrom<chroma_proto::FilterOperator> for Filter {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::FilterOperator) -> Result<Self, Self::Error> {
        let where_metadata = value.r#where.map(TryInto::try_into).transpose()?;
        let where_document = value.where_document.map(TryInto::try_into).transpose()?;
        let where_clause = match (where_metadata, where_document) {
            (Some(w), Some(wd)) => Some(Where::conjunction(vec![w, wd])),
            (Some(w), None) | (None, Some(w)) => Some(w),
            _ => None,
        };

        Ok(Self {
            query_ids: value.ids.map(|uids| uids.ids),
            where_clause,
        })
    }
}

impl TryFrom<Filter> for chroma_proto::FilterOperator {
    type Error = QueryConversionError;

    fn try_from(value: Filter) -> Result<Self, Self::Error> {
        Ok(Self {
            ids: value.query_ids.map(|ids| chroma_proto::UserIds { ids }),
            r#where: value.where_clause.map(TryInto::try_into).transpose()?,
            where_document: None,
        })
    }
}

/// The `Knn` operator searches for the nearest neighbours of the specified embedding. This is intended to use by executor
///
/// # Parameters
/// - `embedding`: The target embedding to search around
/// - `fetch`: The number of records to fetch around the target
#[derive(Clone, Debug)]
pub struct Knn {
    pub embedding: Vec<f32>,
    pub fetch: u32,
}

impl From<KnnBatch> for Vec<Knn> {
    fn from(value: KnnBatch) -> Self {
        value
            .embeddings
            .into_iter()
            .map(|embedding| Knn {
                embedding,
                fetch: value.fetch,
            })
            .collect()
    }
}

/// The `KnnBatch` operator searches for the nearest neighbours of the specified embedding. This is intended to use by frontend
///
/// # Parameters
/// - `embedding`: The target embedding to search around
/// - `fetch`: The number of records to fetch around the target
#[derive(Clone, Debug)]
pub struct KnnBatch {
    pub embeddings: Vec<Vec<f32>>,
    pub fetch: u32,
}

impl TryFrom<chroma_proto::KnnOperator> for KnnBatch {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::KnnOperator) -> Result<Self, Self::Error> {
        Ok(Self {
            embeddings: value
                .embeddings
                .into_iter()
                .map(|vec| vec.try_into().map(|(v, _)| v))
                .collect::<Result<_, _>>()?,
            fetch: value.fetch,
        })
    }
}

impl TryFrom<KnnBatch> for chroma_proto::KnnOperator {
    type Error = QueryConversionError;

    fn try_from(value: KnnBatch) -> Result<Self, Self::Error> {
        Ok(Self {
            embeddings: value
                .embeddings
                .into_iter()
                .map(|embedding| {
                    let dim = embedding.len();
                    chroma_proto::Vector::try_from((embedding, ScalarEncoding::FLOAT32, dim))
                })
                .collect::<Result<_, _>>()?,
            fetch: value.fetch,
        })
    }
}

/// The `Limit` operator selects a range or records sorted by their offset ids
///
/// # Parameters
/// - `skip`: The number of records to skip in the beginning
/// - `fetch`: The number of records to fetch after `skip`
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Limit {
    #[serde(default)]
    pub offset: u32,
    #[serde(default)]
    pub limit: Option<u32>,
}

impl From<chroma_proto::LimitOperator> for Limit {
    fn from(value: chroma_proto::LimitOperator) -> Self {
        Self {
            offset: value.offset,
            limit: value.limit,
        }
    }
}

impl From<Limit> for chroma_proto::LimitOperator {
    fn from(value: Limit) -> Self {
        Self {
            offset: value.offset,
            limit: value.limit,
        }
    }
}

/// The `RecordDistance` represents a measure of embedding (identified by `offset_id`) with respect to query embedding
#[derive(Clone, Debug)]
pub struct RecordMeasure {
    pub offset_id: u32,
    pub measure: f32,
}

impl PartialEq for RecordMeasure {
    fn eq(&self, other: &Self) -> bool {
        self.offset_id.eq(&other.offset_id)
    }
}

impl Eq for RecordMeasure {}

impl Ord for RecordMeasure {
    fn cmp(&self, other: &Self) -> Ordering {
        self.measure.total_cmp(&other.measure)
    }
}

impl PartialOrd for RecordMeasure {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
pub struct KnnOutput {
    pub distances: Vec<RecordMeasure>,
}

/// The `Merge` operator selects the top records from the batch vectors of records
/// which are all sorted in descending order. If the same record occurs multiple times
/// only one copy will remain in the final result.
///
/// # Parameters
/// - `k`: The total number of records to take after merge
///
/// # Usage
/// It can be used to merge the query results from different operators
#[derive(Clone, Debug)]
pub struct Merge {
    pub k: u32,
}

impl Merge {
    pub fn merge<M: Eq + Ord>(&self, input: Vec<Vec<M>>) -> Vec<M> {
        let mut batch_iters = input.into_iter().map(Vec::into_iter).collect::<Vec<_>>();

        let mut max_heap = batch_iters
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, itr)| itr.next().map(|rec| (rec, idx)))
            .collect::<BinaryHeap<_>>();

        let mut fusion = Vec::with_capacity(self.k as usize);
        while let Some((m, idx)) = max_heap.pop() {
            if self.k <= fusion.len() as u32 {
                break;
            }
            if let Some(next_m) = batch_iters[idx].next() {
                max_heap.push((next_m, idx));
            }
            if fusion.last().is_some_and(|tail| tail == &m) {
                continue;
            }
            fusion.push(m);
        }
        fusion
    }
}

/// The `Projection` operator retrieves record content by offset ids
///
/// # Parameters
/// - `document`: Whether to retrieve document
/// - `embedding`: Whether to retrieve embedding
/// - `metadata`: Whether to retrieve metadata
#[derive(Clone, Debug, Default)]
pub struct Projection {
    pub document: bool,
    pub embedding: bool,
    pub metadata: bool,
}

impl From<chroma_proto::ProjectionOperator> for Projection {
    fn from(value: chroma_proto::ProjectionOperator) -> Self {
        Self {
            document: value.document,
            embedding: value.embedding,
            metadata: value.metadata,
        }
    }
}

impl From<Projection> for chroma_proto::ProjectionOperator {
    fn from(value: Projection) -> Self {
        Self {
            document: value.document,
            embedding: value.embedding,
            metadata: value.metadata,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionRecord {
    pub id: String,
    pub document: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub metadata: Option<Metadata>,
}

impl ProjectionRecord {
    pub fn size_bytes(&self) -> u64 {
        (self.id.len()
            + self
                .document
                .as_ref()
                .map(|doc| doc.len())
                .unwrap_or_default()
            + self
                .embedding
                .as_ref()
                .map(|emb| size_of_val(&emb[..]))
                .unwrap_or_default()
            + self
                .metadata
                .as_ref()
                .map(logical_size_of_metadata)
                .unwrap_or_default()) as u64
    }
}

impl Eq for ProjectionRecord {}

impl TryFrom<chroma_proto::ProjectionRecord> for ProjectionRecord {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::ProjectionRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            document: value.document,
            embedding: value
                .embedding
                .map(|vec| vec.try_into().map(|(v, _)| v))
                .transpose()?,
            metadata: value.metadata.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<ProjectionRecord> for chroma_proto::ProjectionRecord {
    type Error = QueryConversionError;

    fn try_from(value: ProjectionRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            document: value.document,
            embedding: value
                .embedding
                .map(|embedding| {
                    let embedding_dimension = embedding.len();
                    chroma_proto::Vector::try_from((
                        embedding,
                        ScalarEncoding::FLOAT32,
                        embedding_dimension,
                    ))
                })
                .transpose()?,
            metadata: value.metadata.map(|metadata| metadata.into()),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionOutput {
    pub records: Vec<ProjectionRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetResult {
    pub pulled_log_bytes: u64,
    pub result: ProjectionOutput,
}

impl GetResult {
    pub fn size_bytes(&self) -> u64 {
        self.result
            .records
            .iter()
            .map(ProjectionRecord::size_bytes)
            .sum()
    }
}

impl TryFrom<chroma_proto::GetResult> for GetResult {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::GetResult) -> Result<Self, Self::Error> {
        Ok(Self {
            pulled_log_bytes: value.pulled_log_bytes,
            result: ProjectionOutput {
                records: value
                    .records
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            },
        })
    }
}

impl TryFrom<GetResult> for chroma_proto::GetResult {
    type Error = QueryConversionError;

    fn try_from(value: GetResult) -> Result<Self, Self::Error> {
        Ok(Self {
            pulled_log_bytes: value.pulled_log_bytes,
            records: value
                .result
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// The `KnnProjection` operator retrieves record content by offset ids
/// It is based on `ProjectionOperator`, and it attaches the distance
/// of the records to the target embedding to the record content
///
/// # Parameters
/// - `projection`: The parameters of the `ProjectionOperator`
/// - `distance`: Whether to attach distance information
#[derive(Clone, Debug)]
pub struct KnnProjection {
    pub projection: Projection,
    pub distance: bool,
}

impl TryFrom<chroma_proto::KnnProjectionOperator> for KnnProjection {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::KnnProjectionOperator) -> Result<Self, Self::Error> {
        Ok(Self {
            projection: value
                .projection
                .ok_or(QueryConversionError::field("projection"))?
                .into(),
            distance: value.distance,
        })
    }
}

impl From<KnnProjection> for chroma_proto::KnnProjectionOperator {
    fn from(value: KnnProjection) -> Self {
        Self {
            projection: Some(value.projection.into()),
            distance: value.distance,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KnnProjectionRecord {
    pub record: ProjectionRecord,
    pub distance: Option<f32>,
}

impl TryFrom<chroma_proto::KnnProjectionRecord> for KnnProjectionRecord {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::KnnProjectionRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            record: value
                .record
                .ok_or(QueryConversionError::field("record"))?
                .try_into()?,
            distance: value.distance,
        })
    }
}

impl TryFrom<KnnProjectionRecord> for chroma_proto::KnnProjectionRecord {
    type Error = QueryConversionError;

    fn try_from(value: KnnProjectionRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            record: Some(value.record.try_into()?),
            distance: value.distance,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct KnnProjectionOutput {
    pub records: Vec<KnnProjectionRecord>,
}

impl TryFrom<chroma_proto::KnnResult> for KnnProjectionOutput {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::KnnResult) -> Result<Self, Self::Error> {
        Ok(Self {
            records: value
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<KnnProjectionOutput> for chroma_proto::KnnResult {
    type Error = QueryConversionError;

    fn try_from(value: KnnProjectionOutput) -> Result<Self, Self::Error> {
        Ok(Self {
            records: value
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct KnnBatchResult {
    pub pulled_log_bytes: u64,
    pub results: Vec<KnnProjectionOutput>,
}

impl KnnBatchResult {
    pub fn size_bytes(&self) -> u64 {
        self.results
            .iter()
            .flat_map(|res| {
                res.records
                    .iter()
                    .map(|rec| rec.record.size_bytes() + size_of_val(&rec.distance) as u64)
            })
            .sum()
    }
}

impl TryFrom<chroma_proto::KnnBatchResult> for KnnBatchResult {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::KnnBatchResult) -> Result<Self, Self::Error> {
        Ok(Self {
            pulled_log_bytes: value.pulled_log_bytes,
            results: value
                .results
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<KnnBatchResult> for chroma_proto::KnnBatchResult {
    type Error = QueryConversionError;

    fn try_from(value: KnnBatchResult) -> Result<Self, Self::Error> {
        Ok(Self {
            pulled_log_bytes: value.pulled_log_bytes,
            results: value
                .results
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum QueryVector {
    Dense(Vec<f32>),
    Sparse(SparseVector),
}

impl TryFrom<chroma_proto::QueryVector> for QueryVector {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::QueryVector) -> Result<Self, Self::Error> {
        let vector = value.vector.ok_or(QueryConversionError::field("vector"))?;
        match vector {
            chroma_proto::query_vector::Vector::Dense(dense) => {
                Ok(QueryVector::Dense(dense.try_into().map(|(v, _)| v)?))
            }
            chroma_proto::query_vector::Vector::Sparse(sparse) => {
                Ok(QueryVector::Sparse(sparse.into()))
            }
        }
    }
}

impl TryFrom<QueryVector> for chroma_proto::QueryVector {
    type Error = QueryConversionError;

    fn try_from(value: QueryVector) -> Result<Self, Self::Error> {
        match value {
            QueryVector::Dense(vec) => {
                let dim = vec.len();
                Ok(chroma_proto::QueryVector {
                    vector: Some(chroma_proto::query_vector::Vector::Dense(
                        chroma_proto::Vector::try_from((vec, ScalarEncoding::FLOAT32, dim))?,
                    )),
                })
            }
            QueryVector::Sparse(sparse) => Ok(chroma_proto::QueryVector {
                vector: Some(chroma_proto::query_vector::Vector::Sparse(sparse.into())),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct KnnQuery {
    pub embedding: QueryVector,
    pub key: String,
    pub limit: u32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Rank {
    pub expr: Option<RankExpr>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum RankExpr {
    #[serde(rename = "$abs")]
    Absolute(Box<RankExpr>),
    #[serde(rename = "$div")]
    Division {
        left: Box<RankExpr>,
        right: Box<RankExpr>,
    },
    #[serde(rename = "$exp")]
    Exponentiation(Box<RankExpr>),
    #[serde(rename = "$knn")]
    Knn {
        embedding: QueryVector,
        #[serde(default = "RankExpr::default_knn_key")]
        key: String,
        #[serde(default = "RankExpr::default_knn_limit")]
        limit: u32,
        #[serde(default)]
        default: Option<f32>,
        #[serde(default)]
        ordinal: bool,
    },
    #[serde(rename = "$log")]
    Logarithm(Box<RankExpr>),
    #[serde(rename = "$max")]
    Maximum(Vec<RankExpr>),
    #[serde(rename = "$min")]
    Minimum(Vec<RankExpr>),
    #[serde(rename = "$mul")]
    Multiplication(Vec<RankExpr>),
    #[serde(rename = "$sub")]
    Subtraction {
        left: Box<RankExpr>,
        right: Box<RankExpr>,
    },
    #[serde(rename = "$sum")]
    Summation(Vec<RankExpr>),
    #[serde(rename = "$val")]
    Value(f32),
}

impl RankExpr {
    pub fn default_knn_key() -> String {
        "#embedding".to_string()
    }

    pub fn default_knn_limit() -> u32 {
        128
    }

    pub fn knn_queries(&self) -> Vec<KnnQuery> {
        match self {
            RankExpr::Absolute(expr)
            | RankExpr::Exponentiation(expr)
            | RankExpr::Logarithm(expr) => expr.knn_queries(),
            RankExpr::Division { left, right } | RankExpr::Subtraction { left, right } => left
                .knn_queries()
                .into_iter()
                .chain(right.knn_queries())
                .collect(),
            RankExpr::Maximum(exprs)
            | RankExpr::Minimum(exprs)
            | RankExpr::Multiplication(exprs)
            | RankExpr::Summation(exprs) => exprs.iter().flat_map(RankExpr::knn_queries).collect(),
            RankExpr::Value(_) => Vec::new(),
            RankExpr::Knn {
                embedding,
                key,
                limit,
                default: _,
                ordinal: _,
            } => vec![KnnQuery {
                embedding: embedding.clone(),
                key: key.clone(),
                limit: *limit,
            }],
        }
    }
}

impl TryFrom<chroma_proto::Rank> for Rank {
    type Error = QueryConversionError;

    fn try_from(proto_rank: chroma_proto::Rank) -> Result<Self, Self::Error> {
        // Convert proto to RankExpr, then wrap in Rank struct
        let expr = RankExpr::try_from(proto_rank)?;
        Ok(Rank { expr: Some(expr) })
    }
}

impl TryFrom<chroma_proto::Rank> for RankExpr {
    type Error = QueryConversionError;

    fn try_from(proto_rank: chroma_proto::Rank) -> Result<Self, Self::Error> {
        match proto_rank.rank {
            Some(chroma_proto::rank::Rank::Absolute(expr)) => {
                Ok(RankExpr::Absolute(Box::new(RankExpr::try_from(*expr)?)))
            }
            Some(chroma_proto::rank::Rank::Division(div)) => {
                let left = div.left.ok_or(QueryConversionError::field("left"))?;
                let right = div.right.ok_or(QueryConversionError::field("right"))?;
                Ok(RankExpr::Division {
                    left: Box::new(RankExpr::try_from(*left)?),
                    right: Box::new(RankExpr::try_from(*right)?),
                })
            }
            Some(chroma_proto::rank::Rank::Exponentiation(expr)) => Ok(RankExpr::Exponentiation(
                Box::new(RankExpr::try_from(*expr)?),
            )),
            Some(chroma_proto::rank::Rank::Knn(knn)) => {
                let embedding = knn
                    .embedding
                    .ok_or(QueryConversionError::field("embedding"))?
                    .try_into()?;
                Ok(RankExpr::Knn {
                    embedding,
                    key: knn.key,
                    limit: knn.limit,
                    default: knn.default,
                    ordinal: knn.ordinal,
                })
            }
            Some(chroma_proto::rank::Rank::Logarithm(expr)) => {
                Ok(RankExpr::Logarithm(Box::new(RankExpr::try_from(*expr)?)))
            }
            Some(chroma_proto::rank::Rank::Maximum(max)) => {
                let exprs = max
                    .ranks
                    .into_iter()
                    .map(RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RankExpr::Maximum(exprs))
            }
            Some(chroma_proto::rank::Rank::Minimum(min)) => {
                let exprs = min
                    .ranks
                    .into_iter()
                    .map(RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RankExpr::Minimum(exprs))
            }
            Some(chroma_proto::rank::Rank::Multiplication(mul)) => {
                let exprs = mul
                    .ranks
                    .into_iter()
                    .map(RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RankExpr::Multiplication(exprs))
            }
            Some(chroma_proto::rank::Rank::Subtraction(sub)) => {
                let left = sub.left.ok_or(QueryConversionError::field("left"))?;
                let right = sub.right.ok_or(QueryConversionError::field("right"))?;
                Ok(RankExpr::Subtraction {
                    left: Box::new(RankExpr::try_from(*left)?),
                    right: Box::new(RankExpr::try_from(*right)?),
                })
            }
            Some(chroma_proto::rank::Rank::Summation(sum)) => {
                let exprs = sum
                    .ranks
                    .into_iter()
                    .map(RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RankExpr::Summation(exprs))
            }
            Some(chroma_proto::rank::Rank::Value(value)) => Ok(RankExpr::Value(value)),
            None => Err(QueryConversionError::field("rank")),
        }
    }
}

impl TryFrom<Rank> for chroma_proto::Rank {
    type Error = QueryConversionError;

    fn try_from(rank: Rank) -> Result<Self, Self::Error> {
        // If expr is None, we need to handle this case appropriately
        // For now, we'll return an error since the protobuf expects a rank
        match rank.expr {
            Some(expr) => expr.try_into(),
            None => Err(QueryConversionError::field("rank")),
        }
    }
}

impl TryFrom<RankExpr> for chroma_proto::Rank {
    type Error = QueryConversionError;

    fn try_from(rank_expr: RankExpr) -> Result<Self, Self::Error> {
        let proto_rank = match rank_expr {
            RankExpr::Absolute(expr) => {
                chroma_proto::rank::Rank::Absolute(Box::new(chroma_proto::Rank::try_from(*expr)?))
            }
            RankExpr::Division { left, right } => {
                chroma_proto::rank::Rank::Division(Box::new(chroma_proto::rank::Division {
                    left: Some(Box::new(chroma_proto::Rank::try_from(*left)?)),
                    right: Some(Box::new(chroma_proto::Rank::try_from(*right)?)),
                }))
            }
            RankExpr::Exponentiation(expr) => chroma_proto::rank::Rank::Exponentiation(Box::new(
                chroma_proto::Rank::try_from(*expr)?,
            )),
            RankExpr::Knn {
                embedding,
                key,
                limit,
                default,
                ordinal,
            } => chroma_proto::rank::Rank::Knn(chroma_proto::rank::Knn {
                embedding: Some(embedding.try_into()?),
                key,
                limit,
                default,
                ordinal,
            }),
            RankExpr::Logarithm(expr) => {
                chroma_proto::rank::Rank::Logarithm(Box::new(chroma_proto::Rank::try_from(*expr)?))
            }
            RankExpr::Maximum(exprs) => {
                let proto_ranks = exprs
                    .into_iter()
                    .map(chroma_proto::Rank::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::rank::Rank::Maximum(chroma_proto::rank::RankList {
                    ranks: proto_ranks,
                })
            }
            RankExpr::Minimum(exprs) => {
                let proto_ranks = exprs
                    .into_iter()
                    .map(chroma_proto::Rank::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::rank::Rank::Minimum(chroma_proto::rank::RankList {
                    ranks: proto_ranks,
                })
            }
            RankExpr::Multiplication(exprs) => {
                let proto_ranks = exprs
                    .into_iter()
                    .map(chroma_proto::Rank::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::rank::Rank::Multiplication(chroma_proto::rank::RankList {
                    ranks: proto_ranks,
                })
            }
            RankExpr::Subtraction { left, right } => {
                chroma_proto::rank::Rank::Subtraction(Box::new(chroma_proto::rank::Subtraction {
                    left: Some(Box::new(chroma_proto::Rank::try_from(*left)?)),
                    right: Some(Box::new(chroma_proto::Rank::try_from(*right)?)),
                }))
            }
            RankExpr::Summation(exprs) => {
                let proto_ranks = exprs
                    .into_iter()
                    .map(chroma_proto::Rank::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::rank::Rank::Summation(chroma_proto::rank::RankList {
                    ranks: proto_ranks,
                })
            }
            RankExpr::Value(value) => chroma_proto::rank::Rank::Value(value),
        };

        Ok(chroma_proto::Rank {
            rank: Some(proto_rank),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ToSchema)]
pub enum SelectField {
    // Predefined fields
    Document,
    Embedding,
    Metadata,
    Score,
    MetadataField(String),
}

impl Serialize for SelectField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            SelectField::Document => serializer.serialize_str("#document"),
            SelectField::Embedding => serializer.serialize_str("#embedding"),
            SelectField::Metadata => serializer.serialize_str("#metadata"),
            SelectField::Score => serializer.serialize_str("#score"),
            SelectField::MetadataField(field) => serializer.serialize_str(field),
        }
    }
}

impl<'de> Deserialize<'de> for SelectField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "#document" => SelectField::Document,
            "#embedding" => SelectField::Embedding,
            "#metadata" => SelectField::Metadata,
            "#score" => SelectField::Score,
            // Any other string is treated as a metadata field key
            field => SelectField::MetadataField(field.to_string()),
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Select {
    #[serde(default)]
    pub fields: HashSet<SelectField>,
}

impl TryFrom<chroma_proto::SelectOperator> for Select {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SelectOperator) -> Result<Self, Self::Error> {
        let fields = value
            .fields
            .into_iter()
            .map(|field| {
                // Try to deserialize each string as a SelectField
                serde_json::from_value(serde_json::Value::String(field))
                    .map_err(|_| QueryConversionError::field("fields"))
            })
            .collect::<Result<HashSet<_>, _>>()?;

        Ok(Self { fields })
    }
}

impl TryFrom<Select> for chroma_proto::SelectOperator {
    type Error = QueryConversionError;

    fn try_from(value: Select) -> Result<Self, Self::Error> {
        let fields = value
            .fields
            .into_iter()
            .map(|field| {
                // Serialize each SelectField back to string
                serde_json::to_value(&field)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
                    .ok_or(QueryConversionError::field("fields"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { fields })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SearchRecord {
    pub id: String,
    pub document: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub metadata: Option<Metadata>,
    pub score: Option<f32>,
}

impl TryFrom<chroma_proto::SearchRecord> for SearchRecord {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SearchRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            document: value.document,
            embedding: value
                .embedding
                .map(|vec| vec.try_into().map(|(v, _)| v))
                .transpose()?,
            metadata: value.metadata.map(TryInto::try_into).transpose()?,
            score: value.score,
        })
    }
}

impl TryFrom<SearchRecord> for chroma_proto::SearchRecord {
    type Error = QueryConversionError;

    fn try_from(value: SearchRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            document: value.document,
            embedding: value
                .embedding
                .map(|embedding| {
                    let embedding_dimension = embedding.len();
                    chroma_proto::Vector::try_from((
                        embedding,
                        ScalarEncoding::FLOAT32,
                        embedding_dimension,
                    ))
                })
                .transpose()?,
            metadata: value.metadata.map(Into::into),
            score: value.score,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SearchPayloadResult {
    pub records: Vec<SearchRecord>,
}

impl TryFrom<chroma_proto::SearchPayloadResult> for SearchPayloadResult {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SearchPayloadResult) -> Result<Self, Self::Error> {
        Ok(Self {
            records: value
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<SearchPayloadResult> for chroma_proto::SearchPayloadResult {
    type Error = QueryConversionError;

    fn try_from(value: SearchPayloadResult) -> Result<Self, Self::Error> {
        Ok(Self {
            records: value
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SearchResult {
    pub results: Vec<SearchPayloadResult>,
    pub pulled_log_bytes: u64,
}

impl SearchResult {
    pub fn size_bytes(&self) -> u64 {
        self.results
            .iter()
            .flat_map(|result| {
                result.records.iter().map(|record| {
                    (record.id.len()
                        + record
                            .document
                            .as_ref()
                            .map(|doc| doc.len())
                            .unwrap_or_default()
                        + record
                            .embedding
                            .as_ref()
                            .map(|emb| size_of_val(&emb[..]))
                            .unwrap_or_default()
                        + record
                            .metadata
                            .as_ref()
                            .map(logical_size_of_metadata)
                            .unwrap_or_default()
                        + record.score.as_ref().map(size_of_val).unwrap_or_default())
                        as u64
                })
            })
            .sum()
    }
}

impl TryFrom<chroma_proto::SearchResult> for SearchResult {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SearchResult) -> Result<Self, Self::Error> {
        Ok(Self {
            results: value
                .results
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            pulled_log_bytes: value.pulled_log_bytes,
        })
    }
}

impl TryFrom<SearchResult> for chroma_proto::SearchResult {
    type Error = QueryConversionError;

    fn try_from(value: SearchResult) -> Result<Self, Self::Error> {
        Ok(Self {
            results: value
                .results
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            pulled_log_bytes: value.pulled_log_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_vector_dense_proto_conversion() {
        let dense_vec = vec![0.1, 0.2, 0.3, 0.4, 0.5];
        let query_vector = QueryVector::Dense(dense_vec.clone());

        // Convert to proto
        let proto: chroma_proto::QueryVector = query_vector.clone().try_into().unwrap();

        // Convert back
        let converted: QueryVector = proto.try_into().unwrap();

        assert_eq!(converted, query_vector);
        if let QueryVector::Dense(v) = converted {
            assert_eq!(v, dense_vec);
        } else {
            panic!("Expected dense vector");
        }
    }

    #[test]
    fn test_query_vector_sparse_proto_conversion() {
        let sparse = SparseVector::new(vec![0, 5, 10], vec![0.1, 0.5, 0.9]);
        let query_vector = QueryVector::Sparse(sparse.clone());

        // Convert to proto
        let proto: chroma_proto::QueryVector = query_vector.clone().try_into().unwrap();

        // Convert back
        let converted: QueryVector = proto.try_into().unwrap();

        assert_eq!(converted, query_vector);
        if let QueryVector::Sparse(s) = converted {
            assert_eq!(s, sparse);
        } else {
            panic!("Expected sparse vector");
        }
    }

    #[test]
    fn test_filter_json_serialization() {
        // Test basic filter serialization
        let filter = Filter {
            query_ids: Some(vec!["id1".to_string(), "id2".to_string()]),
            where_clause: None,
        };

        let json = serde_json::to_string(&filter).unwrap();
        let deserialized: Filter = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.query_ids, filter.query_ids);
        assert_eq!(deserialized.where_clause, filter.where_clause);

        // Test filter deserialization from JSON with composite where clause
        // This includes both document filters ($contains, $regex) and metadata filters ($gte, $eq)
        let json_str = r#"{
            "query_ids": ["doc1", "doc2", "doc3"],
            "where_clause": {
                "$and": [
                    {
                        "content": {
                            "$contains": "machine learning"
                        }
                    },
                    {
                        "author": "John Doe"
                    },
                    {
                        "year": {
                            "$gte": 2020
                        }
                    },
                    {
                        "description": {
                            "$regex": "^[A-Z].*learning.*"
                        }
                    },
                    {
                        "tags": {
                            "$in": ["AI", "ML", "Deep Learning"]
                        }
                    }
                ]
            }
        }"#;

        let filter: Filter = serde_json::from_str(json_str).unwrap();

        // Verify query_ids
        assert_eq!(
            filter.query_ids,
            Some(vec![
                "doc1".to_string(),
                "doc2".to_string(),
                "doc3".to_string()
            ])
        );

        // Verify where_clause structure
        assert!(filter.where_clause.is_some());
        let where_clause = filter.where_clause.unwrap();

        // Should be a composite AND expression
        if let crate::metadata::Where::Composite(composite) = where_clause {
            assert_eq!(composite.operator, crate::metadata::BooleanOperator::And);
            assert_eq!(composite.children.len(), 5);

            // Check first child - document $contains filter
            if let crate::metadata::Where::Document(doc) = &composite.children[0] {
                assert_eq!(doc.operator, crate::metadata::DocumentOperator::Contains);
                assert_eq!(doc.pattern, "machine learning");
            } else {
                panic!("Expected document filter as first child");
            }

            // Check second child - metadata equality filter (direct form)
            if let crate::metadata::Where::Metadata(meta) = &composite.children[1] {
                assert_eq!(meta.key, "author");
                if let crate::metadata::MetadataComparison::Primitive(op, val) = &meta.comparison {
                    assert_eq!(*op, crate::metadata::PrimitiveOperator::Equal);
                    assert_eq!(
                        *val,
                        crate::metadata::MetadataValue::Str("John Doe".to_string())
                    );
                } else {
                    panic!("Expected primitive comparison for author");
                }
            } else {
                panic!("Expected metadata filter as second child");
            }

            // Check third child - metadata $gte filter
            if let crate::metadata::Where::Metadata(meta) = &composite.children[2] {
                assert_eq!(meta.key, "year");
                if let crate::metadata::MetadataComparison::Primitive(op, val) = &meta.comparison {
                    assert_eq!(*op, crate::metadata::PrimitiveOperator::GreaterThanOrEqual);
                    assert_eq!(*val, crate::metadata::MetadataValue::Int(2020));
                } else {
                    panic!("Expected primitive comparison for year");
                }
            } else {
                panic!("Expected metadata filter as third child");
            }

            // Check fourth child - document $regex filter
            if let crate::metadata::Where::Document(doc) = &composite.children[3] {
                assert_eq!(doc.operator, crate::metadata::DocumentOperator::Regex);
                assert_eq!(doc.pattern, "^[A-Z].*learning.*");
            } else {
                panic!("Expected document regex filter as fourth child");
            }

            // Check fifth child - metadata $in filter
            if let crate::metadata::Where::Metadata(meta) = &composite.children[4] {
                assert_eq!(meta.key, "tags");
                if let crate::metadata::MetadataComparison::Set(op, val) = &meta.comparison {
                    assert_eq!(*op, crate::metadata::SetOperator::In);
                    if let crate::metadata::MetadataSetValue::Str(tags) = val {
                        assert_eq!(tags.len(), 3);
                        assert!(tags.contains(&"AI".to_string()));
                        assert!(tags.contains(&"ML".to_string()));
                        assert!(tags.contains(&"Deep Learning".to_string()));
                    } else {
                        panic!("Expected string set for tags");
                    }
                } else {
                    panic!("Expected set comparison for tags");
                }
            } else {
                panic!("Expected metadata filter as fifth child");
            }
        } else {
            panic!("Expected composite where clause");
        }

        // Test filter with empty query_ids
        let json_str = r#"{
            "query_ids": [],
            "where_clause": null
        }"#;

        let filter: Filter = serde_json::from_str(json_str).unwrap();
        assert_eq!(filter.query_ids, Some(vec![]));
        assert_eq!(filter.where_clause, None);

        // Test filter with null query_ids
        let json_str = r#"{
            "query_ids": null,
            "where_clause": null
        }"#;

        let filter: Filter = serde_json::from_str(json_str).unwrap();
        assert_eq!(filter.query_ids, None);
        assert_eq!(filter.where_clause, None);
    }

    #[test]
    fn test_limit_json_serialization() {
        let limit = Limit {
            offset: 10,
            limit: Some(20),
        };

        let json = serde_json::to_string(&limit).unwrap();
        let deserialized: Limit = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.offset, limit.offset);
        assert_eq!(deserialized.limit, limit.limit);
    }

    #[test]
    fn test_query_vector_json_serialization() {
        // Test dense vector
        let dense = QueryVector::Dense(vec![0.1, 0.2, 0.3]);
        let json = serde_json::to_string(&dense).unwrap();
        let deserialized: QueryVector = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, dense);

        // Test sparse vector
        let sparse = QueryVector::Sparse(SparseVector::new(vec![0, 5, 10], vec![0.1, 0.5, 0.9]));
        let json = serde_json::to_string(&sparse).unwrap();
        let deserialized: QueryVector = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, sparse);
    }

    #[test]
    fn test_select_field_json_serialization() {
        use std::collections::HashSet;

        // Test predefined fields
        let doc_field = SelectField::Document;
        assert_eq!(serde_json::to_string(&doc_field).unwrap(), "\"#document\"");

        let embed_field = SelectField::Embedding;
        assert_eq!(
            serde_json::to_string(&embed_field).unwrap(),
            "\"#embedding\""
        );

        let meta_field = SelectField::Metadata;
        assert_eq!(serde_json::to_string(&meta_field).unwrap(), "\"#metadata\"");

        let score_field = SelectField::Score;
        assert_eq!(serde_json::to_string(&score_field).unwrap(), "\"#score\"");

        // Test metadata field
        let custom_field = SelectField::MetadataField("custom_key".to_string());
        assert_eq!(
            serde_json::to_string(&custom_field).unwrap(),
            "\"custom_key\""
        );

        // Test deserialization
        let deserialized: SelectField = serde_json::from_str("\"#document\"").unwrap();
        assert!(matches!(deserialized, SelectField::Document));

        let deserialized: SelectField = serde_json::from_str("\"custom_field\"").unwrap();
        assert!(matches!(deserialized, SelectField::MetadataField(s) if s == "custom_field"));

        // Test Select struct with multiple fields
        let mut fields = HashSet::new();
        fields.insert(SelectField::Document);
        fields.insert(SelectField::Embedding);
        fields.insert(SelectField::MetadataField("author".to_string()));

        let select = Select { fields };
        let json = serde_json::to_string(&select).unwrap();
        let deserialized: Select = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.fields.len(), 3);
        assert!(deserialized.fields.contains(&SelectField::Document));
        assert!(deserialized.fields.contains(&SelectField::Embedding));
        assert!(deserialized
            .fields
            .contains(&SelectField::MetadataField("author".to_string())));
    }

    #[test]
    fn test_merge_basic_integers() {
        use std::cmp::Reverse;

        let merge = Merge { k: 5 };

        // Input: sorted vectors of Reverse(u32) - ascending order of inner values
        let input = vec![
            vec![Reverse(1), Reverse(4), Reverse(7), Reverse(10)],
            vec![Reverse(2), Reverse(5), Reverse(8)],
            vec![Reverse(3), Reverse(6), Reverse(9), Reverse(11), Reverse(12)],
        ];

        let result = merge.merge(input);

        // Should get top-5 smallest values (largest Reverse values)
        assert_eq!(result.len(), 5);
        assert_eq!(
            result,
            vec![Reverse(1), Reverse(2), Reverse(3), Reverse(4), Reverse(5)]
        );
    }

    #[test]
    fn test_merge_u32_descending() {
        let merge = Merge { k: 6 };

        // Regular u32 in descending order (largest first)
        let input = vec![
            vec![100u32, 75, 50, 25],
            vec![90, 60, 30],
            vec![95, 85, 70, 40, 10],
        ];

        let result = merge.merge(input);

        // Should get top-6 largest u32 values
        assert_eq!(result.len(), 6);
        assert_eq!(result, vec![100, 95, 90, 85, 75, 70]);
    }

    #[test]
    fn test_merge_i32_descending() {
        let merge = Merge { k: 5 };

        // i32 values in descending order (including negatives)
        let input = vec![
            vec![50i32, 10, -10, -50],
            vec![30, 0, -30],
            vec![40, 20, -20, -40],
        ];

        let result = merge.merge(input);

        // Should get top-5 largest i32 values
        assert_eq!(result.len(), 5);
        assert_eq!(result, vec![50, 40, 30, 20, 10]);
    }

    #[test]
    fn test_merge_with_duplicates() {
        let merge = Merge { k: 10 };

        // Input with duplicates using regular u32 in descending order
        let input = vec![
            vec![100u32, 80, 80, 60, 40],
            vec![90, 80, 50, 30],
            vec![100, 70, 60, 20],
        ];

        let result = merge.merge(input);

        // Duplicates should be removed
        assert_eq!(result, vec![100, 90, 80, 70, 60, 50, 40, 30, 20]);
    }

    #[test]
    fn test_merge_empty_vectors() {
        let merge = Merge { k: 5 };

        // All empty with u32
        let input: Vec<Vec<u32>> = vec![vec![], vec![], vec![]];
        let result = merge.merge(input);
        assert_eq!(result.len(), 0);

        // Some empty, some with data (u64)
        let input = vec![vec![], vec![1000u64, 750, 500], vec![], vec![850, 600]];
        let result = merge.merge(input);
        assert_eq!(result, vec![1000, 850, 750, 600, 500]);

        // Single non-empty vector (i32)
        let input = vec![vec![], vec![100i32, 50, 25], vec![]];
        let result = merge.merge(input);
        assert_eq!(result, vec![100, 50, 25]);
    }

    #[test]
    fn test_merge_k_boundary_conditions() {
        // k = 0 with u32
        let merge = Merge { k: 0 };
        let input = vec![vec![100u32, 50], vec![75, 25]];
        let result = merge.merge(input);
        assert_eq!(result.len(), 0);

        // k = 1 with i64
        let merge = Merge { k: 1 };
        let input = vec![vec![1000i64, 500], vec![750, 250], vec![900, 100]];
        let result = merge.merge(input);
        assert_eq!(result, vec![1000]);

        // k larger than total unique elements with u128
        let merge = Merge { k: 100 };
        let input = vec![vec![10000u128, 5000], vec![8000, 3000]];
        let result = merge.merge(input);
        assert_eq!(result, vec![10000, 8000, 5000, 3000]);
    }

    #[test]
    fn test_merge_with_strings() {
        let merge = Merge { k: 4 };

        // Strings must be sorted in descending order (largest first) for the max heap merge
        let input = vec![
            vec!["zebra".to_string(), "dog".to_string(), "apple".to_string()],
            vec!["elephant".to_string(), "banana".to_string()],
            vec!["fish".to_string(), "cat".to_string()],
        ];

        let result = merge.merge(input);

        // Should get top-4 lexicographically largest strings
        assert_eq!(
            result,
            vec![
                "zebra".to_string(),
                "fish".to_string(),
                "elephant".to_string(),
                "dog".to_string()
            ]
        );
    }

    #[test]
    fn test_merge_with_custom_struct() {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        struct Score {
            value: i32,
            id: String,
        }

        let merge = Merge { k: 3 };

        // Custom structs sorted by value (descending), then by id
        let input = vec![
            vec![
                Score {
                    value: 100,
                    id: "a".to_string(),
                },
                Score {
                    value: 80,
                    id: "b".to_string(),
                },
                Score {
                    value: 60,
                    id: "c".to_string(),
                },
            ],
            vec![
                Score {
                    value: 90,
                    id: "d".to_string(),
                },
                Score {
                    value: 70,
                    id: "e".to_string(),
                },
            ],
            vec![
                Score {
                    value: 95,
                    id: "f".to_string(),
                },
                Score {
                    value: 85,
                    id: "g".to_string(),
                },
            ],
        ];

        let result = merge.merge(input);

        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            Score {
                value: 100,
                id: "a".to_string()
            }
        );
        assert_eq!(
            result[1],
            Score {
                value: 95,
                id: "f".to_string()
            }
        );
        assert_eq!(
            result[2],
            Score {
                value: 90,
                id: "d".to_string()
            }
        );
    }

    #[test]
    fn test_merge_preserves_order() {
        use std::cmp::Reverse;

        let merge = Merge { k: 10 };

        // For Reverse, smaller inner values are "larger" in ordering
        // So vectors should be sorted with smallest inner values first
        let input = vec![
            vec![Reverse(2), Reverse(6), Reverse(10), Reverse(14)],
            vec![Reverse(4), Reverse(8), Reverse(12), Reverse(16)],
            vec![Reverse(1), Reverse(3), Reverse(5), Reverse(7), Reverse(9)],
        ];

        let result = merge.merge(input);

        // Verify output maintains order - should be sorted by Reverse ordering
        // which means ascending inner values
        for i in 1..result.len() {
            assert!(
                result[i - 1] >= result[i],
                "Output should be in descending Reverse order"
            );
            assert!(
                result[i - 1].0 <= result[i].0,
                "Inner values should be in ascending order"
            );
        }

        // Check we got the right elements
        assert_eq!(
            result,
            vec![
                Reverse(1),
                Reverse(2),
                Reverse(3),
                Reverse(4),
                Reverse(5),
                Reverse(6),
                Reverse(7),
                Reverse(8),
                Reverse(9),
                Reverse(10)
            ]
        );
    }

    #[test]
    fn test_merge_single_vector() {
        let merge = Merge { k: 3 };

        // Single vector input with u64
        let input = vec![vec![1000u64, 800, 600, 400, 200]];

        let result = merge.merge(input);

        assert_eq!(result, vec![1000, 800, 600]);
    }

    #[test]
    fn test_merge_all_same_values() {
        let merge = Merge { k: 5 };

        // All vectors contain the same value (using i16)
        let input = vec![vec![42i16, 42, 42], vec![42, 42], vec![42, 42, 42, 42]];

        let result = merge.merge(input);

        // Should deduplicate to single value
        assert_eq!(result, vec![42]);
    }

    #[test]
    fn test_merge_mixed_types_sizes() {
        // Test with usize (common in real usage)
        let merge = Merge { k: 4 };
        let input = vec![
            vec![1000usize, 500, 100],
            vec![800, 300],
            vec![900, 600, 200],
        ];
        let result = merge.merge(input);
        assert_eq!(result, vec![1000, 900, 800, 600]);

        // Test with negative integers (i32)
        let merge = Merge { k: 5 };
        let input = vec![vec![10i32, 0, -10, -20], vec![5, -5, -15], vec![15, -25]];
        let result = merge.merge(input);
        assert_eq!(result, vec![15, 10, 5, 0, -5]);
    }
}
