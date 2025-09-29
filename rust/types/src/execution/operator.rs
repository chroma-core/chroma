use chroma_error::{ChromaError, ErrorCodes};
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

impl ChromaError for ScanToProtoError {
    fn code(&self) -> ErrorCodes {
        match self {
            ScanToProtoError::CollectionToProto(e) => e.code(),
        }
    }
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
#[derive(Clone, Debug, Default, Serialize)]
pub struct Filter {
    #[serde(default)]
    pub query_ids: Option<Vec<String>>,
    #[serde(default)]
    pub where_clause: Option<Where>,
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // For the new search API, the entire JSON is the where clause
        let where_json = Value::deserialize(deserializer)?;
        let where_clause =
            if where_json.is_null() || where_json.as_object().is_some_and(|obj| obj.is_empty()) {
                None
            } else {
                Some(parse_where(&where_json).map_err(|e| D::Error::custom(e.to_string()))?)
            };

        Ok(Filter {
            query_ids: None, // Always None for new search API
            where_clause,
        })
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
    pub query: QueryVector,
    pub key: String,
    pub limit: u32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Rank {
    pub expr: Option<RankExpr>,
}

impl Rank {
    pub fn knn_queries(&self) -> Vec<KnnQuery> {
        self.expr
            .as_ref()
            .map(RankExpr::knn_queries)
            .unwrap_or_default()
    }
}

impl TryFrom<chroma_proto::RankOperator> for Rank {
    type Error = QueryConversionError;

    fn try_from(proto_rank: chroma_proto::RankOperator) -> Result<Self, Self::Error> {
        Ok(Rank {
            expr: proto_rank.expr.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<Rank> for chroma_proto::RankOperator {
    type Error = QueryConversionError;

    fn try_from(rank: Rank) -> Result<Self, Self::Error> {
        Ok(chroma_proto::RankOperator {
            expr: rank.expr.map(TryInto::try_into).transpose()?,
        })
    }
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
        query: QueryVector,
        #[serde(default = "RankExpr::default_knn_key")]
        key: String,
        #[serde(default = "RankExpr::default_knn_limit")]
        limit: u32,
        #[serde(default)]
        default: Option<f32>,
        #[serde(default)]
        return_rank: bool,
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
                query,
                key,
                limit,
                default: _,
                return_rank: _,
            } => vec![KnnQuery {
                query: query.clone(),
                key: key.clone(),
                limit: *limit,
            }],
        }
    }
}

impl TryFrom<chroma_proto::RankExpr> for RankExpr {
    type Error = QueryConversionError;

    fn try_from(proto_expr: chroma_proto::RankExpr) -> Result<Self, Self::Error> {
        match proto_expr.rank {
            Some(chroma_proto::rank_expr::Rank::Absolute(expr)) => {
                Ok(RankExpr::Absolute(Box::new(RankExpr::try_from(*expr)?)))
            }
            Some(chroma_proto::rank_expr::Rank::Division(div)) => {
                let left = div.left.ok_or(QueryConversionError::field("left"))?;
                let right = div.right.ok_or(QueryConversionError::field("right"))?;
                Ok(RankExpr::Division {
                    left: Box::new(RankExpr::try_from(*left)?),
                    right: Box::new(RankExpr::try_from(*right)?),
                })
            }
            Some(chroma_proto::rank_expr::Rank::Exponentiation(expr)) => Ok(
                RankExpr::Exponentiation(Box::new(RankExpr::try_from(*expr)?)),
            ),
            Some(chroma_proto::rank_expr::Rank::Knn(knn)) => {
                let query = knn
                    .query
                    .ok_or(QueryConversionError::field("query"))?
                    .try_into()?;
                Ok(RankExpr::Knn {
                    query,
                    key: knn.key,
                    limit: knn.limit,
                    default: knn.default,
                    return_rank: knn.return_rank,
                })
            }
            Some(chroma_proto::rank_expr::Rank::Logarithm(expr)) => {
                Ok(RankExpr::Logarithm(Box::new(RankExpr::try_from(*expr)?)))
            }
            Some(chroma_proto::rank_expr::Rank::Maximum(max)) => {
                let exprs = max
                    .exprs
                    .into_iter()
                    .map(RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RankExpr::Maximum(exprs))
            }
            Some(chroma_proto::rank_expr::Rank::Minimum(min)) => {
                let exprs = min
                    .exprs
                    .into_iter()
                    .map(RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RankExpr::Minimum(exprs))
            }
            Some(chroma_proto::rank_expr::Rank::Multiplication(mul)) => {
                let exprs = mul
                    .exprs
                    .into_iter()
                    .map(RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RankExpr::Multiplication(exprs))
            }
            Some(chroma_proto::rank_expr::Rank::Subtraction(sub)) => {
                let left = sub.left.ok_or(QueryConversionError::field("left"))?;
                let right = sub.right.ok_or(QueryConversionError::field("right"))?;
                Ok(RankExpr::Subtraction {
                    left: Box::new(RankExpr::try_from(*left)?),
                    right: Box::new(RankExpr::try_from(*right)?),
                })
            }
            Some(chroma_proto::rank_expr::Rank::Summation(sum)) => {
                let exprs = sum
                    .exprs
                    .into_iter()
                    .map(RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RankExpr::Summation(exprs))
            }
            Some(chroma_proto::rank_expr::Rank::Value(value)) => Ok(RankExpr::Value(value)),
            None => Err(QueryConversionError::field("rank")),
        }
    }
}

impl TryFrom<RankExpr> for chroma_proto::RankExpr {
    type Error = QueryConversionError;

    fn try_from(rank_expr: RankExpr) -> Result<Self, Self::Error> {
        let proto_rank = match rank_expr {
            RankExpr::Absolute(expr) => chroma_proto::rank_expr::Rank::Absolute(Box::new(
                chroma_proto::RankExpr::try_from(*expr)?,
            )),
            RankExpr::Division { left, right } => chroma_proto::rank_expr::Rank::Division(
                Box::new(chroma_proto::rank_expr::RankPair {
                    left: Some(Box::new(chroma_proto::RankExpr::try_from(*left)?)),
                    right: Some(Box::new(chroma_proto::RankExpr::try_from(*right)?)),
                }),
            ),
            RankExpr::Exponentiation(expr) => chroma_proto::rank_expr::Rank::Exponentiation(
                Box::new(chroma_proto::RankExpr::try_from(*expr)?),
            ),
            RankExpr::Knn {
                query,
                key,
                limit,
                default,
                return_rank,
            } => chroma_proto::rank_expr::Rank::Knn(chroma_proto::rank_expr::Knn {
                query: Some(query.try_into()?),
                key,
                limit,
                default,
                return_rank,
            }),
            RankExpr::Logarithm(expr) => chroma_proto::rank_expr::Rank::Logarithm(Box::new(
                chroma_proto::RankExpr::try_from(*expr)?,
            )),
            RankExpr::Maximum(exprs) => {
                let proto_exprs = exprs
                    .into_iter()
                    .map(chroma_proto::RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::rank_expr::Rank::Maximum(chroma_proto::rank_expr::RankList {
                    exprs: proto_exprs,
                })
            }
            RankExpr::Minimum(exprs) => {
                let proto_exprs = exprs
                    .into_iter()
                    .map(chroma_proto::RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::rank_expr::Rank::Minimum(chroma_proto::rank_expr::RankList {
                    exprs: proto_exprs,
                })
            }
            RankExpr::Multiplication(exprs) => {
                let proto_exprs = exprs
                    .into_iter()
                    .map(chroma_proto::RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::rank_expr::Rank::Multiplication(chroma_proto::rank_expr::RankList {
                    exprs: proto_exprs,
                })
            }
            RankExpr::Subtraction { left, right } => chroma_proto::rank_expr::Rank::Subtraction(
                Box::new(chroma_proto::rank_expr::RankPair {
                    left: Some(Box::new(chroma_proto::RankExpr::try_from(*left)?)),
                    right: Some(Box::new(chroma_proto::RankExpr::try_from(*right)?)),
                }),
            ),
            RankExpr::Summation(exprs) => {
                let proto_exprs = exprs
                    .into_iter()
                    .map(chroma_proto::RankExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::rank_expr::Rank::Summation(chroma_proto::rank_expr::RankList {
                    exprs: proto_exprs,
                })
            }
            RankExpr::Value(value) => chroma_proto::rank_expr::Rank::Value(value),
        };

        Ok(chroma_proto::RankExpr {
            rank: Some(proto_rank),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ToSchema)]
pub enum Key {
    // Predefined keys
    Document,
    Embedding,
    Metadata,
    Score,
    MetadataField(String),
}

impl Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Key::Document => serializer.serialize_str("#document"),
            Key::Embedding => serializer.serialize_str("#embedding"),
            Key::Metadata => serializer.serialize_str("#metadata"),
            Key::Score => serializer.serialize_str("#score"),
            Key::MetadataField(field) => serializer.serialize_str(field),
        }
    }
}

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "#document" => Key::Document,
            "#embedding" => Key::Embedding,
            "#metadata" => Key::Metadata,
            "#score" => Key::Score,
            // Any other string is treated as a metadata field key
            field => Key::MetadataField(field.to_string()),
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Select {
    #[serde(default)]
    pub keys: HashSet<Key>,
}

impl TryFrom<chroma_proto::SelectOperator> for Select {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SelectOperator) -> Result<Self, Self::Error> {
        let keys = value
            .keys
            .into_iter()
            .map(|key| {
                // Try to deserialize each string as a Key
                serde_json::from_value(serde_json::Value::String(key))
                    .map_err(|_| QueryConversionError::field("keys"))
            })
            .collect::<Result<HashSet<_>, _>>()?;

        Ok(Self { keys })
    }
}

impl TryFrom<Select> for chroma_proto::SelectOperator {
    type Error = QueryConversionError;

    fn try_from(value: Select) -> Result<Self, Self::Error> {
        let keys = value
            .keys
            .into_iter()
            .map(|key| {
                // Serialize each Key back to string
                serde_json::to_value(&key)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
                    .ok_or(QueryConversionError::field("keys"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { keys })
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

#[derive(Clone, Debug, Default)]
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
    fn test_filter_json_deserialization() {
        // For the new search API, deserialization treats the entire JSON as a where clause

        // Test 1: Simple direct metadata comparison
        let simple_where = r#"{"author": "John Doe"}"#;
        let filter: Filter = serde_json::from_str(simple_where).unwrap();
        assert_eq!(filter.query_ids, None);
        assert!(filter.where_clause.is_some());

        // Test 2: ID filter using #id with $in operator
        let id_filter_json = serde_json::json!({
            "#id": {
                "$in": ["doc1", "doc2", "doc3"]
            }
        });
        let filter: Filter = serde_json::from_value(id_filter_json).unwrap();
        assert_eq!(filter.query_ids, None);
        assert!(filter.where_clause.is_some());

        // Test 3: Complex nested expression with AND, OR, and various operators
        let complex_json = serde_json::json!({
            "$and": [
                {
                    "#id": {
                        "$in": ["doc1", "doc2", "doc3"]
                    }
                },
                {
                    "$or": [
                        {
                            "author": {
                                "$eq": "John Doe"
                            }
                        },
                        {
                            "author": {
                                "$eq": "Jane Smith"
                            }
                        }
                    ]
                },
                {
                    "year": {
                        "$gte": 2020
                    }
                },
                {
                    "tags": {
                        "$contains": "machine-learning"
                    }
                }
            ]
        });

        let filter: Filter = serde_json::from_value(complex_json.clone()).unwrap();
        assert_eq!(filter.query_ids, None);
        assert!(filter.where_clause.is_some());

        // Verify the structure
        if let crate::metadata::Where::Composite(composite) = filter.where_clause.unwrap() {
            assert_eq!(composite.operator, crate::metadata::BooleanOperator::And);
            assert_eq!(composite.children.len(), 4);

            // Check that the second child is an OR
            if let crate::metadata::Where::Composite(or_composite) = &composite.children[1] {
                assert_eq!(or_composite.operator, crate::metadata::BooleanOperator::Or);
                assert_eq!(or_composite.children.len(), 2);
            } else {
                panic!("Expected OR composite in second child");
            }
        } else {
            panic!("Expected AND composite where clause");
        }

        // Test 4: Mixed operators - $ne, $lt, $gt, $lte
        let mixed_operators_json = serde_json::json!({
            "$and": [
                {
                    "status": {
                        "$ne": "deleted"
                    }
                },
                {
                    "score": {
                        "$gt": 0.5
                    }
                },
                {
                    "score": {
                        "$lt": 0.9
                    }
                },
                {
                    "priority": {
                        "$lte": 10
                    }
                }
            ]
        });

        let filter: Filter = serde_json::from_value(mixed_operators_json).unwrap();
        assert_eq!(filter.query_ids, None);
        assert!(filter.where_clause.is_some());

        // Test 5: Deeply nested expression
        let deeply_nested_json = serde_json::json!({
            "$or": [
                {
                    "$and": [
                        {
                            "#id": {
                                "$in": ["id1", "id2"]
                            }
                        },
                        {
                            "$or": [
                                {
                                    "category": "tech"
                                },
                                {
                                    "category": "science"
                                }
                            ]
                        }
                    ]
                },
                {
                    "$and": [
                        {
                            "author": "Admin"
                        },
                        {
                            "published": true
                        }
                    ]
                }
            ]
        });

        let filter: Filter = serde_json::from_value(deeply_nested_json).unwrap();
        assert_eq!(filter.query_ids, None);
        assert!(filter.where_clause.is_some());

        // Verify it's an OR at the top level
        if let crate::metadata::Where::Composite(composite) = filter.where_clause.unwrap() {
            assert_eq!(composite.operator, crate::metadata::BooleanOperator::Or);
            assert_eq!(composite.children.len(), 2);

            // Both children should be AND composites
            for child in &composite.children {
                if let crate::metadata::Where::Composite(and_composite) = child {
                    assert_eq!(
                        and_composite.operator,
                        crate::metadata::BooleanOperator::And
                    );
                } else {
                    panic!("Expected AND composite in OR children");
                }
            }
        } else {
            panic!("Expected OR composite at top level");
        }

        // Test 6: Single ID filter (edge case)
        let single_id_json = serde_json::json!({
            "#id": {
                "$eq": "single-doc-id"
            }
        });

        let filter: Filter = serde_json::from_value(single_id_json).unwrap();
        assert_eq!(filter.query_ids, None);
        assert!(filter.where_clause.is_some());

        // Test 7: Empty object should create empty filter
        let empty_json = serde_json::json!({});
        let filter: Filter = serde_json::from_value(empty_json).unwrap();
        assert_eq!(filter.query_ids, None);
        // Empty object results in None where_clause
        assert_eq!(filter.where_clause, None);

        // Test 8: Combining #id filter with $not_contains and numeric comparisons
        let advanced_json = serde_json::json!({
            "$and": [
                {
                    "#id": {
                        "$in": ["doc1", "doc2", "doc3", "doc4", "doc5"]
                    }
                },
                {
                    "tags": {
                        "$not_contains": "deprecated"
                    }
                },
                {
                    "$or": [
                        {
                            "$and": [
                                {
                                    "confidence": {
                                        "$gte": 0.8
                                    }
                                },
                                {
                                    "verified": true
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "confidence": {
                                        "$gte": 0.6
                                    }
                                },
                                {
                                    "confidence": {
                                        "$lt": 0.8
                                    }
                                },
                                {
                                    "reviews": {
                                        "$gte": 5
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        });

        let filter: Filter = serde_json::from_value(advanced_json).unwrap();
        assert_eq!(filter.query_ids, None);
        assert!(filter.where_clause.is_some());

        // Verify top-level structure
        if let crate::metadata::Where::Composite(composite) = filter.where_clause.unwrap() {
            assert_eq!(composite.operator, crate::metadata::BooleanOperator::And);
            assert_eq!(composite.children.len(), 3);
        } else {
            panic!("Expected AND composite at top level");
        }
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
    fn test_select_key_json_serialization() {
        use std::collections::HashSet;

        // Test predefined keys
        let doc_key = Key::Document;
        assert_eq!(serde_json::to_string(&doc_key).unwrap(), "\"#document\"");

        let embed_key = Key::Embedding;
        assert_eq!(serde_json::to_string(&embed_key).unwrap(), "\"#embedding\"");

        let meta_key = Key::Metadata;
        assert_eq!(serde_json::to_string(&meta_key).unwrap(), "\"#metadata\"");

        let score_key = Key::Score;
        assert_eq!(serde_json::to_string(&score_key).unwrap(), "\"#score\"");

        // Test metadata key
        let custom_key = Key::MetadataField("custom_key".to_string());
        assert_eq!(
            serde_json::to_string(&custom_key).unwrap(),
            "\"custom_key\""
        );

        // Test deserialization
        let deserialized: Key = serde_json::from_str("\"#document\"").unwrap();
        assert!(matches!(deserialized, Key::Document));

        let deserialized: Key = serde_json::from_str("\"custom_field\"").unwrap();
        assert!(matches!(deserialized, Key::MetadataField(s) if s == "custom_field"));

        // Test Select struct with multiple keys
        let mut keys = HashSet::new();
        keys.insert(Key::Document);
        keys.insert(Key::Embedding);
        keys.insert(Key::MetadataField("author".to_string()));

        let select = Select { keys };
        let json = serde_json::to_string(&select).unwrap();
        let deserialized: Select = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.keys.len(), 3);
        assert!(deserialized.keys.contains(&Key::Document));
        assert!(deserialized.keys.contains(&Key::Embedding));
        assert!(deserialized
            .keys
            .contains(&Key::MetadataField("author".to_string())));
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
