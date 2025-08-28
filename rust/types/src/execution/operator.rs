use core::mem::discriminant;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashSet},
    hash::{Hash, Hasher},
};
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    chroma_proto, logical_size_of_metadata, parse_where, CollectionAndSegments, CollectionUuid,
    Metadata, ScalarEncoding, SparseVector, Where, CHROMA_EMBEDDING_KEY,
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
    pub skip: u32,
    #[serde(default)]
    pub fetch: Option<u32>,
}

impl From<chroma_proto::LimitOperator> for Limit {
    fn from(value: chroma_proto::LimitOperator) -> Self {
        Self {
            skip: value.skip,
            fetch: value.fetch,
        }
    }
}

impl From<Limit> for chroma_proto::LimitOperator {
    fn from(value: Limit) -> Self {
        Self {
            skip: value.skip,
            fetch: value.fetch,
        }
    }
}

/// The `RecordDistance` represents a measure of embedding (identified by `offset_id`) with respect to query embedding
#[derive(Clone, Debug)]
pub struct RecordDistance {
    pub offset_id: u32,
    pub measure: f32,
}

impl PartialEq for RecordDistance {
    fn eq(&self, other: &Self) -> bool {
        self.measure.eq(&other.measure)
    }
}

impl Eq for RecordDistance {}

impl Ord for RecordDistance {
    fn cmp(&self, other: &Self) -> Ordering {
        self.measure.total_cmp(&other.measure)
    }
}

impl PartialOrd for RecordDistance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
pub struct KnnOutput {
    pub distances: Vec<RecordDistance>,
}

/// The `KnnMerge` operator selects the records nearest to target from the batch vectors of records
/// which are all sorted by distance in ascending order. If the same record occurs multiple times
/// only one copy will remain in the final result.
///
/// # Parameters
/// - `fetch`: The total number of records to fetch
///
/// # Usage
/// It can be used to merge the query results from different operators
#[derive(Clone, Debug)]
pub struct KnnMerge {
    pub fetch: u32,
}

impl KnnMerge {
    pub fn merge(&self, input: Vec<Vec<RecordDistance>>) -> Vec<RecordDistance> {
        let mut batch_iters = input.into_iter().map(Vec::into_iter).collect::<Vec<_>>();

        // NOTE: `BinaryHeap<_>` is a max-heap, so we use `Reverse` to convert it into a min-heap
        let mut heap_dist = batch_iters
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, itr)| itr.next().map(|rec| Reverse((rec, idx))))
            .collect::<BinaryHeap<_>>();

        let mut distances = Vec::<RecordDistance>::with_capacity(self.fetch as usize);
        while distances.len() < self.fetch as usize {
            if let Some(Reverse((rec, idx))) = heap_dist.pop() {
                if distances.last().is_none()
                    || distances
                        .last()
                        .is_some_and(|last_rec| last_rec.offset_id != rec.offset_id)
                {
                    distances.push(rec);
                }
                if let Some(next_rec) = batch_iters
                    .get_mut(idx)
                    .expect("Enumerated index should be valid")
                    .next()
                {
                    heap_dist.push(Reverse((next_rec, idx)));
                }
            } else {
                break;
            }
        }
        distances
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
pub enum Rank {
    #[serde(rename = "$dense-knn")]
    DenseKnn {
        embedding: Vec<f32>,
        #[serde(default = "Rank::default_key")]
        key: String,
        #[serde(default = "Rank::default_limit")]
        limit: u32,
    },
    #[serde(rename = "$sparse-knn")]
    SparseKnn {
        embedding: SparseVector,
        #[serde(default = "Rank::default_key")]
        key: String,
        #[serde(default = "Rank::default_limit")]
        limit: u32,
    },
}

impl Rank {
    pub fn default_key() -> String {
        CHROMA_EMBEDDING_KEY.to_string()
    }
    pub fn default_limit() -> u32 {
        1024
    }
}

impl Eq for Rank {}

impl Hash for Rank {
    fn hash<H: Hasher>(&self, state: &mut H) {
        discriminant(self).hash(state);
        match self {
            Rank::DenseKnn {
                embedding,
                key,
                limit,
            } => {
                let bits = embedding
                    .iter()
                    .map(|val| (val + 0.0).to_bits())
                    .collect::<Vec<_>>();
                bits.hash(state);
                key.hash(state);
                limit.hash(state);
            }
            Rank::SparseKnn {
                embedding,
                key,
                limit,
            } => {
                let mut sorted_bits = embedding
                    .iter()
                    .map(|(index, val)| (index, (val + 0.0).to_bits()))
                    .collect::<Vec<_>>();
                sorted_bits.sort_unstable();
                sorted_bits.hash(state);
                key.hash(state);
                limit.hash(state);
            }
        }
    }
}

impl TryFrom<chroma_proto::Rank> for Rank {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::Rank) -> Result<Self, Self::Error> {
        let rank = value.rank.ok_or(QueryConversionError::field("rank"))?;
        match rank {
            chroma_proto::rank::Rank::DenseKnn(dense_knn) => Ok(Rank::DenseKnn {
                embedding: dense_knn
                    .embedding
                    .ok_or(QueryConversionError::field("embedding"))?
                    .try_into()
                    .map(|(v, _)| v)?,
                key: dense_knn.key,
                limit: dense_knn.limit,
            }),
            chroma_proto::rank::Rank::SparseKnn(sparse_knn) => Ok(Rank::SparseKnn {
                embedding: sparse_knn
                    .embedding
                    .ok_or(QueryConversionError::field("embedding"))?
                    .into(),
                key: sparse_knn.key,
                limit: sparse_knn.limit,
            }),
        }
    }
}

impl TryFrom<Rank> for chroma_proto::Rank {
    type Error = QueryConversionError;

    fn try_from(value: Rank) -> Result<Self, Self::Error> {
        match value {
            Rank::DenseKnn {
                embedding,
                key,
                limit,
            } => {
                let dim = embedding.len();
                Ok(chroma_proto::Rank {
                    rank: Some(chroma_proto::rank::Rank::DenseKnn(
                        chroma_proto::rank::DenseKnn {
                            embedding: Some(chroma_proto::Vector::try_from((
                                embedding,
                                ScalarEncoding::FLOAT32,
                                dim,
                            ))?),
                            key,
                            limit,
                        },
                    )),
                })
            }
            Rank::SparseKnn {
                embedding,
                key,
                limit,
            } => Ok(chroma_proto::Rank {
                rank: Some(chroma_proto::rank::Rank::SparseKnn(
                    chroma_proto::rank::SparseKnn {
                        embedding: Some(embedding.into()),
                        key,
                        limit,
                    },
                )),
            }),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Score {
    #[serde(rename = "$abs")]
    Absolute(Box<Score>),
    #[serde(rename = "$div")]
    Division { left: Box<Score>, right: Box<Score> },
    #[serde(rename = "$exp")]
    Exponentiation(Box<Score>),
    #[serde(rename = "$log")]
    Logarithm(Box<Score>),
    #[serde(rename = "$max")]
    Maximum(Vec<Score>),
    #[serde(rename = "$min")]
    Minimum(Vec<Score>),
    #[serde(rename = "$mul")]
    Multiplication(Vec<Score>),
    #[serde(rename = "$rank")]
    Rank {
        #[serde(default)]
        default: Option<f32>,
        #[serde(default)]
        ordinal: bool,
        source: Box<Rank>,
    },
    #[serde(rename = "$sub")]
    Subtraction { left: Box<Score>, right: Box<Score> },
    #[serde(rename = "$sum")]
    Summation(Vec<Score>),
    #[serde(rename = "$val")]
    Value(f32),
}

impl Score {
    pub fn ranks(&self) -> HashSet<Rank> {
        match self {
            Score::Absolute(score) | Score::Exponentiation(score) | Score::Logarithm(score) => {
                score.ranks()
            }
            Score::Division { left, right } | Score::Subtraction { left, right } => {
                left.ranks().into_iter().chain(right.ranks()).collect()
            }
            Score::Maximum(scores)
            | Score::Minimum(scores)
            | Score::Multiplication(scores)
            | Score::Summation(scores) => scores.iter().flat_map(Score::ranks).collect(),
            Score::Value(_) => HashSet::new(),
            Score::Rank {
                default: _,
                ordinal: _,
                source,
            } => HashSet::from_iter([*source.clone()]),
        }
    }
}

impl TryFrom<chroma_proto::Score> for Score {
    type Error = QueryConversionError;

    fn try_from(proto_score: chroma_proto::Score) -> Result<Self, Self::Error> {
        match proto_score.score {
            Some(chroma_proto::score::Score::Absolute(abs)) => {
                Ok(Score::Absolute(Box::new(Score::try_from(*abs)?)))
            }
            Some(chroma_proto::score::Score::Division(div)) => {
                let left = div.left.ok_or(QueryConversionError::field("left"))?;
                let right = div.right.ok_or(QueryConversionError::field("right"))?;
                Ok(Score::Division {
                    left: Box::new(Score::try_from(*left)?),
                    right: Box::new(Score::try_from(*right)?),
                })
            }
            Some(chroma_proto::score::Score::Exponentiation(exp)) => {
                Ok(Score::Exponentiation(Box::new(Score::try_from(*exp)?)))
            }
            Some(chroma_proto::score::Score::Logarithm(log)) => {
                Ok(Score::Logarithm(Box::new(Score::try_from(*log)?)))
            }
            Some(chroma_proto::score::Score::Maximum(max)) => {
                let scores = max
                    .scores
                    .into_iter()
                    .map(Score::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Score::Maximum(scores))
            }
            Some(chroma_proto::score::Score::Minimum(min)) => {
                let scores = min
                    .scores
                    .into_iter()
                    .map(Score::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Score::Minimum(scores))
            }
            Some(chroma_proto::score::Score::Multiplication(mul)) => {
                let scores = mul
                    .scores
                    .into_iter()
                    .map(Score::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Score::Multiplication(scores))
            }
            Some(chroma_proto::score::Score::Rank(rank)) => {
                let source = rank.source.ok_or(QueryConversionError::field("source"))?;
                Ok(Score::Rank {
                    default: rank.default,
                    ordinal: rank.ordinal,
                    source: Box::new(Rank::try_from(source)?),
                })
            }
            Some(chroma_proto::score::Score::Subtraction(sub)) => {
                let left = sub.left.ok_or(QueryConversionError::field("left"))?;
                let right = sub.right.ok_or(QueryConversionError::field("right"))?;
                Ok(Score::Subtraction {
                    left: Box::new(Score::try_from(*left)?),
                    right: Box::new(Score::try_from(*right)?),
                })
            }
            Some(chroma_proto::score::Score::Summation(sum)) => {
                let scores = sum
                    .scores
                    .into_iter()
                    .map(Score::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Score::Summation(scores))
            }
            Some(chroma_proto::score::Score::Value(value)) => Ok(Score::Value(value)),
            None => Err(QueryConversionError::field("score")),
        }
    }
}

impl TryFrom<chroma_proto::ScoreOperator> for Score {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::ScoreOperator) -> Result<Self, Self::Error> {
        value
            .score
            .ok_or(QueryConversionError::field("score"))?
            .try_into()
    }
}

impl TryFrom<Score> for chroma_proto::ScoreOperator {
    type Error = QueryConversionError;

    fn try_from(value: Score) -> Result<Self, Self::Error> {
        Ok(Self {
            score: Some(value.try_into()?),
        })
    }
}

impl TryFrom<Score> for chroma_proto::Score {
    type Error = QueryConversionError;

    fn try_from(score: Score) -> Result<Self, Self::Error> {
        let proto_score = match score {
            Score::Absolute(score) => chroma_proto::score::Score::Absolute(Box::new(
                chroma_proto::Score::try_from(*score)?,
            )),
            Score::Division { left, right } => {
                chroma_proto::score::Score::Division(Box::new(chroma_proto::score::Division {
                    left: Some(Box::new(chroma_proto::Score::try_from(*left)?)),
                    right: Some(Box::new(chroma_proto::Score::try_from(*right)?)),
                }))
            }
            Score::Exponentiation(score) => chroma_proto::score::Score::Exponentiation(Box::new(
                chroma_proto::Score::try_from(*score)?,
            )),
            Score::Logarithm(score) => chroma_proto::score::Score::Logarithm(Box::new(
                chroma_proto::Score::try_from(*score)?,
            )),
            Score::Maximum(scores) => {
                let proto_scores = scores
                    .into_iter()
                    .map(chroma_proto::Score::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::score::Score::Maximum(chroma_proto::score::ScoreList {
                    scores: proto_scores,
                })
            }
            Score::Minimum(scores) => {
                let proto_scores = scores
                    .into_iter()
                    .map(chroma_proto::Score::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::score::Score::Minimum(chroma_proto::score::ScoreList {
                    scores: proto_scores,
                })
            }
            Score::Multiplication(scores) => {
                let proto_scores = scores
                    .into_iter()
                    .map(chroma_proto::Score::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::score::Score::Multiplication(chroma_proto::score::ScoreList {
                    scores: proto_scores,
                })
            }
            Score::Rank {
                default,
                ordinal,
                source,
            } => chroma_proto::score::Score::Rank(chroma_proto::score::RankScore {
                default,
                ordinal,
                source: Some(chroma_proto::Rank::try_from(*source)?),
            }),
            Score::Subtraction { left, right } => chroma_proto::score::Score::Subtraction(
                Box::new(chroma_proto::score::Subtraction {
                    left: Some(Box::new(chroma_proto::Score::try_from(*left)?)),
                    right: Some(Box::new(chroma_proto::Score::try_from(*right)?)),
                }),
            ),
            Score::Summation(scores) => {
                let proto_scores = scores
                    .into_iter()
                    .map(chroma_proto::Score::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                chroma_proto::score::Score::Summation(chroma_proto::score::ScoreList {
                    scores: proto_scores,
                })
            }
            Score::Value(value) => chroma_proto::score::Score::Value(value),
        };

        Ok(chroma_proto::Score {
            score: Some(proto_score),
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Select {
    #[serde(default)]
    pub fields: HashSet<String>,
}

impl TryFrom<chroma_proto::SelectOperator> for Select {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SelectOperator) -> Result<Self, Self::Error> {
        Ok(Self {
            fields: value.fields.into_iter().collect(),
        })
    }
}

impl TryFrom<Select> for chroma_proto::SelectOperator {
    type Error = QueryConversionError;

    fn try_from(value: Select) -> Result<Self, Self::Error> {
        Ok(Self {
            fields: value.fields.into_iter().collect(),
        })
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

impl From<chroma_proto::SearchPayloadResult> for SearchPayloadResult {
    fn from(value: chroma_proto::SearchPayloadResult) -> Self {
        Self {
            records: value
                .records
                .into_iter()
                .filter_map(|r| r.try_into().ok())
                .collect(),
        }
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
            results: value.results.into_iter().map(Into::into).collect(),
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
