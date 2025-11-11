use serde::{de::Error, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    fmt,
    hash::Hash,
    ops::{Add, Div, Mul, Neg, Sub},
};
use thiserror::Error;

use crate::{
    chroma_proto, logical_size_of_metadata, parse_where, CollectionAndSegments, CollectionUuid,
    DocumentExpression, DocumentOperator, Metadata, MetadataComparison, MetadataExpression,
    MetadataSetValue, MetadataValue, PrimitiveOperator, ScalarEncoding, SetOperator, SparseVector,
    Where,
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

/// Filter the search results.
///
/// Combines document ID filtering with metadata and document content predicates.
/// For the Search API, use `where_clause` with Key expressions.
///
/// # Fields
///
/// * `query_ids` - Optional list of document IDs to filter (legacy, prefer Where expressions)
/// * `where_clause` - Predicate on document metadata, content, or IDs
///
/// # Examples
///
/// ## Simple metadata filter
///
/// ```
/// use chroma_types::operator::{Filter, Key};
///
/// let filter = Filter {
///     query_ids: None,
///     where_clause: Some(Key::field("status").eq("published")),
/// };
/// ```
///
/// ## Combined filters
///
/// ```
/// use chroma_types::operator::{Filter, Key};
///
/// let filter = Filter {
///     query_ids: None,
///     where_clause: Some(
///         Key::field("status").eq("published")
///             & Key::field("year").gte(2020)
///             & Key::field("category").is_in(vec!["tech", "science"])
///     ),
/// };
/// ```
///
/// ## Document content filter
///
/// ```
/// use chroma_types::operator::{Filter, Key};
///
/// let filter = Filter {
///     query_ids: None,
///     where_clause: Some(Key::Document.contains("machine learning")),
/// };
/// ```
#[derive(Clone, Debug, Default)]
pub struct Filter {
    pub query_ids: Option<Vec<String>>,
    pub where_clause: Option<Where>,
}

impl Serialize for Filter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // For the search API, serialize directly as the where clause (or empty object if None)
        // If query_ids are present, they should be combined with the where_clause as Key::ID.is_in([...])

        match (&self.query_ids, &self.where_clause) {
            (None, None) => {
                // No filter at all - serialize empty object
                let map = serializer.serialize_map(Some(0))?;
                map.end()
            }
            (None, Some(where_clause)) => {
                // Only where clause - serialize it directly
                where_clause.serialize(serializer)
            }
            (Some(ids), None) => {
                // Only query_ids - create Where clause: Key::ID.is_in(ids)
                let id_where = Where::Metadata(MetadataExpression {
                    key: "#id".to_string(),
                    comparison: MetadataComparison::Set(
                        SetOperator::In,
                        MetadataSetValue::Str(ids.clone()),
                    ),
                });
                id_where.serialize(serializer)
            }
            (Some(ids), Some(where_clause)) => {
                // Both present - combine with AND: Key::ID.is_in(ids) & where_clause
                let id_where = Where::Metadata(MetadataExpression {
                    key: "#id".to_string(),
                    comparison: MetadataComparison::Set(
                        SetOperator::In,
                        MetadataSetValue::Str(ids.clone()),
                    ),
                });
                let combined = Where::conjunction(vec![id_where, where_clause.clone()]);
                combined.serialize(serializer)
            }
        }
    }
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

/// Pagination control for search results.
///
/// Controls how many results to return and how many to skip for pagination.
///
/// # Fields
///
/// * `offset` - Number of results to skip (default: 0)
/// * `limit` - Maximum results to return (None = no limit)
///
/// # Examples
///
/// ```
/// use chroma_types::operator::Limit;
///
/// // First page: results 0-9
/// let limit = Limit {
///     offset: 0,
///     limit: Some(10),
/// };
///
/// // Second page: results 10-19
/// let limit = Limit {
///     offset: 10,
///     limit: Some(10),
/// };
///
/// // No limit: all results
/// let limit = Limit {
///     offset: 0,
///     limit: None,
/// };
/// ```
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
        self.measure
            .total_cmp(&other.measure)
            .then_with(|| self.offset_id.cmp(&other.offset_id))
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

/// A query vector for KNN search.
///
/// Supports both dense and sparse vector formats.
///
/// # Variants
///
/// ## Dense
///
/// Standard dense embeddings as a vector of floats.
///
/// ```
/// use chroma_types::operator::QueryVector;
///
/// let dense = QueryVector::Dense(vec![0.1, 0.2, 0.3, 0.4]);
/// ```
///
/// ## Sparse
///
/// Sparse vectors with explicit indices and values.
///
/// ```
/// use chroma_types::operator::QueryVector;
/// use chroma_types::SparseVector;
///
/// let sparse = QueryVector::Sparse(SparseVector::new(
///     vec![0, 5, 10, 50],      // indices
///     vec![0.5, 0.3, 0.8, 0.2] // values
/// ));
/// ```
///
/// # Examples
///
/// ## Dense vector in KNN
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let rank = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 100,
///     default: None,
///     return_rank: false,
/// };
/// ```
///
/// ## Sparse vector in KNN
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
/// use chroma_types::SparseVector;
///
/// let rank = RankExpr::Knn {
///     query: QueryVector::Sparse(SparseVector::new(
///         vec![1, 5, 10],
///         vec![0.5, 0.3, 0.8]
///     )),
///     key: Key::field("sparse_embedding"),
///     limit: 100,
///     default: None,
///     return_rank: false,
/// };
/// ```
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

impl From<Vec<f32>> for QueryVector {
    fn from(vec: Vec<f32>) -> Self {
        QueryVector::Dense(vec)
    }
}

impl From<SparseVector> for QueryVector {
    fn from(sparse: SparseVector) -> Self {
        QueryVector::Sparse(sparse)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct KnnQuery {
    pub query: QueryVector,
    pub key: Key,
    pub limit: u32,
}

/// Wrapper for ranking expressions in search queries.
///
/// Contains an optional ranking expression. When None, results are returned in
/// natural storage order without scoring.
///
/// # Fields
///
/// * `expr` - The ranking expression (None = no ranking)
///
/// # Examples
///
/// ```
/// use chroma_types::operator::{Rank, RankExpr, QueryVector, Key};
///
/// // With ranking
/// let rank = Rank {
///     expr: Some(RankExpr::Knn {
///         query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///         key: Key::Embedding,
///         limit: 100,
///         default: None,
///         return_rank: false,
///     }),
/// };
///
/// // No ranking (natural order)
/// let rank = Rank {
///     expr: None,
/// };
/// ```
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

/// A ranking expression for scoring and ordering search results.
///
/// Ranking expressions determine which documents appear in results and their order.
/// Lower scores indicate better matches (distance-based scoring).
///
/// # Variants
///
/// ## Knn - K-Nearest Neighbor Search
///
/// The primary ranking method for vector similarity search.
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let rank = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 100,        // Consider top 100 candidates
///     default: None,     // No default score for missing documents
///     return_rank: false, // Return distances, not rank positions
/// };
/// ```
///
/// ## Value - Constant
///
/// Represents a constant score.
///
/// ```
/// use chroma_types::operator::RankExpr;
///
/// let rank = RankExpr::Value(0.5);
/// ```
///
/// ## Arithmetic Operations
///
/// Combine ranking expressions using standard operators (+, -, *, /).
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let knn1 = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 100,
///     default: None,
///     return_rank: false,
/// };
///
/// let knn2 = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.2, 0.3, 0.4]),
///     key: Key::field("other_embedding"),
///     limit: 100,
///     default: None,
///     return_rank: false,
/// };
///
/// // Weighted combination: 70% knn1 + 30% knn2
/// let combined = knn1 * 0.7 + knn2 * 0.3;
///
/// // Normalized
/// let normalized = combined / 2.0;
/// ```
///
/// ## Mathematical Functions
///
/// Apply mathematical transformations to scores.
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let knn = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 100,
///     default: None,
///     return_rank: false,
/// };
///
/// // Exponential - amplifies differences
/// let amplified = knn.clone().exp();
///
/// // Logarithm - compresses range (add constant to avoid log(0))
/// let compressed = (knn.clone() + 1.0).log();
///
/// // Absolute value
/// let absolute = knn.clone().abs();
///
/// // Min/Max - clamping
/// let clamped = knn.min(1.0).max(0.0);
/// ```
///
/// # Examples
///
/// ## Basic vector search
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let rank = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 100,
///     default: None,
///     return_rank: false,
/// };
/// ```
///
/// ## Hybrid search with weighted combination
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let dense = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 200,
///     default: None,
///     return_rank: false,
/// };
///
/// let sparse = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]), // Use sparse in practice
///     key: Key::field("sparse_embedding"),
///     limit: 200,
///     default: None,
///     return_rank: false,
/// };
///
/// // 70% semantic + 30% keyword
/// let hybrid = dense * 0.7 + sparse * 0.3;
/// ```
///
/// ## Reciprocal Rank Fusion (RRF)
///
/// Use the `rrf()` function for combining rankings with different score scales.
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key, rrf};
///
/// let dense = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 200,
///     default: None,
///     return_rank: true, // RRF requires rank positions
/// };
///
/// let sparse = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::field("sparse_embedding"),
///     limit: 200,
///     default: None,
///     return_rank: true, // RRF requires rank positions
/// };
///
/// let rrf_rank = rrf(
///     vec![dense, sparse],
///     Some(60),           // k parameter (smoothing)
///     Some(vec![0.7, 0.3]), // weights
///     false,              // normalize weights
/// ).unwrap();
/// ```
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
        key: Key,
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
    pub fn default_knn_key() -> Key {
        Key::Embedding
    }

    pub fn default_knn_limit() -> u32 {
        16
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

    /// Applies exponential transformation: e^rank.
    ///
    /// Amplifies differences between scores.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// let knn = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///     key: Key::Embedding,
    ///     limit: 100,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// let amplified = knn.exp();
    /// ```
    pub fn exp(self) -> Self {
        RankExpr::Exponentiation(Box::new(self))
    }

    /// Applies natural logarithm transformation: ln(rank).
    ///
    /// Compresses the score range. Add a constant to avoid log(0).
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// let knn = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///     key: Key::Embedding,
    ///     limit: 100,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// // Add constant to avoid log(0)
    /// let compressed = (knn + 1.0).log();
    /// ```
    pub fn log(self) -> Self {
        RankExpr::Logarithm(Box::new(self))
    }

    /// Takes absolute value of the ranking expression.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// let knn1 = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///     key: Key::Embedding,
    ///     limit: 100,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// let knn2 = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.2, 0.3, 0.4]),
    ///     key: Key::field("other"),
    ///     limit: 100,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// // Absolute difference
    /// let diff = (knn1 - knn2).abs();
    /// ```
    pub fn abs(self) -> Self {
        RankExpr::Absolute(Box::new(self))
    }

    /// Returns maximum of this expression and another.
    ///
    /// Can be chained to clamp scores to a maximum value.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// let knn = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///     key: Key::Embedding,
    ///     limit: 100,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// // Clamp to maximum of 1.0
    /// let clamped = knn.clone().max(1.0);
    ///
    /// // Clamp to range [0.0, 1.0]
    /// let range_clamped = knn.min(0.0).max(1.0);
    /// ```
    pub fn max(self, other: impl Into<RankExpr>) -> Self {
        let other = other.into();

        match self {
            RankExpr::Maximum(mut exprs) => match other {
                RankExpr::Maximum(other_exprs) => {
                    exprs.extend(other_exprs);
                    RankExpr::Maximum(exprs)
                }
                _ => {
                    exprs.push(other);
                    RankExpr::Maximum(exprs)
                }
            },
            _ => match other {
                RankExpr::Maximum(mut exprs) => {
                    exprs.insert(0, self);
                    RankExpr::Maximum(exprs)
                }
                _ => RankExpr::Maximum(vec![self, other]),
            },
        }
    }

    /// Returns minimum of this expression and another.
    ///
    /// Can be chained to clamp scores to a minimum value.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// let knn = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///     key: Key::Embedding,
    ///     limit: 100,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// // Clamp to minimum of 0.0 (ensure non-negative)
    /// let clamped = knn.clone().min(0.0);
    ///
    /// // Clamp to range [0.0, 1.0]
    /// let range_clamped = knn.min(0.0).max(1.0);
    /// ```
    pub fn min(self, other: impl Into<RankExpr>) -> Self {
        let other = other.into();

        match self {
            RankExpr::Minimum(mut exprs) => match other {
                RankExpr::Minimum(other_exprs) => {
                    exprs.extend(other_exprs);
                    RankExpr::Minimum(exprs)
                }
                _ => {
                    exprs.push(other);
                    RankExpr::Minimum(exprs)
                }
            },
            _ => match other {
                RankExpr::Minimum(mut exprs) => {
                    exprs.insert(0, self);
                    RankExpr::Minimum(exprs)
                }
                _ => RankExpr::Minimum(vec![self, other]),
            },
        }
    }
}

impl Add for RankExpr {
    type Output = RankExpr;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            RankExpr::Summation(mut exprs) => match rhs {
                RankExpr::Summation(rhs_exprs) => {
                    exprs.extend(rhs_exprs);
                    RankExpr::Summation(exprs)
                }
                _ => {
                    exprs.push(rhs);
                    RankExpr::Summation(exprs)
                }
            },
            _ => match rhs {
                RankExpr::Summation(mut exprs) => {
                    exprs.insert(0, self);
                    RankExpr::Summation(exprs)
                }
                _ => RankExpr::Summation(vec![self, rhs]),
            },
        }
    }
}

impl Add<f32> for RankExpr {
    type Output = RankExpr;

    fn add(self, rhs: f32) -> Self::Output {
        self + RankExpr::Value(rhs)
    }
}

impl Add<RankExpr> for f32 {
    type Output = RankExpr;

    fn add(self, rhs: RankExpr) -> Self::Output {
        RankExpr::Value(self) + rhs
    }
}

impl Sub for RankExpr {
    type Output = RankExpr;

    fn sub(self, rhs: Self) -> Self::Output {
        RankExpr::Subtraction {
            left: Box::new(self),
            right: Box::new(rhs),
        }
    }
}

impl Sub<f32> for RankExpr {
    type Output = RankExpr;

    fn sub(self, rhs: f32) -> Self::Output {
        self - RankExpr::Value(rhs)
    }
}

impl Sub<RankExpr> for f32 {
    type Output = RankExpr;

    fn sub(self, rhs: RankExpr) -> Self::Output {
        RankExpr::Value(self) - rhs
    }
}

impl Mul for RankExpr {
    type Output = RankExpr;

    fn mul(self, rhs: Self) -> Self::Output {
        match self {
            RankExpr::Multiplication(mut exprs) => match rhs {
                RankExpr::Multiplication(rhs_exprs) => {
                    exprs.extend(rhs_exprs);
                    RankExpr::Multiplication(exprs)
                }
                _ => {
                    exprs.push(rhs);
                    RankExpr::Multiplication(exprs)
                }
            },
            _ => match rhs {
                RankExpr::Multiplication(mut exprs) => {
                    exprs.insert(0, self);
                    RankExpr::Multiplication(exprs)
                }
                _ => RankExpr::Multiplication(vec![self, rhs]),
            },
        }
    }
}

impl Mul<f32> for RankExpr {
    type Output = RankExpr;

    fn mul(self, rhs: f32) -> Self::Output {
        self * RankExpr::Value(rhs)
    }
}

impl Mul<RankExpr> for f32 {
    type Output = RankExpr;

    fn mul(self, rhs: RankExpr) -> Self::Output {
        RankExpr::Value(self) * rhs
    }
}

impl Div for RankExpr {
    type Output = RankExpr;

    fn div(self, rhs: Self) -> Self::Output {
        RankExpr::Division {
            left: Box::new(self),
            right: Box::new(rhs),
        }
    }
}

impl Div<f32> for RankExpr {
    type Output = RankExpr;

    fn div(self, rhs: f32) -> Self::Output {
        self / RankExpr::Value(rhs)
    }
}

impl Div<RankExpr> for f32 {
    type Output = RankExpr;

    fn div(self, rhs: RankExpr) -> Self::Output {
        RankExpr::Value(self) / rhs
    }
}

impl Neg for RankExpr {
    type Output = RankExpr;

    fn neg(self) -> Self::Output {
        RankExpr::Value(-1.0) * self
    }
}

impl From<f32> for RankExpr {
    fn from(v: f32) -> Self {
        RankExpr::Value(v)
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
                    key: Key::from(knn.key),
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
                key: key.to_string(),
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

/// Represents a field key in search queries.
///
/// Used for both selecting fields to return and building filter expressions.
/// Predefined keys access special fields, while custom keys access metadata.
///
/// # Predefined Keys
///
/// - `Key::Document` - Document text content (`#document`)
/// - `Key::Embedding` - Vector embeddings (`#embedding`)
/// - `Key::Metadata` - All metadata fields (`#metadata`)
/// - `Key::Score` - Search scores (`#score`)
///
/// # Custom Keys
///
/// Use `Key::field()` or `Key::from()` to reference metadata fields:
///
/// ```
/// use chroma_types::operator::Key;
///
/// let key = Key::field("author");
/// let key = Key::from("title");
/// ```
///
/// # Examples
///
/// ## Building filters
///
/// ```
/// use chroma_types::operator::Key;
///
/// // Equality
/// let filter = Key::field("status").eq("published");
///
/// // Comparisons
/// let filter = Key::field("year").gte(2020);
/// let filter = Key::field("score").lt(0.9);
///
/// // Set operations
/// let filter = Key::field("category").is_in(vec!["tech", "science"]);
/// let filter = Key::field("status").not_in(vec!["deleted", "archived"]);
///
/// // Document content
/// let filter = Key::Document.contains("machine learning");
/// let filter = Key::Document.regex(r"\bAPI\b");
///
/// // Combining filters
/// let filter = Key::field("status").eq("published")
///     & Key::field("year").gte(2020);
/// ```
///
/// ## Selecting fields
///
/// ```
/// use chroma_types::plan::SearchPayload;
/// use chroma_types::operator::Key;
///
/// let search = SearchPayload::default()
///     .select([
///         Key::Document,
///         Key::Score,
///         Key::field("title"),
///         Key::field("author"),
///     ]);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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
        Ok(Key::from(s))
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Document => write!(f, "#document"),
            Key::Embedding => write!(f, "#embedding"),
            Key::Metadata => write!(f, "#metadata"),
            Key::Score => write!(f, "#score"),
            Key::MetadataField(field) => write!(f, "{}", field),
        }
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        match s {
            "#document" => Key::Document,
            "#embedding" => Key::Embedding,
            "#metadata" => Key::Metadata,
            "#score" => Key::Score,
            // Any other string is treated as a metadata field key
            field => Key::MetadataField(field.to_string()),
        }
    }
}

impl From<String> for Key {
    fn from(s: String) -> Self {
        Key::from(s.as_str())
    }
}

impl Key {
    /// Creates a Key for a custom metadata field.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// let status = Key::field("status");
    /// let year = Key::field("year");
    /// let author = Key::field("author");
    /// ```
    pub fn field(name: impl Into<String>) -> Self {
        Key::MetadataField(name.into())
    }

    /// Creates an equality filter: `field == value`.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// // String equality
    /// let filter = Key::field("status").eq("published");
    ///
    /// // Numeric equality
    /// let filter = Key::field("count").eq(42);
    ///
    /// // Boolean equality
    /// let filter = Key::field("featured").eq(true);
    /// ```
    pub fn eq<T: Into<MetadataValue>>(self, value: T) -> Where {
        Where::Metadata(MetadataExpression {
            key: self.to_string(),
            comparison: MetadataComparison::Primitive(PrimitiveOperator::Equal, value.into()),
        })
    }

    /// Creates an inequality filter: `field != value`.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// let filter = Key::field("status").ne("deleted");
    /// let filter = Key::field("count").ne(0);
    /// ```
    pub fn ne<T: Into<MetadataValue>>(self, value: T) -> Where {
        Where::Metadata(MetadataExpression {
            key: self.to_string(),
            comparison: MetadataComparison::Primitive(PrimitiveOperator::NotEqual, value.into()),
        })
    }

    /// Creates a greater-than filter: `field > value` (numeric only).
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// let filter = Key::field("score").gt(0.5);
    /// let filter = Key::field("year").gt(2020);
    /// ```
    pub fn gt<T: Into<MetadataValue>>(self, value: T) -> Where {
        Where::Metadata(MetadataExpression {
            key: self.to_string(),
            comparison: MetadataComparison::Primitive(PrimitiveOperator::GreaterThan, value.into()),
        })
    }

    /// Creates a greater-than-or-equal filter: `field >= value` (numeric only).
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// let filter = Key::field("score").gte(0.5);
    /// let filter = Key::field("year").gte(2020);
    /// ```
    pub fn gte<T: Into<MetadataValue>>(self, value: T) -> Where {
        Where::Metadata(MetadataExpression {
            key: self.to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::GreaterThanOrEqual,
                value.into(),
            ),
        })
    }

    /// Creates a less-than filter: `field < value` (numeric only).
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// let filter = Key::field("score").lt(0.9);
    /// let filter = Key::field("year").lt(2025);
    /// ```
    pub fn lt<T: Into<MetadataValue>>(self, value: T) -> Where {
        Where::Metadata(MetadataExpression {
            key: self.to_string(),
            comparison: MetadataComparison::Primitive(PrimitiveOperator::LessThan, value.into()),
        })
    }

    /// Creates a less-than-or-equal filter: `field <= value` (numeric only).
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// let filter = Key::field("score").lte(0.9);
    /// let filter = Key::field("year").lte(2024);
    /// ```
    pub fn lte<T: Into<MetadataValue>>(self, value: T) -> Where {
        Where::Metadata(MetadataExpression {
            key: self.to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::LessThanOrEqual,
                value.into(),
            ),
        })
    }

    /// Creates a set membership filter: `field IN values`.
    ///
    /// Accepts any iterator (Vec, array, slice, etc.).
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// // With Vec
    /// let filter = Key::field("year").is_in(vec![2023, 2024, 2025]);
    ///
    /// // With array
    /// let filter = Key::field("category").is_in(["tech", "science", "math"]);
    ///
    /// // With owned strings
    /// let categories = vec!["tech".to_string(), "science".to_string()];
    /// let filter = Key::field("category").is_in(categories);
    /// ```
    pub fn is_in<I, T>(self, values: I) -> Where
    where
        I: IntoIterator<Item = T>,
        Vec<T>: Into<MetadataSetValue>,
    {
        let vec: Vec<T> = values.into_iter().collect();
        Where::Metadata(MetadataExpression {
            key: self.to_string(),
            comparison: MetadataComparison::Set(SetOperator::In, vec.into()),
        })
    }

    /// Creates a set exclusion filter: `field NOT IN values`.
    ///
    /// Accepts any iterator (Vec, array, slice, etc.).
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// // Exclude deleted and archived
    /// let filter = Key::field("status").not_in(vec!["deleted", "archived"]);
    ///
    /// // Exclude specific years
    /// let filter = Key::field("year").not_in(vec![2019, 2020]);
    /// ```
    pub fn not_in<I, T>(self, values: I) -> Where
    where
        I: IntoIterator<Item = T>,
        Vec<T>: Into<MetadataSetValue>,
    {
        let vec: Vec<T> = values.into_iter().collect();
        Where::Metadata(MetadataExpression {
            key: self.to_string(),
            comparison: MetadataComparison::Set(SetOperator::NotIn, vec.into()),
        })
    }

    /// Creates a substring filter (case-sensitive, document content only).
    ///
    /// Note: Currently only works with `Key::Document`. Pattern must have at least
    /// 3 literal characters for accurate results.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// let filter = Key::Document.contains("machine learning");
    /// let filter = Key::Document.contains("API");
    /// ```
    pub fn contains<S: Into<String>>(self, text: S) -> Where {
        Where::Document(DocumentExpression {
            operator: DocumentOperator::Contains,
            pattern: text.into(),
        })
    }

    /// Creates a negative substring filter (case-sensitive, document content only).
    ///
    /// Note: Currently only works with `Key::Document`.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// let filter = Key::Document.not_contains("deprecated");
    /// let filter = Key::Document.not_contains("beta");
    /// ```
    pub fn not_contains<S: Into<String>>(self, text: S) -> Where {
        Where::Document(DocumentExpression {
            operator: DocumentOperator::NotContains,
            pattern: text.into(),
        })
    }

    /// Creates a regex filter (case-sensitive, document content only).
    ///
    /// Note: Currently only works with `Key::Document`. Pattern must have at least
    /// 3 literal characters for accurate results.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// // Match whole word "API"
    /// let filter = Key::Document.regex(r"\bAPI\b");
    ///
    /// // Match version pattern
    /// let filter = Key::Document.regex(r"v\d+\.\d+\.\d+");
    /// ```
    pub fn regex<S: Into<String>>(self, pattern: S) -> Where {
        Where::Document(DocumentExpression {
            operator: DocumentOperator::Regex,
            pattern: pattern.into(),
        })
    }

    /// Creates a negative regex filter (case-sensitive, document content only).
    ///
    /// Note: Currently only works with `Key::Document`.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::operator::Key;
    ///
    /// // Exclude beta versions
    /// let filter = Key::Document.not_regex(r"beta");
    ///
    /// // Exclude test documents
    /// let filter = Key::Document.not_regex(r"\btest\b");
    /// ```
    pub fn not_regex<S: Into<String>>(self, pattern: S) -> Where {
        Where::Document(DocumentExpression {
            operator: DocumentOperator::NotRegex,
            pattern: pattern.into(),
        })
    }
}

/// Field selection for search results.
///
/// Specifies which fields to include in the results. IDs are always included.
///
/// # Fields
///
/// * `keys` - Set of keys to include in results
///
/// # Available Keys
///
/// * `Key::Document` - Document text content
/// * `Key::Embedding` - Vector embeddings
/// * `Key::Metadata` - All metadata fields
/// * `Key::Score` - Search scores
/// * `Key::field("name")` - Specific metadata field
///
/// # Performance
///
/// Selecting fewer fields improves performance by reducing data transfer:
/// - Minimal: IDs only (default, fastest)
/// - Moderate: Scores + specific metadata fields
/// - Heavy: Documents + embeddings (larger payloads)
///
/// # Examples
///
/// ```
/// use chroma_types::operator::{Select, Key};
/// use std::collections::HashSet;
///
/// // Select predefined fields
/// let select = Select {
///     keys: [Key::Document, Key::Score].into_iter().collect(),
/// };
///
/// // Select specific metadata fields
/// let select = Select {
///     keys: [
///         Key::field("title"),
///         Key::field("author"),
///         Key::Score,
///     ].into_iter().collect(),
/// };
///
/// // Select everything
/// let select = Select {
///     keys: [
///         Key::Document,
///         Key::Embedding,
///         Key::Metadata,
///         Key::Score,
///     ].into_iter().collect(),
/// };
/// ```
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

/// A single search result record.
///
/// Contains the document ID and optionally document content, embeddings, metadata,
/// and search score based on what was selected in the search query.
///
/// # Fields
///
/// * `id` - Document ID (always present)
/// * `document` - Document text content (if selected)
/// * `embedding` - Vector embedding (if selected)
/// * `metadata` - Document metadata (if selected)
/// * `score` - Search score (present when ranking is used, lower = better match)
///
/// # Examples
///
/// ```
/// use chroma_types::operator::SearchRecord;
///
/// fn process_results(records: Vec<SearchRecord>) {
///     for record in records {
///         println!("ID: {}", record.id);
///         
///         if let Some(score) = record.score {
///             println!("  Score: {:.3}", score);
///         }
///         
///         if let Some(doc) = record.document {
///             println!("  Document: {}", doc);
///         }
///         
///         if let Some(meta) = record.metadata {
///             println!("  Metadata: {:?}", meta);
///         }
///     }
/// }
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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

/// Results for a single search payload.
///
/// Contains all matching records for one search query.
///
/// # Fields
///
/// * `records` - Vector of search records, ordered by score (ascending)
///
/// # Examples
///
/// ```
/// use chroma_types::operator::{SearchPayloadResult, SearchRecord};
///
/// fn process_search_result(result: SearchPayloadResult) {
///     println!("Found {} results", result.records.len());
///     
///     for (i, record) in result.records.iter().enumerate() {
///         println!("{}. {} (score: {:?})", i + 1, record.id, record.score);
///     }
/// }
/// ```
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

/// Results from a batch search operation.
///
/// Contains results for each search payload in the batch, maintaining the same order
/// as the input searches.
///
/// # Fields
///
/// * `results` - Results for each search payload (indexed by search position)
/// * `pulled_log_bytes` - Total bytes pulled from log (for internal metrics)
///
/// # Examples
///
/// ## Single search
///
/// ```
/// use chroma_types::operator::SearchResult;
///
/// fn process_single_search(result: SearchResult) {
///     // Single search, so results[0] contains our records
///     let records = &result.results[0].records;
///     
///     for record in records {
///         println!("{}: score={:?}", record.id, record.score);
///     }
/// }
/// ```
///
/// ## Batch search
///
/// ```
/// use chroma_types::operator::SearchResult;
///
/// fn process_batch_search(result: SearchResult) {
///     // Multiple searches in batch
///     for (i, search_result) in result.results.iter().enumerate() {
///         println!("\nSearch {}:", i + 1);
///         for record in &search_result.records {
///             println!("  {}: score={:?}", record.id, record.score);
///         }
///     }
/// }
/// ```
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

/// Reciprocal Rank Fusion (RRF) - combines multiple ranking strategies.
///
/// RRF is ideal for hybrid search where you want to merge results from different
/// ranking methods (e.g., dense and sparse embeddings) with different score scales.
/// It uses rank positions instead of raw scores, making it scale-agnostic.
///
/// # Formula
///
/// ```text
/// score = -Σ(weight_i / (k + rank_i))
/// ```
///
/// Where:
/// - `weight_i` = weight for ranking i (default: 1.0)
/// - `rank_i` = rank position from ranking i (0, 1, 2...)
/// - `k` = smoothing parameter (default: 60)
///
/// Score is negative because Chroma uses ascending order (lower = better).
///
/// # Arguments
///
/// * `ranks` - List of ranking expressions (must have `return_rank=true`)
/// * `k` - Smoothing parameter (None = 60). Higher values reduce emphasis on top ranks.
/// * `weights` - Weight for each ranking (None = all 1.0)
/// * `normalize` - If true, normalize weights to sum to 1.0
///
/// # Returns
///
/// A combined RankExpr or an error if:
/// - `ranks` is empty
/// - `weights` length doesn't match `ranks` length
/// - `weights` sum to zero when normalizing
///
/// # Examples
///
/// ## Basic RRF with default parameters
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key, rrf};
///
/// let dense = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 200,
///     default: None,
///     return_rank: true, // Required for RRF
/// };
///
/// let sparse = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::field("sparse_embedding"),
///     limit: 200,
///     default: None,
///     return_rank: true, // Required for RRF
/// };
///
/// // Equal weights, k=60 (defaults)
/// let combined = rrf(vec![dense, sparse], None, None, false).unwrap();
/// ```
///
/// ## RRF with custom weights
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key, rrf};
///
/// # let dense = RankExpr::Knn {
/// #     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
/// #     key: Key::Embedding,
/// #     limit: 200,
/// #     default: None,
/// #     return_rank: true,
/// # };
/// # let sparse = RankExpr::Knn {
/// #     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
/// #     key: Key::field("sparse_embedding"),
/// #     limit: 200,
/// #     default: None,
/// #     return_rank: true,
/// # };
/// // 70% dense, 30% sparse
/// let combined = rrf(
///     vec![dense, sparse],
///     Some(60),
///     Some(vec![0.7, 0.3]),
///     false,
/// ).unwrap();
/// ```
///
/// ## RRF with normalized weights
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key, rrf};
///
/// # let dense = RankExpr::Knn {
/// #     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
/// #     key: Key::Embedding,
/// #     limit: 200,
/// #     default: None,
/// #     return_rank: true,
/// # };
/// # let sparse = RankExpr::Knn {
/// #     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
/// #     key: Key::field("sparse_embedding"),
/// #     limit: 200,
/// #     default: None,
/// #     return_rank: true,
/// # };
/// // Weights [75, 25] normalized to [0.75, 0.25]
/// let combined = rrf(
///     vec![dense, sparse],
///     Some(60),
///     Some(vec![75.0, 25.0]),
///     true, // normalize
/// ).unwrap();
/// ```
///
/// ## Adjusting the k parameter
///
/// ```
/// use chroma_types::operator::{RankExpr, QueryVector, Key, rrf};
///
/// # let dense = RankExpr::Knn {
/// #     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
/// #     key: Key::Embedding,
/// #     limit: 200,
/// #     default: None,
/// #     return_rank: true,
/// # };
/// # let sparse = RankExpr::Knn {
/// #     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
/// #     key: Key::field("sparse_embedding"),
/// #     limit: 200,
/// #     default: None,
/// #     return_rank: true,
/// # };
/// // Small k (10) = heavy emphasis on top ranks
/// let top_heavy = rrf(vec![dense.clone(), sparse.clone()], Some(10), None, false).unwrap();
///
/// // Default k (60) = balanced
/// let balanced = rrf(vec![dense.clone(), sparse.clone()], Some(60), None, false).unwrap();
///
/// // Large k (200) = more uniform weighting
/// let uniform = rrf(vec![dense, sparse], Some(200), None, false).unwrap();
/// ```
pub fn rrf(
    ranks: Vec<RankExpr>,
    k: Option<u32>,
    weights: Option<Vec<f32>>,
    normalize: bool,
) -> Result<RankExpr, QueryConversionError> {
    let k = k.unwrap_or(60);

    if ranks.is_empty() {
        return Err(QueryConversionError::validation(
            "RRF requires at least one rank expression",
        ));
    }

    let weights = weights.unwrap_or_else(|| vec![1.0; ranks.len()]);

    if weights.len() != ranks.len() {
        return Err(QueryConversionError::validation(format!(
            "RRF weights length ({}) must match ranks length ({})",
            weights.len(),
            ranks.len()
        )));
    }

    let weights = if normalize {
        let sum: f32 = weights.iter().sum();
        if sum == 0.0 {
            return Err(QueryConversionError::validation(
                "RRF weights sum to zero, cannot normalize",
            ));
        }
        weights.into_iter().map(|w| w / sum).collect()
    } else {
        weights
    };

    let terms: Vec<RankExpr> = weights
        .into_iter()
        .zip(ranks)
        .map(|(w, rank)| RankExpr::Value(w) / (RankExpr::Value(k as f32) + rank))
        .collect();

    // Safe: ranks is validated as non-empty above, so terms cannot be empty.
    // Using unwrap_or_else as defensive programming to avoid panic.
    let sum = terms
        .into_iter()
        .reduce(|a, b| a + b)
        .unwrap_or(RankExpr::Value(0.0));
    Ok(-sum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_from_string() {
        // Test predefined keys
        assert_eq!(Key::from("#document"), Key::Document);
        assert_eq!(Key::from("#embedding"), Key::Embedding);
        assert_eq!(Key::from("#metadata"), Key::Metadata);
        assert_eq!(Key::from("#score"), Key::Score);

        // Test metadata field keys
        assert_eq!(
            Key::from("custom_field"),
            Key::MetadataField("custom_field".to_string())
        );
        assert_eq!(
            Key::from("author"),
            Key::MetadataField("author".to_string())
        );

        // Test String variant
        assert_eq!(Key::from("#embedding".to_string()), Key::Embedding);
        assert_eq!(
            Key::from("year".to_string()),
            Key::MetadataField("year".to_string())
        );
    }

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
