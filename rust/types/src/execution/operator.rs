use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};
use thiserror::Error;

use crate::{
    chroma_proto, logical_size_of_metadata, CollectionAndSegments, CollectionUuid, Metadata,
    ScalarEncoding, Where,
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
#[derive(Clone, Debug)]
pub struct Filter {
    pub query_ids: Option<Vec<String>>,
    pub where_clause: Option<Where>,
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
#[derive(Clone, Debug, Default)]
pub struct Limit {
    pub skip: u32,
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

/// The `RecordDistance` represents how far the embedding (identified by `offset_id`) is to the query embedding
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

/// The `KnnMerge` operator selects the records nearest to target from the batch vectors of records
/// which are all sorted by distance in ascending order
///
/// # Parameters
/// - `fetch`: The total number of records to fetch
///
/// # Inputs
/// - `batch_distances`: The batch vector of records, each sorted by distance in ascending order
///
/// # Outputs
/// - `distances`: The nearest records in either vectors, sorted by distance in ascending order
///
/// # Usage
/// It can be used to merge the query results from different operators
#[derive(Clone, Debug)]
pub struct KnnMerge {
    pub fetch: u32,
}

#[derive(Debug)]
pub struct KnnMergeInput {
    pub batch_distances: Vec<Vec<RecordDistance>>,
}

#[derive(Debug)]
pub struct KnnMergeOutput {
    pub distances: Vec<RecordDistance>,
}

impl KnnMerge {
    pub fn merge(&self, input: KnnMergeInput) -> KnnMergeOutput {
        let mut batch_iters = input
            .batch_distances
            .into_iter()
            .map(Vec::into_iter)
            .collect::<Vec<_>>();

        // NOTE: `BinaryHeap<_>` is a max-heap, so we use `Reverse` to convert it into a min-heap
        let mut heap_dist = batch_iters
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, itr)| itr.next().map(|rec| Reverse((rec, idx))))
            .collect::<BinaryHeap<_>>();

        let mut distances = Vec::new();
        while distances.len() < self.fetch as usize {
            if let Some(Reverse((rec, idx))) = heap_dist.pop() {
                distances.push(rec);
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
        KnnMergeOutput { distances }
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
