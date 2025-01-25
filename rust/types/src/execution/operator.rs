use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use crate::{CollectionAndSegments, CollectionUuid, Metadata, Where};

pub type InitialInput = ();

/// The `Scan` opeartor pins the data used by all downstream operators
///
/// # Parameters
/// - `collection_and_segments`: The consistent snapshot of collection
#[derive(Clone, Debug)]
pub struct Scan {
    pub collection_and_segments: CollectionAndSegments,
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

/// The `Limit` operator selects a range or records sorted by their offset ids
///
/// # Parameters
/// - `skip`: The number of records to skip in the beginning
/// - `fetch`: The number of records to fetch after `skip`
#[derive(Clone, Debug)]
pub struct Limit {
    pub skip: u32,
    pub fetch: Option<u32>,
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
#[derive(Clone, Debug)]
pub struct Projection {
    pub document: bool,
    pub embedding: bool,
    pub metadata: bool,
}

#[derive(Clone, Debug)]
pub struct ProjectionRecord {
    pub id: String,
    pub document: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub metadata: Option<Metadata>,
}

#[derive(Debug)]
pub struct ProjectionOutput {
    pub records: Vec<ProjectionRecord>,
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

#[derive(Clone, Debug)]
pub struct KnnProjectionRecord {
    pub record: ProjectionRecord,
    pub distance: Option<f32>,
}

#[derive(Clone, Debug, Default)]
pub struct KnnProjectionOutput {
    pub records: Vec<KnnProjectionRecord>,
}
