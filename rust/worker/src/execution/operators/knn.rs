use std::cmp::Ordering;

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

/// The `KnnOperator` searches for the nearest neighbours of the specified embedding
///
/// # Parameters
/// - `embedding`: The target embedding to search around
/// - `fetch`: The number of records to fetch around the target
///
/// # Implementation
/// `KnnOperator` has multiple implementations for the `Operator<I, O>` trait:
/// - `Operator<KnnLogInput, KnnLogOutput>`: Searches the nearest embeddings in the materialized log
/// - `Operator<KnnHnswInput, KnnHnswOutput>`: Searches the nearest embeddings in the HNSW index
///
/// # Usage
/// It can be used to derive the range of offset ids that should be used by the next operator
#[derive(Clone, Debug)]
pub struct KnnOperator {
    pub embedding: Vec<f32>,
    pub fetch: u32,
}
