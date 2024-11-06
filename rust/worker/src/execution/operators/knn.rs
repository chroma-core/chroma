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
/// It can take different types of input and produce different types of output
///
/// # Parameters
/// - `embedding`: The target embedding to search around
/// - `fetch`: The number of records to fetch around the target
///
/// # Inputs
/// In general, this operator takes inputs representing the domain of records to search within
///
/// # Outputs
/// In general, this operator produces records with embedding nearest to the target embedding
///
/// # Usage
/// It can be used to derive the range of offset ids that should be used by the next operator
#[derive(Clone, Debug)]
pub struct KnnOperator {
    pub embedding: Vec<f32>,
    pub fetch: u32,
}
