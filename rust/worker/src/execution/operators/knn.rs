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

#[derive(Debug)]
pub struct KnnOperator {
    pub embedding: Vec<f32>,
    pub fetch: u32,
}
