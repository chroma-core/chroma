use std::collections::BinaryHeap;

use async_trait::async_trait;
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::SpannPosting;
use chroma_types::{operator::RecordMeasure, SignedRoaringBitmap};
use thiserror::Error;

use chroma_system::Operator;

// Public fields for testing.
#[derive(Debug)]
pub struct SpannBfPlInput {
    // TODO(Sanket): We might benefit from a flat structure which might be more cache friendly.
    // Posting list data.
    pub posting_list: Vec<SpannPosting>,
    // Number of results to return.
    pub k: usize,
    // Bitmap of records to include/exclude.
    pub filter: SignedRoaringBitmap,
    // Distance function.
    pub distance_function: DistanceFunction,
    // Query embedding.
    pub query: Vec<f32>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct SpannBfPlOutput {
    pub records: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
pub enum SpannBfPlError {}

impl ChromaError for SpannBfPlError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Debug, Clone)]
pub struct SpannBfPlOperator {}

#[allow(dead_code)]
impl SpannBfPlOperator {
    #[allow(dead_code)]
    pub fn new() -> Box<Self> {
        Box::new(SpannBfPlOperator {})
    }
}

#[async_trait]
impl Operator<SpannBfPlInput, SpannBfPlOutput> for SpannBfPlOperator {
    type Error = SpannBfPlError;

    async fn run(&self, input: &SpannBfPlInput) -> Result<SpannBfPlOutput, SpannBfPlError> {
        let mut max_heap = BinaryHeap::with_capacity(input.k);
        for posting in input.posting_list.iter() {
            let skip_entry = match &input.filter {
                SignedRoaringBitmap::Include(rbm) => !rbm.contains(posting.doc_offset_id),
                SignedRoaringBitmap::Exclude(rbm) => rbm.contains(posting.doc_offset_id),
            };
            if skip_entry {
                continue;
            }
            let dist = input
                .distance_function
                .distance(&posting.doc_embedding, &input.query);
            let record = RecordMeasure {
                offset_id: posting.doc_offset_id,
                measure: dist,
            };
            if max_heap.len() < input.k {
                max_heap.push(record);
            } else if let Some(furthest_distance) = max_heap.peek() {
                if &record < furthest_distance {
                    max_heap.pop();
                    max_heap.push(record);
                }
            }
        }
        Ok(SpannBfPlOutput {
            records: max_heap.into_sorted_vec(),
        })
    }
}

#[cfg(test)]
mod test {
    use chroma_distance::DistanceFunction;
    use chroma_index::spann::types::SpannPosting;
    use chroma_system::Operator;
    use chroma_types::SignedRoaringBitmap;
    use roaring::RoaringBitmap;

    use crate::execution::operators::spann_bf_pl::{SpannBfPlInput, SpannBfPlOperator};

    // Basic operator test.
    #[tokio::test]
    async fn test_spann_bf_pl_operator() {
        let mut posting_list = Vec::new();
        for i in 1..=100 {
            posting_list.push(SpannPosting {
                doc_offset_id: i,
                doc_embedding: vec![i as f32; 2],
            });
        }

        let input = SpannBfPlInput {
            posting_list,
            k: 10,
            filter: SignedRoaringBitmap::Exclude(RoaringBitmap::new()),
            distance_function: DistanceFunction::Euclidean,
            query: vec![0.0; 2],
        };

        let operator = SpannBfPlOperator::new();
        let output = operator.run(&input).await.unwrap();
        println!("Output {:?}", output);
        assert_eq!(output.records.len(), 10);
        // Output should be the smallest 10 records.
        for i in 1..=10 {
            assert_eq!(output.records[i - 1].offset_id, i as u32);
        }
    }
}
