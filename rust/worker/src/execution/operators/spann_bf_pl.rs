use std::collections::BinaryHeap;

use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::SpannPosting;
use chroma_types::SignedRoaringBitmap;
use thiserror::Error;
use tonic::async_trait;

use crate::execution::operator::Operator;

use super::knn::RecordDistance;

#[derive(Debug)]
pub struct SpannBfPlInput {
    // Posting list data.
    posting_list: Vec<SpannPosting>,
    // Number of results to return.
    k: usize,
    // Bitmap of records to include/exclude.
    filter: SignedRoaringBitmap,
    // Distance function.
    distance_function: DistanceFunction,
    // Query embedding.
    query: Vec<f32>,
}

#[derive(Debug)]
pub struct SpannBfPlOutput {
    records: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum SpannBfPlError {}

impl ChromaError for SpannBfPlError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Debug)]
pub struct SpannBfPlOperator {}

impl SpannBfPlOperator {
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
            let record = RecordDistance {
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
