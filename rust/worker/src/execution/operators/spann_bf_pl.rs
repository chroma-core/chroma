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
pub(crate) struct SpannBfPlInput {
    // TODO(Sanket): We might benefit from a flat structure which might be more cache friendly.
    // Posting list data.
    pub(crate) posting_list: Vec<SpannPosting>,
    // Number of results to return.
    pub(crate) k: usize,
    // Bitmap of records to include/exclude.
    pub(crate) filter: SignedRoaringBitmap,
    // Distance function.
    pub(crate) distance_function: DistanceFunction,
    // Query embedding.
    pub(crate) query: Vec<f32>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SpannBfPlOutput {
    pub(crate) records: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub(crate) enum SpannBfPlError {}

impl ChromaError for SpannBfPlError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpannBfPlOperator {}

#[allow(dead_code)]
impl SpannBfPlOperator {
    #[allow(dead_code)]
    pub(crate) fn new() -> Box<Self> {
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
