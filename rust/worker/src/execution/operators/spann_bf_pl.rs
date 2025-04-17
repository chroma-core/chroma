use std::collections::BinaryHeap;

use async_trait::async_trait;
use chroma_blockstore::arrow::block::Block;
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::distributed_spann::{SpannSegmentReader, SpannSegmentReaderError};
use chroma_types::{SignedRoaringBitmap, SpannPostingList};
use thiserror::Error;

use chroma_system::Operator;

use super::knn::RecordDistance;

// Public fields for testing.
#[derive(Debug)]
pub struct SpannBfPlInput<'referred_data> {
    // TODO(Sanket): We might benefit from a flat structure which might be more cache friendly.
    // Block of postings.
    pub block: Block,
    // Key to search.
    pub head_id: u32,
    // Number of results to return.
    pub k: usize,
    // Bitmap of records to include/exclude.
    pub filter: SignedRoaringBitmap,
    // Distance function.
    pub distance_function: DistanceFunction,
    // Query embedding.
    pub query: Vec<f32>,
    // spann reader.
    pub reader: Option<SpannSegmentReader<'referred_data>>,
    pub dimension: usize,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct SpannBfPlOutput {
    pub records: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum SpannBfPlError {
    #[error("Posting list not found in the block")]
    PostingListNotFound,
    #[error("Error checking if version is outdated {0}")]
    VersionCheckError(#[source] SpannSegmentReaderError),
    #[error("Segment reader not found")]
    SegmentReaderNotFound,
}

impl ChromaError for SpannBfPlError {
    fn code(&self) -> ErrorCodes {
        match self {
            SpannBfPlError::PostingListNotFound => ErrorCodes::Internal,
            SpannBfPlError::VersionCheckError(e) => e.code(),
            SpannBfPlError::SegmentReaderNotFound => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpannBfPlOperator {}

impl SpannBfPlOperator {
    pub fn new() -> Box<Self> {
        Box::new(SpannBfPlOperator {})
    }
}

#[async_trait]
impl<'referred_data> Operator<SpannBfPlInput<'referred_data>, SpannBfPlOutput>
    for SpannBfPlOperator
{
    type Error = SpannBfPlError;

    async fn run(&self, input: &SpannBfPlInput) -> Result<SpannBfPlOutput, SpannBfPlError> {
        let posting_list: SpannPostingList<'_> = match input.block.get("", input.head_id) {
            Some(value) => value,
            None => {
                return Err(SpannBfPlError::PostingListNotFound);
            }
        };
        let mut max_heap = BinaryHeap::with_capacity(input.k);
        for (index, doc_offset_id) in posting_list.doc_offset_ids.iter().enumerate() {
            let skip_entry = match &input.filter {
                SignedRoaringBitmap::Include(rbm) => !rbm.contains(*doc_offset_id),
                SignedRoaringBitmap::Exclude(rbm) => rbm.contains(*doc_offset_id),
            };
            if skip_entry {
                continue;
            }
            let reader = match &input.reader {
                Some(reader) => reader,
                None => return Err(SpannBfPlError::SegmentReaderNotFound),
            };
            let version = posting_list.doc_versions[index];
            if reader
                .is_outdated(*doc_offset_id, version)
                .await
                .map_err(SpannBfPlError::VersionCheckError)?
            {
                continue;
            }
            let embedding = &posting_list.doc_embeddings
                [index * input.dimension..(index + 1) * input.dimension];
            let dist = input.distance_function.distance(embedding, &input.query);
            let record = RecordDistance {
                offset_id: *doc_offset_id,
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

// TODO(Sanket): Add test.
