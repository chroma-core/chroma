use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::SignedRoaringBitmap;
use thiserror::Error;
use tonic::async_trait;

use crate::{
    execution::operator::Operator,
    segment::spann_segment::{SpannSegmentReader, SpannSegmentReaderContext},
};

use super::knn::RecordDistance;

#[derive(Debug)]
pub struct SpannBfPlInput {
    // Needed for checking if a particular version is outdated.
    reader_context: SpannSegmentReaderContext,
    // Posting list data.
    doc_offset_ids: Vec<u32>,
    doc_versions: Vec<u32>,
    doc_embeddings: Vec<f32>,
    // Number of results to return.
    k: usize,
    // Bitmap of records to include/exclude.
    filter: SignedRoaringBitmap,
    // Distance function.
    distance_function: DistanceFunction,
    // Dimension of the embeddings.
    dimension: usize,
}

#[derive(Debug)]
pub struct SpannBfPlOutput {
    records: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum SpannBfPlError {
    #[error("Error creating spann segment reader")]
    SpannSegmentReaderCreationError,
    #[error("Error querying reader")]
    SpannSegmentReaderError,
}

impl ChromaError for SpannBfPlError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::SpannSegmentReaderCreationError => ErrorCodes::Internal,
            Self::SpannSegmentReaderError => ErrorCodes::Internal,
        }
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
        let spann_reader = SpannSegmentReader::from_segment(
            &input.reader_context.segment,
            &input.reader_context.blockfile_provider,
            &input.reader_context.hnsw_provider,
            input.reader_context.dimension,
        )
        .await
        .map_err(|_| SpannBfPlError::SpannSegmentReaderCreationError)?;

        Ok(SpannBfPlOutput {
            records: Vec::new(),
        })
    }
}
