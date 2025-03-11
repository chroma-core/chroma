use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::distributed_spann::{SpannSegmentReader, SpannSegmentReaderContext};
use chroma_system::Operator;
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct SpannCentersSearchInput {
    // TODO(Sanket): Ship the reader instead of constructing here.
    pub(crate) reader_context: SpannSegmentReaderContext,
    // Assumes that query is already normalized in case of cosine.
    pub(crate) normalized_query: Vec<f32>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SpannCentersSearchOutput {
    pub(crate) center_ids: Vec<usize>,
}

#[derive(Error, Debug)]
pub enum SpannCentersSearchError {
    #[error("Error creating spann segment reader")]
    SpannSegmentReaderCreationError,
    #[error("Error querying RNG")]
    RngQueryError,
}

impl ChromaError for SpannCentersSearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::SpannSegmentReaderCreationError => ErrorCodes::Internal,
            Self::RngQueryError => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpannCentersSearchOperator {}

impl SpannCentersSearchOperator {
    #[allow(dead_code)]
    pub fn new() -> Box<Self> {
        Box::new(SpannCentersSearchOperator {})
    }
}

#[async_trait]
impl Operator<SpannCentersSearchInput, SpannCentersSearchOutput> for SpannCentersSearchOperator {
    type Error = SpannCentersSearchError;

    async fn run(
        &self,
        input: &SpannCentersSearchInput,
    ) -> Result<SpannCentersSearchOutput, SpannCentersSearchError> {
        let spann_reader = SpannSegmentReader::from_segment(
            &input.reader_context.segment,
            &input.reader_context.blockfile_provider,
            &input.reader_context.hnsw_provider,
            input.reader_context.dimension,
        )
        .await
        .map_err(|_| SpannCentersSearchError::SpannSegmentReaderCreationError)?;
        // RNG Query.
        let res = spann_reader
            .rng_query(&input.normalized_query)
            .await
            .map_err(|_| SpannCentersSearchError::RngQueryError)?;
        Ok(SpannCentersSearchOutput { center_ids: res.0 })
    }
}
