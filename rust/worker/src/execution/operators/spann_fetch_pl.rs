use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::SpannPosting;
use thiserror::Error;

use crate::{
    execution::operator::{Operator, OperatorType},
    segment::spann_segment::{SpannSegmentReader, SpannSegmentReaderContext},
};

#[derive(Debug)]
pub(crate) struct SpannFetchPlInput {
    // TODO(Sanket): Ship the reader instead of constructing here.
    pub(crate) reader_context: SpannSegmentReaderContext,
    pub(crate) head_id: u32,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SpannFetchPlOutput {
    pub(crate) posting_list: Vec<SpannPosting>,
}

#[derive(Error, Debug)]
pub enum SpannFetchPlError {
    #[error("Error creating spann segment reader")]
    SpannSegmentReaderCreationError,
    #[error("Error querying reader")]
    SpannSegmentReaderError,
}

impl ChromaError for SpannFetchPlError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::SpannSegmentReaderCreationError => ErrorCodes::Internal,
            Self::SpannSegmentReaderError => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpannFetchPlOperator {}

impl SpannFetchPlOperator {
    #[allow(dead_code)]
    pub fn new() -> Box<Self> {
        Box::new(SpannFetchPlOperator {})
    }
}

#[async_trait]
impl Operator<SpannFetchPlInput, SpannFetchPlOutput> for SpannFetchPlOperator {
    type Error = SpannFetchPlError;

    async fn run(
        &self,
        input: &SpannFetchPlInput,
    ) -> Result<SpannFetchPlOutput, SpannFetchPlError> {
        let spann_reader = SpannSegmentReader::from_segment(
            &input.reader_context.segment,
            &input.reader_context.blockfile_provider,
            &input.reader_context.hnsw_provider,
            input.reader_context.dimension,
        )
        .await
        .map_err(|_| SpannFetchPlError::SpannSegmentReaderCreationError)?;
        let posting_list = spann_reader
            .fetch_posting_list(input.head_id)
            .await
            .map_err(|_| SpannFetchPlError::SpannSegmentReaderError)?;
        Ok(SpannFetchPlOutput { posting_list })
    }

    // This operator is IO bound.
    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
