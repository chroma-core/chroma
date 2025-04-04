use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::SpannPosting;
use chroma_segment::distributed_spann::SpannSegmentReaderContext;
use chroma_system::{Operator, OperatorType};
use thiserror::Error;
use tracing::{Instrument, Span};

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
        let spann_reader = input
            .reader_context
            .spann_provider
            .read(
                &input.reader_context.collection,
                &input.reader_context.segment,
                input.reader_context.dimension,
            )
            .instrument(tracing::trace_span!(parent: Span::current(), "Construct spann reader", head_id = input.head_id.to_string()))
            .await
            .map_err(|_| SpannFetchPlError::SpannSegmentReaderCreationError)?;
        let posting_list = spann_reader
            .fetch_posting_list(input.head_id)
            .instrument(tracing::trace_span!(parent: Span::current(), "Fetch Pl", head_id = input.head_id.to_string()))
            .await
            .map_err(|_| SpannFetchPlError::SpannSegmentReaderError)?;
        Ok(SpannFetchPlOutput { posting_list })
    }

    // This operator is IO bound.
    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
