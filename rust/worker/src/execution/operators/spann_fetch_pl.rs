use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::SpannPosting;
use chroma_segment::distributed_spann::SpannSegmentReader;
use chroma_system::{Operator, OperatorType};
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct SpannFetchPlInput<'referred_data> {
    pub(crate) reader: Option<SpannSegmentReader<'referred_data>>,
    pub(crate) head_id: u32,
}

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
impl Operator<SpannFetchPlInput<'_>, SpannFetchPlOutput> for SpannFetchPlOperator {
    type Error = SpannFetchPlError;

    async fn run(
        &self,
        input: &SpannFetchPlInput,
    ) -> Result<SpannFetchPlOutput, SpannFetchPlError> {
        match &input.reader {
            Some(reader) => {
                let posting_list = reader
                    .fetch_posting_list(input.head_id)
                    .await
                    .map_err(|_| SpannFetchPlError::SpannSegmentReaderError)?;
                Ok(SpannFetchPlOutput { posting_list })
            }
            None => {
                return Err(SpannFetchPlError::SpannSegmentReaderCreationError);
            }
        }
    }

    // This operator is IO bound.
    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
