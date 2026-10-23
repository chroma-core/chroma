use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::spann::types::SpannPosting;
use chroma_segment::distributed_spann::SpannSegmentReaderShard;
use chroma_system::{Operator, OperatorType};
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct SpannFetchPlInput<'referred_data> {
    pub(crate) reader: Option<SpannSegmentReaderShard<'referred_data>>,
    pub(crate) head_id: u32,
}

#[derive(Debug)]
pub(crate) struct SpannFetchPlOutput {
    pub(crate) posting_list: Vec<SpannPosting>,
}

#[derive(Error, Debug)]
pub enum SpannFetchPlError {
    #[error("Error creating spann segment reader")]
    SpannSegmentReaderShardCreationError,
    #[error("Error querying reader")]
    SpannSegmentReaderShardError,
}

impl ChromaError for SpannFetchPlError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::SpannSegmentReaderShardCreationError => ErrorCodes::Internal,
            Self::SpannSegmentReaderShardError => ErrorCodes::Internal,
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
                    .map_err(|_| SpannFetchPlError::SpannSegmentReaderShardError)?;
                Ok(SpannFetchPlOutput { posting_list })
            }
            None => {
                return Err(SpannFetchPlError::SpannSegmentReaderShardCreationError);
            }
        }
    }

    // This operator is IO bound.
    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
