use async_trait::async_trait;
use chroma_blockstore::arrow::block::Block;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{Operator, OperatorType};
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct SpannFetchBlockInput {
    pub(crate) provider: BlockfileProvider,
    pub(crate) block_id: uuid::Uuid,
}

#[derive(Debug)]
pub(crate) struct SpannFetchBlockOutput {
    pub(crate) block: Block,
}

#[derive(Error, Debug)]
pub enum SpannFetchBlockError {
    #[error("Error fetching block from block manager {0}")]
    FetchBlockError(#[source] Box<dyn ChromaError>),
    #[error("Block manager returned None")]
    BlockNotFound,
}

impl ChromaError for SpannFetchBlockError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::FetchBlockError(e) => e.code(),
            Self::BlockNotFound => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpannFetchBlockOperator {}

impl SpannFetchBlockOperator {
    pub fn new() -> Box<Self> {
        Box::new(SpannFetchBlockOperator {})
    }
}

#[async_trait]
impl Operator<SpannFetchBlockInput, SpannFetchBlockOutput> for SpannFetchBlockOperator {
    type Error = SpannFetchBlockError;

    async fn run(
        &self,
        input: &SpannFetchBlockInput,
    ) -> Result<SpannFetchBlockOutput, SpannFetchBlockError> {
        let block = input
            .provider
            .get_block(&input.block_id)
            .await
            .map_err(|e| SpannFetchBlockError::FetchBlockError(Box::new(e)))?
            .ok_or(SpannFetchBlockError::BlockNotFound)?;
        Ok(SpannFetchBlockOutput { block })
    }

    // This operator is IO bound.
    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
