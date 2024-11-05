use chroma_error::{ChromaError, ErrorCodes};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlockfileError {
    #[error("Key not found")]
    NotFoundError,
    #[error("Block not found")]
    BlockNotFound,
}

impl ChromaError for BlockfileError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockfileError::NotFoundError => ErrorCodes::InvalidArgument,
            BlockfileError::BlockNotFound => ErrorCodes::Internal,
        }
    }
}
