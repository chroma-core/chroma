use chroma_error::{ChromaError, ErrorCodes};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlockfileError {
    #[error("Key not found")]
    NotFoundError,
    #[error("Invalid Key Type")]
    InvalidKeyType,
    #[error("Invalid Value Type")]
    InvalidValueType,
    #[error("Transaction already in progress")]
    TransactionInProgress,
    #[error("Transaction not in progress")]
    TransactionNotInProgress,
    #[error("Block not found")]
    BlockNotFound,
}

impl ChromaError for BlockfileError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockfileError::NotFoundError
            | BlockfileError::InvalidKeyType
            | BlockfileError::InvalidValueType => ErrorCodes::InvalidArgument,
            BlockfileError::TransactionInProgress | BlockfileError::TransactionNotInProgress => {
                ErrorCodes::FailedPrecondition
            }
            BlockfileError::BlockNotFound => ErrorCodes::Internal,
        }
    }
}
