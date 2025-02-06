use crate::{ChromaError, ErrorCodes};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Validation error: {0}")]
pub struct ChromaValidationError(#[from] validator::ValidationErrors);

impl ChromaError for ChromaValidationError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}
