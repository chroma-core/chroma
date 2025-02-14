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

impl From<(&'static str, validator::ValidationError)> for ChromaValidationError {
    fn from((field, error): (&'static str, validator::ValidationError)) -> Self {
        let mut errors = validator::ValidationErrors::new();
        errors.add(field, error);
        Self(errors)
    }
}
