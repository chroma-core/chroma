use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    Base64DecodeError, CollectionConfigurationToInternalConfigurationError, GetCollectionError,
    UpdateCollectionError,
};
use thiserror::Error;

// Generic HTTP server error types live in `frontend-core::errors` so that any
// HTTP frontend in this workspace can reuse them. `ValidationError` below
// stays here because it has Chroma-specific variants.
pub use frontend_core::errors::{ErrorResponse, ServerError};

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Collection ID is not a valid UUIDv4")]
    CollectionId,
    #[error("Inconsistent dimensions in provided embeddings")]
    DimensionInconsistent,
    #[error("Collection expecting embedding with dimension of {0}, got {1}")]
    DimensionMismatch(u32, u32),
    #[error("Base64 decoding error: {0}")]
    Base64Decode(#[from] Base64DecodeError),
    #[error("Error getting collection: {0}")]
    GetCollection(#[from] GetCollectionError),
    #[error("Error updating collection: {0}")]
    UpdateCollection(#[from] UpdateCollectionError),
    #[error("Error parsing collection configuration: {0}")]
    ParseCollectionConfiguration(#[from] CollectionConfigurationToInternalConfigurationError),
    #[error("{0}")]
    InvalidArgument(String),
}

impl ChromaError for ValidationError {
    fn code(&self) -> ErrorCodes {
        match self {
            ValidationError::CollectionId => ErrorCodes::InvalidArgument,
            ValidationError::DimensionInconsistent => ErrorCodes::InvalidArgument,
            ValidationError::DimensionMismatch(_, _) => ErrorCodes::InvalidArgument,
            ValidationError::Base64Decode(_) => ErrorCodes::InvalidArgument,
            ValidationError::GetCollection(err) => err.code(),
            ValidationError::UpdateCollection(err) => err.code(),
            ValidationError::ParseCollectionConfiguration(_) => ErrorCodes::InvalidArgument,
            ValidationError::InvalidArgument(_) => ErrorCodes::InvalidArgument,
        }
    }
}
