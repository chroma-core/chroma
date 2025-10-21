use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    Base64DecodeError, CollectionConfigurationToInternalConfigurationError, GetCollectionError,
    UpdateCollectionError,
};
use serde::Serialize;
use std::fmt;
use thiserror::Error;
use utoipa::ToSchema;

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
        }
    }
}

/// Wrapper around `dyn ChromaError` that implements `IntoResponse`. This means that route handlers can return `Result<_, ServerError>` and use the `?` operator to return arbitrary errors.
pub struct ServerError(Box<dyn ChromaError>);

impl<E: ChromaError + 'static> From<E> for ServerError {
    fn from(e: E) -> Self {
        ServerError(Box::new(e))
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    error: String,
    message: String,
}

impl ErrorResponse {
    pub fn new(error: String, message: String) -> Self {
        Self { error, message }
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status_code: StatusCode = self.0.code().into();

        let error = ErrorResponse {
            error: self.0.code().name().to_string(),
            message: self.0.to_string(),
        };

        (status_code, Json(error)).into_response()
    }
}
