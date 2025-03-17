use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{GetCollectionError, UpdateCollectionError};
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
    #[error("Error getting collection: {0}")]
    GetCollection(#[from] GetCollectionError),
    #[error("Error updating collection: {0}")]
    UpdateCollection(#[from] UpdateCollectionError),
    #[error("SPANN is still in development. Not allowed to created spann indexes")]
    SpannNotImplemented,
}

impl ChromaError for ValidationError {
    fn code(&self) -> ErrorCodes {
        match self {
            ValidationError::CollectionId => ErrorCodes::InvalidArgument,
            ValidationError::DimensionInconsistent => ErrorCodes::InvalidArgument,
            ValidationError::DimensionMismatch(_, _) => ErrorCodes::InvalidArgument,
            ValidationError::GetCollection(err) => err.code(),
            ValidationError::UpdateCollection(err) => err.code(),
            ValidationError::SpannNotImplemented => ErrorCodes::Unimplemented,
        }
    }
}

pub(crate) fn chroma_error_code_to_status_code(error_code: ErrorCodes) -> StatusCode {
    match error_code {
        ErrorCodes::Success => StatusCode::OK,
        ErrorCodes::Cancelled => StatusCode::BAD_REQUEST,
        ErrorCodes::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCodes::InvalidArgument => StatusCode::BAD_REQUEST,
        ErrorCodes::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        ErrorCodes::NotFound => StatusCode::NOT_FOUND,
        ErrorCodes::AlreadyExists => StatusCode::CONFLICT,
        ErrorCodes::PermissionDenied => StatusCode::FORBIDDEN,
        ErrorCodes::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        ErrorCodes::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        ErrorCodes::Aborted => StatusCode::BAD_REQUEST,
        ErrorCodes::OutOfRange => StatusCode::BAD_REQUEST,
        ErrorCodes::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        ErrorCodes::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCodes::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCodes::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCodes::Unauthenticated => StatusCode::UNAUTHORIZED,
        ErrorCodes::VersionMismatch => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(crate) fn status_code_to_chroma_error(status_code: StatusCode) -> ErrorCodes {
    match status_code {
        StatusCode::OK => ErrorCodes::Success,
        StatusCode::BAD_REQUEST => ErrorCodes::InvalidArgument,
        StatusCode::UNAUTHORIZED => ErrorCodes::Unauthenticated,
        StatusCode::FORBIDDEN => ErrorCodes::PermissionDenied,
        StatusCode::NOT_FOUND => ErrorCodes::NotFound,
        StatusCode::CONFLICT => ErrorCodes::AlreadyExists,
        StatusCode::TOO_MANY_REQUESTS => ErrorCodes::ResourceExhausted,
        StatusCode::INTERNAL_SERVER_ERROR => ErrorCodes::Internal,
        StatusCode::SERVICE_UNAVAILABLE => ErrorCodes::Unavailable,
        StatusCode::NOT_IMPLEMENTED => ErrorCodes::Unimplemented,
        StatusCode::GATEWAY_TIMEOUT => ErrorCodes::DeadlineExceeded,
        StatusCode::PRECONDITION_FAILED => ErrorCodes::FailedPrecondition,
        _ => ErrorCodes::Unknown,
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
        tracing::error!("Error: {:?}", self.0);
        let status_code = chroma_error_code_to_status_code(self.0.code());

        let error = ErrorResponse {
            error: self.0.code().name().to_string(),
            message: self.0.to_string(),
        };

        (status_code, Json(error)).into_response()
    }
}
