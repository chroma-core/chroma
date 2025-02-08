use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{GetCollectionError, UpdateCollectionError};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Collection ID is not a valid UUIDv4")]
    CollectionId,
    #[error("Inconsistent dimensions in provided embeddings")]
    DimensionInconsistent,
    #[error("Collection expecting embedding with dimension of {0}, got {1}")]
    DimensionMismatch(u32, u32),
    #[error("Deleting collection records without filter")]
    EmptyDelete,
    #[error("Empty metadata")]
    EmptyMetadata,
    #[error("Error getting collection: {0}")]
    GetCollection(#[from] GetCollectionError),
    #[error("Invalid name: {0}")]
    Name(String),
    #[error("Error updatding collection: {0}")]
    UpdateCollection(#[from] UpdateCollectionError),
}

impl ChromaError for ValidationError {
    fn code(&self) -> ErrorCodes {
        match self {
            ValidationError::CollectionId => ErrorCodes::InvalidArgument,
            ValidationError::DimensionInconsistent => ErrorCodes::InvalidArgument,
            ValidationError::DimensionMismatch(_, _) => ErrorCodes::InvalidArgument,
            ValidationError::EmptyDelete => ErrorCodes::InvalidArgument,
            ValidationError::EmptyMetadata => ErrorCodes::InvalidArgument,
            ValidationError::GetCollection(err) => err.code(),
            ValidationError::Name(_) => ErrorCodes::InvalidArgument,
            ValidationError::UpdateCollection(err) => err.code(),
        }
    }
}

/// Wrapper around `dyn ChromaError` that implements `IntoResponse`. This means that route handlers can return `Result<_, ServerError>` and use the `?` operator to return arbitrary errors.
pub(crate) struct ServerError(Box<dyn ChromaError>);

impl<E: ChromaError + 'static> From<E> for ServerError {
    fn from(e: E) -> Self {
        ServerError(Box::new(e))
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        tracing::error!("Error: {:?}", self.0);
        let status_code = match self.0.code() {
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
        };

        let error = match self.0.code() {
            ErrorCodes::InvalidArgument => "InvalidArgumentError",
            ErrorCodes::NotFound => "NotFoundError",
            ErrorCodes::Internal => "InternalError",
            ErrorCodes::VersionMismatch => "VersionMismatchError",
            _ => "ChromaError",
        }
        .to_string();

        let error = ErrorResponse {
            error,
            message: self.0.to_string(),
        };

        (status_code, Json(error)).into_response()
    }
}
