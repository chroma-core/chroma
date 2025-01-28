use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chroma_error::ChromaError;
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum ValidationError {
    #[error("Collection ID is not a valid UUIDv4")]
    InvalidCollectionId,
}

impl ChromaError for ValidationError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ValidationError::InvalidCollectionId => chroma_error::ErrorCodes::InvalidArgument,
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
        let status_code = match self.0.code() {
            chroma_error::ErrorCodes::Success => StatusCode::OK,
            chroma_error::ErrorCodes::Cancelled => StatusCode::BAD_REQUEST,
            chroma_error::ErrorCodes::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
            chroma_error::ErrorCodes::InvalidArgument => StatusCode::BAD_REQUEST,
            chroma_error::ErrorCodes::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            chroma_error::ErrorCodes::NotFound => StatusCode::NOT_FOUND,
            chroma_error::ErrorCodes::AlreadyExists => StatusCode::CONFLICT,
            chroma_error::ErrorCodes::PermissionDenied => StatusCode::FORBIDDEN,
            chroma_error::ErrorCodes::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            chroma_error::ErrorCodes::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
            chroma_error::ErrorCodes::Aborted => StatusCode::BAD_REQUEST,
            chroma_error::ErrorCodes::OutOfRange => StatusCode::BAD_REQUEST,
            chroma_error::ErrorCodes::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            chroma_error::ErrorCodes::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            chroma_error::ErrorCodes::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            chroma_error::ErrorCodes::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
            chroma_error::ErrorCodes::Unauthenticated => StatusCode::UNAUTHORIZED,
            chroma_error::ErrorCodes::VersionMismatch => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let error = ErrorResponse {
            error: status_code.to_string(),
            message: self.0.to_string(),
        };

        (status_code, Json(error)).into_response()
    }
}
