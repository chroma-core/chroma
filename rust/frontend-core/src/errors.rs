use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chroma_error::ChromaError;
use serde::Serialize;
use std::fmt;
use utoipa::ToSchema;

/// Wrapper around `dyn ChromaError` that implements `IntoResponse`. This means that route handlers can return `Result<_, ServerError>` and use the `?` operator to return arbitrary errors.
pub struct ServerError(pub Box<dyn ChromaError>);

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
            error: self.0.name().to_string(),
            message: self.0.to_string(),
        };

        (status_code, Json(error)).into_response()
    }
}
