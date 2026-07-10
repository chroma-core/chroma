use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chroma_api_types::{StaleReadError, STALE_READ_ERROR_NAME};
use chroma_error::{source_chain_contains, ChromaError};
use chroma_log::PushLogsError;
use chroma_types::{ConditionalCommitError, ConditionalTransactionError};
use serde::Serialize;
use std::fmt;
use utoipa::ToSchema;

const BACKOFF_ERROR_NAME: &str = "Backoff";
const CONDITIONAL_WRITE_CONFLICT_ERROR_NAME: &str = "ConditionalWriteConflictError";
const TRANSACTIONS_NOT_SUPPORTED_ERROR_NAME: &str = "TransactionsNotSupported";

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

fn error_name(err: &(dyn ChromaError + 'static)) -> &'static str {
    if source_chain_contains(err, |source| {
        matches!(
            source.downcast_ref::<PushLogsError>(),
            Some(PushLogsError::ConditionalWriteConflict)
        )
    }) {
        return CONDITIONAL_WRITE_CONFLICT_ERROR_NAME;
    }
    if source_chain_contains(err, |source| source.is::<StaleReadError>()) {
        return STALE_READ_ERROR_NAME;
    }
    if source_chain_contains(err, |source| {
        matches!(
            source.downcast_ref::<ConditionalCommitError>(),
            Some(
                ConditionalCommitError::TransactionsNotSupported { .. }
                    | ConditionalCommitError::TransactionsDisabled
            )
        ) || matches!(
            source.downcast_ref::<ConditionalTransactionError>(),
            Some(ConditionalTransactionError::TransactionsDisabled)
        )
    }) {
        return TRANSACTIONS_NOT_SUPPORTED_ERROR_NAME;
    }
    if source_chain_contains(err, |source| {
        matches!(
            source.downcast_ref::<ConditionalCommitError>(),
            Some(ConditionalCommitError::Backoff)
        ) || matches!(
            source.downcast_ref::<PushLogsError>(),
            Some(PushLogsError::Backoff | PushLogsError::BackoffCompaction)
        )
    }) {
        return BACKOFF_ERROR_NAME;
    }
    err.code().name()
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status_code: StatusCode = self.0.code().into();

        let error = ErrorResponse {
            error: error_name(self.0.as_ref()).to_string(),
            message: self.0.to_string(),
        };

        (status_code, Json(error)).into_response()
    }
}
