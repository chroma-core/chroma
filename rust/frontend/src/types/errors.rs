use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chroma_api_types::ErrorIndexingStatus;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    AddCollectionRecordsError, Base64DecodeError,
    CollectionConfigurationToInternalConfigurationError, DeleteCollectionRecordsError,
    GetCollectionError, IndexStatusResponse, UpdateCollectionError, UpdateCollectionRecordsError,
    UpsertCollectionRecordsError,
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

trait ErrorIndexingStatusProvider {
    fn indexing_status(&self) -> Option<&IndexStatusResponse>;
}

impl ErrorIndexingStatusProvider for AddCollectionRecordsError {
    fn indexing_status(&self) -> Option<&IndexStatusResponse> {
        match self {
            AddCollectionRecordsError::Backoff { indexing_status }
            | AddCollectionRecordsError::BackoffCompaction { indexing_status } => {
                indexing_status.as_ref()
            }
            _ => None,
        }
    }
}

impl ErrorIndexingStatusProvider for UpdateCollectionRecordsError {
    fn indexing_status(&self) -> Option<&IndexStatusResponse> {
        match self {
            UpdateCollectionRecordsError::Backoff { indexing_status }
            | UpdateCollectionRecordsError::BackoffCompaction { indexing_status } => {
                indexing_status.as_ref()
            }
            _ => None,
        }
    }
}

impl ErrorIndexingStatusProvider for UpsertCollectionRecordsError {
    fn indexing_status(&self) -> Option<&IndexStatusResponse> {
        match self {
            UpsertCollectionRecordsError::Backoff { indexing_status }
            | UpsertCollectionRecordsError::BackoffCompaction { indexing_status } => {
                indexing_status.as_ref()
            }
            _ => None,
        }
    }
}

impl ErrorIndexingStatusProvider for DeleteCollectionRecordsError {
    fn indexing_status(&self) -> Option<&IndexStatusResponse> {
        match self {
            DeleteCollectionRecordsError::Backoff { indexing_status }
            | DeleteCollectionRecordsError::BackoffCompaction { indexing_status } => {
                indexing_status.as_ref()
            }
            _ => None,
        }
    }
}

fn to_error_indexing_status(status: &IndexStatusResponse) -> ErrorIndexingStatus {
    ErrorIndexingStatus {
        op_indexing_progress: status.op_indexing_progress,
        num_unindexed_ops: status.num_unindexed_ops,
        num_indexed_ops: status.num_indexed_ops,
        total_ops: status.total_ops,
    }
}

fn indexing_status_for_error(err: &(dyn ChromaError + 'static)) -> Option<ErrorIndexingStatus> {
    let err = err as &(dyn std::error::Error + 'static);

    err.downcast_ref::<AddCollectionRecordsError>()
        .and_then(ErrorIndexingStatusProvider::indexing_status)
        .map(to_error_indexing_status)
        .or_else(|| {
            err.downcast_ref::<UpdateCollectionRecordsError>()
                .and_then(ErrorIndexingStatusProvider::indexing_status)
                .map(to_error_indexing_status)
        })
        .or_else(|| {
            err.downcast_ref::<UpsertCollectionRecordsError>()
                .and_then(ErrorIndexingStatusProvider::indexing_status)
                .map(to_error_indexing_status)
        })
        .or_else(|| {
            err.downcast_ref::<DeleteCollectionRecordsError>()
                .and_then(ErrorIndexingStatusProvider::indexing_status)
                .map(to_error_indexing_status)
        })
}

#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    error: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexing_status: Option<ErrorIndexingStatus>,
}

impl ErrorResponse {
    pub fn new(error: String, message: String) -> Self {
        Self {
            error,
            message,
            indexing_status: None,
        }
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status_code: StatusCode = self.0.code().into();

        let error = ErrorResponse {
            error: self.0.code().name().to_string(),
            message: self.0.to_string(),
            indexing_status: indexing_status_for_error(self.0.as_ref()),
        };

        (status_code, Json(error)).into_response()
    }
}
