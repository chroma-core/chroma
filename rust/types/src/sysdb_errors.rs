//! Unified error types for the SysDb service.
//!
//! This module provides a backend-agnostic error type that all backends return.
//! The server layer only sees `SysDbError`, not backend-specific errors.

use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::grpc::Status as GrpcStatus;
use google_cloud_gax::retry::TryAs;
use google_cloud_spanner::client::Error as SpannerClientError;
use google_cloud_spanner::session::SessionError;
use thiserror::Error;
use tonic::Status;

/// Unified error type for all SysDb operations.
///
/// Backends convert their internal errors into this type, allowing the server
/// layer to handle errors uniformly regardless of which backend is being used.
#[derive(Debug, Error)]
pub enum SysDbError {
    /// Wraps Spanner-specific errors
    #[error("Spanner error: {0}")]
    Spanner(#[from] SpannerClientError),

    /// Resource not found (tenant, database, collection, etc.)
    #[error("Not found: {0}")]
    NotFound(String),

    /// Resource already exists (duplicate tenant name, etc.)
    #[error("Already exists: {0}")]
    AlreadyExists(String),

    /// Invalid argument provided
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Operation not supported on this backend
    #[error("Operation not supported on this backend: {0}")]
    NotSupported(&'static str),

    /// Internal/unexpected error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Invalid UUID
    #[error("Invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),

    /// Failed to read column
    #[error("Failed to read column: {0}")]
    FailedToReadColumn(#[source] google_cloud_spanner::row::Error),
}

impl ChromaError for SysDbError {
    fn code(&self) -> ErrorCodes {
        match self {
            SysDbError::Spanner(_) => ErrorCodes::Internal,
            SysDbError::NotFound(_) => ErrorCodes::NotFound,
            SysDbError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            SysDbError::InvalidArgument(_) => ErrorCodes::InvalidArgument,
            SysDbError::NotSupported(_) => ErrorCodes::Internal,
            SysDbError::Internal(_) => ErrorCodes::Internal,
            SysDbError::InvalidUuid(_) => ErrorCodes::InvalidArgument,
            SysDbError::FailedToReadColumn(_) => ErrorCodes::Internal,
        }
    }
}

impl From<SysDbError> for Status {
    fn from(e: SysDbError) -> Status {
        match e {
            SysDbError::NotFound(msg) => Status::not_found(msg),
            SysDbError::AlreadyExists(msg) => Status::already_exists(msg),
            SysDbError::InvalidArgument(msg) => Status::invalid_argument(msg),
            SysDbError::NotSupported(msg) => Status::unimplemented(msg),
            SysDbError::Spanner(err) => Status::internal(err.to_string()),
            SysDbError::Internal(msg) => Status::internal(msg),
            SysDbError::InvalidUuid(err) => Status::invalid_argument(err.to_string()),
            SysDbError::FailedToReadColumn(msg) => Status::internal(msg.to_string()),
        }
    }
}

impl From<GrpcStatus> for SysDbError {
    fn from(status: GrpcStatus) -> Self {
        // Convert GrpcStatus to SpannerClientError
        SysDbError::Spanner(SpannerClientError::from(status))
    }
}

impl From<SessionError> for SysDbError {
    fn from(err: SessionError) -> Self {
        // Convert SessionError to SpannerClientError
        SysDbError::Spanner(SpannerClientError::from(err))
    }
}

impl TryAs<GrpcStatus> for SysDbError {
    fn try_as(&self) -> Option<&GrpcStatus> {
        match self {
            // For Spanner errors, delegate to SpannerClientError's TryAs implementation
            // This allows Spanner to retry on abortable errors (e.g., transaction conflicts)
            SysDbError::Spanner(err) => err.try_as(),
            // Domain errors don't contain a GrpcStatus, so we return None.
            // This means Spanner won't retry these errors, which is correct
            // for domain errors like NotFound, AlreadyExists, etc.
            _ => None,
        }
    }
}
