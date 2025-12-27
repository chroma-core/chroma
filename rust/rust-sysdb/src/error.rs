//! Unified error types for the SysDb service.
//!
//! This module provides a backend-agnostic error type that all backends return.
//! The server layer only sees `SysDbError`, not backend-specific errors.

use chroma_error::{ChromaError, ErrorCodes};
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
    Spanner(String),

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
            SysDbError::Spanner(msg) => Status::internal(msg),
            SysDbError::Internal(msg) => Status::internal(msg),
        }
    }
}
