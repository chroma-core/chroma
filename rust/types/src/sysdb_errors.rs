//! Unified error types for the SysDb service.
//!
//! This module provides a backend-agnostic error type that all backends return.
//! The server layer only sees `SysDbError`, not backend-specific errors.

use std::num::TryFromIntError;

use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::grpc::Status as GrpcStatus;
use google_cloud_gax::retry::TryAs;
use google_cloud_spanner::client::Error as SpannerClientError;
use google_cloud_spanner::session::SessionError;
use thiserror::Error;
use tonic::Status;

use crate::{CollectionToProtoError, MetadataValueConversionError, SegmentConversionError};

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

    /// Internal/unexpected error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Invalid UUID
    #[error("Invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),

    /// Failed to read column
    #[error("Failed to read column: {0}")]
    FailedToReadColumn(#[source] google_cloud_spanner::row::Error),

    /// Schema missing
    #[error("Schema missing: {0}")]
    SchemaMissing(String),

    /// Schema must be valid JSON
    #[error("Schema must be valid JSON: {0}")]
    InvalidSchemaJson(#[from] serde_json::Error),

    /// Invalid segment
    #[error("Invalid segment: {0}")]
    InvalidSegment(#[from] SegmentConversionError),

    /// Segments must be exactly 3
    #[error("Segments must be exactly 3")]
    InvalidSegmentsCount,

    /// Invalid metadata
    #[error("Invalid metadata: {0}")]
    InvalidMetadata(#[from] MetadataValueConversionError),

    /// Dimension must be non-negative
    #[error("Failed to convert i32 dim to u32: {0}")]
    InvalidDimension(#[from] TryFromIntError),

    /// Failed to convert collection to proto
    #[error("Failed to convert collection to proto: {0}")]
    CollectionToProtoError(#[from] CollectionToProtoError),
}

impl ChromaError for SysDbError {
    fn code(&self) -> ErrorCodes {
        match self {
            SysDbError::Spanner(_) => ErrorCodes::Internal,
            SysDbError::NotFound(_) => ErrorCodes::NotFound,
            SysDbError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            SysDbError::InvalidArgument(_) => ErrorCodes::InvalidArgument,
            SysDbError::Internal(_) => ErrorCodes::Internal,
            SysDbError::InvalidUuid(_) => ErrorCodes::InvalidArgument,
            SysDbError::FailedToReadColumn(_) => ErrorCodes::Internal,
            SysDbError::SchemaMissing(_) => ErrorCodes::Internal,
            SysDbError::InvalidSchemaJson(_) => ErrorCodes::Internal,
            SysDbError::InvalidSegment(e) => e.code(),
            SysDbError::InvalidSegmentsCount => ErrorCodes::Internal,
            SysDbError::InvalidMetadata(e) => e.code(),
            SysDbError::InvalidDimension(_) => ErrorCodes::Internal,
            SysDbError::CollectionToProtoError(e) => e.code(),
        }
    }
}

impl From<SysDbError> for Status {
    fn from(e: SysDbError) -> Status {
        Status::new(e.code().into(), e.to_string())
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
