//! Internal domain types for the SysDb service.
//!
//! These types provide a layer of indirection between the protobuf types
//! and the backend implementations. This allows:
//! - Changing the wire format without affecting backends
//! - Backend-specific optimizations without affecting the API
//! - Cleaner internal APIs that aren't tied to protobuf conventions

use chroma_types::{
    chroma_proto, Collection, CollectionUuid, Database, Metadata, Schema, Segment, Tenant,
};
use uuid::Uuid;

use crate::backend::{Assignable, Backend, BackendFactory, Runnable};

// ============================================================================
// Request Types (proto -> internal)
// ============================================================================

/// Validates that a string is a valid UUID.
fn validate_uuid(id: &str) -> Result<Uuid, SysDbError> {
    Uuid::parse_str(id).map_err(SysDbError::InvalidUuid)
}

/// Internal request for creating a tenant.
#[derive(Debug, Clone)]
pub struct CreateTenantRequest {
    pub id: String,
}

impl TryFrom<chroma_proto::CreateTenantRequest> for CreateTenantRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::CreateTenantRequest) -> Result<Self, Self::Error> {
        Ok(Self { id: req.name })
    }
}

/// Internal request for getting a tenant.
#[derive(Debug, Clone)]
pub struct GetTenantRequest {
    pub id: String,
}

impl TryFrom<chroma_proto::GetTenantRequest> for GetTenantRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::GetTenantRequest) -> Result<Self, Self::Error> {
        Ok(Self { id: req.name })
    }
}

/// Internal request for setting tenant resource name.
#[derive(Debug, Clone)]
pub struct SetTenantResourceNameRequest {
    pub tenant_id: String,
    pub resource_name: String,
}

impl TryFrom<chroma_proto::SetTenantResourceNameRequest> for SetTenantResourceNameRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::SetTenantResourceNameRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            tenant_id: req.id,
            resource_name: req.resource_name,
        })
    }
}

/// Internal request for creating a database.
#[derive(Debug, Clone)]
pub struct CreateDatabaseRequest {
    pub id: Uuid,
    pub name: String,
    pub tenant_id: String,
}

impl TryFrom<chroma_proto::CreateDatabaseRequest> for CreateDatabaseRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::CreateDatabaseRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: validate_uuid(&req.id)?,
            name: req.name,
            tenant_id: req.tenant,
        })
    }
}

/// Internal request for getting a database.
#[derive(Debug, Clone)]
pub struct GetDatabaseRequest {
    pub name: String,
    pub tenant_id: String,
}

impl TryFrom<chroma_proto::GetDatabaseRequest> for GetDatabaseRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::GetDatabaseRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            name: req.name,
            tenant_id: req.tenant,
        })
    }
}

/// Internal request for creating a collection.
#[derive(Debug, Clone)]
pub struct CreateCollectionRequest {
    pub id: CollectionUuid,
    pub name: String,
    pub dimension: Option<u32>,
    pub index_schema: Schema,
    pub segments: Vec<Segment>,
    pub metadata: Option<Metadata>,
    pub get_or_create: bool,
    pub tenant_id: String,
    pub database_name: String,
}

impl TryFrom<chroma_proto::CreateCollectionRequest> for CreateCollectionRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::CreateCollectionRequest) -> Result<Self, Self::Error> {
        // Validate schema_str is provided and parse as strongly-typed Schema
        let schema_str = req
            .schema_str
            .ok_or_else(|| SysDbError::SchemaMissing("schema_str is required".to_string()))?;

        let index_schema: Schema = serde_json::from_str(&schema_str)?;

        // Convert and validate segments
        let segments: Result<Vec<Segment>, _> =
            req.segments.into_iter().map(Segment::try_from).collect();

        let segments = segments.map_err(SysDbError::InvalidSegment)?;

        // Validate exactly 3 segments
        if segments.len() != 3 {
            return Err(SysDbError::InvalidSegmentsCount);
        }

        // Convert metadata if provided, filtering out legacy "hnsw:" keys
        let metadata = req
            .metadata
            .map(|proto_metadata| -> Result<Metadata, SysDbError> {
                let mut metadata =
                    Metadata::try_from(proto_metadata).map_err(SysDbError::InvalidMetadata)?;

                // Filter out legacy metadata keys starting with "hnsw:"
                metadata.retain(|key, _| !key.starts_with("hnsw:"));

                Ok(metadata)
            })
            .transpose()?;

        // Convert dimension from i32 to u32 (validate non-negative)
        let dimension = req
            .dimension
            .map(|d| u32::try_from(d).map_err(SysDbError::InvalidDimension))
            .transpose()?;

        Ok(Self {
            id: CollectionUuid(validate_uuid(&req.id)?),
            name: req.name,
            dimension,
            index_schema,
            segments,
            metadata,
            get_or_create: req.get_or_create.unwrap_or(false),
            tenant_id: req.tenant,
            database_name: req.database,
        })
    }
}

// ============================================================================
// Assignable Trait Implementations
// ============================================================================

impl Assignable for CreateTenantRequest {
    type Output = Vec<Backend>;

    fn assign(&self, factory: &BackendFactory) -> Vec<Backend> {
        // Fan out to all backends
        vec![
            Backend::Spanner(factory.spanner().clone()),
            // TODO: Backend::Aurora(factory.aurora().clone()),
        ]
    }
}

impl Assignable for GetTenantRequest {
    type Output = Backend;

    fn assign(&self, factory: &BackendFactory) -> Backend {
        // Single backend operation
        Backend::Spanner(factory.spanner().clone())
    }
}

impl Assignable for SetTenantResourceNameRequest {
    type Output = Vec<Backend>;

    fn assign(&self, factory: &BackendFactory) -> Vec<Backend> {
        // Fan out to all backends
        vec![
            Backend::Spanner(factory.spanner().clone()),
            // TODO: Backend::Aurora(factory.aurora().clone()),
        ]
    }
}

impl Assignable for CreateDatabaseRequest {
    type Output = Backend;

    fn assign(&self, factory: &BackendFactory) -> Backend {
        // Route by db_name prefix (for now, default to Spanner)
        // TODO: Check self.name prefix to route to Aurora if needed
        Backend::Spanner(factory.spanner().clone())
    }
}

impl Assignable for GetDatabaseRequest {
    type Output = Backend;

    fn assign(&self, factory: &BackendFactory) -> Backend {
        // Route by db_name prefix (for now, default to Spanner)
        // TODO: Check self.name prefix to route to Aurora if needed
        Backend::Spanner(factory.spanner().clone())
    }
}

impl Assignable for CreateCollectionRequest {
    type Output = Backend;

    fn assign(&self, factory: &BackendFactory) -> Backend {
        // Route by database_name prefix (for now, default to Spanner)
        // TODO: Check self.database_name prefix to route to Aurora if needed
        Backend::Spanner(factory.spanner().clone())
    }
}

// ============================================================================
// Runnable Trait Implementations
// ============================================================================

#[async_trait::async_trait]
impl Runnable for CreateTenantRequest {
    type Response = CreateTenantResponse;
    type Input = Vec<Backend>;

    async fn run(self, backends: Vec<Backend>) -> Result<Self::Response, SysDbError> {
        for backend in backends {
            backend.create_tenant(self.clone()).await?;
        }
        Ok(CreateTenantResponse {})
    }
}

#[async_trait::async_trait]
impl Runnable for GetTenantRequest {
    type Response = GetTenantResponse;
    type Input = Backend;

    async fn run(self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.get_tenant(self).await
    }
}

#[async_trait::async_trait]
impl Runnable for SetTenantResourceNameRequest {
    type Response = SetTenantResourceNameResponse;
    type Input = Vec<Backend>;

    async fn run(self, backends: Vec<Backend>) -> Result<Self::Response, SysDbError> {
        for backend in backends {
            backend.set_tenant_resource_name(self.clone()).await?;
        }
        Ok(SetTenantResourceNameResponse {})
    }
}

#[async_trait::async_trait]
impl Runnable for CreateDatabaseRequest {
    type Response = CreateDatabaseResponse;
    type Input = Backend;

    async fn run(self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.create_database(self).await
    }
}

#[async_trait::async_trait]
impl Runnable for GetDatabaseRequest {
    type Response = GetDatabaseResponse;
    type Input = Backend;

    async fn run(self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.get_database(self).await
    }
}

#[async_trait::async_trait]
impl Runnable for CreateCollectionRequest {
    type Response = CreateCollectionResponse;
    type Input = Backend;

    async fn run(self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.create_collection(self).await
    }
}

// ============================================================================
// Response Types (internal -> proto)
// ============================================================================

/// Internal response for creating a tenant.
#[derive(Debug, Clone)]
pub struct CreateTenantResponse {
    // Empty - tenant creation returns no data
}

impl From<CreateTenantResponse> for chroma_proto::CreateTenantResponse {
    fn from(_: CreateTenantResponse) -> Self {
        chroma_proto::CreateTenantResponse {}
    }
}

/// Internal response for getting a tenant.
#[derive(Debug, Clone)]
pub struct GetTenantResponse {
    pub tenant: Tenant,
}

impl From<GetTenantResponse> for chroma_proto::GetTenantResponse {
    fn from(r: GetTenantResponse) -> Self {
        chroma_proto::GetTenantResponse {
            tenant: Some(r.tenant.into()),
        }
    }
}

/// Internal response for setting tenant resource name.
#[derive(Debug, Clone)]
pub struct SetTenantResourceNameResponse {
    // Empty - set resource name returns no data
}

impl From<SetTenantResourceNameResponse> for chroma_proto::SetTenantResourceNameResponse {
    fn from(_: SetTenantResourceNameResponse) -> Self {
        chroma_proto::SetTenantResourceNameResponse {}
    }
}

/// Internal response for creating a database.
#[derive(Debug, Clone)]
pub struct CreateDatabaseResponse {
    // Empty - database creation returns no data
}

impl From<CreateDatabaseResponse> for chroma_proto::CreateDatabaseResponse {
    fn from(_: CreateDatabaseResponse) -> Self {
        chroma_proto::CreateDatabaseResponse {}
    }
}

/// Internal response for getting a database.
#[derive(Debug, Clone)]
pub struct GetDatabaseResponse {
    pub database: Database,
}

impl From<GetDatabaseResponse> for chroma_proto::GetDatabaseResponse {
    fn from(r: GetDatabaseResponse) -> Self {
        chroma_proto::GetDatabaseResponse {
            database: Some(r.database.into()),
        }
    }
}

pub struct SpannerRow {
    pub row: Row,
}

// ============================================================================
// Row Conversion Implementations (DAO layer)
// ============================================================================
impl TryFrom<SpannerRow> for Database {
    type Error = SysDbError;

    fn try_from(wrapped_row: SpannerRow) -> Result<Self, Self::Error> {
        let id: String = wrapped_row
            .row
            .column_by_name("id")
            .map_err(SysDbError::FailedToReadColumn)?;
        let name: String = wrapped_row
            .row
            .column_by_name("name")
            .map_err(SysDbError::FailedToReadColumn)?;
        let tenant: String = wrapped_row
            .row
            .column_by_name("tenant_id")
            .map_err(SysDbError::FailedToReadColumn)?;

        Ok(Database {
            id: Uuid::parse_str(&id).map_err(SysDbError::InvalidUuid)?,
            name,
            tenant,
        })
    }
}

impl TryFrom<SpannerRow> for Tenant {
    type Error = SysDbError;

    fn try_from(wrapped_row: SpannerRow) -> Result<Self, Self::Error> {
        let id: String = wrapped_row
            .row
            .column_by_name("id")
            .map_err(SysDbError::FailedToReadColumn)?;

        let resource_name: Option<String> = wrapped_row
            .row
            .column_by_name("resource_name")
            .map_err(SysDbError::FailedToReadColumn)?;

        let last_compaction_time: i64 = wrapped_row
            .row
            .column_by_name("last_compaction_time")
            .map_err(SysDbError::FailedToReadColumn)?;

        Ok(Tenant {
            id,
            resource_name,
            last_compaction_time,
        })
    }
}

/// Unified error types for the SysDb service.
///
/// This module provides a backend-agnostic error type that all backends return.
/// The server layer only sees `SysDbError`, not backend-specific errors.
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
/// Internal response for creating a collection.
#[derive(Debug, Clone)]
pub struct CreateCollectionResponse {
    pub collection: Collection,
    pub created: bool,
}

impl TryFrom<CreateCollectionResponse> for chroma_proto::CreateCollectionResponse {
    type Error = SysDbError;

    fn try_from(r: CreateCollectionResponse) -> Result<Self, Self::Error> {
        Ok(chroma_proto::CreateCollectionResponse {
            collection: Some(r.collection.try_into()?),
            created: r.created,
        })
    }
}
