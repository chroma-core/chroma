//! Internal domain types for the SysDb service.
//!
//! These types provide a layer of indirection between the protobuf types
//! and the backend implementations. This allows:
//! - Changing the wire format without affecting backends
//! - Backend-specific optimizations without affecting the API
//! - Cleaner internal APIs that aren't tied to protobuf conventions

use chroma_types::{
    chroma_proto, Collection, CollectionToProtoError, CollectionUuid, Database, DatabaseUuid,
    InternalCollectionConfiguration, Metadata, MetadataValue, MetadataValueConversionError, Schema,
    Segment, SegmentConversionError, Tenant,
};
use prost_types::Timestamp;
use uuid::Uuid;

use crate::backend::{Assignable, Backend, BackendFactory, Runnable};

use std::num::TryFromIntError;

use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::grpc::Status as GrpcStatus;
use google_cloud_gax::retry::TryAs;
use google_cloud_spanner::session::SessionError;
use google_cloud_spanner::{client::Error as SpannerClientError, row::Row};
use thiserror::Error;
use tonic::Status;

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

/// Filter for querying collections.
///
/// All fields are optional - use the builder methods to construct filters fluently.
///
/// # Examples
///
/// ```ignore
/// // Get by IDs
/// let filter = CollectionFilter::default().ids(vec![collection_id]);
///
/// // Get by name in a database
/// let filter = CollectionFilter::default()
///     .tenant_id("tenant-uuid")
///     .database_name("my_db")
///     .name("my_collection");
///
/// // List with pagination
/// let filter = CollectionFilter::default()
///     .tenant_id(tenant)
///     .database_name(db)
///     .limit(10)
///     .offset(0);
/// ```
#[derive(Debug, Clone, Default)]
pub struct CollectionFilter {
    /// Filter by collection ID(s)
    pub ids: Option<Vec<CollectionUuid>>,
    /// Filter by collection name (within a database)
    pub name: Option<String>,
    /// Filter by tenant ID
    pub tenant_id: Option<String>,
    /// Filter by database name
    pub database_name: Option<String>,
    /// Include soft-deleted collections (default: false)
    pub include_soft_deleted: bool,
    /// Maximum number of results to return
    pub limit: Option<u32>,
    /// Number of results to skip
    pub offset: Option<u32>,
}

impl CollectionFilter {
    /// Filter by collection IDs
    pub fn ids(mut self, ids: Vec<CollectionUuid>) -> Self {
        self.ids = Some(ids);
        self
    }

    /// Filter by collection name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Filter by tenant ID
    pub fn tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Filter by database name
    pub fn database_name(mut self, name: impl Into<String>) -> Self {
        self.database_name = Some(name.into());
        self
    }

    /// Include soft-deleted collections
    pub fn include_soft_deleted(mut self, include: bool) -> Self {
        self.include_soft_deleted = include;
        self
    }

    /// Set maximum number of results to return
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set number of results to skip
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }
}

/// Internal request for getting collections.
#[derive(Debug, Clone)]
pub struct GetCollectionsRequest {
    pub filter: CollectionFilter,
}

impl TryFrom<chroma_proto::GetCollectionsRequest> for GetCollectionsRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::GetCollectionsRequest) -> Result<Self, Self::Error> {
        // Build filter from proto fields
        let mut filter = CollectionFilter::default();

        // Collect all IDs from both `id` and `ids_filter`
        let mut all_ids: Vec<CollectionUuid> = Vec::new();

        if let Some(id_str) = req.id {
            let id = CollectionUuid(validate_uuid(&id_str)?);
            all_ids.push(id);
        }

        if let Some(ids_filter) = req.ids_filter {
            for id_str in ids_filter.ids {
                let id = CollectionUuid(validate_uuid(&id_str)?);
                all_ids.push(id);
            }
        }

        if !all_ids.is_empty() {
            filter = filter.ids(all_ids);
        }

        // Add optional fields if provided
        if let Some(name) = req.name {
            filter = filter.name(name);
        }

        if !req.tenant.is_empty() {
            filter = filter.tenant_id(req.tenant);
        }
        if !req.database.is_empty() {
            filter = filter.database_name(req.database);
        }

        // Handle limit and offset
        if let Some(limit) = req.limit {
            let limit = u32::try_from(limit).map_err(|_| {
                SysDbError::InvalidArgument(format!("limit must be non-negative, got {}", limit))
            })?;
            filter = filter.limit(limit);
        }
        if let Some(offset) = req.offset {
            if req.limit.is_none() {
                return Err(SysDbError::InvalidArgument(
                    "offset requires limit to be specified".to_string(),
                ));
            }
            let offset = u32::try_from(offset).map_err(|_| {
                SysDbError::InvalidArgument(format!("offset must be non-negative, got {}", offset))
            })?;
            filter = filter.offset(offset);
        }

        // Handle include_soft_deleted
        if let Some(include_soft_deleted) = req.include_soft_deleted {
            filter = filter.include_soft_deleted(include_soft_deleted);
        }

        Ok(Self { filter })
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

impl Assignable for GetCollectionsRequest {
    type Output = Backend;

    fn assign(&self, factory: &BackendFactory) -> Backend {
        // Route by database_name prefix (for now, default to Spanner)
        // TODO: Check self.filter.database_name prefix to route to Aurora if needed
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

#[async_trait::async_trait]
impl Runnable for GetCollectionsRequest {
    type Response = GetCollectionsResponse;
    type Input = Backend;

    async fn run(self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.get_collections(self).await
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

pub struct SpannerRows {
    pub rows: Vec<Row>,
}

// ============================================================================
// Row Conversion Implementation (Spanner DAO layer)
// ============================================================================

/// Convert a vector of Spanner rows (from a JOIN query) into a Collection.
///
/// The rows are expected to come from a query that JOINs:
/// - collections table
/// - collection_metadata table (LEFT JOIN, may have multiple rows per metadata key)
/// - collection_compaction_cursors table (LEFT JOIN, single row for a specific region)
///
/// Column names expected:
/// - collection_id, name, dimension, database_id, database_name, tenant_id, updated_at
/// - metadata_key, metadata_str_value, metadata_int_value, metadata_float_value, metadata_bool_value
/// - last_compacted_offset, version, total_records_post_compaction, size_bytes_post_compaction,
///   last_compaction_time_secs, version_file_name, index_schema
impl TryFrom<SpannerRows> for Collection {
    type Error = SysDbError;

    fn try_from(rows: SpannerRows) -> Result<Self, Self::Error> {
        if rows.rows.is_empty() {
            return Err(SysDbError::NotFound("no rows returned".to_string()));
        }

        // Extract collection fields from the first row (same for all rows)
        let first_row = &rows.rows[0];

        let collection_id_str: String = first_row
            .column_by_name("collection_id")
            .map_err(SysDbError::FailedToReadColumn)?;
        let name: String = first_row
            .column_by_name("name")
            .map_err(SysDbError::FailedToReadColumn)?;
        let dimension: Option<i64> = first_row
            .column_by_name("dimension")
            .map_err(SysDbError::FailedToReadColumn)?;
        let database_id_str: String = first_row
            .column_by_name("database_id")
            .map_err(SysDbError::FailedToReadColumn)?;
        let database_name: String = first_row
            .column_by_name("database_name")
            .map_err(SysDbError::FailedToReadColumn)?;
        let tenant_id: String = first_row
            .column_by_name("tenant_id")
            .map_err(SysDbError::FailedToReadColumn)?;

        // Spanner returns TIMESTAMP as prost_types::Timestamp
        let updated_at: Timestamp = first_row
            .column_by_name("updated_at")
            .map_err(SysDbError::FailedToReadColumn)?;

        let last_compacted_offset: Option<i64> = first_row
            .column_by_name("last_compacted_offset")
            .map_err(SysDbError::FailedToReadColumn)?;
        let version: Option<i64> = first_row
            .column_by_name("version")
            .map_err(SysDbError::FailedToReadColumn)?;
        let last_compaction_time_ts: Option<Timestamp> = first_row
            .column_by_name("last_compaction_time_secs")
            .map_err(SysDbError::FailedToReadColumn)?;
        let version_file_name: Option<String> = first_row
            .column_by_name("version_file_name")
            .map_err(SysDbError::FailedToReadColumn)?;
        let total_records_post_compaction: i64 = first_row
            .column_by_name("total_records_post_compaction")
            .map_err(SysDbError::FailedToReadColumn)?;
        let size_bytes_post_compaction: i64 = first_row
            .column_by_name("size_bytes_post_compaction")
            .map_err(SysDbError::FailedToReadColumn)?;
        let compaction_failure_count: i64 = first_row
            .column_by_name("compaction_failure_count")
            .map_err(SysDbError::FailedToReadColumn)?;
        let schema_json: String = first_row
            .column_by_name("index_schema")
            .map_err(SysDbError::FailedToReadColumn)?;

        // Aggregate metadata from all rows
        let mut collection_metadata = Metadata::new();
        for row in &rows.rows {
            // metadata_key may be NULL if there's no metadata (LEFT JOIN)
            if let Ok(Some(key)) = row.column_by_name::<Option<String>>("metadata_key") {
                let str_val: Option<String> = row
                    .column_by_name("metadata_str_value")
                    .map_err(SysDbError::FailedToReadColumn)?;
                let int_val: Option<i64> = row
                    .column_by_name("metadata_int_value")
                    .map_err(SysDbError::FailedToReadColumn)?;
                let float_val: Option<f64> = row
                    .column_by_name("metadata_float_value")
                    .map_err(SysDbError::FailedToReadColumn)?;
                let bool_val: Option<bool> = row
                    .column_by_name("metadata_bool_value")
                    .map_err(SysDbError::FailedToReadColumn)?;

                if let Some(s) = str_val {
                    collection_metadata.insert(key, MetadataValue::Str(s));
                } else if let Some(i) = int_val {
                    collection_metadata.insert(key, MetadataValue::Int(i));
                } else if let Some(f) = float_val {
                    collection_metadata.insert(key, MetadataValue::Float(f));
                } else if let Some(b) = bool_val {
                    collection_metadata.insert(key, MetadataValue::Bool(b));
                }
            }
        }

        // Parse schema JSON (index_schema is NOT NULL, so parsing should succeed)
        let parsed_schema: Schema =
            serde_json::from_str(&schema_json).map_err(SysDbError::InvalidSchemaJson)?;

        // Convert prost_types::Timestamp to SystemTime
        let updated_at_system_time = std::time::UNIX_EPOCH
            + std::time::Duration::new(updated_at.seconds as u64, updated_at.nanos as u32);

        // Convert last_compaction_time from Timestamp to seconds
        let last_compaction_time_secs_u64 = last_compaction_time_ts
            .map(|ts| ts.seconds as u64)
            .unwrap_or(0);

        Ok(Collection {
            collection_id: CollectionUuid(
                Uuid::parse_str(&collection_id_str).map_err(SysDbError::InvalidUuid)?,
            ),
            name,
            config: InternalCollectionConfiguration::default_hnsw(),
            schema: Some(parsed_schema),
            metadata: if collection_metadata.is_empty() {
                None
            } else {
                Some(collection_metadata)
            },
            dimension: dimension.map(|d| d as i32),
            tenant: tenant_id,
            database: database_name,
            log_position: last_compacted_offset.unwrap_or(0),
            version: version.map(|v| v as i32).unwrap_or(0),
            total_records_post_compaction: total_records_post_compaction as u64,
            size_bytes_post_compaction: size_bytes_post_compaction as u64,
            last_compaction_time_secs: last_compaction_time_secs_u64,
            version_file_path: version_file_name,
            root_collection_id: None,
            lineage_file_path: None,
            updated_at: updated_at_system_time,
            database_id: DatabaseUuid(
                Uuid::parse_str(&database_id_str).map_err(SysDbError::InvalidUuid)?,
            ),
            compaction_failure_count: compaction_failure_count as i32,
        })
    }
}

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

/// Internal response for getting collections.
#[derive(Debug, Clone)]
pub struct GetCollectionsResponse {
    pub collections: Vec<Collection>,
}

impl TryFrom<GetCollectionsResponse> for chroma_proto::GetCollectionsResponse {
    type Error = SysDbError;

    fn try_from(r: GetCollectionsResponse) -> Result<Self, Self::Error> {
        let collections: Result<Vec<_>, _> =
            r.collections.into_iter().map(|c| c.try_into()).collect();
        Ok(chroma_proto::GetCollectionsResponse {
            collections: collections?,
        })
    }
}
