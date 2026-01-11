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
use chroma_types::sysdb_errors::SysDbError;

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
            filter = filter.limit(limit as u32);
        }
        if let Some(offset) = req.offset {
            filter = filter.offset(offset as u32);
        }

        // Handle include_soft_deleted
        if let Some(include_soft_deleted) = req.include_soft_deleted {
            filter = filter.include_soft_deleted(include_soft_deleted);
        }

        Ok(Self { filter })
    }
}

/// Internal request for getting a collection with its segments.
#[derive(Debug, Clone)]
pub struct GetCollectionWithSegmentsRequest {
    pub id: CollectionUuid,
}

impl TryFrom<chroma_proto::GetCollectionWithSegmentsRequest> for GetCollectionWithSegmentsRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::GetCollectionWithSegmentsRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: CollectionUuid(validate_uuid(&req.id)?),
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

impl Assignable for GetCollectionsRequest {
    type Output = Backend;

    fn assign(&self, factory: &BackendFactory) -> Backend {
        // Route by database_name prefix (for now, default to Spanner)
        // TODO: Check self.filter.database_name prefix to route to Aurora if needed
        Backend::Spanner(factory.spanner().clone())
    }
}

impl Assignable for GetCollectionWithSegmentsRequest {
    type Output = Backend;

    fn assign(&self, factory: &BackendFactory) -> Backend {
        // Single collection lookup - default to Spanner
        // TODO: Determine routing based on collection metadata if needed
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

#[async_trait::async_trait]
impl Runnable for GetCollectionWithSegmentsRequest {
    type Response = GetCollectionWithSegmentsResponse;
    type Input = Backend;

    async fn run(self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.get_collection_with_segments(self).await
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

/// Internal response for getting a collection with its segments.
#[derive(Debug, Clone)]
pub struct GetCollectionWithSegmentsResponse {
    pub collection: Collection,
    pub segments: Vec<Segment>,
}

impl TryFrom<GetCollectionWithSegmentsResponse>
    for chroma_proto::GetCollectionWithSegmentsResponse
{
    type Error = SysDbError;

    fn try_from(r: GetCollectionWithSegmentsResponse) -> Result<Self, Self::Error> {
        // Segment -> chroma_proto::Segment is infallible (From, not TryFrom)
        let segments: Vec<chroma_proto::Segment> = r.segments.into_iter().map(Into::into).collect();
        Ok(chroma_proto::GetCollectionWithSegmentsResponse {
            collection: Some(r.collection.try_into()?),
            segments,
        })
    }
}
