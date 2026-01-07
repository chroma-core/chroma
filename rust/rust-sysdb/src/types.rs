//! Internal domain types for the SysDb service.
//!
//! These types provide a layer of indirection between the protobuf types
//! and the backend implementations. This allows:
//! - Changing the wire format without affecting backends
//! - Backend-specific optimizations without affecting the API
//! - Cleaner internal APIs that aren't tied to protobuf conventions

use chroma_types::{chroma_proto, Database, Tenant};
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
    pub id: Uuid,
}

impl TryFrom<chroma_proto::CreateTenantRequest> for CreateTenantRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::CreateTenantRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: validate_uuid(&req.name)?,
        })
    }
}

/// Internal request for getting a tenant.
#[derive(Debug, Clone)]
pub struct GetTenantRequest {
    pub id: Uuid,
}

impl TryFrom<chroma_proto::GetTenantRequest> for GetTenantRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::GetTenantRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: validate_uuid(&req.name)?,
        })
    }
}

/// Internal request for setting tenant resource name.
#[derive(Debug, Clone)]
pub struct SetTenantResourceNameRequest {
    pub id: Uuid,
    pub resource_name: String,
}

impl TryFrom<chroma_proto::SetTenantResourceNameRequest> for SetTenantResourceNameRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::SetTenantResourceNameRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: validate_uuid(&req.id)?,
            resource_name: req.resource_name,
        })
    }
}

/// Internal request for creating a database.
#[derive(Debug, Clone)]
pub struct CreateDatabaseRequest {
    pub id: Uuid,
    pub name: String,
    pub tenant_id: Uuid,
}

impl TryFrom<chroma_proto::CreateDatabaseRequest> for CreateDatabaseRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::CreateDatabaseRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: validate_uuid(&req.id)?,
            name: req.name,
            tenant_id: validate_uuid(&req.tenant)?,
        })
    }
}

/// Internal request for getting a database.
#[derive(Debug, Clone)]
pub struct GetDatabaseRequest {
    pub name: String,
    pub tenant_id: Uuid,
}

impl TryFrom<chroma_proto::GetDatabaseRequest> for GetDatabaseRequest {
    type Error = SysDbError;

    fn try_from(req: chroma_proto::GetDatabaseRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            name: req.name,
            tenant_id: validate_uuid(&req.tenant)?,
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

// ============================================================================
// Runnable Trait Implementations
// ============================================================================

#[async_trait::async_trait]
impl Runnable for CreateTenantRequest {
    type Response = CreateTenantResponse;
    type Input = Vec<Backend>;

    async fn run(&self, backends: Vec<Backend>) -> Result<Self::Response, SysDbError> {
        for backend in backends {
            backend.create_tenant(self).await?;
        }
        Ok(CreateTenantResponse {})
    }
}

#[async_trait::async_trait]
impl Runnable for GetTenantRequest {
    type Response = GetTenantResponse;
    type Input = Backend;

    async fn run(&self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.get_tenant(self).await
    }
}

#[async_trait::async_trait]
impl Runnable for SetTenantResourceNameRequest {
    type Response = SetTenantResourceNameResponse;
    type Input = Vec<Backend>;

    async fn run(&self, backends: Vec<Backend>) -> Result<Self::Response, SysDbError> {
        for backend in backends {
            backend.set_tenant_resource_name(self).await?;
        }
        Ok(SetTenantResourceNameResponse {})
    }
}

#[async_trait::async_trait]
impl Runnable for CreateDatabaseRequest {
    type Response = CreateDatabaseResponse;
    type Input = Backend;

    async fn run(&self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.create_database(self).await
    }
}

#[async_trait::async_trait]
impl Runnable for GetDatabaseRequest {
    type Response = GetDatabaseResponse;
    type Input = Backend;

    async fn run(&self, backend: Backend) -> Result<Self::Response, SysDbError> {
        backend.get_database(self).await
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
