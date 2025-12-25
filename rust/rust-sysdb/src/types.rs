//! Internal domain types for the SysDb service.
//!
//! These types provide a layer of indirection between the protobuf types
//! and the backend implementations. This allows:
//! - Changing the wire format without affecting backends
//! - Backend-specific optimizations without affecting the API
//! - Cleaner internal APIs that aren't tied to protobuf conventions

use chroma_types::chroma_proto;

// ============================================================================
// Request Types (proto -> internal)
// ============================================================================

/// Internal request for creating a tenant.
#[derive(Debug, Clone)]
pub struct CreateTenantRequest {
    pub name: String,
}

impl From<chroma_proto::CreateTenantRequest> for CreateTenantRequest {
    fn from(req: chroma_proto::CreateTenantRequest) -> Self {
        Self { name: req.name }
    }
}

/// Internal request for getting a tenant.
#[derive(Debug, Clone)]
pub struct GetTenantRequest {
    pub name: String,
}

impl From<chroma_proto::GetTenantRequest> for GetTenantRequest {
    fn from(req: chroma_proto::GetTenantRequest) -> Self {
        Self { name: req.name }
    }
}

/// Internal request for setting tenant resource name.
#[derive(Debug, Clone)]
pub struct SetTenantResourceNameRequest {
    pub id: String,
    pub resource_name: String,
}

impl From<chroma_proto::SetTenantResourceNameRequest> for SetTenantResourceNameRequest {
    fn from(req: chroma_proto::SetTenantResourceNameRequest) -> Self {
        Self {
            id: req.id,
            resource_name: req.resource_name,
        }
    }
}

/// Internal request for creating a database.
#[derive(Debug, Clone)]
pub struct CreateDatabaseRequest {
    pub id: String,
    pub name: String,
    pub tenant: String,
}

impl From<chroma_proto::CreateDatabaseRequest> for CreateDatabaseRequest {
    fn from(req: chroma_proto::CreateDatabaseRequest) -> Self {
        Self {
            id: req.id,
            name: req.name,
            tenant: req.tenant,
        }
    }
}

/// Internal request for getting a database.
#[derive(Debug, Clone)]
pub struct GetDatabaseRequest {
    pub name: String,
    pub tenant: String,
}

impl From<chroma_proto::GetDatabaseRequest> for GetDatabaseRequest {
    fn from(req: chroma_proto::GetDatabaseRequest) -> Self {
        Self {
            name: req.name,
            tenant: req.tenant,
        }
    }
}

// ============================================================================
// Domain Types (internal)
// ============================================================================

/// Internal tenant representation.
#[derive(Debug, Clone)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub resource_name: Option<String>,
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

// ============================================================================
// Domain Types -> Proto Conversions
// ============================================================================

impl From<Tenant> for chroma_proto::Tenant {
    fn from(t: Tenant) -> Self {
        chroma_proto::Tenant {
            name: t.name,
            resource_name: t.resource_name,
        }
    }
}

/// Internal database representation.
#[derive(Debug, Clone)]
pub struct Database {
    pub id: String,
    pub name: String,
    pub tenant_id: String,
}

impl From<Database> for chroma_proto::Database {
    fn from(d: Database) -> Self {
        chroma_proto::Database {
            id: d.id,
            name: d.name,
            tenant: d.tenant_id,
        }
    }
}
