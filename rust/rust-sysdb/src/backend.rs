//! Backend abstraction for the SysDb service.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      gRPC Server                            │
//! │                     (server.rs)                             │
//! │              Implements all SysDb RPCs                      │
//! │         Converts proto <-> internal types                   │
//! └─────────────────────┬───────────────────────────────────────┘
//!                       │
//!                       ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                       Router                                │
//! │                    (router.rs)                              │
//! │  • Takes RouteRequest (op, db_name, tenant)                 │
//! │  • Returns Vec<Backend> (fan-out support)                   │
//! │  • Routes by db_name prefix or operation type               │
//! └─────────────────────┬───────────────────────────────────────┘
//!                       │
//!                       ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Backend (enum)                            │
//! │                   (backend.rs)                              │
//! │                                                             │
//! │   enum Backend {                                            │
//! │       Spanner(SpannerBackend),                              │
//! │       Aurora(AuroraBackend),  // TODO                       │
//! │   }                                                         │
//! │                                                             │
//! │   impl Backend {                                            │
//! │       // All RPC methods - dispatch to inner variant        │
//! │   }                                                         │
//! └──────────┬─────────────────────────────────────┬────────────┘
//!            │                                     │
//!            ▼                                     ▼
//! ┌─────────────────────┐           ┌─────────────────────┐
//! │   SpannerBackend    │           │   AuroraBackend     │
//! │   (spanner.rs)      │           │   (aurora.rs)       │
//! │                     │           │                     │
//! │ • Uses google-cloud │           │ • Uses sqlx/PgPool  │
//! │   -spanner crate    │           │ • PostgreSQL dialect│
//! │ • Single file       │           │ • Added later       │
//! └─────────────────────┘           └─────────────────────┘
//! ```

use crate::error::SysDbError;
use crate::spanner::SpannerBackend;
use crate::types::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse,
    Database, GetDatabaseRequest, GetDatabaseResponse, GetTenantRequest, GetTenantResponse,
    SetTenantResourceNameRequest, SetTenantResourceNameResponse,
};

/// Backend enum that wraps all supported database backends.
///
/// Each variant holds a backend implementation. Methods on this enum
/// dispatch to the appropriate backend based on the variant.
#[derive(Clone)]
pub enum Backend {
    /// Google Cloud Spanner backend
    Spanner(SpannerBackend),
    // TODO: Add Aurora(AuroraBackend)
}

impl Backend {
    // ============================================================
    // Tenant Operations
    // ============================================================

    /// Create a new tenant.
    pub async fn create_tenant(
        &self,
        req: &CreateTenantRequest,
    ) -> Result<CreateTenantResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.create_tenant(req).await,
        }
    }

    /// Get a tenant by name. Returns None if not found.
    pub async fn get_tenant(
        &self,
        req: &GetTenantRequest,
    ) -> Result<Option<GetTenantResponse>, SysDbError> {
        match self {
            Backend::Spanner(s) => s.get_tenant(req).await,
        }
    }

    /// Set the resource name for a tenant.
    pub async fn set_tenant_resource_name(
        &self,
        req: &SetTenantResourceNameRequest,
    ) -> Result<SetTenantResourceNameResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.set_tenant_resource_name(req).await,
        }
    }

    // ============================================================
    // Database Operations
    // ============================================================

    /// Create a new database.
    pub async fn create_database(
        &self,
        req: &CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.create_database(req).await,
        }
    }

    /// Get a database by name and tenant.
    /// Returns None if the database does not exist.
    pub async fn get_database(
        &self,
        req: &GetDatabaseRequest,
    ) -> Result<Option<GetDatabaseResponse>, SysDbError> {
        match self {
            Backend::Spanner(s) => s.get_database(req).await,
        }
    }

    /// List databases for a tenant.
    pub async fn list_databases(
        &self,
        tenant: &str,
        limit: Option<i32>,
        offset: i32,
    ) -> Result<Vec<Database>, SysDbError> {
        match self {
            Backend::Spanner(s) => s.list_databases(tenant, limit, offset).await,
        }
    }

    /// Delete a database.
    pub async fn delete_database(&self, name: &str, tenant: &str) -> Result<(), SysDbError> {
        match self {
            Backend::Spanner(s) => s.delete_database(name, tenant).await,
        }
    }

    // ============================================================
    // Lifecycle
    // ============================================================

    /// Close the backend connection.
    pub async fn close(self) {
        match self {
            Backend::Spanner(s) => s.close().await,
        }
    }
}
