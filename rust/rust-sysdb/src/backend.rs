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
//! │         Calls assign() then run() on request types          │
//! └─────────────────────┬───────────────────────────────────────┘
//!                       │
//!                       ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │              Request Types (types.rs)                       │
//! │                                                             │
//! │   impl Assignable for CreateTenantRequest {                 │
//! │       type Output = Vec<Backend>;                           │
//! │       fn assign(&self, factory: &BackendFactory) -> ...     │
//! │   }                                                         │
//! │                                                             │
//! │   impl Runnable for CreateTenantRequest {                   │
//! │       type Input = Vec<Backend>;                            │
//! │       async fn run(&self, backends: Vec<Backend>) -> ...    │
//! │   }                                                         │
//! │                                                             │
//! │   • Each request type determines routing (assign)           │
//! │   • Each request type defines execution (run)               │
//! └──────────┬─────────────────────────────────────┬────────────┘
//!            │                                     │
//!            │ assign()                            │ run()
//!            │                                     │
//!            ▼                                     ▼
//! ┌──────────────────────────┐    ┌──────────────────────────┐
//! │   BackendFactory         │    │   Backend (enum)         │
//! │   (backend.rs)           │    │   (backend.rs)           │
//! │                          │    │                          │
//! │   struct BackendFactory {│    │   enum Backend {         │
//! │       spanner: ...       │    │       Spanner(...),      │
//! │       // aurora: ...     │    │       // Aurora(...)     │
//! │   }                      │    │   }                      │
//! │                          │    │                          │
//! │   • Holds all backends   │    │   • Dispatches to        │
//! │   • Provides accessors   │    │     concrete backends    │
//! └──────────────────────────┘    └──────────┬───────────────┘
//!                                             │
//!                                             ▼
//!                          ┌──────────────────────────────────┐
//!                          │   Concrete Backends              │
//!                          │                                  │
//!                          │  ┌────────────┐  ┌────────────┐  │
//!                          │  │ Spanner    │  │ Aurora     │  │
//!                          │  │ Backend    │  │ Backend    │  │
//!                          │  │            │  │            │  │
//!                          │  │ • google-  │  │ • sqlx/    │  │
//!                          │  │   cloud-   │  │   PgPool   │  │
//!                          │  │   spanner  │  │ • Postgres │  │
//!                          │  └────────────┘  └────────────┘  │
//!                          └──────────────────────────────────┘
//! ```

use crate::spanner::SpannerBackend;
use crate::types::SysDbError;
use crate::types::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse,
    GetDatabaseRequest, GetDatabaseResponse, GetTenantRequest, GetTenantResponse,
    SetTenantResourceNameRequest, SetTenantResourceNameResponse,
};
use chroma_types::chroma_proto::Database;

/// Factory that holds all configured backend instances.
///
/// This factory provides access to all backends (Spanner, Aurora, etc.)
/// without requiring knowledge of specific backend types in the assign logic.
#[derive(Clone)]
pub struct BackendFactory {
    spanner: SpannerBackend,
    // TODO: aurora: AuroraBackend,
}

impl BackendFactory {
    /// Create a new BackendFactory with the given backends.
    ///
    /// TODO: Update to `new(spanner: SpannerBackend, aurora: AuroraBackend)` when Aurora is added.
    pub fn new(spanner: SpannerBackend) -> Self {
        Self { spanner }
    }

    /// Get a reference to the Spanner backend.
    pub fn spanner(&self) -> &SpannerBackend {
        &self.spanner
    }

    // TODO: pub fn aurora(&self) -> &AuroraBackend {
    //     &self.aurora
    // }

    /// Close all backends.
    pub async fn close(self) {
        self.spanner.close().await;
        // TODO: self.aurora.close().await;
    }
}

/// Trait for request types that can determine which backends should handle them.
///
/// Each request type implements this trait to specify which backend(s)
/// should process the request. The associated type `Output` can be
/// either `Backend` (for single backend operations) or `Vec<Backend>`
/// (for fan-out operations).
pub trait Assignable {
    /// The type of backend(s) this operation requires.
    /// Can be `Backend` for single backend or `Vec<Backend>` for multiple.
    type Output;

    /// Assign this request to the appropriate backend(s).
    fn assign(&self, factory: &BackendFactory) -> Self::Output;
}

/// Trait for request types that can execute their operation on backends.
///
/// This trait encapsulates the pattern of executing operations on backends.
/// The associated type `Input` must match the `Output` from
/// `Assignable` - either `Backend` for single backend operations or
/// `Vec<Backend>` for fan-out operations.
#[async_trait::async_trait]
pub trait Runnable {
    /// The response type for this operation.
    type Response;
    /// The type of backend(s) this operation accepts.
    /// Must match `Assignable::Output` for the same request type.
    type Input;

    /// Execute this request on the given backend(s).
    async fn run(&self, backends: Self::Input) -> Result<Self::Response, SysDbError>;
}

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

    /// Get a tenant by name.
    ///
    /// Returns `SysDbError::NotFound` if the tenant does not exist.
    pub async fn get_tenant(
        &self,
        req: &GetTenantRequest,
    ) -> Result<GetTenantResponse, SysDbError> {
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
    ///
    /// Returns `SysDbError::NotFound` if the database does not exist.
    pub async fn get_database(
        &self,
        req: &GetDatabaseRequest,
    ) -> Result<GetDatabaseResponse, SysDbError> {
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
