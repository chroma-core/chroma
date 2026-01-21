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

use std::collections::HashMap;

use crate::config::SpannerBackendConfig;
use crate::spanner::SpannerBackend;
use crate::types::SysDbError;
use crate::types::{
    CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse, FlushCompactionRequest,
    FlushCompactionResponse, GetCollectionWithSegmentsRequest, GetCollectionWithSegmentsResponse,
    GetCollectionsRequest, GetCollectionsResponse, GetDatabaseRequest, GetDatabaseResponse,
    GetTenantRequest, GetTenantResponse, SetTenantResourceNameRequest,
    SetTenantResourceNameResponse, UpdateCollectionRequest, UpdateCollectionResponse,
};
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_storage::config::{RegionalStorage, TopologicalStorage};
use chroma_types::chroma_proto::Database;
use chroma_types::{MultiCloudMultiRegionConfiguration, TopologyName};

/// Factory that holds all configured backend instances.
///
/// This factory provides access to all backends (Spanner, Aurora, etc.)
/// without requiring knowledge of specific backend types in the assign logic.
#[derive(Clone)]
pub struct BackendFactory {
    topology_to_backend: HashMap<TopologyName, SpannerBackend>,
    // TODO: aurora: AuroraBackend,
}

/// Type alias for the MCMR configuration used by the sysdb service.
pub type SysdbMcmrConfig = MultiCloudMultiRegionConfiguration<RegionalStorage, TopologicalStorage>;

#[async_trait::async_trait]
impl Configurable<SysdbMcmrConfig> for BackendFactory {
    async fn try_from_config(
        config: &SysdbMcmrConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let local_region_name = config.preferred().clone();
        let mut topology_to_backend = HashMap::new();

        for topology in config.topologies() {
            let backend_config = SpannerBackendConfig {
                spanner: &topology.config().spanner,
                regions: topology.regions().to_vec(),
                local_region: local_region_name.clone(),
            };
            let backend = SpannerBackend::try_from_config(&backend_config, registry).await?;
            topology_to_backend.insert(topology.name().clone(), backend);
        }

        Ok(Self::new(topology_to_backend))
    }
}

impl BackendFactory {
    /// Create a new BackendFactory with the given backends.
    pub fn new(topology_to_backend: HashMap<TopologyName, SpannerBackend>) -> Self {
        Self {
            topology_to_backend,
        }
    }

    /// Get a reference to the Spanner backend belonging to the given topology.
    pub fn spanner(&self, topology: &TopologyName) -> &SpannerBackend {
        &self.topology_to_backend[topology]
    }

    /// Get a reference to one of the Spanner backends.
    pub fn one_spanner(&self) -> &SpannerBackend {
        if self.topology_to_backend.is_empty() {
            panic!("No spanner backends found");
        }
        self.topology_to_backend.iter().next().unwrap().1
    }

    // TODO: pub fn aurora(&self) -> &AuroraBackend {
    //     &self.aurora
    // }

    /// Close all backends.
    pub async fn close(self) {
        for backend in self.topology_to_backend.into_values() {
            backend.close().await;
        }
        // TODO: self.aurora.close().await;
    }

    pub fn get_all_backends(&self) -> Vec<Backend> {
        self.topology_to_backend
            .values()
            .map(|b| Backend::Spanner(b.clone()))
            .collect()
        // TODO: return vec![Backend::Aurora(b.clone())];
    }

    /// Get a backend routed by the topology prefix in the database name.
    /// If the database name has a topology prefix (before '+'), use it to route to the correct backend.
    /// Otherwise, fall back to one_spanner().
    pub fn backend_from_database_name(&self, db_name: &chroma_types::DatabaseName) -> Backend {
        if let Some(topo_str) = db_name.topology() {
            if let Ok(topology) = TopologyName::new(topo_str) {
                return Backend::Spanner(self.spanner(&topology).clone());
            }
        }
        // Fall back to default backend if no topology or invalid topology
        // TODO(Sanket): Should fall back to Aurora here.
        tracing::warn!("No topology found in database name, falling back to default backend");
        Backend::Spanner(self.one_spanner().clone())
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
    async fn run(self, backends: Self::Input) -> Result<Self::Response, SysDbError>;
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
        req: CreateTenantRequest,
    ) -> Result<CreateTenantResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.create_tenant(req).await,
        }
    }

    /// Get a tenant by name.
    ///
    /// Returns `SysDbError::NotFound` if the tenant does not exist.
    pub async fn get_tenant(&self, req: GetTenantRequest) -> Result<GetTenantResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.get_tenant(req).await,
        }
    }

    /// Set the resource name for a tenant.
    pub async fn set_tenant_resource_name(
        &self,
        req: SetTenantResourceNameRequest,
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
        req: CreateDatabaseRequest,
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
        req: GetDatabaseRequest,
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
    // Collection Operations
    // ============================================================

    /// Create a new collection.
    pub async fn create_collection(
        &self,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.create_collection(req).await,
        }
    }

    /// Get collections by filter.
    ///
    /// Returns an empty list if no matching collections are found.
    pub async fn get_collections(
        &self,
        req: GetCollectionsRequest,
    ) -> Result<GetCollectionsResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.get_collections(req).await,
        }
    }

    /// Get a collection with its segments.
    ///
    /// Returns `SysDbError::NotFound` if the collection does not exist.
    pub async fn get_collection_with_segments(
        &self,
        req: GetCollectionWithSegmentsRequest,
    ) -> Result<GetCollectionWithSegmentsResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.get_collection_with_segments(req).await,
        }
    }

    /// Update a collection.
    ///
    /// Supports updating name, dimension, metadata, and configuration.
    /// Returns `SysDbError::NotFound` if the collection does not exist.
    /// Returns `SysDbError::AlreadyExists` if the new name conflicts with an existing collection.
    pub async fn update_collection(
        &self,
        req: UpdateCollectionRequest,
    ) -> Result<UpdateCollectionResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.update_collection(req).await,
        }
    }

    /// Flush collection compaction results to the database.
    pub async fn flush_collection_compaction(
        &self,
        req: FlushCompactionRequest,
    ) -> Result<FlushCompactionResponse, SysDbError> {
        match self {
            Backend::Spanner(s) => s.flush_collection_compaction(req).await,
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
