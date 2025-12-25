//! Request routing for the SysDb service.
//!
//! The `Router` decides which backend(s) should handle a given request based on:
//! - The operation type (some operations fan out to all backends)
//! - The database name prefix (for routing to specific backends)

use crate::backend::Backend;
use crate::error::SysDbError;
use crate::spanner::SpannerBackend;

/// Context for making routing decisions.
#[derive(Debug)]
pub struct RouteRequest<'a> {
    /// The operation being performed
    pub op: Operation,
    /// Database name (used for prefix-based routing)
    pub db_name: Option<&'a str>,
    /// Tenant name
    pub tenant: Option<&'a str>,
}

/// All operations that can be routed.
#[derive(Debug, Clone, Copy)]
pub enum Operation {
    // Tenant operations
    CreateTenant,
    GetTenant,
    SetTenantResourceName,

    // Database operations
    CreateDatabase,
    GetDatabase,
    ListDatabases,
    DeleteDatabase,
    FinishDatabaseDeletion,

    // Collection operations
    CreateCollection,
    GetCollection,
    GetCollections,
    GetCollectionByResourceName,
    GetCollectionWithSegments,
    CountCollections,
    CheckCollections,
    UpdateCollection,
    DeleteCollection,
    FinishCollectionDeletion,
    ForkCollection,
    CountForks,
    RestoreCollection,
    GetCollectionSize,

    // Segment operations
    CreateSegment,
    GetSegments,
    UpdateSegment,
    DeleteSegment,

    // Version/compaction operations
    ListCollectionVersions,
    MarkVersionForDeletion,
    DeleteCollectionVersion,
    BatchGetCollectionVersionFilePaths,
    BatchGetCollectionSoftDeleteStatus,
    FlushCollectionCompaction,
    GetLastCompactionTimeForTenant,
    SetLastCompactionTimeForTenant,

    // Garbage collection operations
    ListCollectionsToGc,

    // Attached function operations
    AttachFunction,
    GetAttachedFunctions,
    DetachFunction,
    FinishCreateAttachedFunction,
    CleanupExpiredPartialAttachedFunctions,
    GetFunctions,
    GetAttachedFunctionsToGc,
    FinishAttachedFunctionDeletion,
    FlushCollectionCompactionAndAttachedFunction,

    // Misc
    ResetState,
}

/// Router that directs requests to appropriate backend(s).
///
/// Holds all configured backends and provides routing logic based on
/// operation type and request parameters. Both Spanner and Aurora
/// backends are required.
#[derive(Clone)]
pub struct Router {
    spanner: SpannerBackend,
    // TODO: Add aurora: AuroraBackend (required, not optional)
}

impl Router {
    /// Create a new router with the given backends.
    ///
    /// TODO: Update to `new(spanner: SpannerBackend, aurora: AuroraBackend)` when Aurora is added.
    pub fn new(spanner: SpannerBackend) -> Self {
        Self { spanner }
    }

    /// Route a request to the appropriate backend(s).
    ///
    /// Returns a vector of backends that should handle this request.
    /// Most operations return a single backend, but some (like CreateTenant)
    /// may fan out to multiple backends.
    ///
    /// # Errors
    /// Returns an error if `db_name` is None for operations that require it.
    pub fn route(&self, req: &RouteRequest) -> Result<Vec<Backend>, SysDbError> {
        use Operation::*;

        match req.op {
            // Tenant operations may need to fan out to all backends
            CreateTenant => Ok(self.all_backends()),
            GetTenant => Ok(vec![self.default_backend()]),
            SetTenantResourceName => Ok(self.all_backends()),

            // Database operations - route by db_name prefix
            CreateDatabase | GetDatabase | DeleteDatabase | FinishDatabaseDeletion => {
                let db_name = req.db_name.ok_or_else(|| {
                    SysDbError::InvalidArgument("db_name required for this operation".to_string())
                })?;
                Ok(vec![self.route_by_db_name(db_name)])
            }

            // ListDatabases may need to fan out and merge results
            ListDatabases => Ok(self.all_backends()),

            // Collection operations - route by db_name prefix
            CreateCollection
            | GetCollection
            | GetCollections
            | GetCollectionByResourceName
            | GetCollectionWithSegments
            | CountCollections
            | CheckCollections
            | UpdateCollection
            | DeleteCollection
            | FinishCollectionDeletion
            | ForkCollection
            | CountForks
            | RestoreCollection
            | GetCollectionSize => {
                let db_name = req.db_name.ok_or_else(|| {
                    SysDbError::InvalidArgument("db_name required for this operation".to_string())
                })?;
                Ok(vec![self.route_by_db_name(db_name)])
            }

            // Segment operations - route by db_name prefix
            CreateSegment | GetSegments | UpdateSegment | DeleteSegment => {
                let db_name = req.db_name.ok_or_else(|| {
                    SysDbError::InvalidArgument("db_name required for this operation".to_string())
                })?;
                Ok(vec![self.route_by_db_name(db_name)])
            }

            // Version/compaction operations - route by db_name prefix
            ListCollectionVersions
            | MarkVersionForDeletion
            | DeleteCollectionVersion
            | BatchGetCollectionVersionFilePaths
            | BatchGetCollectionSoftDeleteStatus
            | FlushCollectionCompaction => {
                let db_name = req.db_name.ok_or_else(|| {
                    SysDbError::InvalidArgument("db_name required for this operation".to_string())
                })?;
                Ok(vec![self.route_by_db_name(db_name)])
            }

            // Tenant-level operations
            GetLastCompactionTimeForTenant | SetLastCompactionTimeForTenant => {
                Ok(vec![self.default_backend()])
            }

            // GC operations - may need to check all backends
            ListCollectionsToGc => Ok(self.all_backends()),

            // Attached function operations - route by db_name prefix
            AttachFunction
            | GetAttachedFunctions
            | DetachFunction
            | FinishCreateAttachedFunction
            | CleanupExpiredPartialAttachedFunctions
            | GetFunctions
            | GetAttachedFunctionsToGc
            | FinishAttachedFunctionDeletion
            | FlushCollectionCompactionAndAttachedFunction => {
                let db_name = req.db_name.ok_or_else(|| {
                    SysDbError::InvalidArgument("db_name required for this operation".to_string())
                })?;
                Ok(vec![self.route_by_db_name(db_name)])
            }

            // Reset affects all backends
            ResetState => Ok(self.all_backends()),
        }
    }

    /// Get the default backend (used when no routing hint is available).
    fn default_backend(&self) -> Backend {
        Backend::Spanner(self.spanner.clone())
    }

    /// Get all configured backends (both Spanner and Aurora).
    fn all_backends(&self) -> Vec<Backend> {
        vec![Backend::Spanner(self.spanner.clone())]
        // TODO: Add Aurora backend:
        // vec![
        //     Backend::Spanner(self.spanner.clone()),
        //     Backend::Aurora(self.aurora.clone()),
        // ]
    }

    /// Route based on database name prefix.
    ///
    /// Currently defaults to Spanner. When Aurora is added, this will
    /// check for prefixes accordingly.
    fn route_by_db_name(&self, _db_name: &str) -> Backend {
        // Default to Spanner
        Backend::Spanner(self.spanner.clone())
    }

    /// Close all backends.
    pub async fn close(self) {
        self.spanner.close().await;
        // TODO: Close Aurora backend:
        // self.aurora.close().await;
    }
}
