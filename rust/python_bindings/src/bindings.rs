use chroma_cache::FoyerCacheConfig;
use chroma_config::Configurable;
use chroma_frontend::{
    executor::{local::LocalExecutor, Executor},
    frontend::Frontend,
    get_collection_with_segments_provider::{
        CacheInvalidationRetryConfig, CollectionsWithSegmentsProvider,
        CollectionsWithSegmentsProviderConfig,
    },
    LocalCompactionManager,
};
use chroma_log::Log;
use chroma_segment::{
    local_segment_manager::{LocalSegmentManager, LocalSegmentManagerConfig},
    sqlite_metadata::SqliteMetadataWriter,
};
use chroma_sqlite::{config::SqliteDBConfig, db::SqliteDb};
use chroma_sysdb::{sqlite::SqliteSysDb, sysdb::SysDb};
use chroma_system::System;
use chroma_types::{
    AddCollectionRecordsError, AddCollectionRecordsRequest, GetCollectionError, Metadata,
};
use numpy::PyReadonlyArray1;
use pyo3::{
    exceptions::{PyOSError, PyRuntimeError, PyValueError},
    pyclass, pymethods, Py, PyAny, PyObject, PyResult, Python,
};
use std::time::SystemTime;

const DEFAULT_DATABASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

#[pyclass]
pub(crate) struct Bindings {
    _runtime: tokio::runtime::Runtime,
    // TODO(hammadb): In order to make CI green, we proxy all
    // calls back into python.
    // We should slowly start moving the logic from python to rust
    proxy_frontend: Py<PyAny>,
    _frontend: Frontend,
    _compaction_manager_handle: chroma_system::ComponentHandle<LocalCompactionManager>,
}

#[pyclass]
pub struct PythonBindingsConfig {
    #[pyo3(get, set)]
    sqlite_db_config: SqliteDBConfig,
}

#[pymethods]
impl PythonBindingsConfig {
    #[new]
    pub fn py_new(sqlite_db_config: SqliteDBConfig) -> Self {
        PythonBindingsConfig { sqlite_db_config }
    }
}

//////////////////////// PyMethods Implementation ////////////////////////
#[pymethods]
impl Bindings {
    #[new]
    #[allow(dead_code)]
    pub fn py_new(
        proxy_frontend: Py<PyAny>,
        sqlite_db_config: SqliteDBConfig,
        persist_path: String,
    ) -> PyResult<Self> {
        // TODO: runtime config
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let system = System::new();

        //////////////////////////// Frontend Setup ////////////////////////////

        // This set up code is extremely janky, I've left comments
        // on the parts that need to be cleaned up.
        // TODO(hammadb): Clean up this code - this is just to unblock us in short term
        // TODO: clean up this construction
        let sqlite_db =
            match runtime.block_on(async { SqliteDb::try_from_config(&sqlite_db_config).await }) {
                Ok(db) => db,
                Err(e) => {
                    // TODO: error
                    return Err(PyOSError::new_err(format!(
                        "Failed to create sqlite db: {}",
                        e
                    )));
                }
            };

        let cache_config = chroma_cache::CacheConfig::Memory(FoyerCacheConfig::default());
        let segment_manager_config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: cache_config,
            persist_path,
        };
        // Create the hnsw segment manager.
        let segment_manager = match runtime.block_on(LocalSegmentManager::try_from_config(&(
            segment_manager_config,
            sqlite_db.clone(),
        ))) {
            Ok(sm) => sm,
            Err(e) => {
                return Err(PyOSError::new_err(format!(
                    "Failed to create segment manager: {}",
                    e
                )))
            }
        };
        let sqlite_sysdb = SqliteSysDb::new(sqlite_db.clone());
        let sysdb = Box::new(SysDb::Sqlite(sqlite_sysdb));
        // TODO: get the log configuration from the config sysdb
        let log = Box::new(Log::Sqlite(chroma_log::sqlite_log::SqliteLog::new(
            sqlite_db.clone(),
            "default".to_string(),
            "default".to_string(),
        )));

        // Spawn the compaction manager.
        let metadata_writer = SqliteMetadataWriter {
            db: sqlite_db.clone(),
        };
        let handle = system.start_component(LocalCompactionManager::new(
            log.clone(),
            metadata_writer,
            segment_manager.clone(),
        ));

        // TODO: clean up the cache configuration and decide the source of truth owner
        // make cache not a no-op
        let collection_cache_config = CollectionsWithSegmentsProviderConfig {
            // No retry to sysdb on local chroma
            cache_invalidation_retry_policy: CacheInvalidationRetryConfig::new(0, 0),
            permitted_parallelism: 32,
            cache: chroma_cache::CacheConfig::Nop,
        };

        let collections_cache = match runtime.block_on(async {
            CollectionsWithSegmentsProvider::try_from_config(&(
                collection_cache_config,
                sysdb.clone(),
            ))
            .await
        }) {
            Ok(cache) => cache,
            Err(e) => {
                // TODO: error type
                return Err(PyOSError::new_err(format!(
                    "Failed to create collections cache: {}",
                    e
                )));
            }
        };

        // TODO: executor should NOT be exposed to the bindings module. try_from_config should work.
        // The reason this works this way right now is because try_from_config cannot share the sqlite_db
        // across the downstream components.
        let executor = Executor::Local(LocalExecutor::new(segment_manager, sqlite_db));
        let frontend = Frontend::new(false, sysdb.clone(), collections_cache, log, executor);

        Ok(Bindings {
            proxy_frontend,
            _runtime: runtime,
            _frontend: frontend,
            _compaction_manager_handle: handle,
        })
    }

    /// Returns the current eopch time in ns
    /// TODO(hammadb): This should proxy to ServerAPI
    #[allow(dead_code)]
    fn heartbeat(&self) -> PyResult<u128> {
        let duration_since_epoch =
            match std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(duration) => duration,
                Err(_) => return Err(PyOSError::new_err("Failed to get system time")),
            };
        Ok(duration_since_epoch.as_nanos())
    }

    // TODO(hammadb): Determine our pattern for optional arguments in python
    // options include using Option or passing defaults from python
    // or using pyargs annotations such as
    // #[pyargs(limit = "None", offset = "None")]

    ////////////////////////////// Admin API //////////////////////////////

    fn create_database(&self, name: String, tenant: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "create_database", (name, tenant))
    }

    fn get_database(&self, name: String, tenant: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "get_database", (name, tenant))
    }

    fn delete_database(&self, name: String, tenant: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "delete_database", (name, tenant))
    }

    #[pyo3(signature = (limit = None, offset = None, tenant = "DEFAULT_TENANT".to_string()))]
    fn list_databases(
        &self,
        limit: Option<i32>,
        offset: Option<i32>,
        tenant: String,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "list_databases", (limit, offset, tenant))
    }

    fn create_tenant(&self, name: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "create_tenant", (name,))
    }

    fn get_tenant(&self, name: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend.call_method1(py, "get_tenant", (name,))
    }

    ////////////////////////////// Base API //////////////////////////////
    #[allow(clippy::too_many_arguments)]
    fn create_collection(
        &self,
        name: String,
        configuration: PyObject,
        metadata: PyObject,
        get_or_create: bool,
        tenant: String,
        database: String,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        self.proxy_frontend.call_method1(
            py,
            "create_collection",
            (
                name,
                configuration,
                metadata,
                get_or_create,
                tenant,
                database,
            ),
        )
    }

    //////////////////////////// Record Methods ////////////////////////////

    #[pyo3(
        signature = (ids, collection_id, embeddings, metadatas = None, documents = None, uris = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn add(
        &self,
        ids: Vec<String>,
        collection_id: String,
        embeddings: Vec<PyReadonlyArray1<f32>>,
        metadatas: Option<Vec<Option<Metadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> PyResult<bool> {
        let embeddings = py_embeddings_to_vec_f32(embeddings)?;

        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
        );

        let req = AddCollectionRecordsRequest {
            ids,
            collection_id,
            // TODO: WHY IS THIS Option for Add?
            embeddings: Some(embeddings),
            metadatas,
            documents,
            uris,
            tenant_id: tenant,
            database_name: database,
        };

        // TODO: Error handling cleanup
        let mut frontend_clone = self._frontend.clone();
        match self
            ._runtime
            .block_on(async { frontend_clone.add(req).await })
        {
            Ok(_) => Ok(true),
            Err(e) => match e {
                AddCollectionRecordsError::Collection(e) => match e {
                    GetCollectionError::NotFound(_) => {
                        Err(PyValueError::new_err("Collection not found"))
                    }
                    GetCollectionError::Internal(e) => {
                        Err(PyRuntimeError::new_err(format!("Internal Error: {}", e)))
                    }
                },
                AddCollectionRecordsError::Internal(e) => {
                    Err(PyRuntimeError::new_err(format!("Internal Error: {}", e)))
                }
            },
        }
    }

    // @override
    // def _get(
    //     self,
    //     collection_id: UUID,
    //     ids: Optional[IDs] = None,
    //     where: Optional[Where] = None,
    //     sort: Optional[str] = None,
    //     limit: Optional[int] = None,
    //     offset: Optional[int] = None,
    //     page: Optional[int] = None,
    //     page_size: Optional[int] = None,
    //     where_document: Optional[WhereDocument] = None,
    //     include: Include = ["metadatas", "documents"],  # type: ignore[list-item]
    //     tenant: str = DEFAULT_TENANT,
    //     database: str = DEFAULT_DATABASE,
    // ) -> GetResult:
    //     return self.proxy_segment_api._get(  # type: ignore[no-any-return]
    //         collection_id,
    //         ids,
    //         where,
    //         sort,
    //         limit,
    //         offset,
    //         page,
    //         page_size,
    //         where_document,
    //         include,
    //         tenant,
    //         database,
    //     )
}

///////////////////// Data Transformation Functions /////////////////

/// Converts a Vec<PyReadonlyArray1<f32>> to a Vec<Vec<f32>>
/// # Note
/// - We cannot impl TryFrom etc because we don't own the types or the trait
fn py_embeddings_to_vec_f32(embeddings: Vec<PyReadonlyArray1<f32>>) -> PyResult<Vec<Vec<f32>>> {
    let mut embeddings_vec = Vec::with_capacity(embeddings.len());
    for embedding in embeddings {
        // We have to copy the data from the PyReadonlyArray1 to a Vec<f32>
        // due to how the incoming python data is owned by the caller
        // We can't assume we can take ownership of the data
        // There are clever ways to avoid this copy, but they are not worth the complexity
        // at this time
        let e_minor = match embedding.as_slice() {
            Ok(e) => e,
            Err(e) => return Err(e.into()),
        };
        let as_vec = e_minor.to_vec();
        embeddings_vec.push(as_vec);
    }
    Ok(embeddings_vec)
}
