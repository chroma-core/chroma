use chroma_config::Configurable;
use chroma_frontend::{
    executor::{config::ExecutorConfig, Executor},
    frontend::Frontend,
    get_collection_with_segments_provider::{
        CacheInvalidationRetryConfig, CollectionsWithSegmentsProvider,
        CollectionsWithSegmentsProviderConfig,
    },
};
use chroma_log::Log;
use chroma_sqlite::{config::SqliteDBConfig, db::SqliteDb};
use chroma_sysdb::{sqlite::SqliteSysDb, sysdb::SysDb};
use chroma_system::System;
use pyo3::{exceptions::PyOSError, pyclass, pymethods, Py, PyAny, PyObject, PyResult, Python};
use std::time::SystemTime;

#[pyclass]
pub(crate) struct Bindings {
    // TODO(sanketkedia, hammadb): Add ServerAPI handle here
    // server_api_handle: ComponentHandle<ServerAPI>,
    _runtime: tokio::runtime::Runtime,
    // TODO(hammadb): In order to make CI green, we proxy all
    // calls back into python.
    // We should slowly start moving the logic from python to rust
    proxy_frontend: Py<PyAny>,
    _frontend: Frontend,
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
    pub fn py_new(proxy_frontend: Py<PyAny>, sqlite_db_config: SqliteDBConfig) -> PyResult<Self> {
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

        let sqlite_sysdb = SqliteSysDb::new(sqlite_db);
        // TODO: verify this clone is safe / consistent
        let sysdb = Box::new(SysDb::Sqlite(sqlite_sysdb));
        let log = Box::new(Log::InMemory(chroma_log::in_memory_log::InMemoryLog::new()));

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
        let executor_config = ExecutorConfig::Local;
        let executor = match runtime
            .block_on(async { Executor::try_from_config(&(executor_config, system)).await })
        {
            Ok(executor) => executor,
            Err(e) => {
                // TODO: error type
                return Err(PyOSError::new_err(format!(
                    "Failed to create executor: {}",
                    e
                )));
            }
        };

        let frontend = Frontend::new(false, sysdb.clone(), collections_cache, log, executor);

        Ok(Bindings {
            proxy_frontend,
            _runtime: runtime,
            _frontend: frontend,
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
}
