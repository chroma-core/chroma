use crate::errors::{ChromaPyResult, WrappedPyErr, WrappedSerdeJsonError, WrappedUuidError};
use chroma_cache::FoyerCacheConfig;
use chroma_config::Configurable;
use chroma_frontend::{
    executor::{local::LocalExecutor, Executor},
    frontend::Frontend,
    get_collection_with_segments_provider::{
        CacheInvalidationRetryConfig, CollectionsWithSegmentsProvider,
        CollectionsWithSegmentsProviderConfig,
    },
};
use chroma_log::{LocalCompactionManager, Log};
use chroma_segment::local_segment_manager::{LocalSegmentManager, LocalSegmentManagerConfig};
use chroma_sqlite::{config::SqliteDBConfig, db::SqliteDb};
use chroma_sysdb::{sqlite::SqliteSysDb, sysdb::SysDb};
use chroma_system::System;
use chroma_types::{
    Collection, CreateCollectionRequest, CreateDatabaseRequest, CreateTenantRequest, Database,
    DeleteDatabaseRequest, GetDatabaseRequest, GetResponse, GetTenantRequest, GetTenantResponse,
    HeartbeatError, IncludeList, ListDatabasesRequest, Metadata, QueryResponse, UpdateMetadata,
};
use numpy::PyReadonlyArray1;
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python};
use std::time::SystemTime;

const DEFAULT_DATABASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

#[pyclass]
pub(crate) struct Bindings {
    runtime: tokio::runtime::Runtime,
    frontend: Frontend,
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
        sqlite_db_config: SqliteDBConfig,
        persist_path: String,
        hnsw_cache_size: usize,
    ) -> ChromaPyResult<Self> {
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
            runtime.block_on(async { SqliteDb::try_from_config(&sqlite_db_config).await })?;

        let cache_config = FoyerCacheConfig {
            capacity: hnsw_cache_size,
            ..Default::default()
        };

        let cache_config = chroma_cache::CacheConfig::Memory(cache_config);
        let segment_manager_config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: cache_config,
            persist_path: persist_path.clone(),
        };
        // Create the hnsw segment manager.
        let segment_manager = runtime.block_on(LocalSegmentManager::try_from_config(&(
            segment_manager_config,
            sqlite_db.clone(),
        )))?;
        let sqlite_sysdb = SqliteSysDb::new(sqlite_db.clone());
        let sysdb = Box::new(SysDb::Sqlite(sqlite_sysdb));
        // TODO: get the log configuration from the config sysdb
        let mut log = Box::new(Log::Sqlite(chroma_log::sqlite_log::SqliteLog::new(
            sqlite_db.clone(),
            "default".to_string(),
            "default".to_string(),
        )));

        // Spawn the compaction manager.
        let handle = system.start_component(LocalCompactionManager::new(
            log.clone(),
            sqlite_db.clone(),
            segment_manager.clone(),
            sysdb.clone(),
        ));
        if let Log::Sqlite(sqlite_log) = log.as_ref() {
            sqlite_log.init_compactor_handle(handle.clone())?;
        }

        // TODO: clean up the cache configuration and decide the source of truth owner
        // make cache not a no-op
        let collection_cache_config = CollectionsWithSegmentsProviderConfig {
            // No retry to sysdb on local chroma
            cache_invalidation_retry_policy: CacheInvalidationRetryConfig::new(0, 0),
            permitted_parallelism: 32,
            cache: chroma_cache::CacheConfig::Nop,
        };

        let collections_cache = runtime.block_on(async {
            CollectionsWithSegmentsProvider::try_from_config(&(
                collection_cache_config,
                sysdb.clone(),
            ))
            .await
        })?;

        // TODO: executor should NOT be exposed to the bindings module. try_from_config should work.
        // The reason this works this way right now is because try_from_config cannot share the sqlite_db
        // across the downstream components.
        let executor = Executor::Local(LocalExecutor::new(
            segment_manager,
            sqlite_db,
            handle.clone(),
        ));
        let max_batch_size = match runtime.block_on(log.get_max_batch_size()) {
            Ok(max_batch_size) => max_batch_size,
            Err(e) => {
                return Err(PyOSError::new_err(format!(
                    "Failed to get max batch size: {}",
                    e
                )))
            }
        };
        let frontend = Frontend::new(
            false,
            sysdb.clone(),
            collections_cache,
            log,
            executor,
            max_batch_size,
        );

        Ok(Bindings {
            runtime,
            frontend,
            _compaction_manager_handle: handle,
        })
    }

    /// Returns the current eopch time in ns
    /// TODO(hammadb): This should proxy to ServerAPI
    #[allow(dead_code)]
    fn heartbeat(&self) -> ChromaPyResult<u128> {
        let duration_since_epoch = std::time::SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(HeartbeatError::CouldNotGetTime)?;
        Ok(duration_since_epoch.as_nanos())
    }

    #[allow(dead_code)]
    fn get_max_batch_size(&self) -> u32 {
        self._frontend.clone().get_max_batch_size()
    }

    // TODO(hammadb): Determine our pattern for optional arguments in python
    // options include using Option or passing defaults from python
    // or using pyargs annotations such as
    // #[pyargs(limit = "None", offset = "None")]

    ////////////////////////////// Admin API //////////////////////////////

    fn create_database(&self, name: String, tenant: String, _py: Python<'_>) -> ChromaPyResult<()> {
        let request = CreateDatabaseRequest::try_new(tenant, name)?;
        let mut frontend = self.frontend.clone();

        self.runtime
            .block_on(async { frontend.create_database(request).await })?;

        Ok(())
    }

    fn get_database(
        &self,
        name: String,
        tenant: String,
        _py: Python<'_>,
    ) -> ChromaPyResult<Database> {
        let request = GetDatabaseRequest::try_new(tenant, name)?;

        let mut frontend = self.frontend.clone();
        let database = self
            .runtime
            .block_on(async { frontend.get_database(request).await })?;

        Ok(database)
    }

    fn delete_database(&self, name: String, tenant: String) -> ChromaPyResult<()> {
        let request = DeleteDatabaseRequest::try_new(tenant, name)?;
        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.delete_database(request).await })?;

        Ok(())
    }

    #[pyo3(signature = (limit = None, offset = None, tenant = "DEFAULT_TENANT".to_string()))]
    fn list_databases(
        &self,
        limit: Option<u32>,
        offset: Option<u32>,
        tenant: String,
    ) -> ChromaPyResult<Vec<Database>> {
        let request = ListDatabasesRequest::try_new(tenant, limit, offset.unwrap_or(0))?;
        let mut frontend = self.frontend.clone();

        let databases = self
            .runtime
            .block_on(async { frontend.list_databases(request).await })?;
        Ok(databases)
    }

    fn create_tenant(&self, name: String) -> ChromaPyResult<()> {
        let request = CreateTenantRequest::try_new(name)?;
        let mut frontend = self.frontend.clone();

        self.runtime
            .block_on(async { frontend.create_tenant(request).await })?;
        Ok(())
    }

    fn get_tenant(&self, name: String) -> ChromaPyResult<GetTenantResponse> {
        let request = GetTenantRequest::try_new(name)?;
        let mut frontend = self.frontend.clone();

        let tenant = self
            .runtime
            .block_on(async { frontend.get_tenant(request).await })?;
        Ok(tenant)
    }

    ////////////////////////////// Base API //////////////////////////////
    #[allow(clippy::too_many_arguments)]
    #[pyo3(
        signature = (name, configuration, metadata = None, get_or_create = false, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    fn create_collection(
        &self,
        name: String,
        configuration: Option<PyObject>,
        metadata: Option<Metadata>,
        get_or_create: bool,
        tenant: String,
        database: String,
        py: Python<'_>,
    ) -> ChromaPyResult<Collection> {
        let configuration_json = match configuration {
            Some(configuration) => {
                let configuration_json_str = configuration
                    .call_method0(py, "to_json_str")
                    .map_err(WrappedPyErr)?
                    .extract::<String>(py)
                    .map_err(WrappedPyErr)?;

                Some(
                    serde_json::from_str::<serde_json::Value>(&configuration_json_str)
                        .map_err(WrappedSerdeJsonError)?,
                )
            }
            None => None,
        };

        let request = CreateCollectionRequest::try_new(
            tenant,
            database,
            name,
            metadata,
            configuration_json,
            get_or_create,
        )?;

        let mut frontend = self.frontend.clone();
        let collection = self
            .runtime
            .block_on(async { frontend.create_collection(request).await })?;

        Ok(collection)
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
        embeddings: Vec<Vec<f32>>,
        metadatas: Option<Vec<Option<Metadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<bool> {
        // TODO: move validate embeddings into this conversion
        // let embeddings = py_embeddings_to_vec_f32(embeddings).map_err(WrappedPyErr)?;
        let mut frontend_clone = self._frontend.clone();

        if self.get_max_batch_size() < ids.len() as u32 {
            return Err(PyValueError::new_err(format!(
                "Batch size of {} is greater than max batch size of {}",
                ids.len(),
                self.get_max_batch_size()
            )));
        }

        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        let mut frontend_clone = self.frontend.clone();
        self.runtime.block_on(async {
            frontend_clone
                .validate_embedding(collection_id, Some(&embeddings), true, |embedding| {
                    Some(embedding.len())
                })
                .await
        })?;

        let req = chroma_types::AddCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            Some(embeddings),
            documents,
            uris,
            metadatas,
        )?;

        let mut frontend_clone = self.frontend.clone();
        self.runtime
            .block_on(async { frontend_clone.add(req).await })?;
        Ok(true)
    }

    #[pyo3(
        signature = (collection_id, ids, embeddings = None, metadatas = None, documents = None, uris = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn update(
        &self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Option<Vec<PyReadonlyArray1<f32>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<bool> {
        let mut frontend_clone = self.frontend.clone();

        let embeddings = match embeddings {
            Some(embeddings) => {
                py_embeddings_to_opt_vec_f32(Some(embeddings)).map_err(WrappedPyErr)?
            }
            None => None,
        };

        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        self.runtime.block_on(async {
            frontend_clone
                .validate_embedding(collection_id, embeddings.as_ref(), false, |e| {
                    e.as_ref().map(|e| e.len())
                })
                .await
        })?;

        let req = chroma_types::UpdateCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;

        self.runtime
            .block_on(async { frontend_clone.update(req).await })?;

        Ok(true)
    }

    #[pyo3(
            signature = (collection_id, ids = None, r#where = None, limit = None, offset = 0, where_document = None, include = ["metadatas".to_string(), "documents".to_string()].to_vec(), tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
        )]
    #[allow(clippy::too_many_arguments)]
    fn get(
        &self,
        collection_id: String,
        ids: Option<Vec<String>>,
        r#where: Option<String>,
        limit: Option<u32>,
        offset: u32,
        where_document: Option<String>,
        include: Vec<String>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<GetResponse> {
        // TODO: Rethink the error handling strategy
        let r#where = chroma_types::RawWhereFields::from_json_str(
            r#where.as_deref(),
            where_document.as_deref(),
        )?
        .parse()?;

        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        let include = IncludeList::try_from(include)?;

        let request = chroma_types::GetRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            r#where,
            limit,
            offset,
            include,
        )?;

        let mut frontend_clone = self.frontend.clone();
        let result = self
            .runtime
            .block_on(async { frontend_clone.get(request).await })?;
        Ok(result)
    }

    #[pyo3(
        signature = (collection_id, query_embeddings, n_results, r#where = None, where_document = None, include = ["metadatas".to_string(), "documents".to_string()].to_vec(), tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn query(
        &self,
        collection_id: String,
        query_embeddings: Vec<Vec<f32>>,
        n_results: u32,
        r#where: Option<String>,
        where_document: Option<String>,
        include: Vec<String>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<QueryResponse> {
        // let query_embeddings = py_embeddings_to_vec_f32(query_embeddings).map_err(WrappedPyErr)?;

        let r#where = chroma_types::RawWhereFields::from_json_str(
            r#where.as_deref(),
            where_document.as_deref(),
        )?
        .parse()?;

        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        let include = IncludeList::try_from(include)?;

        let request = chroma_types::QueryRequest::try_new(
            tenant,
            database,
            collection_id,
            None,
            r#where,
            query_embeddings,
            n_results,
            include,
        )?;

        let mut frontend_clone = self.frontend.clone();
        let response = self
            .runtime
            .block_on(async { frontend_clone.query(request).await })?;
        Ok(response)
    }
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

fn py_embeddings_to_opt_vec_f32(
    embeddings: Option<Vec<PyReadonlyArray1<f32>>>,
) -> PyResult<Option<Vec<Option<Vec<f32>>>>> {
    match embeddings {
        Some(embeddings) => {
            let mut embeddings_vec = Vec::with_capacity(embeddings.len());
            for embedding in embeddings {
                let e_minor = match embedding.as_slice() {
                    Ok(e) => e,
                    Err(e) => return Err(e.into()),
                };
                let as_vec = e_minor.to_vec();
                embeddings_vec.push(Some(as_vec));
            }
            Ok(Some(embeddings_vec))
        }
        None => Ok(None),
    }
}
