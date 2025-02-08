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
    Collection, CollectionMetadataUpdate, CountCollectionsRequest, CountResponse,
    CreateCollectionRequest, CreateDatabaseRequest, CreateTenantRequest, Database,
    DeleteCollectionRequest, DeleteDatabaseRequest, GetCollectionRequest, GetDatabaseRequest,
    GetResponse, GetTenantRequest, GetTenantResponse, HeartbeatError, IncludeList,
    ListCollectionsRequest, ListDatabasesRequest, Metadata, QueryResponse, UpdateCollectionRequest,
    UpdateMetadata,
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, types::PyAnyMethods, PyObject, Python};
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
        allow_reset: bool,
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
        let sqlite_sysdb = SqliteSysDb::new(
            sqlite_db.clone(),
            "default".to_string(),
            "default".to_string(),
        );
        let sysdb = Box::new(SysDb::Sqlite(sqlite_sysdb));
        // TODO: get the log configuration from the config sysdb
        let mut log = Box::new(Log::Sqlite(chroma_log::sqlite_log::SqliteLog::new(
            sqlite_db.clone(),
            "default".to_string(),
            "default".to_string(),
        )));
        let max_batch_size = runtime.block_on(log.get_max_batch_size())?;

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
        let frontend = Frontend::new(
            allow_reset,
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
        self.frontend.clone().get_max_batch_size()
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
    fn count_collections(&self, tenant: String, database: String) -> ChromaPyResult<u32> {
        let request = CountCollectionsRequest::try_new(tenant, database)?;
        let mut frontend = self.frontend.clone();
        let count = self
            .runtime
            .block_on(async { frontend.count_collections(request).await })?;
        Ok(count)
    }

    #[pyo3(signature = (limit = None, offset = 0, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string()))]
    fn list_collections(
        &self,
        limit: Option<u32>,
        offset: Option<u32>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<Vec<Collection>> {
        let request =
            ListCollectionsRequest::try_new(tenant, database, limit, offset.unwrap_or(0))?;
        let mut frontend = self.frontend.clone();
        let collections = self
            .runtime
            .block_on(async { frontend.list_collections(request).await })?;
        Ok(collections)
    }

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

    fn get_collection(
        &self,
        name: String,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<Collection> {
        let request = GetCollectionRequest::try_new(tenant, database, name)?;
        let mut frontend = self.frontend.clone();
        let collection = self
            .runtime
            .block_on(async { frontend.get_collection(request).await })?;
        Ok(collection)
    }

    #[pyo3(
        signature = (collection_id, new_name = None, new_metadata = None)
    )]
    fn update_collection(
        &self,
        collection_id: String,
        new_name: Option<String>,
        new_metadata: Option<UpdateMetadata>,
    ) -> ChromaPyResult<()> {
        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        let request = UpdateCollectionRequest::try_new(
            collection_id,
            new_name,
            new_metadata.map(CollectionMetadataUpdate::UpdateMetadata),
        )?;

        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.update_collection(request).await })?;

        Ok(())
    }

    fn delete_collection(
        &self,
        name: String,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<()> {
        let request = DeleteCollectionRequest::try_new(tenant, database, name)?;
        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.delete_collection(request).await })?;
        Ok(())
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
        // TODO: Create proper error type for this
        if self.get_max_batch_size() < ids.len() as u32 {
            return Err(WrappedPyErr::from(PyValueError::new_err(format!(
                "Batch size of {} is greater than max batch size of {}",
                ids.len(),
                self.get_max_batch_size()
            )))
            .into());
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
        embeddings: Option<Vec<Option<Vec<f32>>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<bool> {
        // TODO: Create proper error type for this
        if self.get_max_batch_size() < ids.len() as u32 {
            return Err(WrappedPyErr::from(PyValueError::new_err(format!(
                "Batch size of {} is greater than max batch size of {}",
                ids.len(),
                self.get_max_batch_size()
            )))
            .into());
        }

        let mut frontend_clone = self.frontend.clone();

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
        signature = (collection_id, ids, embeddings = None, metadatas = None, documents = None, uris = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn upsert(
        &self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f32>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<bool> {
        // TODO: Create proper error type for this
        if self.get_max_batch_size() < ids.len() as u32 {
            return Err(WrappedPyErr::from(PyValueError::new_err(format!(
                "Batch size of {} is greater than max batch size of {}",
                ids.len(),
                self.get_max_batch_size()
            )))
            .into());
        }

        let mut frontend_clone = self.frontend.clone();

        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        self.runtime.block_on(async {
            frontend_clone
                .validate_embedding(collection_id, embeddings.as_ref(), false, |e| Some(e.len()))
                .await
        })?;

        let req = chroma_types::UpsertCollectionRecordsRequest::try_new(
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
            .block_on(async { frontend_clone.upsert(req).await })?;
        Ok(true)
    }

    #[pyo3(
        signature = (collection_id, ids = None, r#where = None, where_document = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn delete(
        &self,
        collection_id: String,
        ids: Option<Vec<String>>,
        r#where: Option<String>,
        where_document: Option<String>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<()> {
        // TODO: Rethink the error handling strategy
        let r#where = chroma_types::RawWhereFields::from_json_str(
            r#where.as_deref(),
            where_document.as_deref(),
        )?
        .parse()?;

        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        let request = chroma_types::DeleteCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            r#where,
        )?;

        let mut frontend_clone = self.frontend.clone();
        self.runtime
            .block_on(async { frontend_clone.delete(request).await })?;
        Ok(())
    }

    #[pyo3(
        signature = (collection_id, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    fn count(
        &self,
        collection_id: String,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<CountResponse> {
        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        let request = chroma_types::CountRequest::try_new(tenant, database, collection_id)?;

        let mut frontend_clone = self.frontend.clone();
        let result = self
            .runtime
            .block_on(async { frontend_clone.count(request).await })?;
        Ok(result)
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

    fn reset(&self) -> ChromaPyResult<bool> {
        let mut frontend = self.frontend.clone();
        self.runtime.block_on(async { frontend.reset().await })?;
        Ok(true)
    }

    fn get_version(&self, py: Python<'_>) -> ChromaPyResult<String> {
        let version = py
            .import("chromadb")
            .map_err(WrappedPyErr)?
            .getattr("__version__")
            .map_err(WrappedPyErr)?
            .extract::<String>()
            .map_err(WrappedPyErr)?;
        Ok(version)
    }
}
