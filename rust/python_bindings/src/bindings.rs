use crate::errors::{ChromaPyResult, WrappedPyErr, WrappedUuidError};
use chroma_cache::FoyerCacheConfig;
use chroma_cli::chroma_cli;
use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{
    config::default_min_records_for_invocation,
    executor::config::{ExecutorConfig, LocalExecutorConfig},
    get_collection_with_segments_provider::{
        CacheInvalidationRetryConfig, CollectionsWithSegmentsProviderConfig,
    },
    Frontend, FrontendConfig,
};
use chroma_log::config::{LogConfig, SqliteLogConfig};
use chroma_segment::local_segment_manager::LocalSegmentManagerConfig;
use chroma_sqlite::config::SqliteDBConfig;
use chroma_sysdb::{SqliteSysDbConfig, SysDbConfig};
use chroma_system::System;
use chroma_types::{
    Collection, CollectionConfiguration, CollectionMetadataUpdate, CountCollectionsRequest,
    CountResponse, CreateCollectionRequest, CreateDatabaseRequest, CreateTenantRequest, Database,
    DeleteCollectionRequest, DeleteDatabaseRequest, GetCollectionRequest, GetDatabaseRequest,
    GetResponse, GetTenantRequest, GetTenantResponse, HeartbeatError, IncludeList,
    InternalCollectionConfiguration, InternalUpdateCollectionConfiguration, KnnIndex,
    ListCollectionsRequest, ListDatabasesRequest, Metadata, QueryResponse,
    UpdateCollectionConfiguration, UpdateCollectionRequest, UpdateMetadata, WrappedSerdeJsonError,
};
use pyo3::{exceptions::PyValueError, pyclass, pyfunction, pymethods, types::PyAnyMethods, Python};
use std::time::SystemTime;
const DEFAULT_DATABASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

#[pyclass]
pub(crate) struct Bindings {
    runtime: tokio::runtime::Runtime,
    frontend: Frontend,
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

#[pyfunction]
#[pyo3(signature = (py_args=None))]
#[allow(dead_code)]
pub fn cli(py_args: Option<Vec<String>>) -> ChromaPyResult<()> {
    let args = py_args.unwrap_or_else(|| std::env::args().collect());
    let args = if args.is_empty() {
        vec!["chroma".to_string()]
    } else {
        args
    };
    chroma_cli(args);
    Ok(())
}

//////////////////////// PyMethods Implementation ////////////////////////
#[pymethods]
impl Bindings {
    #[new]
    #[pyo3(signature = (allow_reset, sqlite_db_config, hnsw_cache_size, persist_path=None))]
    pub fn py_new(
        allow_reset: bool,
        sqlite_db_config: SqliteDBConfig,
        hnsw_cache_size: usize,
        persist_path: Option<String>,
    ) -> ChromaPyResult<Self> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let system = System::new();
        let registry = Registry::new();

        //////////////////////////// Frontend Setup ////////////////////////////

        let cache_config = FoyerCacheConfig {
            capacity: hnsw_cache_size,
            ..Default::default()
        };
        let cache_config = chroma_cache::CacheConfig::Memory(cache_config);
        let segment_manager_config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: cache_config,
            persist_path,
        };

        // TODO: consume the log configuration from the input python
        let sysdb_config = SysDbConfig::Sqlite(SqliteSysDbConfig {
            log_topic_namespace: "default".to_string(),
            log_tenant: "default".to_string(),
        });

        let log_config = LogConfig::Sqlite(SqliteLogConfig {
            tenant_id: "default".to_string(),
            topic_namespace: "default".to_string(),
        });

        let collection_cache_config = CollectionsWithSegmentsProviderConfig {
            // No retry to sysdb on local chroma
            cache_invalidation_retry_policy: CacheInvalidationRetryConfig::new(0, 0),
            permitted_parallelism: 32,
            cache: chroma_cache::CacheConfig::Nop,
            cache_ttl_secs: 60,
        };

        let executor_config = ExecutorConfig::Local(LocalExecutorConfig {});

        let knn_index = KnnIndex::Hnsw;
        let enable_schema = true;

        let frontend_config = FrontendConfig {
            allow_reset,
            segment_manager: Some(segment_manager_config),
            sqlitedb: Some(sqlite_db_config),
            sysdb: sysdb_config,
            collections_with_segments_provider: collection_cache_config,
            log: log_config,
            executor: executor_config,
            default_knn_index: knn_index,
            tenants_to_migrate_immediately: vec![],
            tenants_to_migrate_immediately_threshold: None,
            enable_schema,
            min_records_for_invocation: default_min_records_for_invocation(),
        };

        let frontend = runtime.block_on(async {
            Frontend::try_from_config(&(frontend_config, system), &registry).await
        })?;

        Ok(Bindings { runtime, frontend })
    }

    /// Returns the current eopch time in ns
    #[allow(dead_code)]
    fn heartbeat(&self) -> ChromaPyResult<u128> {
        let duration_since_epoch = std::time::SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(HeartbeatError::from)?;
        Ok(duration_since_epoch.as_nanos())
    }

    #[allow(dead_code)]
    fn get_max_batch_size(&self) -> u32 {
        self.frontend.clone().get_max_batch_size()
    }

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
        signature = (name, configuration_json_str = None, schema_str = None, metadata = None, get_or_create = false, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    fn create_collection(
        &self,
        name: String,
        configuration_json_str: Option<String>,
        schema_str: Option<String>,
        metadata: Option<Metadata>,
        get_or_create: bool,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<Collection> {
        let configuration_json = match configuration_json_str {
            Some(configuration_json_str) => {
                let configuration_json = serde_json::from_str(&configuration_json_str)
                    .map_err(WrappedSerdeJsonError::SerdeJsonError)?;

                Some(configuration_json)
            }
            None => None,
        };

        let configuration = match configuration_json {
            Some(c) => Some(InternalCollectionConfiguration::try_from_config(
                c,
                self.frontend.get_default_knn_index(),
                metadata.clone(),
            )?),
            None => Some(InternalCollectionConfiguration::try_from_config(
                CollectionConfiguration {
                    hnsw: None,
                    spann: None,
                    embedding_function: None,
                },
                self.frontend.get_default_knn_index(),
                metadata.clone(),
            )?),
        };

        let schema = match schema_str {
            Some(schema_str) => {
                serde_json::from_str(&schema_str).map_err(WrappedSerdeJsonError::SerdeJsonError)?
            }
            None => None,
        };

        let request = CreateCollectionRequest::try_new(
            tenant,
            database,
            name,
            metadata,
            configuration,
            schema,
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
        signature = (collection_id, new_name = None, new_metadata = None, new_configuration_json_str = None)
    )]
    fn update_collection(
        &self,
        collection_id: String,
        new_name: Option<String>,
        new_metadata: Option<UpdateMetadata>,
        new_configuration_json_str: Option<String>,
    ) -> ChromaPyResult<()> {
        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        let configuration_json = match new_configuration_json_str {
            Some(new_configuration_json_str) => {
                let new_configuration_json = serde_json::from_str::<UpdateCollectionConfiguration>(
                    &new_configuration_json_str,
                )
                .map_err(WrappedSerdeJsonError::SerdeJsonError)?;

                Some(new_configuration_json)
            }
            None => None,
        };

        let configuration = match configuration_json {
            Some(c) => Some(InternalUpdateCollectionConfiguration::try_from(c)?),
            None => None,
        };

        let request = UpdateCollectionRequest::try_new(
            collection_id,
            new_name,
            new_metadata.map(CollectionMetadataUpdate::UpdateMetadata),
            configuration,
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

        let req = chroma_types::AddCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            embeddings,
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
        signature = (collection_id, ids, embeddings, metadatas = None, documents = None, uris = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn upsert(
        &self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<bool> {
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
        py: Python<'_>,
    ) -> ChromaPyResult<GetResponse> {
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
        let result = py.allow_threads(move || {
            self.runtime
                .block_on(async { Box::pin(frontend_clone.get(request)).await })
        })?;
        Ok(result)
    }

    #[pyo3(
        signature = (collection_id, ids, query_embeddings, n_results, r#where = None, where_document = None, include = ["metadatas".to_string(), "documents".to_string()].to_vec(), tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn query(
        &self,
        collection_id: String,
        ids: Option<Vec<String>>,
        query_embeddings: Vec<Vec<f32>>,
        n_results: u32,
        r#where: Option<String>,
        where_document: Option<String>,
        include: Vec<String>,
        tenant: String,
        database: String,
        py: Python<'_>,
    ) -> ChromaPyResult<QueryResponse> {
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
            ids,
            r#where,
            query_embeddings,
            n_results,
            include,
        )?;

        let mut frontend_clone = self.frontend.clone();
        let response = py.allow_threads(move || {
            self.runtime
                .block_on(async { frontend_clone.query(request).await })
        })?;
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
