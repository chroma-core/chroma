use crate::errors::{ChromaPyResult, InvalidDatabaseNameError, WrappedPyErr, WrappedUuidError};
use chroma_api_types::{OccReadMode, OccReadToken};
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
use chroma_log::{
    config::{LogConfig, SqliteLogConfig},
    LocalCompactionManager,
};
use chroma_segment::local_segment_manager::LocalSegmentManagerConfig;
use chroma_sqlite::config::SqliteDBConfig;
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::{SqliteSysDbConfig, SysDbConfig};
use chroma_system::{ComponentHandle, System};
use chroma_types::{
    Collection, CollectionConfiguration, CollectionMetadataUpdate, ConditionalBufferedWrite,
    CountCollectionsRequest, CountResponse, CreateCollectionRequest, CreateDatabaseRequest,
    CreateTenantRequest, Database, DatabaseName, DeleteCollectionRequest, DeleteDatabaseRequest,
    GetCollectionByIdRequest, GetCollectionRequest, GetDatabaseRequest, GetResponse,
    GetTenantRequest, GetTenantResponse, HeartbeatError, IncludeList,
    InternalCollectionConfiguration, InternalUpdateCollectionConfiguration, KnnIndex,
    ListCollectionsRequest, ListDatabasesRequest, Metadata, QueryResponse,
    UpdateCollectionConfiguration, UpdateCollectionRequest, UpdateMetadata, WrappedSerdeJsonError,
};
use pyo3::{
    exceptions::PyValueError,
    pyclass, pyfunction, pymethods,
    types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods},
    Py, PyRefMut, Python,
};
use std::time::SystemTime;
const DEFAULT_DATABASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

#[pyclass]
pub(crate) struct Bindings {
    runtime: tokio::runtime::Runtime,
    system: System,
    sqlite_db: SqliteDb,
    compactor_handle: ComponentHandle<LocalCompactionManager>,
    frontend: Frontend,
    closed: bool,
}

#[pyclass]
pub struct PythonBindingsConfig {
    #[pyo3(get, set)]
    sqlite_db_config: SqliteDBConfig,
}

#[pyclass]
pub struct ConditionalTransaction {
    state: chroma_types::ConditionalTransactionState,
}

impl ConditionalTransaction {
    #[allow(clippy::too_many_arguments)]
    fn http_get_request(
        collection_id: String,
        ids: Option<Vec<String>>,
        r#where: Option<String>,
        limit: Option<u32>,
        offset: Option<u32>,
        where_document: Option<String>,
        include: Vec<String>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<chroma_types::GetRequest> {
        let r#where = chroma_types::RawWhereFields::from_json_str(
            r#where.as_deref(),
            where_document.as_deref(),
        )?
        .parse()?;
        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );
        let include = IncludeList::try_from(include)?;
        Ok(chroma_types::GetRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            r#where,
            limit,
            offset.unwrap_or(0),
            include,
        )?)
    }

    fn collection_uuid(collection_id: String) -> ChromaPyResult<chroma_types::CollectionUuid> {
        Ok(chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        ))
    }

    fn buffered_write_operation_name(write: &ConditionalBufferedWrite) -> &'static str {
        match write {
            ConditionalBufferedWrite::Add(_) => "add",
            ConditionalBufferedWrite::Update(_) => "update",
            ConditionalBufferedWrite::Upsert(_) => "upsert",
            ConditionalBufferedWrite::Delete(_) => "delete",
        }
    }

    fn write_to_py(py: Python<'_>, write: ConditionalBufferedWrite) -> pyo3::PyResult<Py<PyDict>> {
        let operation = PyDict::new(py);
        let payload = PyDict::new(py);
        operation.set_item("operation", Self::buffered_write_operation_name(&write))?;
        match write {
            ConditionalBufferedWrite::Add(request) => {
                payload.set_item("ids", request.ids)?;
                payload.set_item("embeddings", request.embeddings)?;
                payload.set_item("documents", request.documents)?;
                payload.set_item("uris", request.uris)?;
                payload.set_item("metadatas", request.metadatas)?;
            }
            ConditionalBufferedWrite::Update(request) => {
                payload.set_item("ids", request.ids)?;
                payload.set_item("embeddings", request.embeddings)?;
                payload.set_item("documents", request.documents)?;
                payload.set_item("uris", request.uris)?;
                payload.set_item("metadatas", request.metadatas)?;
            }
            ConditionalBufferedWrite::Upsert(request) => {
                payload.set_item("ids", request.ids)?;
                payload.set_item("embeddings", request.embeddings)?;
                payload.set_item("documents", request.documents)?;
                payload.set_item("uris", request.uris)?;
                payload.set_item("metadatas", request.metadatas)?;
            }
            ConditionalBufferedWrite::Delete(request) => {
                payload.set_item("ids", request.ids)?;
                payload.set_item("where", py.None())?;
                payload.set_item("where_document", py.None())?;
                payload.set_item("limit", py.None())?;
            }
        }
        operation.set_item("payload", payload)?;
        Ok(operation.unbind())
    }
}

#[pyclass]
pub struct ConditionalCommitPayload {
    #[pyo3(get)]
    read_token: Option<u64>,
    #[pyo3(get)]
    read_ids: Vec<String>,
    buffered_writes: Vec<ConditionalBufferedWrite>,
}

#[pymethods]
impl ConditionalCommitPayload {
    #[getter]
    fn operation_names(&self) -> Vec<String> {
        self.buffered_writes
            .iter()
            .map(ConditionalTransaction::buffered_write_operation_name)
            .map(str::to_string)
            .collect()
    }

    #[getter]
    fn record_count(&self) -> usize {
        self.buffered_writes
            .iter()
            .map(|write| write.ids().len())
            .sum()
    }

    fn to_json(&self, py: Python<'_>) -> pyo3::PyResult<Py<PyDict>> {
        let payload = PyDict::new(py);
        payload.set_item("read_token", self.read_token)?;
        payload.set_item("read_ids", self.read_ids.clone())?;
        let operations = PyList::empty(py);
        for write in self.buffered_writes.iter().cloned() {
            operations.append(ConditionalTransaction::write_to_py(py, write)?)?;
        }
        payload.set_item("operations", operations)?;
        Ok(payload.unbind())
    }
}

#[pymethods]
impl ConditionalTransaction {
    #[new]
    pub fn py_new() -> Self {
        Self {
            state: chroma_types::ConditionalTransactionState::new(),
        }
    }

    fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_get(
        &self,
        collection_id: String,
        ids: Option<Vec<String>>,
        r#where: Option<String>,
        limit: Option<u32>,
        offset: Option<u32>,
        where_document: Option<String>,
        include: Vec<String>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<Option<u64>> {
        let request = Self::http_get_request(
            collection_id,
            ids,
            r#where,
            limit,
            offset,
            where_document,
            include,
            tenant,
            database,
        )?;
        let request = self.state.prepare_get_request(request)?;
        Ok(match request.occ_read_mode() {
            OccReadMode::AtToken(read_token) => Some(read_token.log_upper_bound_offset()),
            OccReadMode::Capture => None,
            OccReadMode::None => None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn record_get_response(
        &mut self,
        collection_id: String,
        ids: Option<Vec<String>>,
        r#where: Option<String>,
        limit: Option<u32>,
        offset: Option<u32>,
        where_document: Option<String>,
        include: Vec<String>,
        tenant: String,
        database: String,
        returned_ids: Vec<String>,
        read_token: u64,
    ) -> ChromaPyResult<()> {
        let request = Self::http_get_request(
            collection_id,
            ids,
            r#where,
            limit,
            offset,
            where_document,
            include,
            tenant,
            database,
        )?;
        let request = self.state.prepare_get_request(request)?;
        if let OccReadMode::AtToken(expected_read_token) = request.occ_read_mode() {
            if expected_read_token.log_upper_bound_offset() != read_token {
                return Err(
                    chroma_types::ConditionalTransactionError::ReadTokenMismatch {
                        expected_log_upper_bound_offset: expected_read_token
                            .log_upper_bound_offset(),
                        actual_log_upper_bound_offset: read_token,
                    }
                    .into(),
                );
            }
        }
        let response = GetResponse {
            ids: returned_ids,
            occ_read_token: Some(OccReadToken::try_new(read_token)?),
            ..GetResponse::default()
        };
        self.state.record_get_response(&request, &response)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn buffer_add(
        &mut self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        metadatas: Option<Vec<Option<Metadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<()> {
        let collection_id = Self::collection_uuid(collection_id)?;
        let request = chroma_types::AddCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        self.state.buffer_add(request)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn buffer_update(
        &mut self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Option<Vec<Option<Vec<f32>>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<()> {
        let collection_id = Self::collection_uuid(collection_id)?;
        let request = chroma_types::UpdateCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        self.state.buffer_update(request)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn buffer_upsert(
        &mut self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<()> {
        let collection_id = Self::collection_uuid(collection_id)?;
        let request = chroma_types::UpsertCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        self.state.buffer_upsert(request)?;
        Ok(())
    }

    fn buffer_delete(
        &mut self,
        collection_id: String,
        ids: Vec<String>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<()> {
        let collection_id = Self::collection_uuid(collection_id)?;
        let request = chroma_types::DeleteCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            Some(ids),
            None,
            None,
        )?;
        self.state.buffer_delete(request)?;
        Ok(())
    }

    fn prepare_commit(&mut self) -> ChromaPyResult<Option<ConditionalCommitPayload>> {
        match self.state.prepare_commit()? {
            chroma_types::ConditionalCommitAction::NoOp(_) => Ok(None),
            chroma_types::ConditionalCommitAction::Append(request) => {
                Ok(Some(ConditionalCommitPayload {
                    read_token: request.observed_log_offset.map(|offset| offset as u64),
                    read_ids: request.read_ids,
                    buffered_writes: request.buffered_writes,
                }))
            }
        }
    }

    fn finish_commit(
        &mut self,
        first_inserted_record_offset: Option<i64>,
    ) -> ChromaPyResult<ConditionalCommitResult> {
        let result = self.state.finish_commit(first_inserted_record_offset)?;
        Ok(ConditionalCommitResult {
            first_inserted_record_offset: result.first_inserted_record_offset,
            record_count: result.record_count,
        })
    }
}

#[pyclass]
pub struct ConditionalCommitResult {
    #[pyo3(get)]
    first_inserted_record_offset: Option<i64>,
    #[pyo3(get)]
    record_count: usize,
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
pub fn cli(py: Python<'_>, py_args: Option<Vec<String>>) -> ChromaPyResult<()> {
    let args = py_args.unwrap_or_else(|| std::env::args().collect());
    let args = if args.is_empty() {
        vec!["chroma".to_string()]
    } else {
        args
    };
    py.allow_threads(|| chroma_cli(args));
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
            persist_path: persist_path.clone(),
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
            mcmr_sysdb: None,
            collections_with_segments_provider: collection_cache_config,
            log: log_config,
            executor: executor_config,
            default_knn_index: knn_index,
            tenants_to_migrate_immediately: vec![],
            tenants_to_migrate_immediately_threshold: None,
            enable_schema,
            min_records_for_invocation: default_min_records_for_invocation(),
            tenants_with_quantization_enabled: vec![],
            tenants_with_maxscore_enabled: vec![],
            tenants_with_token_bitmap_fts_enabled: vec![],
            tenants_with_transactions_enabled: vec![],
            enable_log_scouting: false,
            enable_transactions: false,
        };

        let frontend = runtime.block_on(async {
            Frontend::try_from_config(&(frontend_config, system.clone()), &registry).await
        })?;
        let sqlite_db = registry.get::<SqliteDb>()?;
        let compactor_handle = registry.get::<ComponentHandle<LocalCompactionManager>>()?;

        Ok(Bindings {
            runtime,
            system,
            sqlite_db,
            compactor_handle,
            frontend,
            closed: false,
        })
    }

    fn close(&mut self) {
        self.shutdown();
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
        let database_name = DatabaseName::new(name).ok_or_else(|| {
            InvalidDatabaseNameError("database name must be at least 3 characters".to_string())
        })?;
        let request = CreateDatabaseRequest::try_new(tenant, database_name)?;
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
        let database_name = DatabaseName::new(name).ok_or_else(|| {
            InvalidDatabaseNameError("database name must be at least 3 characters".to_string())
        })?;
        let request = GetDatabaseRequest::try_new(tenant, database_name)?;

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
        let database_name =
            DatabaseName::new(database.clone()).ok_or(InvalidDatabaseNameError(database))?;
        let request = CountCollectionsRequest::try_new(tenant, database_name)?;
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
        let database_name =
            DatabaseName::new(database.clone()).ok_or(InvalidDatabaseNameError(database))?;
        let request =
            ListCollectionsRequest::try_new(tenant, database_name, limit, offset.unwrap_or(0))?;
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

        let database_name =
            DatabaseName::new(database.clone()).ok_or(InvalidDatabaseNameError(database))?;
        let request = CreateCollectionRequest::try_new(
            tenant,
            database_name,
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
        let database_name =
            DatabaseName::new(database.clone()).ok_or(InvalidDatabaseNameError(database))?;
        let request = GetCollectionRequest::try_new(tenant, database_name, name)?;
        let mut frontend = self.frontend.clone();
        let collection = self
            .runtime
            .block_on(async { frontend.get_collection(request).await })?;
        Ok(collection)
    }

    #[pyo3(signature = (collection_id, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string()))]
    fn get_collection_by_id(
        &self,
        collection_id: String,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<Collection> {
        let database_name =
            DatabaseName::new(database.clone()).ok_or(InvalidDatabaseNameError(database))?;
        let request = GetCollectionByIdRequest::try_new(collection_id, tenant, database_name)?;
        let mut frontend = self.frontend.clone();
        let collection = self
            .runtime
            .block_on(async { frontend.get_collection_by_id(request).await })?;
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
            None,
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
        signature = (collection_id, ids = None, r#where = None, where_document = None, limit = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn delete(
        &self,
        collection_id: String,
        ids: Option<Vec<String>>,
        r#where: Option<String>,
        where_document: Option<String>,
        limit: Option<u32>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<u32> {
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
            limit,
        )?;

        let mut frontend_clone = self.frontend.clone();
        let response = self
            .runtime
            .block_on(async { Box::pin(frontend_clone.delete(request, String::new())).await })?;
        Ok(response.deleted)
    }

    fn begin_conditional_transaction(&self) -> ChromaPyResult<ConditionalTransaction> {
        self.frontend.ensure_conditional_transactions_supported()?;
        Ok(ConditionalTransaction {
            state: chroma_types::ConditionalTransactionState::new(),
        })
    }

    #[pyo3(
        signature = (transaction, collection_id, ids = None, r#where = None, limit = None, offset = 0, where_document = None, include = ["metadatas".to_string(), "documents".to_string()].to_vec(), tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn conditional_get(
        &self,
        mut transaction: PyRefMut<'_, ConditionalTransaction>,
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
        self.frontend
            .ensure_conditional_transactions_supported_for_tenant(&tenant)?;
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
        let request = transaction.state.prepare_get_request(request)?;
        let request_for_finish = request.clone();

        let mut frontend_clone = self.frontend.clone();
        let result = py.allow_threads(move || {
            self.runtime
                .block_on(async { Box::pin(frontend_clone.get(request)).await })
        })?;
        let result = transaction.state.finish_get(&request_for_finish, result)?;
        Ok(result)
    }

    #[pyo3(
        signature = (transaction, ids, collection_id, embeddings, metadatas = None, documents = None, uris = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn conditional_add(
        &self,
        mut transaction: PyRefMut<'_, ConditionalTransaction>,
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

        transaction.state.buffer_add(req)?;
        Ok(true)
    }

    #[pyo3(
        signature = (transaction, collection_id, ids, embeddings = None, metadatas = None, documents = None, uris = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn conditional_update(
        &self,
        mut transaction: PyRefMut<'_, ConditionalTransaction>,
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

        transaction.state.buffer_update(req)?;
        Ok(true)
    }

    #[pyo3(
        signature = (transaction, collection_id, ids, embeddings, metadatas = None, documents = None, uris = None, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn conditional_upsert(
        &self,
        mut transaction: PyRefMut<'_, ConditionalTransaction>,
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

        transaction.state.buffer_upsert(req)?;
        Ok(true)
    }

    #[pyo3(
        signature = (transaction, collection_id, ids, tenant = DEFAULT_TENANT.to_string(), database = DEFAULT_DATABASE.to_string())
    )]
    fn conditional_delete(
        &self,
        mut transaction: PyRefMut<'_, ConditionalTransaction>,
        collection_id: String,
        ids: Vec<String>,
        tenant: String,
        database: String,
    ) -> ChromaPyResult<bool> {
        let collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id).map_err(WrappedUuidError)?,
        );

        let request = chroma_types::DeleteCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_id,
            Some(ids),
            None,
            None,
        )?;

        transaction.state.buffer_delete(request)?;
        Ok(true)
    }

    fn conditional_commit(
        &self,
        mut transaction: PyRefMut<'_, ConditionalTransaction>,
        py: Python<'_>,
    ) -> ChromaPyResult<ConditionalCommitResult> {
        let mut state = std::mem::take(&mut transaction.state);
        let action = match state.prepare_commit() {
            Ok(action) => action,
            Err(err) => {
                transaction.state = state;
                return Err(err.into());
            }
        };
        let request = match action {
            chroma_types::ConditionalCommitAction::NoOp(result) => {
                transaction.state = state;
                return Ok(ConditionalCommitResult {
                    first_inserted_record_offset: result.first_inserted_record_offset,
                    record_count: result.record_count,
                });
            }
            chroma_types::ConditionalCommitAction::Append(request) => request,
        };

        let mut frontend_clone = self.frontend.clone();
        let result = py.allow_threads(move || {
            self.runtime.block_on(async {
                frontend_clone
                    .conditional_commit(request, String::new())
                    .await
            })
        });
        let result = match result {
            Ok(result) => match state.finish_commit(result.first_inserted_record_offset) {
                Ok(result) => result,
                Err(err) => {
                    transaction.state = state;
                    return Err(err.into());
                }
            },
            Err(err) => {
                transaction.state = state;
                return Err(err.into());
            }
        };
        transaction.state = state;
        Ok(ConditionalCommitResult {
            first_inserted_record_offset: result.first_inserted_record_offset,
            record_count: result.record_count,
        })
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

        let request = chroma_types::CountRequest::try_new(
            tenant,
            database,
            collection_id,
            chroma_types::plan::ReadLevel::default(),
        )?;

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

impl Bindings {
    fn shutdown(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.compactor_handle.stop();

        self.runtime.block_on(async {
            let _ = self.compactor_handle.join().await;
            self.system.stop().await;
            self.system.join().await;
            self.sqlite_db.close().await;
        });
    }
}

impl Drop for Bindings {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sqlite::config::{MigrationHash, MigrationMode};
    use tempfile::TempDir;

    fn new_persistent_bindings() -> (TempDir, Bindings) {
        let temp_dir = tempfile::tempdir().expect("temporary directory");
        let persist_path = temp_dir
            .path()
            .to_str()
            .expect("temporary path should be utf8")
            .to_string();
        let sqlite_path = temp_dir.path().join("chroma.sqlite3");
        let sqlite_url = sqlite_path
            .to_str()
            .expect("sqlite path should be utf8")
            .to_string();
        let sqlite_db_config = SqliteDBConfig {
            url: Some(sqlite_url),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let bindings = Bindings::py_new(true, sqlite_db_config, 16, Some(persist_path))
            .expect("persistent bindings");

        (temp_dir, bindings)
    }

    #[test]
    fn close_closes_sqlite_pool() {
        let (temp_dir, mut bindings) = new_persistent_bindings();

        bindings.close();

        assert!(bindings.closed);
        assert!(bindings.sqlite_db.get_conn().is_closed());
        temp_dir.close().expect("persistent directory cleanup");
    }

    #[test]
    fn close_is_idempotent() {
        let (temp_dir, mut bindings) = new_persistent_bindings();

        bindings.close();
        bindings.close();

        assert!(bindings.closed);
        assert!(bindings.sqlite_db.get_conn().is_closed());
        temp_dir.close().expect("persistent directory cleanup");
    }

    #[test]
    fn drop_closes_sqlite_pool() {
        let (temp_dir, bindings) = new_persistent_bindings();
        let sqlite_db = bindings.sqlite_db.clone();

        drop(bindings);

        assert!(sqlite_db.get_conn().is_closed());
        temp_dir.close().expect("persistent directory cleanup");
    }
}
