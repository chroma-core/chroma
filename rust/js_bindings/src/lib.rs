#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

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
use chroma_types::{Collection, CollectionMetadataUpdate, IncludeList, KnnIndex, Metadata, RawWhereFields, UpdateCollectionRequest, UpdateMetadata, UpdateMetadataValue};

const DEFAULT_DATABASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

#[napi]
pub fn cli(args: Option<Vec<String>>) -> napi::Result<()> {
    let args = args.unwrap_or_else(|| std::env::args().collect());
    let args = if args.is_empty() {
        vec!["chroma".to_string()]
    } else {
        args
    };
    chroma_cli(args);
    Ok(())
}

#[napi(object)]
pub struct JsBindingsConfig {
    pub persist_path: String,
    pub allow_reset: Option<bool>,
}

#[napi(object)]
pub struct JsCollection {
    pub id: String,
    pub name: String,
    pub tenant: String,
    pub database: String,
}

impl From<Collection> for JsCollection {
    fn from(c: Collection) -> Self {
        JsCollection {
            id: c.collection_id.0.to_string(),
            name: c.name,
            tenant: c.tenant,
            database: c.database,
        }
    }
}

// Use JSON strings for complex nested types since NAPI doesn't support Option<Vec<Option<_>>>
#[napi(object)]
pub struct JsGetResponse {
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f64>>>,
    pub documents: Option<String>,        // JSON array string of nullable strings
    pub metadatas: Option<String>,        // JSON array string
}

#[napi(object)]
pub struct JsQueryResponse {
    pub ids: Vec<Vec<String>>,
    pub embeddings: Option<String>,       // JSON nested array string
    pub documents: Option<String>,        // JSON nested array string
    pub metadatas: Option<String>,        // JSON nested array string
    pub distances: Option<Vec<Vec<f64>>>,
}

fn metadata_to_json(m: Option<Metadata>) -> serde_json::Value {
    match m {
        None => serde_json::Value::Null,
        Some(metadata) => {
            let map: serde_json::Map<String, serde_json::Value> = metadata
                .into_iter()
                .map(|(k, v)| {
                    let json_val = match v {
                        chroma_types::MetadataValue::Str(s) => serde_json::Value::String(s),
                        chroma_types::MetadataValue::Int(i) => serde_json::Value::Number(i.into()),
                        chroma_types::MetadataValue::Float(f) => {
                            serde_json::Number::from_f64(f).map_or(serde_json::Value::Null, |n| {
                                serde_json::Value::Number(n)
                            })
                        }
                        chroma_types::MetadataValue::Bool(b) => serde_json::Value::Bool(b),
                        chroma_types::MetadataValue::SparseVector(_) => serde_json::Value::Null, // Skip sparse vectors for now
                    };
                    (k, json_val)
                })
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

fn json_to_metadata(v: &serde_json::Value) -> Option<Metadata> {
    match v {
        serde_json::Value::Object(map) => {
            let metadata: Metadata = map
                .iter()
                .filter_map(|(k, v)| {
                    let mv = match v {
                        serde_json::Value::String(s) => {
                            Some(chroma_types::MetadataValue::Str(s.clone()))
                        }
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                Some(chroma_types::MetadataValue::Int(i))
                            } else if let Some(f) = n.as_f64() {
                                Some(chroma_types::MetadataValue::Float(f))
                            } else {
                                None
                            }
                        }
                        serde_json::Value::Bool(b) => Some(chroma_types::MetadataValue::Bool(*b)),
                        _ => None,
                    };
                    mv.map(|v| (k.clone(), v))
                })
                .collect();
            Some(metadata)
        }
        serde_json::Value::Null => None,
        _ => None,
    }
}

fn json_to_update_metadata(v: &serde_json::Value) -> Option<UpdateMetadata> {
    match v {
        serde_json::Value::Object(map) => {
            let metadata: UpdateMetadata = map
                .iter()
                .map(|(k, v)| {
                    let mv = match v {
                        serde_json::Value::String(s) => UpdateMetadataValue::Str(s.clone()),
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                UpdateMetadataValue::Int(i)
                            } else if let Some(f) = n.as_f64() {
                                UpdateMetadataValue::Float(f)
                            } else {
                                UpdateMetadataValue::None
                            }
                        }
                        serde_json::Value::Bool(b) => UpdateMetadataValue::Bool(*b),
                        serde_json::Value::Null => UpdateMetadataValue::None,
                        _ => UpdateMetadataValue::None,
                    };
                    (k.clone(), mv)
                })
                .collect();
            Some(metadata)
        }
        serde_json::Value::Null => None,
        _ => None,
    }
}

#[napi]
pub struct Bindings {
    runtime: tokio::runtime::Runtime,
    frontend: Frontend,
}

#[napi]
impl Bindings {
    #[napi(constructor)]
    pub fn new(config: JsBindingsConfig) -> napi::Result<Self> {
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            napi::Error::from_reason(format!("Failed to create tokio runtime: {}", e))
        })?;

        let _guard = runtime.enter();
        let system = System::new();
        let registry = Registry::new();

        // Configure cache
        let cache_config = FoyerCacheConfig {
            capacity: 1000,
            ..Default::default()
        };
        let cache_config = chroma_cache::CacheConfig::Memory(cache_config);

        // Segment manager config
        let segment_manager_config = LocalSegmentManagerConfig {
            hnsw_index_pool_cache_config: cache_config,
            persist_path: Some(config.persist_path.clone()),
        };

        // SysDB config
        let sysdb_config = SysDbConfig::Sqlite(SqliteSysDbConfig {
            log_topic_namespace: "default".to_string(),
            log_tenant: "default".to_string(),
        });

        // Log config
        let log_config = LogConfig::Sqlite(SqliteLogConfig {
            tenant_id: "default".to_string(),
            topic_namespace: "default".to_string(),
        });

        // Collection cache config
        let collection_cache_config = CollectionsWithSegmentsProviderConfig {
            cache_invalidation_retry_policy: CacheInvalidationRetryConfig::new(0, 0),
            permitted_parallelism: 32,
            cache: chroma_cache::CacheConfig::Nop,
            cache_ttl_secs: 60,
        };

        // Executor config
        let executor_config = ExecutorConfig::Local(LocalExecutorConfig {});

        // SQLite config
        let sqlite_config = SqliteDBConfig {
            url: Some(format!("{}/chroma.sqlite3", config.persist_path)),
            ..Default::default()
        };

        // Frontend config
        let frontend_config = FrontendConfig {
            allow_reset: config.allow_reset.unwrap_or(false),
            segment_manager: Some(segment_manager_config),
            sqlitedb: Some(sqlite_config),
            sysdb: sysdb_config,
            mcmr_sysdb: None,
            collections_with_segments_provider: collection_cache_config,
            log: log_config,
            executor: executor_config,
            default_knn_index: KnnIndex::Hnsw,
            tenants_to_migrate_immediately: vec![],
            tenants_to_migrate_immediately_threshold: None,
            enable_schema: true,
            min_records_for_invocation: default_min_records_for_invocation(),
        };

        let frontend = runtime
            .block_on(async {
                Frontend::try_from_config(&(frontend_config, system), &registry).await
            })
            .map_err(|e| napi::Error::from_reason(format!("Failed to create frontend: {}", e)))?;

        Ok(Bindings { runtime, frontend })
    }

    #[napi]
    pub fn heartbeat(&self) -> napi::Result<f64> {
        let duration = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| napi::Error::from_reason(format!("Time error: {}", e)))?;
        Ok(duration.as_nanos() as f64)
    }

    // ======================== Collection Operations ========================

    #[napi]
    pub fn create_collection(
        &self,
        name: String,
        tenant: Option<String>,
        database: Option<String>,
    ) -> napi::Result<JsCollection> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let request = chroma_types::CreateCollectionRequest::try_new(
            tenant, database, name, None, None, None, false,
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        let collection = self
            .runtime
            .block_on(async { frontend.create_collection(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to create collection: {}", e)))?;

        Ok(collection.into())
    }

    #[napi]
    pub fn get_or_create_collection(
        &self,
        name: String,
        tenant: Option<String>,
        database: Option<String>,
    ) -> napi::Result<JsCollection> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let request = chroma_types::CreateCollectionRequest::try_new(
            tenant, database, name, None, None, None, true,
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        let collection = self
            .runtime
            .block_on(async { frontend.create_collection(request).await })
            .map_err(|e| {
                napi::Error::from_reason(format!("Failed to get_or_create collection: {}", e))
            })?;

        Ok(collection.into())
    }

    #[napi]
    pub fn get_collection(
        &self,
        name: String,
        tenant: Option<String>,
        database: Option<String>,
    ) -> napi::Result<JsCollection> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let request = chroma_types::GetCollectionRequest::try_new(tenant, database, name)
            .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        let collection = self
            .runtime
            .block_on(async { frontend.get_collection(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to get collection: {}", e)))?;

        Ok(collection.into())
    }

    #[napi]
    pub fn delete_collection(
        &self,
        name: String,
        tenant: Option<String>,
        database: Option<String>,
    ) -> napi::Result<()> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let request = chroma_types::DeleteCollectionRequest::try_new(tenant, database, name)
            .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.delete_collection(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to delete collection: {}", e)))?;

        Ok(())
    }

    #[napi]
    pub fn update_collection(
        &self,
        collection_id: String,
        new_name: Option<String>,
        new_metadata_json: Option<String>,
    ) -> napi::Result<()> {
        let collection_uuid = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| napi::Error::from_reason(format!("Invalid collection_id: {}", e)))?,
        );

        // Parse metadata from JSON
        let new_metadata: Option<CollectionMetadataUpdate> = new_metadata_json
            .map(|json| {
                let m = json_to_update_metadata(&serde_json::from_str(&json).unwrap_or_default());
                m.map(CollectionMetadataUpdate::UpdateMetadata)
            })
            .flatten();

        let request = UpdateCollectionRequest::try_new(
            collection_uuid,
            new_name,
            new_metadata,
            None, // configuration
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.update_collection(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to update collection: {}", e)))?;

        Ok(())
    }

    #[napi]
    pub fn list_collections(
        &self,
        tenant: Option<String>,
        database: Option<String>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> napi::Result<Vec<JsCollection>> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let request = chroma_types::ListCollectionsRequest::try_new(tenant, database, limit, offset.unwrap_or(0))
            .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        let collections = self
            .runtime
            .block_on(async { frontend.list_collections(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to list collections: {}", e)))?;

        Ok(collections.into_iter().map(|c| c.into()).collect())
    }

    // ======================== Record Operations ========================

    #[napi]
    pub fn add(
        &self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Vec<Vec<f64>>,
        metadatas_json: Option<String>, // JSON array of metadata objects
        documents_json: Option<String>, // JSON array of strings
        tenant: Option<String>,
        database: Option<String>,
    ) -> napi::Result<bool> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let collection_uuid = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| napi::Error::from_reason(format!("Invalid collection_id: {}", e)))?,
        );

        // Convert f64 to f32
        let embeddings_f32: Vec<Vec<f32>> = embeddings
            .into_iter()
            .map(|v| v.into_iter().map(|f| f as f32).collect())
            .collect();

        // Parse documents from JSON
        let documents: Option<Vec<Option<String>>> = documents_json.map(|json| {
            serde_json::from_str::<Vec<Option<String>>>(&json).unwrap_or_default()
        });

        // Parse metadata from JSON
        let metadatas: Option<Vec<Option<Metadata>>> = metadatas_json.map(|json| {
            let arr: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap_or_default();
            arr.iter().map(json_to_metadata).collect()
        });

        let request = chroma_types::AddCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_uuid,
            ids,
            embeddings_f32,
            documents,
            None, // uris
            metadatas,
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.add(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to add records: {}", e)))?;

        Ok(true)
    }

    #[napi]
    pub fn upsert(
        &self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Vec<Vec<f64>>,
        metadatas_json: Option<String>,
        documents_json: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> napi::Result<bool> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let collection_uuid = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| napi::Error::from_reason(format!("Invalid collection_id: {}", e)))?,
        );

        // Convert f64 to f32
        let embeddings_f32: Vec<Vec<f32>> = embeddings
            .into_iter()
            .map(|v| v.into_iter().map(|f| f as f32).collect())
            .collect();

        // Parse documents from JSON
        let documents: Option<Vec<Option<String>>> = documents_json.map(|json| {
            serde_json::from_str::<Vec<Option<String>>>(&json).unwrap_or_default()
        });

        // Parse metadata from JSON - for upsert we use UpdateMetadata
        let metadatas: Option<Vec<Option<chroma_types::UpdateMetadata>>> = metadatas_json.map(|json| {
            let arr: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap_or_default();
            arr.iter().map(|v| json_to_update_metadata(v)).collect()
        });

        let request = chroma_types::UpsertCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_uuid,
            ids,
            embeddings_f32,
            documents,
            None, // uris
            metadatas,
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.upsert(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to upsert records: {}", e)))?;

        Ok(true)
    }

    #[napi]
    pub fn update(
        &self,
        collection_id: String,
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f64>>>,
        metadatas_json: Option<String>,
        documents_json: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> napi::Result<bool> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let collection_uuid = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| napi::Error::from_reason(format!("Invalid collection_id: {}", e)))?,
        );

        // Convert f64 to f32 - embeddings are optional for update
        let embeddings_f32: Option<Vec<Option<Vec<f32>>>> = embeddings.map(|embs| {
            embs.into_iter()
                .map(|v| Some(v.into_iter().map(|f| f as f32).collect()))
                .collect()
        });

        // Parse documents from JSON
        let documents: Option<Vec<Option<String>>> = documents_json.map(|json| {
            serde_json::from_str::<Vec<Option<String>>>(&json).unwrap_or_default()
        });

        // Parse metadata from JSON
        let metadatas: Option<Vec<Option<chroma_types::UpdateMetadata>>> = metadatas_json.map(|json| {
            let arr: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap_or_default();
            arr.iter().map(|v| json_to_update_metadata(v)).collect()
        });

        let request = chroma_types::UpdateCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_uuid,
            ids,
            embeddings_f32,
            documents,
            None, // uris
            metadatas,
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.update(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to update records: {}", e)))?;

        Ok(true)
    }

    #[napi]
    pub fn query(
        &self,
        collection_id: String,
        query_embeddings: Vec<Vec<f64>>,
        n_results: u32,
        tenant: Option<String>,
        database: Option<String>,
        include: Option<Vec<String>>,
        ids: Option<Vec<String>>,
        where_json: Option<String>,
        where_document_json: Option<String>,
    ) -> napi::Result<JsQueryResponse> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());
        let include = include.unwrap_or_else(|| {
            vec![
                "metadatas".to_string(),
                "documents".to_string(),
                "distances".to_string(),
            ]
        });

        let collection_uuid = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| napi::Error::from_reason(format!("Invalid collection_id: {}", e)))?,
        );

        // Parse where filters
        let where_clause = RawWhereFields::from_json_str(
            where_json.as_deref(),
            where_document_json.as_deref(),
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid where clause: {}", e)))?
        .parse()
        .map_err(|e| napi::Error::from_reason(format!("Invalid where clause: {}", e)))?;

        // Convert f64 to f32
        let query_embeddings_f32: Vec<Vec<f32>> = query_embeddings
            .into_iter()
            .map(|v| v.into_iter().map(|f| f as f32).collect())
            .collect();

        let include_list = IncludeList::try_from(include)
            .map_err(|e| napi::Error::from_reason(format!("Invalid include: {}", e)))?;

        let request = chroma_types::QueryRequest::try_new(
            tenant,
            database,
            collection_uuid,
            ids,
            where_clause,
            query_embeddings_f32,
            n_results,
            include_list,
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        let response = self
            .runtime
            .block_on(async { frontend.query(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to query: {}", e)))?;

        // Convert distances - flatten Option<Option<f32>> to f64
        let distances: Option<Vec<Vec<f64>>> = response.distances.map(|ds| {
            ds.into_iter()
                .map(|inner| {
                    inner
                        .into_iter()
                        .map(|opt| opt.unwrap_or(f32::MAX) as f64)
                        .collect()
                })
                .collect()
        });

        // Serialize complex nested types to JSON strings
        let embeddings_json = response.embeddings.map(|embs| {
            serde_json::to_string(&embs).unwrap_or_else(|_| "[]".to_string())
        });

        let documents_json = response.documents.map(|docs| {
            serde_json::to_string(&docs).unwrap_or_else(|_| "[]".to_string())
        });

        let metadatas_json = response.metadatas.map(|ms| {
            let converted: Vec<Vec<serde_json::Value>> = ms
                .into_iter()
                .map(|inner| inner.into_iter().map(metadata_to_json).collect())
                .collect();
            serde_json::to_string(&converted).unwrap_or_else(|_| "[]".to_string())
        });

        Ok(JsQueryResponse {
            ids: response.ids,
            embeddings: embeddings_json,
            documents: documents_json,
            metadatas: metadatas_json,
            distances,
        })
    }

    #[napi]
    pub fn get(
        &self,
        collection_id: String,
        ids: Option<Vec<String>>,
        limit: Option<u32>,
        offset: Option<u32>,
        tenant: Option<String>,
        database: Option<String>,
        include: Option<Vec<String>>,
        where_json: Option<String>,
        where_document_json: Option<String>,
    ) -> napi::Result<JsGetResponse> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());
        let include =
            include.unwrap_or_else(|| vec!["metadatas".to_string(), "documents".to_string()]);
        let offset = offset.unwrap_or(0);

        let collection_uuid = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| napi::Error::from_reason(format!("Invalid collection_id: {}", e)))?,
        );

        // Parse where filters
        let where_clause = RawWhereFields::from_json_str(
            where_json.as_deref(),
            where_document_json.as_deref(),
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid where clause: {}", e)))?
        .parse()
        .map_err(|e| napi::Error::from_reason(format!("Invalid where clause: {}", e)))?;

        let include_list = IncludeList::try_from(include)
            .map_err(|e| napi::Error::from_reason(format!("Invalid include: {}", e)))?;

        let request = chroma_types::GetRequest::try_new(
            tenant,
            database,
            collection_uuid,
            ids,
            where_clause,
            limit,
            offset,
            include_list,
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        let response = self
            .runtime
            .block_on(async { Box::pin(frontend.get(request)).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to get: {}", e)))?;

        let embeddings: Option<Vec<Vec<f64>>> = response.embeddings.map(|embs| {
            embs.into_iter()
                .map(|v| v.into_iter().map(|f| f as f64).collect())
                .collect()
        });

        let documents_json = response.documents.map(|docs| {
            serde_json::to_string(&docs).unwrap_or_else(|_| "[]".to_string())
        });

        let metadatas_json = response.metadatas.map(|ms| {
            let converted: Vec<serde_json::Value> =
                ms.into_iter().map(metadata_to_json).collect();
            serde_json::to_string(&converted).unwrap_or_else(|_| "[]".to_string())
        });

        Ok(JsGetResponse {
            ids: response.ids,
            embeddings,
            documents: documents_json,
            metadatas: metadatas_json,
        })
    }

    #[napi]
    pub fn count(
        &self,
        collection_id: String,
        tenant: Option<String>,
        database: Option<String>,
    ) -> napi::Result<u32> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let collection_uuid = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| napi::Error::from_reason(format!("Invalid collection_id: {}", e)))?,
        );

        let request = chroma_types::CountRequest::try_new(tenant, database, collection_uuid)
            .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        let count = self
            .runtime
            .block_on(async { frontend.count(request).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to count: {}", e)))?;

        Ok(count)
    }

    #[napi]
    pub fn delete_records(
        &self,
        collection_id: String,
        ids: Option<Vec<String>>,
        tenant: Option<String>,
        database: Option<String>,
        where_json: Option<String>,
        where_document_json: Option<String>,
    ) -> napi::Result<()> {
        let tenant = tenant.unwrap_or_else(|| DEFAULT_TENANT.to_string());
        let database = database.unwrap_or_else(|| DEFAULT_DATABASE.to_string());

        let collection_uuid = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&collection_id)
                .map_err(|e| napi::Error::from_reason(format!("Invalid collection_id: {}", e)))?,
        );

        // Parse where filters
        let where_clause = RawWhereFields::from_json_str(
            where_json.as_deref(),
            where_document_json.as_deref(),
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid where clause: {}", e)))?
        .parse()
        .map_err(|e| napi::Error::from_reason(format!("Invalid where clause: {}", e)))?;

        let request = chroma_types::DeleteCollectionRecordsRequest::try_new(
            tenant,
            database,
            collection_uuid,
            ids,
            where_clause,
        )
        .map_err(|e| napi::Error::from_reason(format!("Invalid request: {}", e)))?;

        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { Box::pin(frontend.delete(request)).await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to delete: {}", e)))?;

        Ok(())
    }

    #[napi]
    pub fn reset(&self) -> napi::Result<bool> {
        let mut frontend = self.frontend.clone();
        self.runtime
            .block_on(async { frontend.reset().await })
            .map_err(|e| napi::Error::from_reason(format!("Failed to reset: {}", e)))?;
        Ok(true)
    }
}
