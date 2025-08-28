#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

use chroma_cache::FoyerCacheConfig;
use chroma_cli::chroma_cli;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_frontend::{
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
  AddCollectionRecordsRequest, CreateCollectionRequest, IncludeList, QueryRequest,
};
use napi::bindgen_prelude::Float32Array;

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

#[napi(js_name = "Bindings")]
pub struct JsBindings {
  frontend: Frontend,
}

#[napi]
impl JsBindings {
  #[napi(factory)]
  pub async fn create(
    allow_reset: bool,
    // sqlite_db_config: SqliteDBConfig,
    hnsw_cache_size: u32,
    persist_path: Option<String>,
  ) -> Self {
    let system = System::new();
    let registry = Registry::new();

    //////////////////////////// Frontend Setup ////////////////////////////

    let cache_config = FoyerCacheConfig {
      capacity: hnsw_cache_size as usize,
      ..Default::default()
    };
    let cache_config = chroma_cache::CacheConfig::Memory(cache_config);
    let segment_manager_config = LocalSegmentManagerConfig {
      hnsw_index_pool_cache_config: cache_config,
      persist_path,
    };

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

    let frontend_config = FrontendConfig {
      allow_reset,
      segment_manager: Some(segment_manager_config),
      sqlitedb: Some(SqliteDBConfig::default()),
      sysdb: sysdb_config,
      collections_with_segments_provider: collection_cache_config,
      log: log_config,
      executor: executor_config,
      default_knn_index: chroma_types::KnnIndex::Hnsw,
      tenants_to_migrate_immediately: vec![],
      tenants_to_migrate_immediately_threshold: None,
    };

    let frontend = Frontend::try_from_config(&(frontend_config, system), &registry)
      .await
      .unwrap();

    JsBindings { frontend }
  }

  #[napi]
  pub async fn create_collection(&self, name: String) -> napi::Result<serde_json::Value> {
    let mut frontend = self.frontend.clone();
    let collection = frontend
      .create_collection(
        CreateCollectionRequest::try_new(
          "default_tenant".to_string(),
          "default_database".to_string(),
          name,
          None,
          None,
          true,
        )
        .unwrap(),
      )
      .await
      .map_err(|e| napi::Error::from_reason(format!("Failed to create collection: {}", e)))?;

    let collection_json = serde_json::to_value(&collection).unwrap();
    Ok(collection_json)
  }

  #[napi]
  pub async fn add(
    &self,
    ids: Vec<String>,
    collection_id: String,
    embeddings: Vec<Float32Array>,
    // metadatas: Option<Vec<Option<Metadata>>>,
    documents: Option<Vec<Option<String>>>,
    // uris: Option<Vec<Option<String>>>,
    tenant: String,
    database: String,
  ) -> napi::Result<()> {
    let mut frontend = self.frontend.clone();

    let collection_id =
      chroma_types::CollectionUuid(uuid::Uuid::parse_str(&collection_id).unwrap());

    frontend
      .add(
        AddCollectionRecordsRequest::try_new(
          tenant,
          database,
          collection_id,
          ids,
          embeddings.into_iter().map(|fa| fa.to_vec()).collect(),
          documents,
          None,
          None,
        )
        .unwrap(),
      )
      .await
      .map_err(|e| napi::Error::from_reason(format!("Failed to add embeddings: {}", e)))?;

    Ok(())
  }

  #[napi]
  pub async fn query(
    &self,
    collection_id: String,
    ids: Option<Vec<String>>,
    query_embeddings: Vec<Float32Array>,
    n_results: u32,
    r#where: Option<serde_json::Value>,
    where_document: Option<serde_json::Value>,
    include: Vec<String>,
    tenant: String,
    database: String,
  ) -> napi::Result<serde_json::Value> {
    let mut frontend = self.frontend.clone();

    let collection_id =
      chroma_types::CollectionUuid(uuid::Uuid::parse_str(&collection_id).unwrap());

    let r#where = chroma_types::RawWhereFields::new(
      r#where.unwrap_or(serde_json::Value::Null),
      where_document.unwrap_or(serde_json::Value::Null),
    )
    .parse()
    .unwrap();

    let include = IncludeList::try_from(include).unwrap();

    let query_result = frontend
      .query(
        QueryRequest::try_new(
          tenant,
          database,
          collection_id,
          ids,
          r#where,
          query_embeddings.into_iter().map(|fa| fa.to_vec()).collect(),
          n_results,
          include,
        )
        .unwrap(),
      )
      .await
      .unwrap();

    let query_result_json = serde_json::to_value(&query_result).unwrap();
    Ok(query_result_json)
  }
}
