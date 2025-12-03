use crate::{
    executor::config::{ExecutorConfig, LocalExecutorConfig},
    CollectionsWithSegmentsProviderConfig,
};
use chroma_log::config::LogConfig;
use chroma_segment::local_segment_manager::LocalSegmentManagerConfig;
use chroma_sqlite::config::SqliteDBConfig;
use chroma_sysdb::SysDbConfig;
use chroma_tracing::{OtelFilter, OtelFilterLevel};
use chroma_types::{default_default_knn_index, KnnIndex};
use figment::providers::{Env, Format, Yaml};
use mdac::CircuitBreakerConfig;
use rust_embed::Embed;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ScorecardRule {
    pub patterns: Vec<String>,
    pub score: u32,
}

fn default_sysdb_config() -> SysDbConfig {
    SysDbConfig::Sqlite(Default::default())
}

fn default_log_config() -> LogConfig {
    LogConfig::Sqlite(Default::default())
}

fn default_executor_config() -> ExecutorConfig {
    ExecutorConfig::Local(LocalExecutorConfig {})
}

fn default_sqlitedb() -> Option<SqliteDBConfig> {
    Some(SqliteDBConfig::default())
}

fn default_segment_manager_config() -> Option<LocalSegmentManagerConfig> {
    Some(LocalSegmentManagerConfig {
        hnsw_index_pool_cache_config: chroma_cache::CacheConfig::Memory(
            chroma_cache::FoyerCacheConfig {
                capacity: 65536,
                ..Default::default()
            },
        ),
        persist_path: None,
    })
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FrontendConfig {
    #[serde(default)]
    pub allow_reset: bool,
    #[serde(default = "default_sqlitedb")]
    pub sqlitedb: Option<SqliteDBConfig>,
    #[serde(default = "default_segment_manager_config")]
    pub segment_manager: Option<LocalSegmentManagerConfig>,
    #[serde(default = "default_sysdb_config")]
    pub sysdb: SysDbConfig,
    #[serde(default)]
    pub collections_with_segments_provider: CollectionsWithSegmentsProviderConfig,
    #[serde(default = "default_log_config")]
    pub log: LogConfig,
    #[serde(default = "default_executor_config")]
    pub executor: ExecutorConfig,
    #[serde(default = "default_default_knn_index")]
    pub default_knn_index: KnnIndex,
    #[serde(default = "Default::default")]
    pub tenants_to_migrate_immediately: Vec<String>,
    #[serde(default = "Default::default")]
    pub tenants_to_migrate_immediately_threshold: Option<String>,
    #[serde(default = "default_enable_schema")]
    pub enable_schema: bool,
    #[serde(default = "default_min_records_for_invocation")]
    pub min_records_for_invocation: u64,
}

impl FrontendConfig {
    pub fn sqlite_in_memory() -> Self {
        Self {
            allow_reset: false,
            sqlitedb: Some(SqliteDBConfig {
                url: None,
                ..Default::default()
            }),
            segment_manager: default_segment_manager_config(),
            sysdb: default_sysdb_config(),
            collections_with_segments_provider: Default::default(),
            log: default_log_config(),
            executor: default_executor_config(),
            default_knn_index: default_default_knn_index(),
            tenants_to_migrate_immediately: vec![],
            tenants_to_migrate_immediately_threshold: None,
            enable_schema: default_enable_schema(),
            min_records_for_invocation: default_min_records_for_invocation(),
        }
    }
}

fn default_otel_service_name() -> String {
    "chromadb".to_string()
}

fn default_otel_filters() -> Vec<OtelFilter> {
    vec![OtelFilter {
        crate_name: "chroma_frontend".to_string(),
        filter_level: OtelFilterLevel::Trace,
    }]
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OpenTelemetryConfig {
    pub endpoint: String,
    #[serde(default = "default_otel_service_name")]
    pub service_name: String,
    #[serde(default = "default_otel_filters")]
    pub filters: Vec<OtelFilter>,
}

fn default_persist_path() -> String {
    "./chroma".to_string()
}

fn default_sqlite_filename() -> String {
    "chroma.sqlite3".to_string()
}

fn default_port() -> u16 {
    8000
}

fn default_listen_address() -> String {
    "0.0.0.0".to_string()
}

fn default_max_payload_size_bytes() -> usize {
    40 * 1024 * 1024 // 40 MB
}

fn default_enable_span_indexing() -> bool {
    false
}

fn default_enable_schema() -> bool {
    true
}

pub fn default_min_records_for_invocation() -> u64 {
    100
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FrontendServerConfig {
    #[serde(flatten)]
    pub frontend: FrontendConfig,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    #[serde(default = "default_max_payload_size_bytes")]
    pub max_payload_size_bytes: usize,
    #[serde(default = "CircuitBreakerConfig::default")]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub scorecard_enabled: bool,
    #[serde(default)]
    pub scorecard: Vec<ScorecardRule>,
    pub open_telemetry: Option<OpenTelemetryConfig>,
    #[serde(default = "default_persist_path")]
    pub persist_path: String,
    #[serde(default = "default_sqlite_filename")]
    pub sqlite_filename: String,
    #[serde(default)]
    pub cors_allow_origins: Option<Vec<String>>,
    #[serde(default = "default_enable_span_indexing")]
    pub enable_span_indexing: bool,
}

const DEFAULT_CONFIG_PATH: &str = "sample_configs/distributed.yaml";
const DEFAULT_SINGLE_NODE_CONFIG_FILENAME: &str = "sample_configs/single_node.yaml";

#[derive(Embed)]
#[folder = "./"]
#[include = "*.yaml"]
struct DefaultConfigurationsFolder;

impl FrontendServerConfig {
    pub fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    pub fn load_from_path(path: &str) -> Self {
        // SAFETY(rescrv): If we cannot read the config, we panic anyway.
        eprintln!(
            "==========\n{}\n==========\n",
            std::fs::read_to_string(path).unwrap()
        );
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(
            Env::prefixed("CHROMA_").map(|k| k.as_str().replace("__", ".").into()),
        );
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }

    pub fn single_node_default() -> Self {
        // TOOD: unify this with load_from_path to get the env overrides
        let config = DefaultConfigurationsFolder::get(DEFAULT_SINGLE_NODE_CONFIG_FILENAME)
            .expect("Failed to load default single node frontend config");
        let config_data = config.data;
        let config_str = std::str::from_utf8(&config_data).expect("Failed to parse config data");
        let f = figment::Figment::from(Yaml::string(config_str));
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::FrontendServerConfig;
    use chroma_cache::CacheConfig;

    #[test]
    fn test_load_config() {
        let config = FrontendServerConfig::load();
        let sysdb_config = config.frontend.sysdb;
        let sysdb_config = match sysdb_config {
            chroma_sysdb::SysDbConfig::Grpc(grpc_sys_db_config) => grpc_sys_db_config,
            chroma_sysdb::SysDbConfig::Sqlite(_) => {
                panic!("Expected grpc sysdb config, got sqlite sysdb config")
            }
        };
        assert_eq!(sysdb_config.host, "sysdb.chroma");
        assert_eq!(sysdb_config.port, 50051);
        assert_eq!(sysdb_config.connect_timeout_ms, 60000);
        assert_eq!(sysdb_config.request_timeout_ms, 60000);
        assert_eq!(sysdb_config.num_channels, 5);
        assert_eq!(
            config
                .frontend
                .collections_with_segments_provider
                .permitted_parallelism,
            180
        );
        match config.frontend.collections_with_segments_provider.cache {
            CacheConfig::Memory(c) => {
                assert_eq!(c.capacity, 1000);
            }
            CacheConfig::Disk(c) => {
                assert_eq!(c.capacity, 1000);
            }
            CacheConfig::Nop => {}
            _ => {}
        }
        assert!(config.frontend.enable_schema);
    }

    #[test]
    fn single_node_full_config_valid() {
        let config = FrontendServerConfig::load_from_path("sample_configs/single_node_full.yaml");
        assert_eq!(config.port, 8000);
    }
}
