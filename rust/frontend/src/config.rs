use crate::{
    compaction_client::compaction_client::CompactionClientConfig, executor::config::ExecutorConfig,
    CollectionsWithSegmentsProviderConfig,
};
use chroma_log::config::LogConfig;
use chroma_segment::local_segment_manager::LocalSegmentManagerConfig;
use chroma_sqlite::config::SqliteDBConfig;
use chroma_sysdb::SysDbConfig;
use figment::providers::{Env, Format, Yaml};
use mdac::CircuitBreakerConfig;
use rust_embed::Embed;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct ScorecardRule {
    pub patterns: Vec<String>,
    pub score: u32,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct FrontendConfig {
    #[serde(default)]
    pub allow_reset: bool,
    pub sqlitedb: Option<SqliteDBConfig>,
    pub segment_manager: Option<LocalSegmentManagerConfig>,
    pub sysdb: SysDbConfig,
    #[serde(default = "CircuitBreakerConfig::default")]
    pub circuit_breaker: CircuitBreakerConfig,
    pub collections_with_segments_provider: CollectionsWithSegmentsProviderConfig,
    pub service_name: String,
    pub otel_endpoint: String,
    pub log: LogConfig,
    pub executor: ExecutorConfig,
    pub scorecard_enabled: bool,
    #[serde(default)]
    pub scorecard: Vec<ScorecardRule>,
    pub compaction_client: CompactionClientConfig,
}

const DEFAULT_CONFIG_PATH: &str = "./frontend_config.yaml";
const DEFAULT_SINGLE_NODE_CONFIG_FILENAME: &str = "single_node_frontend_config.yaml";

#[derive(Embed)]
#[folder = "./"]
#[include = "*.yaml"]
struct DefaultConfigurationsFolder;

impl FrontendConfig {
    pub fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    pub fn load_from_path(path: &str) -> Self {
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
    use crate::config::FrontendConfig;
    use chroma_cache::CacheConfig;

    #[test]
    fn test_load_config() {
        let config = FrontendConfig::load();
        let sysdb_config = config.sysdb;
        let sysdb_config = match sysdb_config {
            chroma_sysdb::SysDbConfig::Grpc(grpc_sys_db_config) => grpc_sys_db_config,
            _ => {
                panic!("Expected grpc sysdb config")
            }
        };
        assert_eq!(sysdb_config.host, "sysdb.chroma");
        assert_eq!(sysdb_config.port, 50051);
        assert_eq!(sysdb_config.connect_timeout_ms, 60000);
        assert_eq!(sysdb_config.request_timeout_ms, 60000);
        assert_eq!(sysdb_config.num_channels, 5);
        assert_eq!(
            config
                .collections_with_segments_provider
                .permitted_parallelism,
            180
        );
        match config.collections_with_segments_provider.cache {
            CacheConfig::Memory(c) => {
                assert_eq!(c.capacity, 1000);
            }
            CacheConfig::Disk(c) => {
                assert_eq!(c.capacity, 1000);
            }
            CacheConfig::Nop => {}
            _ => {}
        }
    }
}
