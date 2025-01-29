use crate::executor::config::ExecutorConfig;
use chroma_cache::CacheConfig;
use chroma_log::config::LogConfig;
use chroma_sysdb::SysDbConfig;
use figment::providers::{Env, Format, Yaml};
use mdac::CircuitBreakerConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct ScorecardRule {
    pub patterns: Vec<String>,
    pub score: u32,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct FrontendConfig {
    pub sysdb: SysDbConfig,
    #[serde(default = "CircuitBreakerConfig::default")]
    pub circuit_breaker: CircuitBreakerConfig,
    pub cache_config: CacheConfig,
    pub service_name: String,
    pub otel_endpoint: String,
    pub log: LogConfig,
    pub executor: ExecutorConfig,
    pub scorecard_enabled: bool,
    #[serde(default)]
    pub scorecard: Vec<ScorecardRule>,
}

const DEFAULT_CONFIG_PATH: &str = "./frontend_config.yaml";

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
}

#[cfg(test)]
mod tests {
    use crate::config::FrontendConfig;
    use chroma_cache::CacheConfig;
    use chroma_sysdb::SysDbConfig::Grpc;

    #[test]
    fn test_load_config() {
        let config = FrontendConfig::load();
        let Grpc(sysdb_config) = config.sysdb;
        assert_eq!(sysdb_config.host, "sysdb.chroma");
        assert_eq!(sysdb_config.port, 50051);
        assert_eq!(sysdb_config.connect_timeout_ms, 60000);
        assert_eq!(sysdb_config.request_timeout_ms, 60000);
        assert_eq!(sysdb_config.num_channels, 5);
        if let CacheConfig::Memory(cache_config) = config.cache_config {
            assert_eq!(cache_config.capacity, 1000);
        } else {
            panic!("Expected Memory cache config");
        }
    }
}
