use figment::providers::{Env, Format, Yaml};

const DEFAULT_CONFIG_PATH: &str = "./garbage_collector_config.yaml";

#[derive(Debug, serde::Deserialize)]
pub(super) struct GarbageCollectorConfig {
    pub(super) service_name: String,
    pub(super) otel_endpoint: String,
    cutoff_time_hours: u32,
    max_collections_to_gc: u32,
    gc_interval_mins: u32,
    sysdb_connection: SysdbConnectionConfig,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct SysdbConnectionConfig {
    host: String,
    port: u32,
    connect_timeout_ms: u32,
    request_timeout_ms: u32,
}

impl GarbageCollectorConfig {
    pub(super) fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    pub(super) fn load_from_path(path: &str) -> Self {
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(
            Env::prefixed("CHROMA_GC_").map(|k| k.as_str().replace("__", ".").into()),
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
