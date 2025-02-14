use chroma_storage::config::StorageConfig;
use chroma_system::DispatcherConfig;
use figment::providers::{Env, Format, Yaml};

const DEFAULT_CONFIG_PATH: &str = "./garbage_collector_config.yaml";

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
// TODO(Sanket):  Remove this dead code annotation.
pub(super) struct GarbageCollectorConfig {
    pub(super) service_name: String,
    pub(super) otel_endpoint: String,
    pub(super) cutoff_time_hours: u32,
    pub(super) max_collections_to_gc: u32,
    pub(super) gc_interval_mins: u32,
    pub(super) disallow_collections: Vec<String>,
    pub(super) sysdb_config: chroma_sysdb::GrpcSysDbConfig,
    pub(super) dispatcher_config: DispatcherConfig,
    pub(super) storage_config: StorageConfig,
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

#[cfg(test)]
mod tests {
    use chroma_storage::config::S3CredentialsConfig;

    use super::*;

    #[test]
    fn test_load_config() {
        let config = GarbageCollectorConfig::load();
        assert_eq!(config.service_name, "garbage-collector");
        assert_eq!(config.otel_endpoint, "http://otel-collector:4317");
        assert_eq!(config.cutoff_time_hours, 12);
        assert_eq!(config.max_collections_to_gc, 1000);
        assert_eq!(config.gc_interval_mins, 120);
        let empty_vec: Vec<String> = vec![];
        assert_eq!(config.disallow_collections, empty_vec);
        assert_eq!(config.sysdb_config.host, "sysdb.chroma");
        assert_eq!(config.sysdb_config.port, 50051);
        assert_eq!(config.sysdb_config.connect_timeout_ms, 60000);
        assert_eq!(config.sysdb_config.request_timeout_ms, 60000);
        assert_eq!(config.dispatcher_config.num_worker_threads, 4);
        assert_eq!(config.dispatcher_config.dispatcher_queue_size, 100);
        assert_eq!(config.dispatcher_config.worker_queue_size, 100);
        match config.storage_config {
            StorageConfig::S3(storage_config) => {
                assert_eq!(storage_config.bucket, "chroma-storage");
                if let S3CredentialsConfig::Minio = storage_config.credentials {
                    assert_eq!(storage_config.connect_timeout_ms, 5000);
                    assert_eq!(storage_config.request_timeout_ms, 30000);
                    assert_eq!(storage_config.upload_part_size_bytes, 536870912);
                    assert_eq!(storage_config.download_part_size_bytes, 8388608);
                } else {
                    panic!("Expected Minio credentials");
                }
            }
            _ => panic!("Expected S3 storage config"),
        }
    }
}
