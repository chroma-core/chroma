use chroma_cache::CacheConfig;
use chroma_storage::config::StorageConfig;
use chroma_system::DispatcherConfig;
use figment::providers::{Env, Format, Yaml};
use std::{collections::HashMap, time::Duration};

use crate::types::CleanupMode;

const DEFAULT_CONFIG_PATH: &str = "./garbage_collector_config.yaml";

fn deserialize_duration_from_seconds<'de, D>(d: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let secs: u64 = serde::Deserialize::deserialize(d)?;
    Ok(Duration::from_secs(secs))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct GarbageCollectorConfig {
    pub(super) service_name: String,
    pub(super) otel_endpoint: String,
    #[serde(
        rename = "relative_cutoff_time_seconds",
        deserialize_with = "deserialize_duration_from_seconds"
    )]
    pub(super) relative_cutoff_time: Duration,
    pub(super) max_collections_to_gc: u32,
    pub(super) gc_interval_mins: u32,
    pub(super) disallow_collections: Vec<String>,
    pub(super) sysdb_config: chroma_sysdb::GrpcSysDbConfig,
    pub(super) dispatcher_config: DispatcherConfig,
    pub(super) storage_config: StorageConfig,
    #[serde(default)]
    pub(super) default_mode: CleanupMode,
    #[serde(default)]
    pub(super) tenant_mode_overrides: Option<HashMap<String, CleanupMode>>,
    pub(super) assignment_policy: chroma_config::assignment::config::AssignmentPolicyConfig,
    pub(super) memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
    pub my_member_id: String,
    #[serde(default = "GarbageCollectorConfig::default_port")]
    pub port: u16,
    #[serde(default)]
    pub root_cache_config: CacheConfig,
}

impl GarbageCollectorConfig {
    pub(super) fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    pub(super) fn load_from_path(path: &str) -> Self {
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(Env::prefixed("CHROMA_GC_").map(|k| match k {
            k if k == "my_member_id" => k.into(),
            k => k.as_str().replace("__", ".").into(),
        }));
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }

    fn default_port() -> u16 {
        50055
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
        assert_eq!(
            config.relative_cutoff_time,
            Duration::from_secs(12 * 60 * 60)
        ); // 12 hours
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
