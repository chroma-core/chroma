use chroma_cache::CacheConfig;
use chroma_config::helpers::deserialize_duration_from_seconds;
use chroma_log::config::LogConfig;
use chroma_storage::config::StorageConfig;
use chroma_system::DispatcherConfig;
use chroma_tracing::{OtelFilter, OtelFilterLevel};
use chroma_types::CollectionUuid;
use figment::providers::{Env, Format, Yaml};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use crate::types::CleanupMode;

const DEFAULT_CONFIG_PATH: &str = "./garbage_collector_config.yaml";

#[derive(Debug, serde::Deserialize, Clone, Default)]
pub struct GarbageCollectorConfig {
    pub(super) service_name: String,
    pub(super) otel_endpoint: String,
    #[serde(default = "GarbageCollectorConfig::default_otel_filters")]
    pub(super) otel_filters: Vec<OtelFilter>,
    #[serde(
        rename = "collection_soft_delete_grace_period_seconds",
        deserialize_with = "deserialize_duration_from_seconds",
        default = "GarbageCollectorConfig::default_collection_soft_delete_grace_period"
    )]
    pub(super) collection_soft_delete_grace_period: Duration,
    #[serde(
        rename = "attached_function_soft_delete_grace_period_seconds",
        deserialize_with = "deserialize_duration_from_seconds",
        default = "GarbageCollectorConfig::default_attached_function_soft_delete_grace_period"
    )]
    pub(super) attached_function_soft_delete_grace_period: Duration,
    #[serde(
        rename = "version_relative_cutoff_time_seconds",
        alias = "relative_cutoff_time_seconds",
        deserialize_with = "deserialize_duration_from_seconds"
    )]
    pub(super) version_cutoff_time: Duration,
    pub(super) max_collections_to_gc: u32,
    #[serde(
        default = "GarbageCollectorConfig::default_max_concurrent_list_files_operations_per_collection"
    )]
    pub(super) max_concurrent_list_files_operations_per_collection: usize,
    pub(super) max_collections_to_fetch: Option<u32>,
    pub(super) gc_interval_mins: u32,
    #[serde(default = "GarbageCollectorConfig::default_min_versions_to_keep")]
    pub min_versions_to_keep: u32,
    #[serde(default = "GarbageCollectorConfig::default_filter_min_versions_if_alive")]
    pub(super) filter_min_versions_if_alive: Option<u64>,
    pub(super) disallow_collections: HashSet<CollectionUuid>,
    pub sysdb_config: chroma_sysdb::GrpcSysDbConfig,
    pub dispatcher_config: DispatcherConfig,
    pub storage_config: StorageConfig,
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
    pub jemalloc_pprof_server_port: Option<u16>,
    #[serde(default)]
    pub enable_log_gc_for_tenant: Vec<String>,
    #[serde(default = "GarbageCollectorConfig::enable_log_gc_for_tenant_threshold")]
    pub enable_log_gc_for_tenant_threshold: String,
    pub log: LogConfig,
    #[serde(default)]
    pub enable_dangerous_option_to_ignore_min_versions_for_wal3: bool,
    #[serde(default = "GarbageCollectorConfig::default_heap_prune_buckets_to_read")]
    pub heap_prune_buckets_to_read: u32,
    #[serde(default = "GarbageCollectorConfig::default_heap_prune_max_items")]
    pub heap_prune_max_items: u32,
    #[serde(default = "GarbageCollectorConfig::default_max_attached_functions_to_gc_per_run")]
    pub max_attached_functions_to_gc_per_run: i32,
}

impl GarbageCollectorConfig {
    fn default_min_versions_to_keep() -> u32 {
        2
    }

    fn default_max_concurrent_list_files_operations_per_collection() -> usize {
        10
    }

    fn default_filter_min_versions_if_alive() -> Option<u64> {
        None
    }

    pub(super) fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    pub fn load_from_path(path: &str) -> Self {
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(Env::prefixed("CHROMA_GC_").map(|k| match k {
            k if k == "my_member_id" => k.into(),
            k => k.as_str().replace("__", ".").into(),
        }));
        if std::path::Path::new(path).exists() {
            let yaml = figment::Figment::from(Yaml::file(path));
            f = yaml.clone().merge(yaml.focus("garbage_collector")).merge(f);
        }
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config from {path}: {}", e),
        }
    }

    fn default_port() -> u16 {
        50055
    }

    fn default_otel_filters() -> Vec<OtelFilter> {
        vec![OtelFilter {
            crate_name: "garbage_collector".to_string(),
            filter_level: OtelFilterLevel::Debug,
        }]
    }

    fn default_collection_soft_delete_grace_period() -> Duration {
        Duration::from_secs(60 * 60 * 24) // 1 day
    }

    fn default_attached_function_soft_delete_grace_period() -> Duration {
        Duration::from_secs(60 * 60 * 24) // 1 day
    }

    fn default_heap_prune_buckets_to_read() -> u32 {
        10 // Scan up to 10 time buckets per shard
    }

    fn default_heap_prune_max_items() -> u32 {
        10000 // Prune up to 10k items per shard per GC pass
    }

    fn default_max_attached_functions_to_gc_per_run() -> i32 {
        100
    }

    fn enable_log_gc_for_tenant_threshold() -> String {
        "00000000-0000-0000-0000-000000000000".to_string()
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
            config.version_cutoff_time,
            Duration::from_secs(12 * 60 * 60)
        ); // 12 hours
        assert_eq!(config.max_collections_to_gc, 1000);
        assert_eq!(config.gc_interval_mins, 120);
        let empty_set: HashSet<CollectionUuid> = HashSet::new();
        assert_eq!(config.disallow_collections, empty_set);
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
