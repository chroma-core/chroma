use chroma_cache::CacheConfig;
use chroma_config::helpers::deserialize_duration_from_seconds;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::config::LogConfig;
use chroma_storage::config::StorageConfig;
use chroma_storage::Storage;
use chroma_system::DispatcherConfig;
use chroma_tracing::{OtelFilter, OtelFilterLevel};
use chroma_types::CollectionUuid;
use figment::providers::{Env, Format, Yaml};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use crate::mcmr::RegionsAndTopologiesConfig;
use crate::types::CleanupMode;
use thiserror::Error;

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
    #[serde(default)]
    pub mcmr_sysdb_config: Option<chroma_sysdb::GrpcSysDbConfig>,
    #[serde(default)]
    pub regions_and_topologies: Option<RegionsAndTopologiesConfig>,
    pub dispatcher_config: DispatcherConfig,
    #[serde(default)]
    pub storage_config: Option<StorageConfig>,
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

#[derive(Debug, Error)]
pub enum GarbageCollectorConfigError {
    #[error(
        "storage_config and regions_and_topologies are mutually exclusive; exactly one must be set"
    )]
    StorageAndRegionsMutuallyExclusive,
    #[error("exactly one of storage_config or regions_and_topologies must be set")]
    MissingStorageAndRegions,
    #[error("preferred region {preferred_region} not found in regions_and_topologies")]
    MissingPreferredRegion { preferred_region: String },
}

impl ChromaError for GarbageCollectorConfigError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
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
        println!("loading config from {path}");
        println!(
            r#"Full config is:
================================================================================
{}
================================================================================
"#,
            std::fs::read_to_string(path)
                .expect("should be able to open and read config to string")
        );
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

    pub fn validate_storage_xor(&self) -> Result<(), Box<dyn ChromaError>> {
        let storage_set = self.storage_config.is_some();
        let regions_set = self.regions_and_topologies.is_some();

        match (storage_set, regions_set) {
            (true, true) => Err(Box::new(
                GarbageCollectorConfigError::StorageAndRegionsMutuallyExclusive,
            )),
            (false, false) => Err(Box::new(
                GarbageCollectorConfigError::MissingStorageAndRegions,
            )),
            (true, false) | (false, true) => Ok(()),
        }
    }

    pub async fn instantiate_storage(
        &self,
        registry: &Registry,
    ) -> Result<Storage, Box<dyn ChromaError>> {
        self.validate_storage_xor()?;
        if let Some(storage_config) = &self.storage_config {
            return Storage::try_from_config(storage_config, registry).await;
        }

        let regions_and_topologies =
            self.regions_and_topologies
                .as_ref()
                .ok_or_else(|| -> Box<dyn ChromaError> {
                    Box::new(GarbageCollectorConfigError::MissingStorageAndRegions)
                })?;
        let preferred_region = regions_and_topologies
            .preferred_region_config()
            .ok_or_else(|| -> Box<dyn ChromaError> {
                Box::new(GarbageCollectorConfigError::MissingPreferredRegion {
                    preferred_region: regions_and_topologies.preferred.to_string(),
                })
            })?;

        Storage::try_from_config(&preferred_region.storage, registry).await
    }
}

#[cfg(test)]
mod tests {
    use chroma_storage::config::{LocalStorageConfig, S3CredentialsConfig};
    use chroma_types::{MultiCloudMultiRegionConfiguration, ProviderRegion, RegionName};

    use crate::mcmr::RegionalStorageConfig;

    use super::*;

    fn local_storage_config(root: String) -> StorageConfig {
        StorageConfig::Local(LocalStorageConfig { root })
    }

    fn regions_and_topologies(storage: StorageConfig) -> RegionsAndTopologiesConfig {
        let region_name = RegionName::new("test-region").unwrap();
        MultiCloudMultiRegionConfiguration::new(
            region_name.clone(),
            vec![ProviderRegion::new(
                region_name,
                "test-provider",
                "test-location",
                RegionalStorageConfig { storage },
            )],
            vec![],
        )
        .unwrap()
    }

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
        match config.storage_config.expect("storage_config should be set") {
            StorageConfig::S3(storage_config) => {
                assert_eq!(storage_config.bucket, "chroma-storage");
                if let S3CredentialsConfig::Minio = storage_config.credentials {
                    assert_eq!(storage_config.connect_timeout_ms, 5000);
                    assert_eq!(storage_config.request_timeout_ms, 30000);
                    assert!(!storage_config.stall_download_enabled);
                    assert!(storage_config.stall_upload_enabled);
                    assert_eq!(storage_config.upload_part_size_bytes, 536870912);
                    assert_eq!(storage_config.download_part_size_bytes, 8388608);
                } else {
                    panic!("Expected Minio credentials");
                }
            }
            _ => panic!("Expected S3 storage config"),
        }
    }

    #[test]
    fn validate_storage_xor_allows_storage_only() {
        let config = GarbageCollectorConfig {
            storage_config: Some(StorageConfig::default()),
            regions_and_topologies: None,
            ..Default::default()
        };

        config
            .validate_storage_xor()
            .expect("storage-only config should pass");
    }

    #[test]
    fn validate_storage_xor_allows_regions_only() {
        let config = GarbageCollectorConfig {
            storage_config: None,
            regions_and_topologies: Some(regions_and_topologies(StorageConfig::default())),
            ..Default::default()
        };

        config
            .validate_storage_xor()
            .expect("regions-only config should pass");
    }

    #[test]
    fn validate_storage_xor_rejects_both_sources() {
        let config = GarbageCollectorConfig {
            storage_config: Some(StorageConfig::default()),
            regions_and_topologies: Some(regions_and_topologies(StorageConfig::default())),
            ..Default::default()
        };

        let err = config
            .validate_storage_xor()
            .expect_err("both storage config sources should fail");
        assert!(
            err.to_string()
                .contains("storage_config and regions_and_topologies are mutually exclusive"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_storage_xor_rejects_neither_source() {
        let config = GarbageCollectorConfig {
            storage_config: None,
            regions_and_topologies: None,
            ..Default::default()
        };

        let err = config
            .validate_storage_xor()
            .expect_err("missing storage config sources should fail");
        assert!(
            err.to_string()
                .contains("exactly one of storage_config or regions_and_topologies must be set"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn instantiate_storage_uses_preferred_region_when_storage_config_is_absent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = GarbageCollectorConfig {
            storage_config: None,
            regions_and_topologies: Some(regions_and_topologies(local_storage_config(
                temp_dir.path().to_str().unwrap().to_string(),
            ))),
            ..Default::default()
        };
        let registry = Registry::new();

        let storage = config
            .instantiate_storage(&registry)
            .await
            .expect("preferred region local storage should instantiate");

        assert!(
            matches!(storage, Storage::Local(_)),
            "expected preferred region local storage, got {storage:?}"
        );
    }
}
