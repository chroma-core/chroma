use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CompactorConfig {
    #[serde(default = "CompactorConfig::default_compaction_manager_queue_size")]
    pub compaction_manager_queue_size: usize,
    #[serde(default = "CompactorConfig::default_max_concurrent_jobs")]
    pub max_concurrent_jobs: usize,
    #[serde(default = "CompactorConfig::default_compaction_interval_sec")]
    pub compaction_interval_sec: u64,
    #[serde(default = "CompactorConfig::default_min_compaction_size")]
    pub min_compaction_size: usize,
    #[serde(default = "CompactorConfig::default_max_compaction_size")]
    pub max_compaction_size: usize,
    #[serde(default = "CompactorConfig::default_max_partition_size")]
    pub max_partition_size: usize,
    #[serde(default = "CompactorConfig::default_disabled_collections")]
    pub disabled_collections: Vec<String>,
    #[serde(default = "CompactorConfig::default_fetch_log_batch_size")]
    pub fetch_log_batch_size: u32,
}

impl CompactorConfig {
    fn default_compaction_manager_queue_size() -> usize {
        1000
    }

    fn default_max_concurrent_jobs() -> usize {
        100
    }

    fn default_compaction_interval_sec() -> u64 {
        10
    }

    fn default_min_compaction_size() -> usize {
        10
    }

    fn default_max_compaction_size() -> usize {
        10_000
    }

    fn default_max_partition_size() -> usize {
        5_000
    }

    fn default_disabled_collections() -> Vec<String> {
        vec![]
    }

    fn default_fetch_log_batch_size() -> u32 {
        100
    }
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfig {
            compaction_manager_queue_size: CompactorConfig::default_compaction_manager_queue_size(),
            max_concurrent_jobs: CompactorConfig::default_max_concurrent_jobs(),
            compaction_interval_sec: CompactorConfig::default_compaction_interval_sec(),
            min_compaction_size: CompactorConfig::default_min_compaction_size(),
            max_compaction_size: CompactorConfig::default_max_compaction_size(),
            max_partition_size: CompactorConfig::default_max_partition_size(),
            disabled_collections: CompactorConfig::default_disabled_collections(),
            fetch_log_batch_size: CompactorConfig::default_fetch_log_batch_size(),
        }
    }
}
