use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TaskRunnerConfig {
    #[serde(default = "TaskRunnerConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "TaskRunnerConfig::default_compaction_manager_queue_size")]
    pub compaction_manager_queue_size: usize,
    #[serde(default = "TaskRunnerConfig::default_job_expiry_seconds")]
    pub job_expiry_seconds: u64,
    #[serde(default = "TaskRunnerConfig::default_max_concurrent_jobs")]
    pub max_concurrent_jobs: usize,
    #[serde(default = "TaskRunnerConfig::default_poll_interval_sec")]
    pub poll_interval_sec: u64,
    #[serde(default = "TaskRunnerConfig::default_max_compaction_size")]
    pub max_compaction_size: usize,
    #[serde(default = "TaskRunnerConfig::default_max_partition_size")]
    pub max_partition_size: usize,
    #[serde(default = "TaskRunnerConfig::default_fetch_log_batch_size")]
    pub fetch_log_batch_size: u32,
    #[serde(default = "TaskRunnerConfig::default_max_failure_count")]
    pub max_failure_count: u8,
}

impl TaskRunnerConfig {
    fn default_enabled() -> bool {
        false // Disabled by default for safety
    }

    fn default_compaction_manager_queue_size() -> usize {
        1000
    }

    fn default_max_concurrent_jobs() -> usize {
        50
    }

    fn default_poll_interval_sec() -> u64 {
        30
    }

    fn default_job_expiry_seconds() -> u64 {
        3600
    }

    fn default_max_compaction_size() -> usize {
        10_000
    }

    fn default_max_partition_size() -> usize {
        5_000
    }

    fn default_fetch_log_batch_size() -> u32 {
        100
    }

    fn default_max_failure_count() -> u8 {
        5
    }
}

impl Default for TaskRunnerConfig {
    fn default() -> Self {
        TaskRunnerConfig {
            enabled: TaskRunnerConfig::default_enabled(),
            compaction_manager_queue_size: TaskRunnerConfig::default_compaction_manager_queue_size(
            ),
            job_expiry_seconds: TaskRunnerConfig::default_job_expiry_seconds(),
            max_concurrent_jobs: TaskRunnerConfig::default_max_concurrent_jobs(),
            poll_interval_sec: TaskRunnerConfig::default_poll_interval_sec(),
            max_compaction_size: TaskRunnerConfig::default_max_compaction_size(),
            max_partition_size: TaskRunnerConfig::default_max_partition_size(),
            fetch_log_batch_size: TaskRunnerConfig::default_fetch_log_batch_size(),
            max_failure_count: TaskRunnerConfig::default_max_failure_count(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CompactorConfig {
    #[serde(default = "CompactorConfig::default_compaction_manager_queue_size")]
    pub compaction_manager_queue_size: usize,
    #[serde(default = "CompactorConfig::default_job_expiry_seconds")]
    pub job_expiry_seconds: u64,
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
    #[serde(default = "CompactorConfig::default_purge_dirty_log_timeout_seconds")]
    pub purge_dirty_log_timeout_seconds: u64,
    #[serde(default = "CompactorConfig::default_repair_log_offsets_timeout_seconds")]
    pub repair_log_offsets_timeout_seconds: u64,
    #[serde(default = "CompactorConfig::default_max_failure_count")]
    pub max_failure_count: u8,
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

    fn default_job_expiry_seconds() -> u64 {
        3600
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

    fn default_purge_dirty_log_timeout_seconds() -> u64 {
        60
    }

    fn default_repair_log_offsets_timeout_seconds() -> u64 {
        60
    }

    fn default_max_failure_count() -> u8 {
        5
    }
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfig {
            compaction_manager_queue_size: CompactorConfig::default_compaction_manager_queue_size(),
            job_expiry_seconds: CompactorConfig::default_job_expiry_seconds(),
            max_concurrent_jobs: CompactorConfig::default_max_concurrent_jobs(),
            compaction_interval_sec: CompactorConfig::default_compaction_interval_sec(),
            min_compaction_size: CompactorConfig::default_min_compaction_size(),
            max_compaction_size: CompactorConfig::default_max_compaction_size(),
            max_partition_size: CompactorConfig::default_max_partition_size(),
            disabled_collections: CompactorConfig::default_disabled_collections(),
            fetch_log_batch_size: CompactorConfig::default_fetch_log_batch_size(),
            purge_dirty_log_timeout_seconds:
                CompactorConfig::default_purge_dirty_log_timeout_seconds(),
            repair_log_offsets_timeout_seconds:
                CompactorConfig::default_repair_log_offsets_timeout_seconds(),
            max_failure_count: CompactorConfig::default_max_failure_count(),
        }
    }
}
