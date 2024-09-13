use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct CompactorConfig {
    pub(crate) compaction_manager_queue_size: usize,
    pub(crate) max_concurrent_jobs: usize,
    pub(crate) compaction_interval_sec: u64,
    pub(crate) min_compaction_size: usize,
    pub(crate) max_compaction_size: usize,
    pub(crate) max_partition_size: usize,
}
