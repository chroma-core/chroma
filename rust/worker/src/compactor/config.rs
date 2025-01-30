use serde::Deserialize;

#[derive(Deserialize)]
pub struct CompactorConfig {
    pub compaction_manager_queue_size: usize,
    pub max_concurrent_jobs: usize,
    pub compaction_interval_sec: u64,
    pub min_compaction_size: usize,
    pub max_compaction_size: usize,
    pub max_partition_size: usize,
    pub disabled_collections: Vec<String>,
}
