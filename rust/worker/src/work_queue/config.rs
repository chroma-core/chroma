use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct WorkQueueConfig {
    pub storage_path: String,
    pub persistence: PersistenceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PersistenceConfig {
    pub time_threshold_seconds: u64,
    pub pending_threshold: usize,
}

impl Default for WorkQueueConfig {
    fn default() -> Self {
        Self {
            storage_path: "work-queue/queue.parquet".to_string(),
            persistence: PersistenceConfig {
                time_threshold_seconds: 2,
                pending_threshold: 100,
            },
        }
    }
}
