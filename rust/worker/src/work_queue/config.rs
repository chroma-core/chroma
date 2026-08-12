use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct WorkQueueConfig {
    pub storage_path: String,
    pub persistence: PersistenceConfig,
    #[serde(default = "WorkQueueConfig::default_retry_backoff_initial_seconds")]
    pub retry_backoff_initial_seconds: u64,
    #[serde(default = "WorkQueueConfig::default_retry_backoff_max_seconds")]
    pub retry_backoff_max_seconds: u64,
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
            retry_backoff_initial_seconds: Self::default_retry_backoff_initial_seconds(),
            retry_backoff_max_seconds: Self::default_retry_backoff_max_seconds(),
        }
    }
}

impl WorkQueueConfig {
    fn default_retry_backoff_initial_seconds() -> u64 {
        10
    }

    fn default_retry_backoff_max_seconds() -> u64 {
        600
    }
}
