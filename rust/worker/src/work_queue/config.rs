use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkQueueConfig {
    pub enabled: bool,
    pub storage_path: String,
    pub persistence: PersistenceConfig,
    pub use_sysdb_filtering: bool,
    pub grpc_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    pub time_threshold_seconds: u64,
    pub operation_threshold: u64,
    pub memory_threshold: usize,
}

impl Default for WorkQueueConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            storage_path: "s3://chroma/work-queue/queue.parquet".to_string(),
            persistence: PersistenceConfig {
                time_threshold_seconds: 30,
                operation_threshold: 100,
                memory_threshold: 10000,
            },
            use_sysdb_filtering: false,
            grpc_port: 50054,
        }
    }
}
