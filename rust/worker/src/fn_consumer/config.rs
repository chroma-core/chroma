use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FnConsumerConfig {
    #[serde(default = "FnConsumerConfig::default_poll_interval_sec")]
    pub poll_interval_sec: u64,
    #[serde(default = "FnConsumerConfig::default_max_concurrent_workers")]
    pub max_concurrent_workers: usize,
    #[serde(default = "FnConsumerConfig::default_get_work_batch_size")]
    pub get_work_batch_size: u32,
    #[serde(default = "FnConsumerConfig::default_job_expiry_seconds")]
    pub job_expiry_seconds: u64,
    pub work_queue_endpoint: String,
    #[serde(default = "FnConsumerConfig::default_fetch_log_batch_size")]
    pub fetch_log_batch_size: u32,
    #[serde(default = "FnConsumerConfig::default_fetch_log_concurrency")]
    pub fetch_log_concurrency: usize,
    #[serde(default = "FnConsumerConfig::default_fetch_log_max_count")]
    pub fetch_log_max_count: u32,
}

impl FnConsumerConfig {
    fn default_poll_interval_sec() -> u64 {
        10
    }
    fn default_max_concurrent_workers() -> usize {
        100
    }
    fn default_get_work_batch_size() -> u32 {
        100
    }
    fn default_job_expiry_seconds() -> u64 {
        3600
    }
    fn default_fetch_log_batch_size() -> u32 {
        100
    }
    fn default_fetch_log_concurrency() -> usize {
        10
    }
    fn default_fetch_log_max_count() -> u32 {
        10_000
    }
}

impl Default for FnConsumerConfig {
    fn default() -> Self {
        Self {
            poll_interval_sec: Self::default_poll_interval_sec(),
            max_concurrent_workers: Self::default_max_concurrent_workers(),
            get_work_batch_size: Self::default_get_work_batch_size(),
            job_expiry_seconds: Self::default_job_expiry_seconds(),
            work_queue_endpoint: String::new(),
            fetch_log_batch_size: Self::default_fetch_log_batch_size(),
            fetch_log_concurrency: Self::default_fetch_log_concurrency(),
            fetch_log_max_count: Self::default_fetch_log_max_count(),
        }
    }
}
