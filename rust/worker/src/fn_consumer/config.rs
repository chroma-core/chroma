use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GrpcWorkQueueConfig {
    #[serde(default = "GrpcWorkQueueConfig::default_host")]
    pub host: String,
    #[serde(default = "GrpcWorkQueueConfig::default_port")]
    pub port: u16,
    #[serde(default = "GrpcWorkQueueConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "GrpcWorkQueueConfig::default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

impl GrpcWorkQueueConfig {
    fn default_host() -> String {
        "work-queue-service.chroma".to_string()
    }

    fn default_port() -> u16 {
        50051
    }

    fn default_connect_timeout_ms() -> u64 {
        10000
    }

    fn default_request_timeout_ms() -> u64 {
        10000
    }
}

impl Default for GrpcWorkQueueConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            port: Self::default_port(),
            connect_timeout_ms: Self::default_connect_timeout_ms(),
            request_timeout_ms: Self::default_request_timeout_ms(),
        }
    }
}

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
    #[serde(alias = "work_queue")]
    pub work_queue: GrpcWorkQueueConfig,
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
}

impl Default for FnConsumerConfig {
    fn default() -> Self {
        Self {
            poll_interval_sec: Self::default_poll_interval_sec(),
            max_concurrent_workers: Self::default_max_concurrent_workers(),
            get_work_batch_size: Self::default_get_work_batch_size(),
            job_expiry_seconds: Self::default_job_expiry_seconds(),
            work_queue: GrpcWorkQueueConfig::default(),
        }
    }
}
