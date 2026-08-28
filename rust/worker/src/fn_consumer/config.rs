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
    #[serde(default = "FnConsumerConfig::default_max_failure_count")]
    pub max_failure_count: i32,
    #[serde(alias = "work_queue")]
    pub work_queue: GrpcWorkQueueConfig,
    /// Optional pod-local memory admission control.
    #[serde(default)]
    pub memory_admission: MemoryAdmissionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Configuration for pod-local function memory admission.
pub struct MemoryAdmissionConfig {
    /// Enables memory-aware planning and next-boundary execution.
    #[serde(default)]
    pub enabled: bool,
    /// Overrides the detected cgroup limit for tests and local development.
    pub memory_limit_bytes: Option<u64>,
    /// Overrides the default `max(10% of limit, 256 MiB)` headroom.
    pub headroom_bytes: Option<u64>,
    /// Multiplier applied to the structural peak-memory estimate.
    #[serde(default = "MemoryAdmissionConfig::default_safety_multiplier")]
    pub safety_multiplier: f64,
    /// Fixed bytes reserved for executor and orchestration state.
    #[serde(default = "MemoryAdmissionConfig::default_fixed_executor_overhead_bytes")]
    pub fixed_executor_overhead_bytes: u64,
    /// Estimated container overhead for each materialized record.
    #[serde(default = "MemoryAdmissionConfig::default_per_record_overhead_bytes")]
    pub per_record_overhead_bytes: u64,
    /// Estimated container overhead for each materialized metadata entry.
    #[serde(default = "MemoryAdmissionConfig::default_per_metadata_entry_overhead_bytes")]
    pub per_metadata_entry_overhead_bytes: u64,
    /// Defaults to four times `max_concurrent_workers` when omitted.
    pub lookahead_size: Option<u32>,
    /// Defaults to one worker wave when omitted.
    pub max_bypass_count: Option<usize>,
}

impl MemoryAdmissionConfig {
    fn default_safety_multiplier() -> f64 {
        2.0
    }

    fn default_fixed_executor_overhead_bytes() -> u64 {
        64 * 1024 * 1024
    }

    fn default_per_record_overhead_bytes() -> u64 {
        256
    }

    fn default_per_metadata_entry_overhead_bytes() -> u64 {
        128
    }
}

impl Default for MemoryAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            memory_limit_bytes: None,
            headroom_bytes: None,
            safety_multiplier: Self::default_safety_multiplier(),
            fixed_executor_overhead_bytes: Self::default_fixed_executor_overhead_bytes(),
            per_record_overhead_bytes: Self::default_per_record_overhead_bytes(),
            per_metadata_entry_overhead_bytes: Self::default_per_metadata_entry_overhead_bytes(),
            lookahead_size: None,
            max_bypass_count: None,
        }
    }
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
    fn default_max_failure_count() -> i32 {
        5
    }
}

impl Default for FnConsumerConfig {
    fn default() -> Self {
        Self {
            poll_interval_sec: Self::default_poll_interval_sec(),
            max_concurrent_workers: Self::default_max_concurrent_workers(),
            get_work_batch_size: Self::default_get_work_batch_size(),
            job_expiry_seconds: Self::default_job_expiry_seconds(),
            max_failure_count: Self::default_max_failure_count(),
            work_queue: GrpcWorkQueueConfig::default(),
            memory_admission: MemoryAdmissionConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_admission_defaults_off_for_existing_configs() {
        let config: FnConsumerConfig = serde_json::from_value(serde_json::json!({
            "work_queue": {}
        }))
        .unwrap();

        assert!(!config.memory_admission.enabled);
        assert_eq!(config.memory_admission.safety_multiplier, 2.0);
        assert_eq!(
            config.memory_admission.fixed_executor_overhead_bytes,
            64 * 1024 * 1024
        );
    }

    #[test]
    fn memory_admission_can_be_enabled_with_restart_config() {
        let config: FnConsumerConfig = serde_json::from_value(serde_json::json!({
            "work_queue": {},
            "memory_admission": {
                "enabled": true,
                "memory_limit_bytes": 1073741824,
                "headroom_bytes": 268435456,
                "lookahead_size": 8,
                "max_bypass_count": 2
            }
        }))
        .unwrap();

        assert!(config.memory_admission.enabled);
        assert_eq!(config.memory_admission.memory_limit_bytes, Some(1073741824));
        assert_eq!(config.memory_admission.headroom_bytes, Some(268435456));
        assert_eq!(config.memory_admission.lookahead_size, Some(8));
        assert_eq!(config.memory_admission.max_bypass_count, Some(2));
    }
}
