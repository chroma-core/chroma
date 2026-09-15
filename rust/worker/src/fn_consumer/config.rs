use crate::work_queue::GRPC_MAX_DECODING_MESSAGE_SIZE;
use serde::{de::Error, Deserialize, Deserializer, Serialize};

// A canonical UUID in GetWorkRequest.excluded_fn_ids encodes as a one-byte
// field tag, a one-byte length, and 36 bytes of UTF-8 data.
const ENCODED_EXCLUDED_FN_ID_BYTES: usize = 38;
// Keep exclusions within half of the server's receive limit so the remaining
// request fields and future wire-contract growth retain ample headroom.
const EXCLUDED_FN_IDS_MESSAGE_BUDGET: usize = GRPC_MAX_DECODING_MESSAGE_SIZE / 2;
pub(crate) const MAX_CONCURRENT_WORKERS: usize =
    EXCLUDED_FN_IDS_MESSAGE_BUDGET / ENCODED_EXCLUDED_FN_ID_BYTES;

fn deserialize_max_concurrent_workers<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let value = usize::deserialize(deserializer)?;
    if value > MAX_CONCURRENT_WORKERS {
        return Err(D::Error::custom(format!(
            "max_concurrent_workers must not exceed {MAX_CONCURRENT_WORKERS}; larger values can make GetWork excluded_fn_ids exceed its gRPC message budget"
        )));
    }
    Ok(value)
}

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
    /// Maximum simultaneous function executions. This is also the maximum
    /// number of in-progress function IDs sent in a GetWork request.
    #[serde(
        default = "FnConsumerConfig::default_max_concurrent_workers",
        deserialize_with = "deserialize_max_concurrent_workers"
    )]
    pub max_concurrent_workers: usize,
    #[serde(default = "FnConsumerConfig::default_get_work_batch_size")]
    pub get_work_batch_size: u32,
    #[serde(default = "FnConsumerConfig::default_job_expiry_seconds")]
    pub job_expiry_seconds: u64,
    #[serde(default = "FnConsumerConfig::default_max_failure_count")]
    pub max_failure_count: i32,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::chroma_proto::GetWorkRequest;
    use prost::Message;

    fn config_with_max_concurrent_workers(max_concurrent_workers: usize) -> serde_json::Value {
        serde_json::json!({
            "max_concurrent_workers": max_concurrent_workers,
            "work_queue": {}
        })
    }

    #[test]
    fn accepts_max_concurrent_workers_within_grpc_budget() {
        let config: FnConsumerConfig =
            serde_json::from_value(config_with_max_concurrent_workers(MAX_CONCURRENT_WORKERS))
                .unwrap();

        assert_eq!(config.max_concurrent_workers, MAX_CONCURRENT_WORKERS);
        assert!(
            config.max_concurrent_workers * ENCODED_EXCLUDED_FN_ID_BYTES
                <= EXCLUDED_FN_IDS_MESSAGE_BUDGET
        );
    }

    #[test]
    fn excluded_fn_id_wire_size_matches_budget_calculation() {
        let request = GetWorkRequest {
            excluded_fn_ids: vec!["00000000-0000-0000-0000-000000000000".to_string()],
            ..Default::default()
        };

        assert_eq!(request.encoded_len(), ENCODED_EXCLUDED_FN_ID_BYTES);
    }

    #[test]
    fn rejects_max_concurrent_workers_over_grpc_budget() {
        let err = serde_json::from_value::<FnConsumerConfig>(config_with_max_concurrent_workers(
            MAX_CONCURRENT_WORKERS + 1,
        ))
        .unwrap_err();

        assert!(err.to_string().contains(&format!(
            "max_concurrent_workers must not exceed {MAX_CONCURRENT_WORKERS}"
        )));
    }
}
