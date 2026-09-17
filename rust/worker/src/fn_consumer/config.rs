use serde::{Deserialize, Serialize};

// A canonical UUID in GetWorkRequest.excluded_fn_ids encodes as a one-byte
// field tag, a one-byte length, and 36 bytes of UTF-8 data.
const ENCODED_EXCLUDED_FN_ID_BYTES: usize = 38;
pub(crate) fn validate_max_concurrent_workers(
    max_concurrent_workers: usize,
    max_request_message_size: usize,
) -> Result<(), String> {
    // Keep exclusions within half of the effective request limit so the
    // remaining fields and future wire-contract growth retain ample headroom.
    let max_workers = max_request_message_size / 2 / ENCODED_EXCLUDED_FN_ID_BYTES;
    if max_concurrent_workers > max_workers {
        return Err(format!(
            "max_concurrent_workers must not exceed {max_workers} for a {max_request_message_size}-byte GetWork gRPC request limit"
        ));
    }
    Ok(())
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
    /// Largest request this client will encode. This must not exceed the work
    /// queue service's configured decoding limit.
    #[serde(default = "GrpcWorkQueueConfig::default_max_encoding_message_size")]
    pub max_encoding_message_size: usize,
    #[serde(default = "GrpcWorkQueueConfig::default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
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

    fn default_max_encoding_message_size() -> usize {
        4 * 1024 * 1024
    }

    fn default_max_decoding_message_size() -> usize {
        4 * 1024 * 1024
    }
}

impl Default for GrpcWorkQueueConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            port: Self::default_port(),
            connect_timeout_ms: Self::default_connect_timeout_ms(),
            request_timeout_ms: Self::default_request_timeout_ms(),
            max_encoding_message_size: Self::default_max_encoding_message_size(),
            max_decoding_message_size: Self::default_max_decoding_message_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FnConsumerConfig {
    #[serde(default = "FnConsumerConfig::default_poll_interval_sec")]
    pub poll_interval_sec: u64,
    /// Maximum simultaneous function executions. This is also the maximum
    /// number of in-progress function IDs sent in a GetWork request.
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

    #[test]
    fn accepts_max_concurrent_workers_within_grpc_budget() {
        let message_size = 38_000;
        let max_workers = message_size / 2 / ENCODED_EXCLUDED_FN_ID_BYTES;

        assert!(validate_max_concurrent_workers(max_workers, message_size).is_ok());
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
        let message_size = 38_000;
        let max_workers = message_size / 2 / ENCODED_EXCLUDED_FN_ID_BYTES;
        let err = validate_max_concurrent_workers(max_workers + 1, message_size).unwrap_err();

        assert!(err.to_string().contains(&format!(
            "max_concurrent_workers must not exceed {max_workers}"
        )));
    }
}
