use serde::Deserialize;
use serde_with::serde_as;
use serde_with::DurationMilliSeconds;
use std::time::Duration;

#[derive(Deserialize, Debug, Clone)]
/// The configuration for the chosen storage.
/// # Options
/// - S3: The configuration for the s3 storage.
/// # Notes
/// See config.rs in the root of the worker crate for an example of how to use
/// config files to configure the worker.
pub enum StorageConfigKind {
    // case-insensitive
    #[serde(alias = "s3")]
    S3(S3StorageConfig),
    #[serde(alias = "local")]
    Local(LocalStorageConfig),
    #[serde(alias = "admissioncontrolleds3")]
    AdmissionControlledS3(AdmissionControlledS3StorageConfig),
}

#[derive(Deserialize, Debug, Clone)]
pub struct StorageConfig {
    #[serde(flatten)]
    pub kind: StorageConfigKind,
    pub inject_latency: Option<InjectedLatencyConfig>,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub enum S3CredentialsConfig {
    Minio,
    AWS,
}

#[serde_as]
#[derive(Deserialize, Debug, Clone)]
pub struct InjectedLatencyConfig {
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(rename = "min_put_latency_ms")]
    pub min_put_latency: Duration,
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(rename = "min_get_latency_ms")]
    pub min_get_latency: Duration,
}

#[serde_as]
#[derive(Deserialize, Debug, Clone)]
/// The configuration for the s3 storage type
/// # Fields
/// - bucket: The name of the bucket to use.
pub struct S3StorageConfig {
    pub bucket: String,
    pub credentials: S3CredentialsConfig,
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(rename = "connect_timeout_ms")]
    pub connect_timeout: Duration,
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(rename = "request_timeout_ms")]
    pub request_timeout: Duration,
    pub upload_part_size_bytes: usize,
    pub download_part_size_bytes: usize,
}

#[derive(Deserialize, Debug, Clone)]
/// The configuration for the local storage type
/// # Fields
/// - root: The root directory to use for storage.
/// # Notes
/// The root directory is the directory where files will be stored.
/// This is not intended to be used in production.
pub struct LocalStorageConfig {
    pub root: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AdmissionControlledS3StorageConfig {
    pub s3_config: S3StorageConfig,
    pub rate_limiting_policy: RateLimitingConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CountBasedPolicyConfig {
    pub max_concurrent_requests: usize,
}

#[derive(Deserialize, Debug, Clone)]
pub enum RateLimitingConfig {
    CountBasedPolicy(CountBasedPolicyConfig),
}
