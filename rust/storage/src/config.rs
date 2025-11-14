use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Serialize, Clone)]
/// The configuration for the chosen storage.
/// # Options
/// - S3: The configuration for the s3 storage.
/// - GCS: The configuration for the Google Cloud Storage.
/// - Local: The configuration for local filesystem storage.
/// - AdmissionControlledS3: S3 with rate limiting and request coalescing.
/// # Notes
/// See config.rs in the root of the worker crate for an example of how to use
/// config files to configure the worker.
pub enum StorageConfig {
    #[serde(alias = "s3")]
    S3(S3StorageConfig),
    #[serde(alias = "gcs")]
    GCS(GcsStorageConfig),
    #[serde(alias = "local")]
    Local(LocalStorageConfig),
    #[serde(alias = "admissioncontrolleds3")]
    #[serde(alias = "admission_controlled_s3")]
    AdmissionControlledS3(AdmissionControlledS3StorageConfig),
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::AdmissionControlledS3(AdmissionControlledS3StorageConfig::default())
    }
}

#[derive(Default, Deserialize, PartialEq, Debug, Clone, Serialize)]
pub enum S3CredentialsConfig {
    #[default]
    Minio,
    Localhost,
    AWS,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
/// The configuration for the s3 storage type
/// # Fields
/// - bucket: The name of the bucket to use.
pub struct S3StorageConfig {
    #[serde(default = "S3StorageConfig::default_bucket")]
    pub bucket: String,
    #[serde(default)]
    pub credentials: S3CredentialsConfig,
    #[serde(default = "S3StorageConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "S3StorageConfig::default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "S3StorageConfig::default_request_retry_count")]
    pub request_retry_count: u32,
    #[serde(default = "S3StorageConfig::default_stall_protection_ms")]
    pub stall_protection_ms: u64,
    #[serde(default = "S3StorageConfig::default_upload_part_size_bytes")]
    pub upload_part_size_bytes: usize,
    #[serde(default = "S3StorageConfig::default_download_part_size_bytes")]
    pub download_part_size_bytes: usize,
}

impl S3StorageConfig {
    fn default_bucket() -> String {
        "chroma-storage".to_string()
    }

    fn default_connect_timeout_ms() -> u64 {
        5000
    }

    fn default_request_timeout_ms() -> u64 {
        60000
    }

    fn default_request_retry_count() -> u32 {
        3
    }

    fn default_stall_protection_ms() -> u64 {
        15000
    }

    fn default_upload_part_size_bytes() -> usize {
        5 * 1024 * 1024
    }

    fn default_download_part_size_bytes() -> usize {
        8 * 1024 * 1024
    }
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        S3StorageConfig {
            bucket: S3StorageConfig::default_bucket(),
            credentials: S3CredentialsConfig::default(),
            connect_timeout_ms: S3StorageConfig::default_connect_timeout_ms(),
            request_timeout_ms: S3StorageConfig::default_request_timeout_ms(),
            request_retry_count: S3StorageConfig::default_request_retry_count(),
            stall_protection_ms: S3StorageConfig::default_stall_protection_ms(),
            upload_part_size_bytes: S3StorageConfig::default_upload_part_size_bytes(),
            download_part_size_bytes: S3StorageConfig::default_download_part_size_bytes(),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
/// The configuration for the local storage type
/// # Fields
/// - root: The root directory to use for storage.
/// # Notes
/// The root directory is the directory where files will be stored.
/// This is not intended to be used in production.
pub struct LocalStorageConfig {
    pub root: String,
}

#[derive(Deserialize, Debug, Default, Clone, Serialize)]
pub struct AdmissionControlledS3StorageConfig {
    #[serde(default)]
    pub s3_config: S3StorageConfig,
    #[serde(default)]
    pub rate_limiting_policy: RateLimitingConfig,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct CountBasedPolicyConfig {
    #[serde(default = "CountBasedPolicyConfig::default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(default = "CountBasedPolicyConfig::default_bandwidth_allocation")]
    pub bandwidth_allocation: Vec<f32>,
}

impl CountBasedPolicyConfig {
    fn default_max_concurrent_requests() -> usize {
        30
    }

    fn default_bandwidth_allocation() -> Vec<f32> {
        vec![0.7, 0.3]
    }
}

impl Default for CountBasedPolicyConfig {
    fn default() -> Self {
        CountBasedPolicyConfig {
            max_concurrent_requests: Self::default_max_concurrent_requests(),
            bandwidth_allocation: Self::default_bandwidth_allocation(),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub enum RateLimitingConfig {
    #[serde(alias = "count_based_policy")]
    CountBasedPolicy(CountBasedPolicyConfig),
}

impl Default for RateLimitingConfig {
    fn default() -> Self {
        RateLimitingConfig::CountBasedPolicy(CountBasedPolicyConfig::default())
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
/// The configuration for the GCS storage type
/// # Fields
/// - bucket: The name of the bucket to use.
/// - project_id: GCS project ID. Defaults to "_" which lets GCS infer from the globally unique bucket name.
/// - connect_timeout_ms: Connection timeout in milliseconds.
/// - request_timeout_ms: Request timeout in milliseconds.
/// - request_retry_count: Number of retry attempts for failed requests.
/// - resumable_upload_threshold_bytes: Size threshold for switching to resumable uploads.
/// - resumable_upload_buffer_size_bytes: Buffer size for resumable uploads.
/// # Notes
/// - Authentication uses Application Default Credentials (ADC) automatically.
pub struct GcsStorageConfig {
    #[serde(default = "GcsStorageConfig::default_bucket")]
    pub bucket: String,
    #[serde(default = "GcsStorageConfig::default_project_id")]
    pub project_id: String,
    #[serde(default = "GcsStorageConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "GcsStorageConfig::default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "GcsStorageConfig::default_request_retry_count")]
    pub request_retry_count: u32,
    #[serde(default = "GcsStorageConfig::default_resumable_upload_threshold_bytes")]
    pub resumable_upload_threshold_bytes: usize,
    #[serde(default = "GcsStorageConfig::default_resumable_upload_buffer_size_bytes")]
    pub resumable_upload_buffer_size_bytes: usize,
}

impl GcsStorageConfig {
    fn default_bucket() -> String {
        "chroma-storage".to_string()
    }

    fn default_project_id() -> String {
        "_".to_string()
    }

    fn default_connect_timeout_ms() -> u64 {
        5000
    }

    fn default_request_timeout_ms() -> u64 {
        60000
    }

    fn default_request_retry_count() -> u32 {
        3
    }

    fn default_resumable_upload_threshold_bytes() -> usize {
        8 * 1024 * 1024
    }

    fn default_resumable_upload_buffer_size_bytes() -> usize {
        16 * 1024 * 1024
    }
}

impl Default for GcsStorageConfig {
    fn default() -> Self {
        GcsStorageConfig {
            bucket: Self::default_bucket(),
            project_id: Self::default_project_id(),
            connect_timeout_ms: Self::default_connect_timeout_ms(),
            request_timeout_ms: Self::default_request_timeout_ms(),
            request_retry_count: Self::default_request_retry_count(),
            resumable_upload_threshold_bytes: Self::default_resumable_upload_threshold_bytes(),
            resumable_upload_buffer_size_bytes: Self::default_resumable_upload_buffer_size_bytes(),
        }
    }
}
