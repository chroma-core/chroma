use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Serialize, Clone)]
/// The configuration for the chosen storage.
/// # Options
/// - S3: The configuration for the s3 storage.
/// - Object: The configuration for the object storage.
/// - Local: The configuration for local filesystem storage.
/// - AdmissionControlledS3: S3 with rate limiting and request coalescing.
/// # Notes
/// See config.rs in the root of the worker crate for an example of how to use
/// config files to configure the worker.
pub enum StorageConfig {
    #[serde(alias = "s3")]
    S3(S3StorageConfig),
    #[serde(alias = "object")]
    Object(ObjectStorageConfig),
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
pub enum ObjectStorageProvider {
    /// GCS uses Application Default Credentials (ADC) automatically
    GCS,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
/// The configuration for the ObjectStorage type
/// # Fields
/// - bucket: The name of the bucket to use.
/// - connect_timeout_ms: Connection timeout in milliseconds.
/// - download_part_size_bytes: Size of each part for parallel range downloads.
/// - provider: Which backend to use for storage.
/// - request_retry_count: Number of retry attempts for failed requests.
/// - request_timeout_ms: Request timeout in milliseconds.
/// - upload_part_size_bytes: Size of each part in multipart uploads.
pub struct ObjectStorageConfig {
    #[serde(default = "ObjectStorageConfig::default_bucket")]
    pub bucket: String,
    #[serde(default = "ObjectStorageConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "ObjectStorageConfig::default_download_part_size_bytes")]
    pub download_part_size_bytes: u64,
    #[serde(default = "ObjectStorageConfig::default_provider")]
    pub provider: ObjectStorageProvider,
    #[serde(default = "ObjectStorageConfig::default_request_retry_count")]
    pub request_retry_count: usize,
    #[serde(default = "ObjectStorageConfig::default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "ObjectStorageConfig::default_upload_part_size_bytes")]
    pub upload_part_size_bytes: u64,
}

impl ObjectStorageConfig {
    fn default_bucket() -> String {
        "chroma-storage".to_string()
    }

    fn default_connect_timeout_ms() -> u64 {
        5000
    }

    fn default_download_part_size_bytes() -> u64 {
        8 * 1024 * 1024 // 8 MB
    }

    fn default_provider() -> ObjectStorageProvider {
        ObjectStorageProvider::GCS
    }

    fn default_request_retry_count() -> usize {
        3
    }

    fn default_request_timeout_ms() -> u64 {
        60000
    }

    fn default_upload_part_size_bytes() -> u64 {
        512 * 1024 * 1024 // 512 MB
    }
}

impl Default for ObjectStorageConfig {
    fn default() -> Self {
        ObjectStorageConfig {
            bucket: Self::default_bucket(),
            connect_timeout_ms: Self::default_connect_timeout_ms(),
            download_part_size_bytes: Self::default_download_part_size_bytes(),
            provider: Self::default_provider(),
            request_retry_count: Self::default_request_retry_count(),
            request_timeout_ms: Self::default_request_timeout_ms(),
            upload_part_size_bytes: Self::default_upload_part_size_bytes(),
        }
    }
}
