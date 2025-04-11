use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Serialize, Clone)]
/// The configuration for the chosen storage.
/// # Options
/// - S3: The configuration for the s3 storage.
/// # Notes
/// See config.rs in the root of the worker crate for an example of how to use
/// config files to configure the worker.
pub enum StorageConfig {
    // case-insensitive
    #[serde(alias = "object_store")]
    ObjectStore(ObjectStoreConfig),
    #[serde(alias = "s3")]
    S3(S3StorageConfig),
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

#[derive(Deserialize, Debug, Clone, Serialize)]
pub enum ObjectStoreType {
    #[serde(alias = "minio")]
    Minio,
    #[serde(alias = "s3")]
    S3,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct ObjectStoreBucketConfig {
    pub name: String,
    pub r#type: ObjectStoreType,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct ObjectStoreConfig {
    pub bucket: ObjectStoreBucketConfig,
    pub upload_part_size_bytes: u64,
    pub download_part_size_bytes: u64,
    pub max_concurrent_requests: usize,
}

#[derive(Default, Deserialize, PartialEq, Debug, Clone, Serialize)]
pub enum S3CredentialsConfig {
    #[default]
    Minio,
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
        30000
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

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct AdmissionControlledS3StorageConfig {
    #[serde(default)]
    pub s3_config: S3StorageConfig,
    #[serde(default)]
    pub rate_limiting_policy: RateLimitingConfig,
}

impl Default for AdmissionControlledS3StorageConfig {
    fn default() -> Self {
        AdmissionControlledS3StorageConfig {
            s3_config: S3StorageConfig {
                bucket: S3StorageConfig::default_bucket(),
                credentials: S3CredentialsConfig::default(),
                connect_timeout_ms: S3StorageConfig::default_connect_timeout_ms(),
                request_timeout_ms: S3StorageConfig::default_request_timeout_ms(),
                upload_part_size_bytes: S3StorageConfig::default_upload_part_size_bytes(),
                download_part_size_bytes: S3StorageConfig::default_download_part_size_bytes(),
            },
            rate_limiting_policy: RateLimitingConfig::default(),
        }
    }
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
