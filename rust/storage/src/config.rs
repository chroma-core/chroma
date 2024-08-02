use serde::Deserialize;

#[derive(Deserialize, Debug)]
/// The configuration for the chosen storage.
/// # Options
/// - S3: The configuration for the s3 storage.
/// # Notes
/// See config.rs in the root of the worker crate for an example of how to use
/// config files to configure the worker.
pub enum StorageConfig {
    // case-insensitive
    #[serde(alias = "s3")]
    S3(S3StorageConfig),
    #[serde(alias = "local")]
    Local(LocalStorageConfig),
}

#[derive(Deserialize, PartialEq, Debug)]
pub enum S3CredentialsConfig {
    Minio,
    AWS,
}

#[derive(Deserialize, Debug)]
/// The configuration for the s3 storage type
/// # Fields
/// - bucket: The name of the bucket to use.
pub struct S3StorageConfig {
    pub(crate) bucket: String,
    pub(crate) credentials: S3CredentialsConfig,
    pub(crate) connect_timeout_ms: u64,
    pub(crate) request_timeout_ms: u64,
    pub(crate) upload_part_size_bytes: usize,
}

#[derive(Deserialize, Debug)]
/// The configuration for the local storage type
/// # Fields
/// - root: The root directory to use for storage.
/// # Notes
/// The root directory is the directory where files will be stored.
/// This is not intended to be used in production.
pub struct LocalStorageConfig {
    pub(crate) root: String,
}
