use serde::Deserialize;

#[derive(Deserialize)]
/// The configuration for the chosen storage.
/// # Options
/// - S3: The configuration for the s3 storage.
/// # Notes
/// See config.rs in the root of the worker crate for an example of how to use
/// config files to configure the worker.
pub(crate) enum StorageConfig {
    S3(S3StorageConfig),
}

#[derive(Deserialize)]
/// The configuration for the s3 storage type
/// # Fields
/// - bucket: The name of the bucket to use.
pub(crate) struct S3StorageConfig {
    pub(crate) bucket: String,
}
