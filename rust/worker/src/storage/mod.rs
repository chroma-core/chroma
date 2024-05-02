use self::config::StorageConfig;
use crate::config::Configurable;
use crate::errors::ChromaError;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncBufRead;
pub(crate) mod config;
pub(crate) mod local;
pub(crate) mod s3;

#[derive(Clone)]
pub(crate) enum Storage {
    S3(s3::S3Storage),
    Local(local::LocalStorage),
}

impl Storage {
    pub(crate) async fn get(
        &self,
        key: &str,
    ) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, String> {
        match self {
            Storage::S3(s3) => s3.get(key).await,
            Storage::Local(local) => local.get(key).await,
        }
    }

    pub(crate) async fn put_file(&self, key: &str, path: &str) -> Result<(), String> {
        match self {
            Storage::S3(s3) => s3.put_file(key, path).await,
            Storage::Local(local) => local.put_file(key, path).await,
        }
    }

    pub(crate) async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        match self {
            Storage::S3(s3) => s3.put_bytes(key, bytes).await,
            Storage::Local(local) => local.put_bytes(key, &bytes).await,
        }
    }
}

pub(crate) async fn from_config(
    config: &StorageConfig,
) -> Result<Box<Storage>, Box<dyn ChromaError>> {
    match &config {
        StorageConfig::S3(_) => Ok(Box::new(Storage::S3(
            s3::S3Storage::try_from_config(config).await?,
        ))),
        StorageConfig::Local(_) => Ok(Box::new(Storage::Local(
            local::LocalStorage::try_from_config(config).await?,
        ))),
    }
}
