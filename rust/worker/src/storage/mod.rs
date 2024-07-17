use self::config::StorageConfig;
use self::s3::S3GetError;
use self::stream::ByteStreamItem;
use crate::config::Configurable;
use crate::errors::{ChromaError, ErrorCodes};
pub(crate) mod config;
pub(crate) mod local;
pub(crate) mod s3;
pub(crate) mod stream;
use futures::Stream;
use thiserror::Error;

#[derive(Clone)]
pub(crate) enum Storage {
    S3(s3::S3Storage),
    Local(local::LocalStorage),
}

#[derive(Error, Debug)]
pub enum GetError {
    #[error("No such key: {0}")]
    NoSuchKey(String),
    #[error("S3 error: {0}")]
    S3Error(#[from] S3GetError),
    #[error("Local storage error: {0}")]
    LocalError(String),
}

impl ChromaError for GetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetError::NoSuchKey(_) => ErrorCodes::NotFound,
            GetError::S3Error(_) => ErrorCodes::Internal,
            GetError::LocalError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum PutError {
    #[error("S3 error: {0}")]
    S3Error(#[from] s3::S3PutError),
    #[error("Local storage error: {0}")]
    LocalError(String),
}

impl ChromaError for PutError {
    fn code(&self) -> ErrorCodes {
        match self {
            PutError::S3Error(_) => ErrorCodes::Internal,
            PutError::LocalError(_) => ErrorCodes::Internal,
        }
    }
}

impl Storage {
    pub(crate) async fn get(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = ByteStreamItem> + Unpin + Send>, GetError> {
        match self {
            Storage::S3(s3) => {
                let res = s3.get(key).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => match e {
                        S3GetError::NoSuchKey(_) => Err(GetError::NoSuchKey(key.to_string())),
                        _ => Err(GetError::S3Error(e)),
                    },
                }
            }
            Storage::Local(local) => {
                let res = local.get(key).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => Err(GetError::LocalError(e)),
                }
            }
        }
    }

    pub(crate) async fn put_file(&self, key: &str, path: &str) -> Result<(), PutError> {
        match self {
            Storage::S3(s3) => s3
                .put_file(key, path)
                .await
                .map_err(|e| PutError::S3Error(e)),
            Storage::Local(local) => local
                .put_file(key, path)
                .await
                .map_err(|e| PutError::LocalError(e)),
        }
    }

    pub(crate) async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), PutError> {
        match self {
            Storage::S3(s3) => s3
                .put_bytes(key, bytes)
                .await
                .map_err(|e| PutError::S3Error(e)),
            Storage::Local(local) => local
                .put_bytes(key, &bytes)
                .await
                .map_err(|e| PutError::LocalError(e)),
        }
    }
}

pub(crate) async fn from_config(config: &StorageConfig) -> Result<Storage, Box<dyn ChromaError>> {
    match &config {
        StorageConfig::S3(_) => Ok(Storage::S3(s3::S3Storage::try_from_config(config).await?)),
        StorageConfig::Local(_) => Ok(Storage::Local(
            local::LocalStorage::try_from_config(config).await?,
        )),
    }
}
