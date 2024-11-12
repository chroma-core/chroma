use std::sync::Arc;

use self::config::StorageConfig;
use self::s3::S3GetError;
use admissioncontrolleds3::AdmissionControlledS3StorageError;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};

pub mod admission_controller;
pub mod admissioncontrolleds3;
pub mod caching;
pub mod config;
pub mod evicting;
pub mod local;
pub mod object_store;
pub mod s3;
pub mod stream;
use local::LocalStorage;
use tempfile::TempDir;
use thiserror::Error;

#[derive(Clone)]
pub enum Storage {
    ObjectStore(object_store::ObjectStore),
    S3(s3::S3Storage),
    Local(local::LocalStorage),
    AdmissionControlledS3(admissioncontrolleds3::AdmissionControlledS3Storage),
}

#[derive(Error, Debug, Clone)]
pub enum GetError {
    #[error("No such key: {0}")]
    NoSuchKey(String),
    #[error("ObjectStore error: {0}")]
    ObjectStoreError(Arc<::object_store::Error>),
    #[error("S3 error: {0}")]
    S3Error(#[from] S3GetError),
    #[error("Local storage error: {0}")]
    LocalError(String),
}

impl ChromaError for GetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetError::NoSuchKey(_) => ErrorCodes::NotFound,
            GetError::ObjectStoreError(_) => ErrorCodes::Internal,
            GetError::S3Error(_) => ErrorCodes::Internal,
            GetError::LocalError(_) => ErrorCodes::Internal,
        }
    }
}

impl From<::object_store::Error> for GetError {
    fn from(e: ::object_store::Error) -> Self {
        match e {
            ::object_store::Error::NotFound { path, source: _ } => {
                GetError::NoSuchKey(path.to_string())
            }
            _ => GetError::ObjectStoreError(Arc::new(e)),
        }
    }
}

#[derive(Error, Debug)]
pub enum PutError {
    #[error("ObjectStore error: {0}")]
    ObjectStoreError(Arc<::object_store::Error>),
    #[error("S3 error: {0}")]
    S3Error(#[from] s3::S3PutError),
    #[error("Local storage error: {0}")]
    LocalError(String),
}

impl ChromaError for PutError {
    fn code(&self) -> ErrorCodes {
        match self {
            PutError::ObjectStoreError(_) => ErrorCodes::Internal,
            PutError::S3Error(_) => ErrorCodes::Internal,
            PutError::LocalError(_) => ErrorCodes::Internal,
        }
    }
}

impl From<std::io::Error> for PutError {
    fn from(e: std::io::Error) -> Self {
        Self::LocalError(e.to_string())
    }
}

impl From<::object_store::Error> for PutError {
    fn from(e: ::object_store::Error) -> Self {
        Self::ObjectStoreError(Arc::new(e))
    }
}

#[derive(Error, Debug)]
pub enum StorageConfigError {
    #[error("Invalid storage config")]
    InvalidStorageConfig,
    #[error("Failed to create bucket: {0}")]
    FailedToCreateBucket(String),
}

impl ChromaError for StorageConfigError {
    fn code(&self) -> ErrorCodes {
        match self {
            StorageConfigError::InvalidStorageConfig => ErrorCodes::InvalidArgument,
            StorageConfigError::FailedToCreateBucket(_) => ErrorCodes::Internal,
        }
    }
}

impl Storage {
    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, GetError> {
        match self {
            Storage::ObjectStore(object_store) => object_store.get(key).await,
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
            Storage::AdmissionControlledS3(admission_controlled_storage) => {
                let res = admission_controlled_storage.get(key.to_string()).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => match e {
                        AdmissionControlledS3StorageError::S3GetError(e) => match e {
                            S3GetError::NoSuchKey(_) => Err(GetError::NoSuchKey(key.to_string())),
                            _ => Err(GetError::S3Error(e)),
                        },
                    },
                }
            }
        }
    }

    pub async fn get_parallel(&self, key: &str) -> Result<Arc<Vec<u8>>, GetError> {
        match self {
            Storage::ObjectStore(object_store) => object_store.get_parallel(key).await,
            Storage::S3(s3) => {
                let res = s3.get_parallel(key).await;
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
            Storage::AdmissionControlledS3(admission_controlled_storage) => {
                let res = admission_controlled_storage
                    .get_parallel(key.to_string())
                    .await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => match e {
                        AdmissionControlledS3StorageError::S3GetError(e) => match e {
                            S3GetError::NoSuchKey(_) => Err(GetError::NoSuchKey(key.to_string())),
                            _ => Err(GetError::S3Error(e)),
                        },
                    },
                }
            }
        }
    }

    pub async fn put_file(&self, key: &str, path: &str) -> Result<(), PutError> {
        match self {
            Storage::ObjectStore(object_store) => object_store.put_file(key, path).await,
            Storage::S3(s3) => s3.put_file(key, path).await.map_err(PutError::S3Error),
            Storage::Local(local) => local
                .put_file(key, path)
                .await
                .map_err(PutError::LocalError),
            Storage::AdmissionControlledS3(as3) => {
                as3.put_file(key, path).await.map_err(PutError::S3Error)
            }
        }
    }

    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), PutError> {
        match self {
            Storage::ObjectStore(object_store) => object_store.put_bytes(key, bytes).await,
            Storage::S3(s3) => s3.put_bytes(key, bytes).await.map_err(PutError::S3Error),
            Storage::Local(local) => local
                .put_bytes(key, &bytes)
                .await
                .map_err(PutError::LocalError),
            Storage::AdmissionControlledS3(as3) => {
                as3.put_bytes(key, bytes).await.map_err(PutError::S3Error)
            }
        }
    }
}

pub async fn from_config(config: &StorageConfig) -> Result<Storage, Box<dyn ChromaError>> {
    match &config {
        StorageConfig::ObjectStore(config) => Ok(Storage::ObjectStore(
            object_store::ObjectStore::try_from_config(config).await?,
        )),
        StorageConfig::S3(_) => Ok(Storage::S3(s3::S3Storage::try_from_config(config).await?)),
        StorageConfig::Local(_) => Ok(Storage::Local(
            local::LocalStorage::try_from_config(config).await?,
        )),
        StorageConfig::AdmissionControlledS3(_) => Ok(Storage::AdmissionControlledS3(
            admissioncontrolleds3::AdmissionControlledS3Storage::try_from_config(config).await?,
        )),
    }
}

pub fn test_storage() -> Storage {
    Storage::Local(LocalStorage::new(
        TempDir::new()
            .expect("Should be able to create a temporary directory.")
            .into_path()
            .to_str()
            .expect("Should be able to convert temporary directory path to string"),
    ))
}
