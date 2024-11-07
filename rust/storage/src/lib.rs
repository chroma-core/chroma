use self::config::StorageConfig;
use self::s3::S3GetError;
use self::stream::ByteStreamItem;
use admissioncontrolleds3::AdmissionControlledS3StorageError;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};

pub mod admissioncontrolleds3;
pub mod config;
pub mod local;
pub mod s3;
pub mod stream;
use config::{InjectedLatencyConfig, StorageConfigKind};
use futures::Stream;
use local::LocalStorage;
use std::{path::Path, sync::Arc};
use tempfile::TempDir;
use thiserror::Error;

#[derive(Clone)]
enum StorageKind {
    S3(s3::S3Storage),
    Local(local::LocalStorage),
    AdmissionControlledS3(admissioncontrolleds3::AdmissionControlledS3Storage),
}

#[derive(Clone)]
pub struct Storage {
    kind: StorageKind,
    injected_latency: Option<InjectedLatencyConfig>,
}

#[derive(Error, Debug, Clone)]
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
    pub fn new_test_storage() -> Self {
        Storage {
            kind: StorageKind::Local(LocalStorage::new(
                TempDir::new()
                    .expect("Should be able to create a temporary directory.")
                    .into_path()
                    .to_str()
                    .expect("Should be able to convert temporary directory path to string"),
            )),
            injected_latency: None,
        }
    }

    pub fn new_test_storage_at<P: AsRef<Path>>(path: P) -> Self {
        Storage {
            kind: StorageKind::Local(LocalStorage::new(
                path.as_ref()
                    .to_str()
                    .expect("Should be able to convert path to string"),
            )),
            injected_latency: None,
        }
    }

    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, GetError> {
        if let Some(latency) = &self.injected_latency {
            tokio::time::sleep(latency.min_put_latency).await;
        }

        match &self.kind {
            StorageKind::S3(s3) => {
                let res = s3.get(key).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => match e {
                        S3GetError::NoSuchKey(_) => Err(GetError::NoSuchKey(key.to_string())),
                        _ => Err(GetError::S3Error(e)),
                    },
                }
            }
            StorageKind::Local(local) => {
                let res = local.get(key).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => Err(GetError::LocalError(e)),
                }
            }
            StorageKind::AdmissionControlledS3(admission_controlled_storage) => {
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
        if let Some(latency) = &self.injected_latency {
            tokio::time::sleep(latency.min_put_latency).await;
        }

        match &self.kind {
            StorageKind::S3(s3) => {
                let res = s3.get_parallel(key).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => match e {
                        S3GetError::NoSuchKey(_) => Err(GetError::NoSuchKey(key.to_string())),
                        _ => Err(GetError::S3Error(e)),
                    },
                }
            }
            StorageKind::Local(local) => {
                let res = local.get(key).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => Err(GetError::LocalError(e)),
                }
            }
            StorageKind::AdmissionControlledS3(admission_controlled_storage) => {
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

    // TODO: Remove this once the upstream switches to consume non-streaming.
    pub async fn get_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = ByteStreamItem> + Unpin + Send>, GetError> {
        if let Some(latency) = &self.injected_latency {
            tokio::time::sleep(latency.min_put_latency).await;
        }

        match &self.kind {
            StorageKind::S3(s3) => {
                let res = s3.get_stream(key).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => match e {
                        S3GetError::NoSuchKey(_) => Err(GetError::NoSuchKey(key.to_string())),
                        _ => Err(GetError::S3Error(e)),
                    },
                }
            }
            StorageKind::Local(local) => {
                let res = local.get_stream(key).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => Err(GetError::LocalError(e)),
                }
            }
            StorageKind::AdmissionControlledS3(admission_controlled_storage) => {
                let res = admission_controlled_storage.get_stream(key).await;
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
        if let Some(latency) = &self.injected_latency {
            tokio::time::sleep(latency.min_put_latency).await;
        }

        match &self.kind {
            StorageKind::S3(s3) => s3.put_file(key, path).await.map_err(PutError::S3Error),
            StorageKind::Local(local) => local
                .put_file(key, path)
                .await
                .map_err(PutError::LocalError),
            StorageKind::AdmissionControlledS3(as3) => {
                as3.put_file(key, path).await.map_err(PutError::S3Error)
            }
        }
    }

    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), PutError> {
        if let Some(latency) = &self.injected_latency {
            tokio::time::sleep(latency.min_put_latency).await;
        }

        match &self.kind {
            StorageKind::S3(s3) => s3.put_bytes(key, bytes).await.map_err(PutError::S3Error),
            StorageKind::Local(local) => local
                .put_bytes(key, &bytes)
                .await
                .map_err(PutError::LocalError),
            StorageKind::AdmissionControlledS3(as3) => {
                as3.put_bytes(key, bytes).await.map_err(PutError::S3Error)
            }
        }
    }
}

pub async fn from_config(config: &StorageConfig) -> Result<Storage, Box<dyn ChromaError>> {
    let kind = match &config.kind {
        StorageConfigKind::S3(_) => Ok(StorageKind::S3(
            s3::S3Storage::try_from_config(config).await?,
        )),
        StorageConfigKind::Local(_) => Ok(StorageKind::Local(
            local::LocalStorage::try_from_config(config).await?,
        )),
        StorageConfigKind::AdmissionControlledS3(_) => Ok(StorageKind::AdmissionControlledS3(
            admissioncontrolleds3::AdmissionControlledS3Storage::try_from_config(config).await?,
        )),
    };

    Ok(Storage {
        kind: kind?,
        injected_latency: config.inject_latency.clone(),
    })
}

#[cfg(test)]
mod tests {
    use crate::config::InjectedLatencyConfig;

    use super::*;
    use config::LocalStorageConfig;
    use futures::StreamExt;
    use rand::{Rng, SeedableRng};
    use std::{io::Write, time::Duration};
    use tempfile::NamedTempFile;

    fn generate_file(file_size: usize) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();

        let mut rng = rand_xorshift::XorShiftRng::seed_from_u64(0);
        let mut remaining_file_size = file_size;

        while remaining_file_size > 0 {
            let chunk_size = std::cmp::min(remaining_file_size, 4096);
            let mut chunk = vec![0u8; chunk_size];
            rng.try_fill(&mut chunk[..]).unwrap();
            temp_file.write_all(&chunk).unwrap();
            remaining_file_size -= chunk_size;
        }

        temp_file
    }

    #[tokio::test]
    async fn test_latency_injection() {
        let latency = InjectedLatencyConfig {
            min_put_latency: Duration::from_millis(1000),
            min_get_latency: Duration::from_millis(1000),
        };

        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = StorageConfig {
            kind: StorageConfigKind::Local(LocalStorageConfig {
                root: temp_dir.path().to_str().unwrap().to_string(),
            }),
            inject_latency: Some(latency.clone()),
        };

        let storage = from_config(&config).await.unwrap();

        let file = generate_file(1024);

        // Test put_file()
        let now = std::time::Instant::now();
        storage
            .put_file("test", file.path().to_str().unwrap())
            .await
            .unwrap();
        let put_duration = now.elapsed();
        assert!(
            put_duration >= latency.min_put_latency,
            "put_file() does not respect min_put_latency_ms: {:?}",
            put_duration
        );

        // Test put_bytes()
        let now = std::time::Instant::now();
        storage
            .put_bytes("test", "test".as_bytes().to_vec())
            .await
            .unwrap();
        let put_duration = now.elapsed();
        assert!(
            put_duration >= latency.min_put_latency,
            "put_bytes() does not respect min_put_latency_ms: {:?}",
            put_duration
        );

        // Test get()
        let now = std::time::Instant::now();
        let _ = storage.get("test").await.unwrap();
        let get_duration = now.elapsed();
        assert!(
            get_duration >= latency.min_get_latency,
            "get() does not respect min_get_latency_ms: {:?}",
            get_duration
        );

        // Test get_stream()
        let now = std::time::Instant::now();
        let mut stream = storage.get_stream("test").await.unwrap();
        while stream.next().await.is_some() {}
        let get_duration = now.elapsed();
        assert!(
            get_duration >= latency.min_get_latency,
            "get_stream() does not respect min_get_latency_ms: {:?}",
            get_duration
        );

        // Test get_parallel()
        let now = std::time::Instant::now();
        let _ = storage.get_parallel("test").await.unwrap();
        let get_duration = now.elapsed();
        assert!(
            get_duration >= latency.min_get_latency,
            "get_parallel() does not respect min_get_latency_ms: {:?}",
            get_duration
        );
    }
}
