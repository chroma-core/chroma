use std::ops::Range;
use std::sync::Arc;

use chroma_error::ChromaError;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore as ObjectStoreTrait, PutOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

use super::{ETag, PathError, StorageConfigError, StorageError};

impl From<object_store::Error> for StorageError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::Generic { store: _, source } => StorageError::Generic {
                source: source.into(),
            },
            object_store::Error::NotFound { path, source } => StorageError::NotFound {
                path,
                source: source.into(),
            },
            object_store::Error::InvalidPath { source } => match source {
                object_store::path::Error::NonUnicode { path, source } => {
                    StorageError::InvalidPath {
                        source: PathError::NonUnicode { path, source },
                    }
                }
                _ => StorageError::Generic {
                    source: Arc::new(source),
                },
            },
            object_store::Error::JoinError { source: _ } => StorageError::JoinError,
            object_store::Error::NotSupported { source } => StorageError::NotSupported {
                source: source.into(),
            },
            object_store::Error::AlreadyExists { path, source } => StorageError::AlreadyExists {
                path,
                source: source.into(),
            },
            object_store::Error::Precondition { path, source } => StorageError::Precondition {
                path,
                source: source.into(),
            },
            object_store::Error::NotModified { path, source } => StorageError::NotModified {
                path,
                source: source.into(),
            },
            object_store::Error::NotImplemented => StorageError::NotImplemented,
            object_store::Error::PermissionDenied { path, source } => {
                StorageError::PermissionDenied {
                    path,
                    source: source.into(),
                }
            }
            object_store::Error::Unauthenticated { path, source } => {
                StorageError::Unauthenticated {
                    path,
                    source: source.into(),
                }
            }
            object_store::Error::UnknownConfigurationKey { store, key } => {
                StorageError::UnknownConfigurationKey { store, key }
            }
            err => StorageError::Generic {
                source: Arc::new(err),
            },
        }
    }
}

#[derive(Clone)]
pub struct ObjectStore {
    object_store: Arc<dyn ObjectStoreTrait>,
    upload_part_size_bytes: u64,
    #[allow(dead_code)]
    download_part_size_bytes: u64,
}

impl ObjectStore {
    pub async fn try_from_config(
        config: &super::config::ObjectStoreConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config.bucket.r#type {
            super::config::ObjectStoreType::Minio => {
                tracing::info!(
                    "Creating Minio object store with bucket: {}",
                    config.bucket.name
                );
                let object_store = object_store::aws::AmazonS3Builder::new()
                    .with_region("us-east-1")
                    .with_endpoint("http://localhost:9000")
                    .with_bucket_name(&config.bucket.name)
                    .with_access_key_id("minio")
                    .with_secret_access_key("minio123")
                    .with_allow_http(true)
                    .build()
                    .map_err(|err| {
                        tracing::error! {"Failed to create object store: {:?}", err};
                        StorageConfigError::InvalidStorageConfig.boxed()
                    })?;
                let object_store = object_store::limit::LimitStore::new(
                    object_store,
                    config.max_concurrent_requests,
                );
                Ok(ObjectStore {
                    object_store: Arc::new(object_store),
                    upload_part_size_bytes: config.upload_part_size_bytes,
                    download_part_size_bytes: config.download_part_size_bytes,
                })
            }
            super::config::ObjectStoreType::S3 => {
                tracing::info!(
                    "Creating S3 object store with bucket: {}",
                    config.bucket.name
                );
                let object_store = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(&config.bucket.name)
                    .build()
                    .map_err(|err| {
                        tracing::error! {"Failed to create object store: {:?}", err};
                        StorageConfigError::InvalidStorageConfig.boxed()
                    })?;
                let object_store = object_store::limit::LimitStore::new(
                    object_store,
                    config.max_concurrent_requests,
                );
                Ok(ObjectStore {
                    object_store: Arc::new(object_store),
                    upload_part_size_bytes: config.upload_part_size_bytes,
                    download_part_size_bytes: config.download_part_size_bytes,
                })
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, StorageError> {
        // tracing::info!("ObjectStore::get called with key: {}", key);
        Ok(self
            .object_store
            .get_opts(&Path::from(key), GetOptions::default())
            .await?
            .bytes()
            .await?
            .to_vec()
            .into())
    }

    pub async fn get_with_e_tag(
        &self,
        _: &str,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        Err(StorageError::NotImplemented)
    }

    pub async fn get_parallel(&self, key: &str) -> Result<Arc<Vec<u8>>, StorageError> {
        let meta = self.object_store.head(&Path::from(key)).await?;
        let file_size = meta.size;
        let mut pieces = vec![];
        let mut output_buffer: Vec<u8> = vec![0; file_size];
        let mut output_slices = output_buffer
            .chunks_mut(
                self.download_part_size_bytes
                    .try_into()
                    .expect("u64 should fit usize"),
            )
            .collect::<Vec<_>>();
        for (idx, output_slice) in output_slices.iter_mut().enumerate() {
            let start = idx as u64 * self.download_part_size_bytes;
            let limit = start + output_slice.len() as u64;
            let start = start.try_into().expect("u64 should fit usize");
            let limit = limit.try_into().expect("u64 should fit usize");
            let options = GetOptions {
                range: Some(GetRange::Bounded(Range { start, end: limit })),
                ..Default::default()
            };
            let object_store = Arc::clone(&self.object_store);
            let path = Path::from(key).to_owned();
            let fut = async move { object_store.get_opts(&path, options).await };
            pieces.push(fut);
        }
        for piece in futures::future::join_all(pieces).await {
            if let Err(err) = piece {
                return Err(err.into());
            }
        }
        Ok(Arc::new(output_buffer))
    }

    pub async fn put_file(
        &self,
        key: &str,
        path: &str,
        _: crate::PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let multipart = self.object_store.put_multipart(&Path::from(key)).await?;
        let multipart = Arc::new(Mutex::new(multipart));
        let file_size = tokio::fs::metadata(path)
            .await
            .map_err(|err| StorageError::Generic {
                source: Arc::new(err),
            })?
            .len();
        let path = path.to_string();
        async fn read_from_part_of_file(
            path: &str,
            offset: u64,
            length: u64,
        ) -> Result<Vec<u8>, std::io::Error> {
            let mut file = tokio::fs::File::open(path).await?;
            let mut buffer = vec![0; length as usize];
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            file.read_exact(&mut buffer).await?;
            Ok(buffer)
        }
        let part_count = file_size.div_ceil(self.upload_part_size_bytes);
        let mut pieces = vec![];
        for i in 0..part_count {
            let path = path.clone();
            let limit = std::cmp::min(
                file_size - i * self.upload_part_size_bytes,
                self.upload_part_size_bytes,
            );
            let multipart = Arc::clone(&multipart);
            pieces.push(async move {
                let bytes =
                    match read_from_part_of_file(&path, i * self.upload_part_size_bytes, limit)
                        .await
                    {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            return Err(StorageError::Generic {
                                source: Arc::new(e),
                            })
                        }
                    };
                let mut multipart = multipart.lock().await;
                multipart.put_part(bytes.into()).await?;
                Ok(())
            })
        }
        futures::future::try_join_all(pieces).await?;
        let mut multipart = multipart.lock().await;
        let result = multipart.complete().await?;
        Ok(result.e_tag.map(ETag))
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: crate::PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let mut object_store_put_options = PutOptions::default();
        if options.if_not_exists {
            object_store_put_options.mode = object_store::PutMode::Create;
        }
        if let Some(etag) = options.if_match.as_ref() {
            object_store_put_options.mode =
                object_store::PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some(etag.0.clone()),
                    version: None,
                });
        }
        // tracing::warn!("put_bytes key: {}, path: {:?}", key, Path::from(key));
        self.object_store
            .put_opts(&Path::from(key), bytes.into(), object_store_put_options)
            .await?;
        Ok(None)
    }

    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        tracing::info!(key = %key, "Deleting object");

        match self.object_store.delete(&Path::from(key)).await {
            Ok(_) => {
                tracing::info!(key = %key, "Successfully deleted object");
                Ok(())
            }
            Err(e) => {
                tracing::error!(error = %e, key = %key, "Failed to delete object");
                Err(e.into())
            }
        }
    }

    pub async fn rename(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        tracing::info!(src = %src_key, dst = %dst_key, "Renaming object");

        // Copy the object
        match self
            .object_store
            .copy(&Path::from(src_key), &Path::from(dst_key))
            .await
        {
            Ok(_) => {
                tracing::info!(src = %src_key, dst = %dst_key, "Successfully copied object");
                // After successful copy, delete the original
                match self.delete(src_key).await {
                    Ok(_) => {
                        tracing::info!(src = %src_key, dst = %dst_key, "Successfully renamed object");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!(error = %e, src = %src_key, "Failed to delete source object after copy");
                        Err(e)
                    }
                }
            }
            Err(e) => {
                tracing::error!(error = %e, src = %src_key, dst = %dst_key, "Failed to copy object");
                Err(e.into())
            }
        }
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut files = Vec::new();
        let mut stream = self.object_store.list(Some(&Path::from(prefix)));

        while let Some(obj) = stream.next().await {
            match obj {
                Ok(obj) => {
                    files.push(obj.location.to_string());
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use crate::PutOptions;

    use super::*;

    #[test]
    fn usize_u64() {
        // We assume this and will panic if not true, so test it.
        assert_eq!(u64::MAX as usize as u64, usize::MAX as u64);
    }

    fn get_object_store() -> ObjectStore {
        ObjectStore {
            object_store: Arc::new(object_store::memory::InMemory::new()),
            upload_part_size_bytes: 1024 * 1024 * 5,
            download_part_size_bytes: 1024 * 1024 * 5,
        }
    }

    #[tokio::test]
    async fn put_get() {
        let object_store = get_object_store();
        let key = "test";
        let bytes = b"test data".to_vec();
        object_store
            .put_bytes(key, bytes.clone(), crate::PutOptions::default())
            .await
            .unwrap();
        let result = object_store.get(key).await.unwrap();
        assert_eq!(result, bytes.into());
    }

    #[tokio::test]
    async fn get_parallel() {
        let object_store = get_object_store();
        let key = "test";
        let bytes = b"test data AaAaZzZz"
            .iter()
            .copied()
            .cycle()
            .take(1024 * 1024 * 50)
            .collect::<Vec<_>>();
        object_store
            .put_bytes(key, bytes.clone(), crate::PutOptions::default())
            .await
            .unwrap();
        let result = object_store.get(key).await.unwrap();
        assert_eq!(result, bytes.into());
    }

    #[tokio::test]
    async fn put_file() {
        let object_store = get_object_store();
        let key = "test";
        let bytes = b"test data".to_vec();
        let path = "test_file";
        tokio::fs::write(path, &bytes).await.unwrap();
        object_store
            .put_file(key, path, PutOptions::default())
            .await
            .unwrap();
        let result = object_store.get(key).await.unwrap();
        assert_eq!(result, bytes.into());
        std::fs::remove_file(path).unwrap();
    }
}
