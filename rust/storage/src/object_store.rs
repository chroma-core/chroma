use std::ops::Range;
use std::sync::Arc;

use chroma_error::ChromaError;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, PutOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

use crate::caching::CachingObjectStore;
use crate::non_destructive::NonDestructiveObjectStore;
use crate::{GetError, PutError, SafeObjectStore, StorageConfigError};

#[derive(Clone)]
pub struct ObjectStore {
    object_store: Arc<dyn SafeObjectStore>,
    upload_part_size_bytes: u64,
    #[allow(dead_code)]
    download_part_size_bytes: u64,
}

impl ObjectStore {
    pub async fn try_from_config(
        config: &super::config::ObjectStoreConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let object_store = match &config.bucket.r#type {
            super::config::ObjectStoreType::Minio => {
                let object_store = object_store::aws::AmazonS3Builder::new()
                    .with_region("us-east-1")
                    .with_endpoint("http://minio.chroma:9000")
                    .with_bucket_name(&config.bucket.name)
                    .with_access_key_id("minio")
                    .with_secret_access_key("minio123")
                    .build()
                    .map_err(|err| {
                        tracing::error! {"Failed to create object store: {:?}", err};
                        Box::new(StorageConfigError::InvalidStorageConfig) as _
                    })?;
                Ok(ObjectStore {
                    object_store: Arc::new(object_store),
                    upload_part_size_bytes: config.upload_part_size_bytes,
                    download_part_size_bytes: config.download_part_size_bytes,
                })
            }
            super::config::ObjectStoreType::S3 => {
                let object_store = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(&config.bucket.name)
                    .build()
                    .map_err(|err| {
                        tracing::error! {"Failed to create object store: {:?}", err};
                        Box::new(StorageConfigError::InvalidStorageConfig) as _
                    })?;
                Ok(ObjectStore {
                    object_store: Arc::new(object_store),
                    upload_part_size_bytes: config.upload_part_size_bytes,
                    download_part_size_bytes: config.download_part_size_bytes,
                })
            }
            super::config::ObjectStoreType::Local => {
                let object_store =
                    object_store::local::LocalFileSystem::new_with_prefix(&config.bucket.name)
                        .map_err(|err| {
                            tracing::error! {"Failed to create object store: {:?}", err};
                            Box::new(StorageConfigError::InvalidStorageConfig) as _
                        })?;
                Ok(ObjectStore {
                    object_store: Arc::new(object_store),
                    upload_part_size_bytes: config.upload_part_size_bytes,
                    download_part_size_bytes: config.download_part_size_bytes,
                })
            }
        }?;
        if let Some(cache) = config.cache.as_ref() {
            let cache = Box::pin(Self::try_from_config(cache)).await?;
            // If the backing object store supports delete, wrap it.
            if object_store.object_store.supports_delete() {
                let non_destructive_backing =
                    Arc::new(NonDestructiveObjectStore::new(object_store.object_store));
                let caching_object_store =
                    CachingObjectStore::new(cache.object_store, non_destructive_backing);
                Ok(ObjectStore {
                    object_store: Arc::new(caching_object_store),
                    upload_part_size_bytes: config.upload_part_size_bytes,
                    download_part_size_bytes: config.download_part_size_bytes,
                })
            } else {
                let caching_object_store =
                    CachingObjectStore::new(cache.object_store, object_store.object_store);
                Ok(ObjectStore {
                    object_store: Arc::new(caching_object_store),
                    upload_part_size_bytes: config.upload_part_size_bytes,
                    download_part_size_bytes: config.download_part_size_bytes,
                })
            }
        } else {
            Ok(object_store)
        }
    }

    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, GetError> {
        Ok(self
            .object_store
            .get_opts(&Path::from(key), GetOptions::default())
            .await?
            .bytes()
            .await?
            .to_vec()
            .into())
    }

    pub async fn get_parallel(&self, key: &str) -> Result<Arc<Vec<u8>>, GetError> {
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

    pub async fn put_file(&self, key: &str, path: &str) -> Result<(), PutError> {
        let multipart = self.object_store.put_multipart(&Path::from(key)).await?;
        let multipart = Arc::new(Mutex::new(multipart));
        let file_size = tokio::fs::metadata(path).await?.len();
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
        let part_count =
            (file_size + self.upload_part_size_bytes - 1) / self.upload_part_size_bytes;
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
                            return Err::<(), PutError>(e.into());
                        }
                    };
                let mut multipart = multipart.lock().await;
                multipart.put_part(bytes.into()).await?;
                Ok(())
            })
        }
        futures::future::try_join_all(pieces).await?;
        let mut multipart = multipart.lock().await;
        multipart.complete().await?;
        Ok(())
    }

    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), PutError> {
        self.object_store
            .put_opts(&Path::from(key), bytes.into(), PutOptions::default())
            .await?;
        Ok(())
    }

    pub fn supports_delete(&self) -> bool {
        self.object_store.supports_delete()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{
        ObjectStoreBucketConfig, ObjectStoreConfig, ObjectStoreType, StorageConfig,
    };

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
        object_store.put_bytes(key, bytes.clone()).await.unwrap();
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
        object_store.put_bytes(key, bytes.clone()).await.unwrap();
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
        object_store.put_file(key, path).await.unwrap();
        let result = object_store.get(key).await.unwrap();
        assert_eq!(result, bytes.into());
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn create_minio_object_store_from_config() {
        let config = StorageConfig::ObjectStore(ObjectStoreConfig {
            bucket: ObjectStoreBucketConfig {
                name: "test".to_string(),
                r#type: ObjectStoreType::Minio,
            },
            upload_part_size_bytes: 1024 * 1024 * 5,
            download_part_size_bytes: 1024 * 1024 * 5,
            max_concurrent_requests: 1,
            cache: None,
        });
        let _object_store = crate::from_config(&config).await.unwrap();
    }

    #[tokio::test]
    async fn create_aws_object_store_from_config() {
        let config = StorageConfig::ObjectStore(ObjectStoreConfig {
            bucket: ObjectStoreBucketConfig {
                name: "test".to_string(),
                r#type: ObjectStoreType::S3,
            },
            upload_part_size_bytes: 1024 * 1024 * 5,
            download_part_size_bytes: 1024 * 1024 * 5,
            max_concurrent_requests: 1,
            cache: None,
        });
        let _object_store = crate::from_config(&config).await.unwrap();
    }

    #[tokio::test]
    async fn create_minio_object_store_from_config_with_cache() {
        let config = StorageConfig::ObjectStore(ObjectStoreConfig {
            bucket: ObjectStoreBucketConfig {
                name: "test".to_string(),
                r#type: ObjectStoreType::Minio,
            },
            upload_part_size_bytes: 1024 * 1024 * 5,
            download_part_size_bytes: 1024 * 1024 * 5,
            max_concurrent_requests: 1,
            cache: Some(Box::new(ObjectStoreConfig {
                bucket: ObjectStoreBucketConfig {
                    name: "cache".to_string(),
                    r#type: ObjectStoreType::Minio,
                },
                upload_part_size_bytes: 1024 * 1024 * 5,
                download_part_size_bytes: 1024 * 1024 * 5,
                max_concurrent_requests: 1,
                cache: None,
            })),
        });
        let storage = crate::from_config(&config).await.unwrap();
        assert!(!storage.supports_delete());
    }

    #[tokio::test]
    async fn create_aws_object_store_from_config_with_cache() {
        let config = StorageConfig::ObjectStore(ObjectStoreConfig {
            bucket: ObjectStoreBucketConfig {
                name: "test".to_string(),
                r#type: ObjectStoreType::S3,
            },
            upload_part_size_bytes: 1024 * 1024 * 5,
            download_part_size_bytes: 1024 * 1024 * 5,
            max_concurrent_requests: 1,
            cache: Some(Box::new(ObjectStoreConfig {
                bucket: ObjectStoreBucketConfig {
                    name: "cache".to_string(),
                    r#type: ObjectStoreType::Minio,
                },
                upload_part_size_bytes: 1024 * 1024 * 5,
                download_part_size_bytes: 1024 * 1024 * 5,
                max_concurrent_requests: 1,
                cache: None,
            })),
        });
        let storage = crate::from_config(&config).await.unwrap();
        assert!(!storage.supports_delete());
    }
}
