use std::sync::Arc;

use chroma_error::ChromaError;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore as ObjectStoreTrait, PutOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

use super::{GetError, PutError, StorageConfigError};

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
                let object_store = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(&config.bucket.name)
                    .build()
                    .map_err(|err| {
                        tracing::error! {"Failed to create object store: {:?}", err};
                        Box::new(StorageConfigError::InvalidStorageConfig) as _
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
        // TODO(rescrv, NOCOMMIT): Implement parallel get for object store
        Ok(self
            .object_store
            .get_opts(&Path::from(key), GetOptions::default())
            .await?
            .bytes()
            .await?
            .to_vec()
            .into())
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
