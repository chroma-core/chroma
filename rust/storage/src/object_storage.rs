// NOTE: This is mostly a copy paste from the standalone s3 impl, which will be cleaned up later
//! Object storage backend implementation using object_store.
//!
//! ## ETag Implementation Note
//! The `UpdateVersion` struct is serialized to JSON and stored as an ETag string.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::{gcp::GoogleCloudStorageBuilder, GetRange, ObjectStore, PutMode, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::config::ObjectStorageProvider;
use crate::{
    config::{ObjectStorageConfig, StorageConfig},
    s3::DeletedObjects,
    ETag, GetOptions, PutOptions, StorageConfigError, StorageError,
};

impl From<object_store::Error> for StorageError {
    fn from(e: object_store::Error) -> Self {
        match e {
            object_store::Error::NotFound { path, source } => StorageError::NotFound {
                path,
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
            object_store::Error::NotSupported { source } => StorageError::NotSupported {
                source: source.into(),
            },
            object_store::Error::InvalidPath { source } => StorageError::Generic {
                source: Arc::new(source),
            },
            object_store::Error::Generic { store, source } => StorageError::Generic {
                source: Arc::new(std::io::Error::other(format!("{}: {}", store, source))),
            },
            object_store::Error::JoinError { source } => StorageError::Generic {
                source: Arc::new(source),
            },
            object_store::Error::UnknownConfigurationKey { store, key } => {
                StorageError::UnknownConfigurationKey { store, key }
            }
            _ => StorageError::Generic {
                source: Arc::new(e),
            },
        }
    }
}

/// Serializable wrapper for UpdateVersion
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectVersionTag {
    e_tag: Option<String>,
    version: Option<String>,
}

impl From<UpdateVersion> for ObjectVersionTag {
    fn from(uv: UpdateVersion) -> Self {
        Self {
            e_tag: uv.e_tag,
            version: uv.version,
        }
    }
}

impl From<ObjectVersionTag> for UpdateVersion {
    fn from(ovt: ObjectVersionTag) -> Self {
        Self {
            e_tag: ovt.e_tag,
            version: ovt.version,
        }
    }
}

/// Convert UpdateVersion to ETag via serialization
impl TryFrom<&UpdateVersion> for ETag {
    type Error = StorageError;

    fn try_from(uv: &UpdateVersion) -> Result<Self, Self::Error> {
        let serializable: ObjectVersionTag = uv.clone().into();
        serde_json::to_string(&serializable)
            .map(ETag)
            .map_err(|e| StorageError::Generic {
                source: Arc::new(e),
            })
    }
}

/// Convert ETag to UpdateVersion via deserialization
impl TryFrom<&ETag> for UpdateVersion {
    type Error = StorageError;

    fn try_from(etag: &ETag) -> Result<Self, Self::Error> {
        let serializable: ObjectVersionTag =
            serde_json::from_str(&etag.0).map_err(|e| StorageError::Generic {
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid ETag format: {}", e),
                )),
            })?;
        Ok(serializable.into())
    }
}

#[derive(Clone)]
pub struct ObjectStorage {
    pub(crate) bucket: String,
    pub(super) download_part_size_bytes: u64,
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) upload_part_size_bytes: u64,
}

impl ObjectStorage {
    pub async fn new(config: &ObjectStorageConfig) -> Result<Self, Box<dyn ChromaError>> {
        if config.download_part_size_bytes == 0 || config.upload_part_size_bytes == 0 {
            return Err(StorageError::Message {
                message: "Cannot partition with zero chunk size".to_string(),
            }
            .boxed());
        }
        let store = match config.provider {
            ObjectStorageProvider::GCS => GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(&config.bucket)
                .with_retry(object_store::RetryConfig {
                    max_retries: config.request_retry_count,
                    retry_timeout: Duration::from_millis(config.request_timeout_ms),
                    ..Default::default()
                })
                .with_client_options(
                    object_store::ClientOptions::new()
                        .with_timeout(Duration::from_millis(config.request_timeout_ms))
                        .with_connect_timeout(Duration::from_millis(config.connect_timeout_ms)),
                )
                .build()
                .map_err(|e| {
                    Box::new(StorageConfigError::FailedToCreateBucket(format!(
                        "Failed to create GCS client: {}",
                        e
                    ))) as Box<dyn ChromaError>
                })?,
        };

        Ok(ObjectStorage {
            bucket: config.bucket.clone(),
            store: Arc::new(store),
            upload_part_size_bytes: config.upload_part_size_bytes,
            download_part_size_bytes: config.download_part_size_bytes,
        })
    }

    pub async fn confirm_same(&self, key: &str, e_tag: &ETag) -> Result<bool, StorageError> {
        let metadata = self.store.head(&key.into()).await?;

        // Serialize metadata's e_tag/version into UpdateVersion for comparison
        let current_update_version = UpdateVersion {
            e_tag: metadata.e_tag.clone(),
            version: metadata.version.clone(),
        };

        let current_etag: ETag = (&current_update_version).try_into()?;
        Ok(current_etag.0 == e_tag.0)
    }

    fn partition(total_size: u64, chunk_size: u64) -> impl Iterator<Item = (u64, u64)> {
        let chunk_count = total_size.div_ceil(chunk_size);
        let chunk_start = (0..chunk_count).map(move |i| i * chunk_size);
        chunk_start
            .clone()
            .zip(chunk_start.skip(1).chain([total_size]))
    }

    async fn multipart_get(&self, key: &str) -> Result<(Bytes, ETag), StorageError> {
        let metadata = self.store.head(&key.into()).await?;
        let object_size = metadata.size;
        let etag = (&UpdateVersion {
            e_tag: metadata.e_tag.clone(),
            version: metadata.version.clone(),
        })
            .try_into()?;
        if object_size == 0 {
            return Ok((Bytes::new(), etag));
        }

        let chunk_ranges = Self::partition(object_size, self.download_part_size_bytes)
            .map(|(start, end)| start..end);

        let mut buffer = BytesMut::zeroed(object_size as usize);
        let get_part_futures = buffer
            .chunks_mut(self.download_part_size_bytes as usize)
            .zip(chunk_ranges)
            .map(|(byte_buffer, byte_range)| async move {
                let bytes = self
                    .store
                    .get_opts(
                        &key.into(),
                        object_store::GetOptions {
                            range: Some(GetRange::Bounded(byte_range)),
                            ..Default::default()
                        },
                    )
                    .await?
                    .bytes()
                    .await?;
                if bytes.len() != byte_buffer.len() {
                    return Err(StorageError::Message {
                        message: format!(
                            "Expected {} bytes in part, got {} bytes",
                            byte_buffer.len(),
                            bytes.len()
                        ),
                    });
                }
                byte_buffer.copy_from_slice(&bytes);
                Ok(())
            })
            .collect::<Vec<_>>();

        let chunk_count = get_part_futures.len();
        stream::iter(get_part_futures)
            .buffer_unordered(chunk_count)
            .try_collect::<Vec<_>>()
            .await?;

        Ok((buffer.freeze(), etag))
    }

    async fn oneshot_get(&self, key: &str) -> Result<(Bytes, ETag), StorageError> {
        let result = self.store.get_opts(&key.into(), Default::default()).await?;
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let etag = (&update_version).try_into()?;
        Ok((result.bytes().await?, etag))
    }

    pub async fn get(&self, key: &str, options: GetOptions) -> Result<(Bytes, ETag), StorageError> {
        if options.request_parallelism {
            self.multipart_get(key).await
        } else {
            self.oneshot_get(key).await
        }
    }

    async fn multipart_put(
        &self,
        key: &str,
        bytes: Bytes,
        _options: PutOptions,
    ) -> Result<ETag, StorageError> {
        let chunk_ranges = Self::partition(bytes.len() as u64, self.upload_part_size_bytes)
            .map(|(start, end)| (start as usize..end as usize));
        let mut upload_handle = self.store.put_multipart(&key.into()).await?;
        let upload_parts = chunk_ranges
            .map(|range| upload_handle.put_part(bytes.slice(range).into()))
            .collect::<Vec<_>>();
        let chunk_count = upload_parts.len();

        stream::iter(upload_parts)
            .buffer_unordered(chunk_count)
            .try_collect::<Vec<_>>()
            .await?;

        let result = upload_handle.complete().await?;

        let update_version = UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        };

        (&update_version).try_into()
    }

    async fn oneshot_put(
        &self,
        key: &str,
        bytes: Bytes,
        options: PutOptions,
    ) -> Result<ETag, StorageError> {
        let mut put_options = object_store::PutOptions::default();

        // Apply conditional operations
        if options.if_not_exists {
            put_options.mode = PutMode::Create;
        } else if let Some(etag) = &options.if_match {
            put_options.mode = PutMode::Update(etag.try_into()?);
        }

        let result = self
            .store
            .put_opts(&key.into(), bytes.into(), put_options)
            .await?;

        let update_version = UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        };

        (&update_version).try_into()
    }

    pub async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        options: PutOptions,
    ) -> Result<ETag, StorageError> {
        // TODO(sicheng): Figure out how to perform conditional multipart upload
        if bytes.len() > self.upload_part_size_bytes as usize
            && options.if_match.is_none()
            && !options.if_not_exists
        {
            self.multipart_put(key, bytes, options).await
        } else {
            self.oneshot_put(key, bytes, options).await
        }
    }

    // TODO(sicheng): This was used for hnsw files on disk and should be cleaned up
    // because we directly load hnsw to memory now
    pub async fn put_file(
        &self,
        key: &str,
        path: &str,
        options: PutOptions,
    ) -> Result<ETag, StorageError> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| StorageError::Generic {
                source: Arc::new(e),
            })?;
        self.oneshot_put(key, bytes.into(), options).await
    }

    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.store.delete(&key.into()).await?;
        Ok(())
    }

    pub async fn delete_many<S: AsRef<str> + std::fmt::Debug, I: IntoIterator<Item = S>>(
        &self,
        keys: I,
    ) -> Result<DeletedObjects, StorageError> {
        let keys = keys.into_iter().collect::<Vec<_>>();

        // Execute deletes in parallel
        let results = stream::iter(keys)
            .map(|key| async move {
                let key_str = key.as_ref().to_string();
                (key_str, self.delete(key.as_ref()).await)
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;

        let mut result = DeletedObjects::default();
        for (key, res) in results {
            match res {
                Ok(_) => result.deleted.push(key),
                Err(e) => result.errors.push(e),
            }
        }

        Ok(result)
    }

    pub async fn rename(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        self.store.rename(&src_key.into(), &dst_key.into()).await?;
        Ok(())
    }

    pub async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        self.store.copy(&src_key.into(), &dst_key.into()).await?;
        Ok(())
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(prefix.into())
        };

        let list_stream = self.store.list(prefix_path.as_ref());

        let keys = list_stream
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await?;

        Ok(keys)
    }
}

#[async_trait]
impl Configurable<StorageConfig> for ObjectStorage {
    async fn try_from_config(
        config: &StorageConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match config {
            StorageConfig::Object(gcs_config) => ObjectStorage::new(gcs_config).await,
            _ => Err(Box::new(StorageConfigError::InvalidStorageConfig)),
        }
    }
}
