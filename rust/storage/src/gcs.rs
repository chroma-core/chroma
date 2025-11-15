//! Google Cloud Storage (GCS) backend implementation using object_store.
//!
//! ## ETag Implementation Note
//! The `UpdateVersion` struct is serialized to JSON and stored as an ETag string.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::{
    gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder},
    path::Path as ObjectPath,
    ObjectStore, PutMode, PutOptions as ObjectStorePutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};

use crate::{
    config::{GcsStorageConfig, StorageConfig},
    s3::DeletedObjects,
    DeleteOptions, ETag, GetOptions, PutOptions, StorageConfigError, StorageError,
};

/// Serializable wrapper for UpdateVersion
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableUpdateVersion {
    e_tag: Option<String>,
    version: Option<String>,
}

impl From<UpdateVersion> for SerializableUpdateVersion {
    fn from(uv: UpdateVersion) -> Self {
        Self {
            e_tag: uv.e_tag,
            version: uv.version,
        }
    }
}

impl From<SerializableUpdateVersion> for UpdateVersion {
    fn from(suv: SerializableUpdateVersion) -> Self {
        Self {
            e_tag: suv.e_tag,
            version: suv.version,
        }
    }
}

/// Convert UpdateVersion to ETag via serialization
impl TryFrom<&UpdateVersion> for ETag {
    type Error = StorageError;

    fn try_from(uv: &UpdateVersion) -> Result<Self, Self::Error> {
        let serializable: SerializableUpdateVersion = uv.clone().into();
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
        let serializable: SerializableUpdateVersion =
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
pub struct GcsStorage {
    pub(crate) bucket: String,
    pub(super) store: Arc<GoogleCloudStorage>,
}

impl GcsStorage {
    pub async fn new(config: &GcsStorageConfig) -> Result<Self, Box<dyn ChromaError>> {
        let store = GoogleCloudStorageBuilder::new()
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
            })?;

        Ok(GcsStorage {
            bucket: config.bucket.clone(),
            store: Arc::new(store),
        })
    }

    pub async fn confirm_same(&self, key: &str, e_tag: &ETag) -> Result<bool, StorageError> {
        let path = ObjectPath::from(key);
        let metadata = self.store.head(&path).await?;

        // Serialize metadata's e_tag/version into UpdateVersion for comparison
        let current_update_version = UpdateVersion {
            e_tag: metadata.e_tag.clone(),
            version: metadata.version.clone(),
        };

        let current_etag: ETag = (&current_update_version).try_into()?;
        Ok(current_etag.0 == e_tag.0)
    }

    pub async fn get(&self, key: &str, _options: GetOptions) -> Result<Arc<Vec<u8>>, StorageError> {
        self.get_with_e_tag(key).await.map(|(buf, _)| buf)
    }

    pub async fn get_with_e_tag(
        &self,
        key: &str,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let path = ObjectPath::from(key);
        let result = self.store.get(&path).await?;

        // Serialize e_tag and version from metadata
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };

        let etag: ETag = (&update_version).try_into()?;

        let bytes = result.bytes().await?;

        Ok((Arc::new(bytes.to_vec()), Some(etag)))
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(Bytes::from(bytes));

        let mut put_options = ObjectStorePutOptions::default();

        // Apply conditional operations
        if options.if_not_exists {
            put_options.mode = PutMode::Create;
        } else if let Some(etag) = &options.if_match {
            let update_version: UpdateVersion = etag.try_into()?;
            put_options.mode = PutMode::Update(update_version);
        }

        let result = self.store.put_opts(&path, payload, put_options).await?;

        // Serialize result's e_tag and version
        let update_version = UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        };

        Ok(Some((&update_version).try_into()?))
    }

    pub async fn put_file(
        &self,
        key: &str,
        path: &str,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| StorageError::Generic {
                source: Arc::new(e),
            })?;
        self.put_bytes(key, bytes, options).await
    }

    pub async fn delete(&self, key: &str, options: DeleteOptions) -> Result<(), StorageError> {
        let path = ObjectPath::from(key);

        // Handle conditional delete
        if let Some(etag) = &options.if_match {
            // For conditional delete, we need to verify the version matches
            // object_store doesn't have native conditional delete, so we implement it manually
            let metadata = self.store.head(&path).await?;

            let current_update_version = UpdateVersion {
                e_tag: metadata.e_tag.clone(),
                version: metadata.version.clone(),
            };

            let current_etag: ETag = (&current_update_version).try_into()?;

            if current_etag.0 != etag.0 {
                return Err(StorageError::Precondition {
                    path: key.to_string(),
                    source: Arc::new(std::io::Error::other(
                        "ETag mismatch for conditional delete",
                    )),
                });
            }
        }

        self.store.delete(&path).await?;

        Ok(())
    }

    pub async fn delete_many<S: AsRef<str> + std::fmt::Debug, I: IntoIterator<Item = S>>(
        &self,
        keys: I,
    ) -> Result<DeletedObjects, StorageError> {
        let keys: Vec<_> = keys.into_iter().collect();

        // Execute deletes in parallel (50 concurrent as in previous implementation)
        let results: Vec<_> = stream::iter(keys)
            .map(|key| async move {
                let key_str = key.as_ref().to_string();
                (
                    key_str,
                    self.delete(key.as_ref(), DeleteOptions::default()).await,
                )
            })
            .buffer_unordered(50)
            .collect()
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
        let src_path = ObjectPath::from(src_key);
        let dst_path = ObjectPath::from(dst_key);

        self.store.rename(&src_path, &dst_path).await?;

        Ok(())
    }

    pub async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let src_path = ObjectPath::from(src_key);
        let dst_path = ObjectPath::from(dst_key);

        self.store.copy(&src_path, &dst_path).await?;

        Ok(())
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(prefix))
        };

        let list_stream = self.store.list(prefix_path.as_ref());

        let keys: Vec<String> = list_stream
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await?;

        Ok(keys)
    }
}

#[async_trait]
impl Configurable<StorageConfig> for GcsStorage {
    async fn try_from_config(
        config: &StorageConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match config {
            StorageConfig::GCS(gcs_config) => GcsStorage::new(gcs_config).await,
            _ => Err(Box::new(StorageConfigError::InvalidStorageConfig)),
        }
    }
}
