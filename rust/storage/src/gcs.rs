use std::sync::Arc;

use async_trait::async_trait;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use google_cloud_gax::{
    exponential_backoff::ExponentialBackoff,
    retry_policy::{AlwaysRetry, RetryPolicyExt},
};
use google_cloud_storage::client::{Storage, StorageControl};

use crate::{
    config::{GcsStorageConfig, StorageConfig},
    s3::DeletedObjects,
    DeleteOptions, ETag, GetOptions, PutOptions, StorageConfigError, StorageError,
};

#[derive(Clone)]
pub struct GcsStorage {
    pub(crate) bucket: String,
    pub(crate) bucket_path: String, // Full path: projects/{project}/buckets/{bucket}
    pub(super) client: Storage,
    pub(super) control_client: StorageControl,
}

impl GcsStorage {
    pub async fn new(config: &GcsStorageConfig) -> Result<Self, Box<dyn ChromaError>> {
        let client = Storage::builder()
            .with_retry_policy(AlwaysRetry.with_attempt_limit(config.request_retry_count))
            .with_backoff_policy(ExponentialBackoff::default())
            .with_resumable_upload_threshold(config.resumable_upload_threshold_bytes)
            .with_resumable_upload_buffer_size(config.resumable_upload_buffer_size_bytes)
            .build()
            .await
            .map_err(|e| {
                Box::new(StorageConfigError::FailedToCreateBucket(format!(
                    "Failed to create GCS client: {}",
                    e
                ))) as Box<dyn ChromaError>
            })?;

        let control_client = StorageControl::builder()
            .with_retry_policy(AlwaysRetry.with_attempt_limit(config.request_retry_count))
            .with_backoff_policy(ExponentialBackoff::default())
            .build()
            .await
            .map_err(|e| {
                Box::new(StorageConfigError::FailedToCreateBucket(format!(
                    "Failed to create GCS control client: {}",
                    e
                ))) as Box<dyn ChromaError>
            })?;

        let bucket_path = if config.project_id == "_" {
            format!("projects/_/buckets/{}", config.bucket)
        } else {
            format!("projects/{}/buckets/{}", config.project_id, config.bucket)
        };

        Ok(GcsStorage {
            bucket: config.bucket.clone(),
            bucket_path,
            client,
            control_client,
        })
    }

    pub async fn confirm_same(&self, _key: &str, _e_tag: &ETag) -> Result<bool, StorageError> {
        Err(StorageError::NotImplemented)
    }

    pub async fn get(&self, key: &str, _options: GetOptions) -> Result<Arc<Vec<u8>>, StorageError> {
        self.get_with_e_tag(key).await.map(|(buf, _)| buf)
    }

    pub async fn get_with_e_tag(
        &self,
        key: &str,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let mut response = self
            .client
            .read_object(&self.bucket_path, key)
            .send()
            .await
            .map_err(|e| {
                let err_string = e.to_string();
                if err_string.contains("404") || err_string.contains("NotFound") {
                    StorageError::NotFound {
                        path: key.to_string(),
                        source: Arc::new(e),
                    }
                } else {
                    StorageError::Generic {
                        source: Arc::new(e),
                    }
                }
            })?;

        let mut contents = Vec::new();
        while let Some(chunk) =
            response
                .next()
                .await
                .transpose()
                .map_err(|e| StorageError::Generic {
                    source: Arc::new(e),
                })?
        {
            contents.extend_from_slice(&chunk);
        }

        Ok((Arc::new(contents), None))
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        _options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let bytes_data = bytes::Bytes::from(bytes);
        let _response = self
            .client
            .write_object(&self.bucket_path, key, bytes_data)
            .send_buffered()
            .await
            .map_err(|e| StorageError::Generic {
                source: Arc::new(e),
            })?;

        // Note: generation/metageneration would be in response but not easily accessible
        Ok(None)
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

    pub async fn delete(&self, key: &str, _options: DeleteOptions) -> Result<(), StorageError> {
        self.control_client
            .delete_object()
            .set_bucket(&self.bucket_path)
            .set_object(key)
            .send()
            .await
            .map_err(|e| {
                let err_string = e.to_string();
                if err_string.contains("404") || err_string.contains("NotFound") {
                    StorageError::NotFound {
                        path: key.to_string(),
                        source: Arc::new(e),
                    }
                } else {
                    StorageError::Generic {
                        source: Arc::new(e),
                    }
                }
            })?;
        Ok(())
    }

    pub async fn delete_many<S: AsRef<str> + std::fmt::Debug, I: IntoIterator<Item = S>>(
        &self,
        keys: I,
    ) -> Result<DeletedObjects, StorageError> {
        let mut result = DeletedObjects::default();

        for key in keys {
            match self.delete(key.as_ref(), DeleteOptions::default()).await {
                Ok(_) => result.deleted.push(key.as_ref().to_string()),
                Err(e) => result.errors.push(e),
            }
        }

        Ok(result)
    }

    pub async fn rename(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        // GCS doesn't have native rename, so copy then delete
        self.copy(src_key, dst_key).await?;
        self.delete(src_key, DeleteOptions::default()).await
    }

    pub async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        // Read source object
        let (data, _) = self.get_with_e_tag(src_key).await?;

        // Write to destination
        self.put_bytes(dst_key, (*data).clone(), PutOptions::default())
            .await?;

        Ok(())
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let response = self
            .control_client
            .list_objects()
            .set_parent(&self.bucket_path)
            .set_prefix(prefix)
            .send()
            .await
            .map_err(|e| StorageError::Generic {
                source: Arc::new(e),
            })?;

        let keys: Vec<String> = response.objects.into_iter().map(|obj| obj.name).collect();

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
