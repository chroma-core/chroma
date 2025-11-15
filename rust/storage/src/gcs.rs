//! Google Cloud Storage (GCS) backend implementation.
//!
//! ## ETag Implementation Note
//!
//! GCS uses generation numbers (i64) for versioning and conditional operations, not ETags.
//! This implementation stores generation numbers as ETags by converting them to strings.
//! This allows conditional operations like `if_match` to work correctly with GCS's API,
//! which requires generation numbers for preconditions (`set_if_generation_match`).

use std::sync::Arc;

use async_trait::async_trait;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use futures::stream::{self, StreamExt};
use google_cloud_gax::{
    error::rpc::Code,
    exponential_backoff::ExponentialBackoff,
    retry_policy::{AlwaysRetry, RetryPolicyExt},
};
use google_cloud_storage::client::{Storage, StorageControl};

use crate::{
    config::{GcsStorageConfig, StorageConfig},
    s3::DeletedObjects,
    DeleteOptions, ETag, GetOptions, PutOptions, StorageConfigError, StorageError,
};

fn from_gcs_error_with_path(e: google_cloud_storage::Error, path: &str) -> StorageError {
    if let Some(status) = e.status() {
        match status.code {
            Code::NotFound => {
                return StorageError::NotFound {
                    path: path.to_string(),
                    source: Arc::new(e),
                }
            }
            Code::AlreadyExists => {
                return StorageError::AlreadyExists {
                    path: path.to_string(),
                    source: Arc::new(e),
                }
            }
            Code::FailedPrecondition => {
                return StorageError::Precondition {
                    path: path.to_string(),
                    source: Arc::new(e),
                }
            }
            Code::ResourceExhausted => return StorageError::Backoff,
            Code::PermissionDenied => {
                return StorageError::PermissionDenied {
                    path: path.to_string(),
                    source: Arc::new(e),
                }
            }
            Code::Unauthenticated => {
                return StorageError::Unauthenticated {
                    path: path.to_string(),
                    source: Arc::new(e),
                }
            }
            _ => {}
        }
    }
    StorageError::Generic {
        source: Arc::new(e),
    }
}

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

    pub async fn confirm_same(&self, key: &str, e_tag: &ETag) -> Result<bool, StorageError> {
        let object = self
            .control_client
            .get_object()
            .set_bucket(&self.bucket_path)
            .set_object(key)
            .send()
            .await
            .map_err(|e| from_gcs_error_with_path(e, key))?;

        // ETag is stored as generation number (see module docs)
        let generation_str = object.generation.to_string();
        Ok(generation_str == e_tag.0)
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
            .map_err(|e| from_gcs_error_with_path(e, key))?;

        // Store generation number as ETag (see module docs)
        let generation = response.object().generation;

        let mut contents = Vec::new();
        while let Some(chunk) = response
            .next()
            .await
            .transpose()
            .map_err(|e| from_gcs_error_with_path(e, key))?
        {
            contents.extend_from_slice(&chunk);
        }

        Ok((Arc::new(contents), Some(ETag(generation.to_string()))))
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let bytes_data = bytes::Bytes::from(bytes);
        let mut req = self.client.write_object(&self.bucket_path, key, bytes_data);

        // Apply conditional operations using generation numbers
        if options.if_not_exists {
            // if_not_exists: only create if object doesn't exist (generation = 0)
            req = req.set_if_generation_match(0);
        } else if let Some(etag) = &options.if_match {
            // if_match: only update if generation matches the provided ETag
            let generation = etag.0.parse::<i64>().map_err(|_| StorageError::Generic {
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Invalid ETag format for GCS: expected generation number, got '{}'",
                        etag.0
                    ),
                )),
            })?;
            req = req.set_if_generation_match(generation);
        }

        let response = req
            .send_buffered()
            .await
            .map_err(|e| from_gcs_error_with_path(e, key))?;

        // Store generation number as ETag (see module docs)
        Ok(Some(ETag(response.generation.to_string())))
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
        let mut req = self
            .control_client
            .delete_object()
            .set_bucket(&self.bucket_path)
            .set_object(key);

        // Apply conditional delete using generation number
        if let Some(etag) = &options.if_match {
            let generation = etag.0.parse::<i64>().map_err(|_| StorageError::Generic {
                source: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Invalid ETag format for GCS: expected generation number, got '{}'",
                        etag.0
                    ),
                )),
            })?;
            req = req.set_if_generation_match(generation);
        }

        req.send()
            .await
            .map_err(|e| from_gcs_error_with_path(e, key))?;
        Ok(())
    }

    pub async fn delete_many<S: AsRef<str> + std::fmt::Debug, I: IntoIterator<Item = S>>(
        &self,
        keys: I,
    ) -> Result<DeletedObjects, StorageError> {
        let keys: Vec<_> = keys.into_iter().collect();

        // Execute deletes in parallel
        let results: Vec<_> = stream::iter(keys)
            .map(|key| async move {
                let key_str = key.as_ref().to_string();
                (
                    key_str,
                    self.delete(key.as_ref(), DeleteOptions::default()).await,
                )
            })
            .buffer_unordered(32)
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
            .map_err(|e| from_gcs_error_with_path(e, prefix))?;

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
