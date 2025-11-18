use std::{any::Any, future::Future, sync::Arc};

use self::config::StorageConfig;
use admissioncontrolleds3::StorageRequestPriority;
use async_trait::async_trait;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};

pub mod admissioncontrolleds3;
pub mod config;
pub mod local;
pub mod object_storage;
pub mod s3;
pub mod stream;
use local::LocalStorage;
use tempfile::TempDir;
use thiserror::Error;

pub use s3::{s3_client_for_test_with_new_bucket, s3_config_for_localhost_with_bucket_name};

/// A StorageError captures all kinds of errors that can come from storage.
//
// This was borrowed from Apache Arrow's ObjectStore crate.
// Copyer:  Robert Escriva
// Commit:  5508978a3c5c4eb65ef6410e097887a8adaba38a
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Converted from Snafu to thiserror.

#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// A fallback error type when no variant matches
    #[error("Generic error: {source}")]
    Generic {
        /// The wrapped error
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// A generic message.
    #[error("Error message: {message}")]
    Message {
        /// The message
        message: String,
    },

    /// Error when the object is not found at given location
    #[error("Object at location {path} not found: {source}")]
    NotFound {
        /// The path to file
        path: String,
        /// The wrapped error
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error for invalid path
    #[error("Encountered object with invalid path: {source}")]
    InvalidPath {
        /// The wrapped error
        source: PathError,
    },

    /// Error when `tokio::spawn` failed
    #[error("Error joining spawned task.")]
    JoinError,

    /// Error when the attempted operation is not supported
    #[error("Operation not supported: {source}")]
    NotSupported {
        /// The wrapped error
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error when the object already exists
    #[error("Object at location {path} already exists: {source}")]
    AlreadyExists {
        /// The path to the
        path: String,
        /// The wrapped error
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error when the required conditions failed for the operation
    #[error("Request precondition failure for path {path}: {source}")]
    Precondition {
        /// The path to the file
        path: String,
        /// The wrapped error
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error when the object at the location isn't modified
    #[error("Object at location {path} not modified: {source}")]
    NotModified {
        /// The path to the file
        path: String,
        /// The wrapped error
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error when an operation is not implemented
    #[error("Operation not yet implemented.")]
    NotImplemented,

    /// Error when the used credentials don't have enough permission
    /// to perform the requested operation
    #[error("The operation lacked the necessary privileges to complete for path {path}: {source}")]
    PermissionDenied {
        /// The path to the file
        path: String,
        /// The wrapped error
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error when the used credentials lack valid authentication
    #[error("The operation lacked valid authentication credentials for path {path}: {source}")]
    Unauthenticated {
        /// The path to the file
        path: String,
        /// The wrapped error
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error when a configuration key is invalid for the store used
    #[error("Configuration key: '{key}' is not valid for store '{store}'.")]
    UnknownConfigurationKey {
        /// The object store used
        store: &'static str,
        /// The configuration key used
        key: String,
    },

    /// Error when a callback returning a ChromaError fails
    #[error("Storage callback error: {info}")]
    CallbackError {
        /// The wrapped error
        info: String,
    },

    // Back off and retry---usually indicates an explicit 429/SlowDown.
    #[error("Back off and retry---usually indicates an explicit 429/SlowDown.")]
    Backoff,
}

impl ChromaError for StorageError {
    fn code(&self) -> ErrorCodes {
        match self {
            StorageError::Generic { .. } => ErrorCodes::Internal,
            StorageError::Message { .. } => ErrorCodes::Internal,
            StorageError::NotFound { .. } => ErrorCodes::NotFound,
            StorageError::InvalidPath { .. } => ErrorCodes::InvalidArgument,
            StorageError::JoinError => ErrorCodes::Internal,
            StorageError::NotSupported { .. } => ErrorCodes::Unimplemented,
            StorageError::AlreadyExists { .. } => ErrorCodes::AlreadyExists,
            StorageError::Precondition { .. } => ErrorCodes::FailedPrecondition,
            StorageError::NotModified { .. } => ErrorCodes::FailedPrecondition,
            StorageError::NotImplemented => ErrorCodes::Unimplemented,
            StorageError::PermissionDenied { .. } => ErrorCodes::PermissionDenied,
            StorageError::Unauthenticated { .. } => ErrorCodes::Unauthenticated,
            StorageError::UnknownConfigurationKey { .. } => ErrorCodes::InvalidArgument,
            StorageError::Backoff => ErrorCodes::ResourceExhausted,
            StorageError::CallbackError { .. } => ErrorCodes::Internal,
        }
    }
}

/// Error returned by [`Path::parse`]
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PathError {
    /// Error when a path contains non-unicode characters
    #[error("Path \"{path}\" contained non-unicode characters: {source}")]
    NonUnicode {
        /// The source path
        path: String,
        /// The underlying `UTF8Error`
        source: std::str::Utf8Error,
    },
}

#[derive(Error, Debug)]
pub enum StorageConfigError {
    #[error("Invalid storage config")]
    InvalidStorageConfig,
    #[error("Failed to create bucket: {0}")]
    FailedToCreateBucket(String),
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Storage {
    S3(s3::S3Storage),
    Object(object_storage::ObjectStorage),
    Local(local::LocalStorage),
    AdmissionControlledS3(admissioncontrolleds3::AdmissionControlledS3Storage),
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Storage::S3(_) => f.debug_tuple("S3").finish(),
            Storage::Object(_) => f.debug_tuple("Object").finish(),
            Storage::Local(_) => f.debug_tuple("Local").finish(),
            Storage::AdmissionControlledS3(_) => f.debug_tuple("AdmissionControlledS3").finish(),
        }
    }
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
    /// Get the bucket name for S3-based storage, or None for local storage
    pub fn bucket_name(&self) -> Option<&str> {
        match self {
            Storage::S3(s3) => Some(&s3.bucket),
            Storage::Object(obj) => Some(&obj.bucket),
            Storage::AdmissionControlledS3(ac_s3) => Some(&ac_s3.storage.bucket),
            Storage::Local(_) => None,
        }
    }

    pub async fn get(&self, key: &str, options: GetOptions) -> Result<Arc<Vec<u8>>, StorageError> {
        match self {
            Storage::S3(s3) => s3.get(key, options).await,
            Storage::Object(obj) => obj
                .get(key, options)
                .await
                .map(|(bytes, _)| Vec::from(bytes).into()),
            Storage::Local(local) => local.get(key).await,
            Storage::AdmissionControlledS3(admission_controlled_storage) => {
                admission_controlled_storage.get(key, options).await
            }
        }
    }

    pub async fn fetch<FetchReturn, FetchFn, FetchFut>(
        &self,
        key: &str,
        options: GetOptions,
        fetch_fn: FetchFn,
    ) -> Result<(FetchReturn, Option<ETag>), StorageError>
    where
        FetchFn: FnOnce(Result<Arc<Vec<u8>>, StorageError>) -> FetchFut + Send + 'static,
        FetchFut: Future<Output = Result<FetchReturn, StorageError>> + Send + 'static,
        FetchReturn: Clone + Any + Sync + Send,
    {
        match self {
            Storage::S3(s3) => {
                let res = s3.get_with_e_tag(key).await?;
                let fetch_result = fetch_fn(Ok(res.0)).await?;
                Ok((fetch_result, res.1))
            }
            Storage::Object(obj) => {
                let (bytes, etag) = obj.get(key, options).await?;
                let fetch_result = fetch_fn(Ok(Vec::from(bytes).into())).await?;
                Ok((fetch_result, Some(etag)))
            }
            Storage::Local(local) => {
                let res = local.get_with_e_tag(key).await?;
                let fetch_result = fetch_fn(Ok(res.0)).await?;
                Ok((fetch_result, res.1))
            }
            Storage::AdmissionControlledS3(admission_controlled_storage) => {
                admission_controlled_storage
                    .fetch(key, options, fetch_fn)
                    .await
            }
        }
    }

    async fn fetch_batch_generic<FetchReturn, FetchFn, FetchFut>(
        &self,
        keys: Vec<&str>,
        options: GetOptions,
        fetch_fn: FetchFn,
    ) -> Result<(FetchReturn, Vec<Option<ETag>>), StorageError>
    where
        FetchFn: FnOnce(Vec<Result<Arc<Vec<u8>>, StorageError>>) -> FetchFut + Send + 'static,
        FetchFut: Future<Output = Result<FetchReturn, StorageError>> + Send + 'static,
        FetchReturn: Clone + Any + Sync + Send,
    {
        let mut results = Vec::new();
        for key in keys {
            let result = self.get_with_e_tag(key, options.clone()).await?;
            results.push(result);
        }
        let bufs = results
            .iter()
            .map(|res| Ok(res.0.clone()))
            .collect::<Vec<_>>();
        let e_tags = results.iter().map(|res| res.1.clone()).collect();
        let fetch_result = fetch_fn(bufs).await?;
        Ok((fetch_result, e_tags))
    }

    pub async fn fetch_batch<FetchReturn, FetchFn, FetchFut>(
        &self,
        keys: Vec<&str>,
        options: GetOptions,
        fetch_fn: FetchFn,
    ) -> Result<(FetchReturn, Vec<Option<ETag>>), StorageError>
    where
        FetchFn: FnOnce(Vec<Result<Arc<Vec<u8>>, StorageError>>) -> FetchFut + Send + 'static,
        FetchFut: Future<Output = Result<FetchReturn, StorageError>> + Send + 'static,
        FetchReturn: Clone + Any + Sync + Send,
    {
        match self {
            Storage::S3(_) => self.fetch_batch_generic(keys, options, fetch_fn).await,
            Storage::Object(_) => self.fetch_batch_generic(keys, options, fetch_fn).await,
            Storage::Local(_) => self.fetch_batch_generic(keys, options, fetch_fn).await,
            Storage::AdmissionControlledS3(admission_controlled_storage) => {
                admission_controlled_storage
                    .fetch_batch(keys, options, fetch_fn)
                    .await
            }
        }
    }

    pub async fn get_with_e_tag(
        &self,
        key: &str,
        options: GetOptions,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        match self {
            Storage::S3(s3) => s3.get_with_e_tag(key).await,
            Storage::Object(obj) => {
                let (bytes, etag) = obj.get(key, options).await?;
                Ok((Vec::from(bytes).into(), Some(etag)))
            }
            Storage::Local(local) => local.get_with_e_tag(key).await,
            Storage::AdmissionControlledS3(admission_controlled_storage) => {
                admission_controlled_storage
                    .get_with_e_tag(key, options)
                    .await
            }
        }
    }

    // NOTE(rescrv):  Returns Ok(true) if the file is definitely the same.  Returns Ok(false) if
    // the file cannot be confirmed to be the same but it exists.  Returns Err on error.  It is up
    // to the user to know how they are confirming the same and to react to Ok(false) even if the
    // file is definitely the same file on storage.
    pub async fn confirm_same(&self, key: &str, e_tag: &ETag) -> Result<bool, StorageError> {
        match self {
            Storage::S3(s3) => s3.confirm_same(key, e_tag).await,
            Storage::Object(obj) => obj.confirm_same(key, e_tag).await,
            Storage::Local(local) => local.confirm_same(key, e_tag).await,
            Storage::AdmissionControlledS3(as3) => as3.confirm_same(key, e_tag).await,
        }
    }

    pub async fn put_file(
        &self,
        key: &str,
        path: &str,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        match self {
            Storage::S3(s3) => s3.put_file(key, path, options).await,
            Storage::Object(obj) => obj.put_file(key, path, options).await.map(Some),
            Storage::Local(local) => local.put_file(key, path, options).await,
            Storage::AdmissionControlledS3(as3) => as3.put_file(key, path, options).await,
        }
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        match self {
            Storage::S3(s3) => s3.put_bytes(key, bytes, options).await,
            Storage::Object(obj) => obj.put(key, bytes.into(), options).await.map(Some),
            Storage::Local(local) => local.put_bytes(key, &bytes, options).await,
            Storage::AdmissionControlledS3(as3) => as3.put_bytes(key, bytes, options).await,
        }
    }

    pub async fn delete(&self, key: &str, options: DeleteOptions) -> Result<(), StorageError> {
        match self {
            Storage::S3(s3) => s3.delete(key, options).await,
            Storage::Object(obj) => {
                if options.if_match.is_some() {
                    return Err(StorageError::Message {
                        message: "if match not supported for object store backend".to_string(),
                    });
                }
                obj.delete(key).await
            }
            Storage::Local(local) => {
                if options.if_match.is_some() {
                    return Err(StorageError::Message {
                        message: "if match not supported for local backend".to_string(),
                    });
                }
                local.delete(key).await
            }
            Storage::AdmissionControlledS3(ac) => ac.delete(key, options).await,
        }
    }

    pub async fn delete_many<S: AsRef<str> + std::fmt::Debug, I: IntoIterator<Item = S>>(
        &self,
        keys: I,
    ) -> Result<crate::s3::DeletedObjects, StorageError> {
        match self {
            Storage::S3(s3) => s3.delete_many(keys).await,
            Storage::Object(obj) => obj.delete_many(keys).await,
            Storage::Local(local) => local.delete_many(keys).await,
            Storage::AdmissionControlledS3(ac) => ac.delete_many(keys).await,
        }
    }

    pub async fn rename(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        match self {
            Storage::S3(s3) => s3.rename(src_key, dst_key).await,
            Storage::Object(obj) => obj.rename(src_key, dst_key).await,
            Storage::Local(local) => local.rename(src_key, dst_key).await,
            Storage::AdmissionControlledS3(_) => Err(StorageError::NotImplemented),
        }
    }

    pub async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        match self {
            Storage::S3(s3) => s3.copy(src_key, dst_key).await,
            Storage::Object(obj) => obj.copy(src_key, dst_key).await,
            Storage::Local(local) => local.copy(src_key, dst_key).await,
            Storage::AdmissionControlledS3(ac) => ac.copy(src_key, dst_key).await,
        }
    }

    pub async fn list_prefix(
        &self,
        prefix: &str,
        options: GetOptions,
    ) -> Result<Vec<String>, StorageError> {
        match self {
            Storage::Local(local) => local.list_prefix(prefix).await,
            Storage::S3(s3) => s3.list_prefix(prefix).await,
            Storage::Object(obj) => obj.list_prefix(prefix).await,
            Storage::AdmissionControlledS3(acs3) => acs3.list_prefix(prefix, options).await,
        }
    }
}

#[async_trait]
impl Configurable<StorageConfig> for Storage {
    async fn try_from_config(
        config: &StorageConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::S3(_) => Ok(Storage::S3(
                s3::S3Storage::try_from_config(config, registry).await?,
            )),
            StorageConfig::Object(_) => Ok(Storage::Object(
                object_storage::ObjectStorage::try_from_config(config, registry).await?,
            )),
            StorageConfig::Local(_) => Ok(Storage::Local(
                local::LocalStorage::try_from_config(config, registry).await?,
            )),
            StorageConfig::AdmissionControlledS3(_) => Ok(Storage::AdmissionControlledS3(
                admissioncontrolleds3::AdmissionControlledS3Storage::try_from_config(
                    config, registry,
                )
                .await?,
            )),
        }
    }
}

pub fn test_storage() -> (TempDir, Storage) {
    let temp_dir = TempDir::new().expect("Should be able to create a temporary directory.");
    let storage =
        Storage::Local(LocalStorage::new(temp_dir.path().to_str().expect(
            "Should be able to convert temporary directory path to string",
        )));
    (temp_dir, storage)
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PutOptions {
    if_not_exists: bool,
    if_match: Option<ETag>,
    priority: StorageRequestPriority,
}

#[derive(Error, Debug)]
pub enum PutOptionsCreateError {
    #[error("If not exists and if match cannot both be used")]
    IfNotExistsAndIfMatchEnabled,
}

impl PutOptions {
    pub fn if_not_exists(priority: StorageRequestPriority) -> Self {
        // SAFETY(rescrv):  This is always safe because of a unit test.
        Self::new(true, None, priority).unwrap()
    }

    pub fn if_matches(e_tag: &ETag, priority: StorageRequestPriority) -> Self {
        // SAFETY(rescrv):  This is always safe because of a unit test.
        Self::new(false, Some(e_tag.clone()), priority).unwrap()
    }

    pub fn with_priority(priority: StorageRequestPriority) -> Self {
        Self {
            priority,
            ..Default::default()
        }
    }

    pub fn new(
        if_not_exists: bool,
        if_match: Option<ETag>,
        priority: StorageRequestPriority,
    ) -> Result<PutOptions, PutOptionsCreateError> {
        if if_not_exists && if_match.is_some() {
            return Err(PutOptionsCreateError::IfNotExistsAndIfMatchEnabled);
        }
        Ok(PutOptions {
            if_not_exists,
            if_match,
            priority,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetOptions {
    priority: StorageRequestPriority,
    requires_strong_consistency: bool,
    // If the underlying storage system would benefit from parallel requests
    // this requests parallel loading of the object.
    request_parallelism: bool,
}

impl GetOptions {
    pub fn new(priority: StorageRequestPriority) -> Self {
        Self {
            priority,
            requires_strong_consistency: false,
            request_parallelism: false,
        }
    }

    pub fn with_strong_consistency(mut self) -> Self {
        self.requires_strong_consistency = true;
        self
    }

    pub fn with_parallelism(mut self) -> Self {
        self.request_parallelism = true;
        self
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DeleteOptions {
    if_match: Option<ETag>,
    priority: StorageRequestPriority,
}

impl DeleteOptions {
    pub fn if_matches(e_tag: &ETag, priority: StorageRequestPriority) -> Self {
        Self::new(Some(e_tag.clone()), priority)
    }

    pub fn with_priority(priority: StorageRequestPriority) -> Self {
        Self {
            priority,
            ..Default::default()
        }
    }

    fn new(if_match: Option<ETag>, priority: StorageRequestPriority) -> DeleteOptions {
        DeleteOptions { if_match, priority }
    }
}

#[derive(Clone, Eq, PartialEq, Debug, serde::Deserialize, serde::Serialize)]
pub struct ETag(pub String);

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_options_ctors() {
        let _x = PutOptions::if_not_exists(StorageRequestPriority::P0);
        let _x = PutOptions::if_matches(&ETag("123".to_string()), StorageRequestPriority::P0);
    }
}
