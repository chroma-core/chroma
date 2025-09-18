use super::config::StorageConfig;
use super::StorageConfigError;
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::{hash::DefaultHasher, path::Path};
use thiserror::Error;

use crate::{ETag, PutOptions, StorageError};

#[derive(Debug, Error)]
#[error("Local storage error")]
pub struct LocalStoraegError;

#[derive(Clone)]
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: &str) -> LocalStorage {
        // Create the local storage with the root path.
        LocalStorage {
            root: Path::new(root).to_path_buf(),
        }
    }

    fn etag_for_bytes(bytes: &[u8]) -> ETag {
        let mut hasher = DefaultHasher::new();
        bytes.hash(&mut hasher);
        ETag(hasher.finish().to_string())
    }

    fn path_for_key(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, StorageError> {
        let file_path = self.path_for_key(key);
        if !file_path.exists() {
            return Err(StorageError::NotFound {
                path: file_path
                    .to_str()
                    .expect("File path should be valid string")
                    .to_string(),
                source: Arc::new(LocalStoraegError),
            });
        }
        match std::fs::read(file_path) {
            Ok(bytes_u8) => Ok(Arc::new(bytes_u8)),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn get_with_e_tag(
        &self,
        key: &str,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let bytes = self.get(key).await?;
        let etag = Self::etag_for_bytes(&bytes);
        Ok((bytes, Some(etag)))
    }

    pub async fn confirm_same(&self, _: &str, _: &ETag) -> Result<bool, StorageError> {
        Err(StorageError::NotImplemented)
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        _options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        // TODO: Handle options
        let file_path = self.path_for_key(key);
        std::fs::create_dir_all(
            file_path
                .parent()
                .expect("Parent should be present for the file path"),
        )
        .unwrap();
        let res = std::fs::write(&file_path, bytes);
        match res {
            Ok(_) => Ok(None),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn put_file(
        &self,
        key: &str,
        path: &str,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let file = std::fs::read(path);
        match file {
            Ok(bytes_u8) => self.put_bytes(key, &bytes_u8, options).await,
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let file_path = self.path_for_key(key);

        match std::fs::remove_file(&file_path) {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn delete_many<S: AsRef<str> + std::fmt::Debug, I: IntoIterator<Item = S>>(
        &self,
        keys: I,
    ) -> Result<crate::s3::DeletedObjects, StorageError> {
        let mut deleted_objects = crate::s3::DeletedObjects::default();
        for key in keys {
            let file_path = self.path_for_key(key.as_ref());
            match std::fs::remove_file(&file_path) {
                Ok(_) => {
                    deleted_objects.deleted.push(key.as_ref().to_string());
                }
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        deleted_objects.errors.push(StorageError::NotFound {
                            path: key.as_ref().to_string(),
                            source: Arc::new(e),
                        });
                    }
                    _ => {
                        deleted_objects.errors.push(StorageError::Generic {
                            source: Arc::new(e),
                        });
                    }
                },
            }
        }
        Ok(deleted_objects)
    }

    pub async fn rename(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let src_path = self.path_for_key(src_key);
        let dst_path = self.path_for_key(dst_key);

        // Create parent directory for destination if it doesn't exist
        if let Some(parent) = std::path::Path::new(&dst_path).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!(error = %e, path = %parent.display(), "Failed to create parent directory");
                return Err(StorageError::Generic {
                    source: Arc::new(e),
                });
            }
        }

        match std::fs::rename(&src_path, &dst_path) {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let src_path = self.path_for_key(src_key);
        let dst_path = self.path_for_key(dst_key);

        // Create parent directory for destination if it doesn't exist
        if let Some(parent) = std::path::Path::new(&dst_path).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!(error = %e, path = %parent.display(), "Failed to create parent directory");
                return Err(StorageError::Generic {
                    source: Arc::new(e),
                });
            }
        }

        match std::fs::copy(&src_path, &dst_path) {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageError::Generic {
                source: Arc::new(e),
            }),
        }
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let search_path = self.path_for_key(prefix);
        if !search_path.exists() {
            return Ok(Vec::new());
        }
        let entries = std::fs::read_dir(search_path).map_err(|e| StorageError::Generic {
            source: Arc::new(e),
        })?;
        entries
            .into_iter()
            .map(|e| {
                e.map_err(|e| StorageError::Generic {
                    source: Arc::new(e),
                })
                .and_then(|e| {
                    e.file_name()
                        .to_str()
                        .map(|s| s.to_string())
                        .ok_or(StorageError::Message {
                            message: "Unable to convert path to string".to_string(),
                        })
                })
            })
            .collect()
    }
}

#[async_trait]
impl Configurable<StorageConfig> for LocalStorage {
    async fn try_from_config(
        config: &StorageConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::Local(local_config) => {
                let storage = LocalStorage::new(&local_config.root);
                Ok(storage)
            }
            _ => Err(Box::new(StorageConfigError::InvalidStorageConfig)),
        }
    }
}
