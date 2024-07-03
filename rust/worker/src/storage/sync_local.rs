use super::stream::{ByteStream, ByteStreamItem};
use super::{config::StorageConfig, s3::StorageConfigError};
use crate::{config::Configurable, errors::ChromaError};
use async_trait::async_trait;
use futures::Stream;

#[derive(Clone)]
pub(crate) struct SyncLocalStorage {
    root: String,
}

impl SyncLocalStorage {
    pub(crate) fn new(root: &str) -> SyncLocalStorage {
        // Create the local storage with the root path.
        return SyncLocalStorage {
            root: root.to_string(),
        };
    }

    pub(crate) async fn get(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = ByteStreamItem> + Unpin + Send>, String> {
        let file_path = format!("{}/{}", self.root, key);
        tracing::debug!("Reading from path: {}", file_path);
        match std::fs::File::open(file_path) {
            Ok(file) => {
                let stream = file.byte_stream();
                return Ok(Box::new(stream));
            }
            Err(e) => {
                return Err::<_, String>(e.to_string());
            }
        }
    }

    pub(crate) async fn put_bytes(&self, key: &str, bytes: &[u8]) -> Result<(), String> {
        let path = format!("{}/{}", self.root, key);
        tracing::debug!("Writing to path: {}", path);
        // Create the path if it doesn't exist, we unwrap since this should only be used in tests
        let as_path = std::path::Path::new(&path);
        let parent = as_path.parent().unwrap();
        std::fs::create_dir_all(parent).unwrap();
        let res = std::fs::write(&path, bytes);
        match res {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err::<(), String>(e.to_string());
            }
        }
    }

    pub(crate) async fn put_file(&self, key: &str, path: &str) -> Result<(), String> {
        let file = std::fs::read(path);
        match file {
            Ok(bytes_u8) => {
                return self.put_bytes(key, &bytes_u8).await;
            }
            Err(e) => {
                return Err::<(), String>(e.to_string());
            }
        }
    }
}

#[async_trait]
impl Configurable<StorageConfig> for SyncLocalStorage {
    async fn try_from_config(config: &StorageConfig) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::Local(local_config) => {
                let storage = SyncLocalStorage::new(&local_config.root);
                return Ok(storage);
            }
            _ => {
                return Err(Box::new(StorageConfigError::InvalidStorageConfig));
            }
        }
    }
}
