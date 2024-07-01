use crate::{config::Configurable, errors::ChromaError};
use async_trait::async_trait;
use tokio::io::AsyncBufRead;

use super::{config::StorageConfig, s3::StorageConfigError};

#[derive(Clone)]
pub(crate) struct LocalStorage {
    root: String,
}

impl LocalStorage {
    pub(crate) fn new(root: &str) -> LocalStorage {
        // Create the local storage with the root path.
        return LocalStorage {
            root: root.to_string(),
        };
    }

    pub(crate) async fn get(
        &self,
        key: &str,
    ) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, String> {
        let file_path = format!("{}/{}", self.root, key);
        tracing::debug!("Reading from path: {}", file_path);
        match tokio::fs::File::open(file_path).await {
            Ok(file) => {
                return Ok(Box::new(tokio::io::BufReader::new(file)));
            }
            Err(e) => {
                println!("Error: {:?}", e);
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
        tokio::fs::create_dir_all(parent).await.unwrap();
        let res = tokio::fs::write(&path, bytes).await;
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
        let file = tokio::fs::read(path).await;
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
impl Configurable<StorageConfig> for LocalStorage {
    async fn try_from_config(config: &StorageConfig) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::Local(local_config) => {
                let storage = LocalStorage::new(&local_config.root);
                return Ok(storage);
            }
            _ => {
                return Err(Box::new(StorageConfigError::InvalidStorageConfig));
            }
        }
    }
}
