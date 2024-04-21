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
        return LocalStorage {
            root: root.to_string(),
        };
    }

    pub(crate) async fn get(
        &self,
        key: &str,
    ) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, String> {
        // Checks if a file exits with the key. If it does, it copies the file to the path.
        let file_path = format!("{}/{}", self.root, key);
        let file = tokio::fs::File::open(file_path).await;
        match file {
            Ok(file) => {
                return Ok(Box::new(tokio::io::BufReader::new(file)));
            }
            Err(e) => {
                return Err::<_, String>(e.to_string());
            }
        }
    }

    pub(crate) async fn put_bytes(&self, key: &str, bytes: &[u8]) -> Result<(), String> {
        let path = format!("{}/{}", self.root, key);
        let res = tokio::fs::write(&path, bytes).await;
        match res {
            Ok(_) => {
                println!("copied file {} to {}", path, key);
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
