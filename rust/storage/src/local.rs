use super::stream::ByteStream;
use super::stream::ByteStreamItem;
use super::{config::StorageConfig, s3::StorageConfigError};
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use futures::Stream;

#[derive(Clone)]
pub struct LocalStorage {
    root: String,
}

impl LocalStorage {
    pub fn new(root: &str) -> LocalStorage {
        // Create the local storage with the root path.
        return LocalStorage {
            root: root.to_string(),
        };
    }

    pub async fn get(
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

    pub async fn put_bytes(&self, key: &str, bytes: &[u8]) -> Result<(), String> {
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

    pub async fn put_file(&self, key: &str, path: &str) -> Result<(), String> {
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
