use super::config::StorageConfig;
use super::StorageConfigError;
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use std::sync::Arc;

#[derive(Clone)]
pub struct LocalStorage {
    root: String,
}

impl LocalStorage {
    pub fn new(root: &str) -> LocalStorage {
        // Create the local storage with the root path.
        LocalStorage {
            root: root.to_string(),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, String> {
        let file_path = format!("{}/{}", self.root, key);
        match std::fs::read(file_path) {
            Ok(bytes_u8) => Ok(Arc::new(bytes_u8)),
            Err(e) => Err::<Arc<Vec<u8>>, String>(e.to_string()),
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
            Ok(_) => Ok(()),
            Err(e) => Err::<(), String>(e.to_string()),
        }
    }

    pub async fn put_file(&self, key: &str, path: &str) -> Result<(), String> {
        let file = std::fs::read(path);
        match file {
            Ok(bytes_u8) => self.put_bytes(key, &bytes_u8).await,
            Err(e) => Err::<(), String>(e.to_string()),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), String> {
        let path = format!("{}/{}", self.root, key);
        tracing::info!(path = %path, "Deleting file");

        match std::fs::remove_file(&path) {
            Ok(_) => {
                tracing::info!(path = %path, "Successfully deleted file");
                Ok(())
            }
            Err(e) => {
                tracing::error!(error = %e, path = %path, "Failed to delete file");
                Err(e.to_string())
            }
        }
    }

    pub async fn rename(&self, src_key: &str, dst_key: &str) -> Result<(), String> {
        let src_path = format!("{}/{}", self.root, src_key);
        let dst_path = format!("{}/{}", self.root, dst_key);
        tracing::info!(src = %src_path, dst = %dst_path, "Renaming file");

        // Create parent directory for destination if it doesn't exist
        if let Some(parent) = std::path::Path::new(&dst_path).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!(error = %e, path = %parent.display(), "Failed to create parent directory");
                return Err(e.to_string());
            }
        }

        match std::fs::rename(&src_path, &dst_path) {
            Ok(_) => {
                tracing::info!(src = %src_path, dst = %dst_path, "Successfully renamed file");
                Ok(())
            }
            Err(e) => {
                tracing::error!(error = %e, src = %src_path, dst = %dst_path, "Failed to rename file");
                Err(e.to_string())
            }
        }
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
