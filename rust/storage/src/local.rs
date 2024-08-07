use super::stream::ByteStream;
use super::stream::ByteStreamItem;
use super::{config::StorageConfig, s3::StorageConfigError};
use crate::GetError;
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use futures::Stream;
use futures::StreamExt;
use std::sync::Arc;
use tracing::Instrument;
use tracing::Span;

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

    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, String> {
        let mut stream = self
            .get_stream(&key)
            .instrument(tracing::trace_span!(parent: Span::current(), "Local Storage get"))
            .await?;
        let read_block_span =
            tracing::trace_span!(parent: Span::current(), "Local storage read bytes to end");
        let buf = read_block_span
            .in_scope(|| async {
                let mut buf: Vec<u8> = Vec::new();
                while let Some(res) = stream.next().await {
                    match res {
                        Ok(chunk) => {
                            buf.extend(chunk);
                        }
                        Err(err) => {
                            tracing::error!("Error reading from storage: {}", err);
                            match err {
                                GetError::LocalError(e) => {
                                    return Err(e);
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
                tracing::info!("Read {:?} bytes from local storage", buf.len());
                Ok(Some(buf))
            })
            .await?;

        match buf {
            Some(buf) => Ok(Arc::new(buf)),
            None => {
                // Buffer is empty. Nothing interesting to do.
                Ok(Arc::new(vec![]))
            }
        }
    }

    pub(super) async fn get_stream(
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
