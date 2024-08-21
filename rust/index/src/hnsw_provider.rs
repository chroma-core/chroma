use super::config::HnswProviderConfig;
use super::{
    HnswIndex, HnswIndexConfig, HnswIndexFromSegmentError, Index, IndexConfig,
    IndexConfigFromSegmentError,
};
use crate::types::PersistentIndex;
use async_trait::async_trait;
use chroma_cache::cache;
use chroma_cache::cache::Cache;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_storage::stream::ByteStreamItem;
use chroma_storage::Storage;
use chroma_types::Segment;
use futures::stream;
use futures::stream::StreamExt;
use parking_lot::RwLock;
use std::fmt::Debug;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tracing::{instrument, Instrument, Span};
use uuid::Uuid;

// These are the files hnswlib writes to disk. This is strong coupling, but we need to know
// what files to read from disk. We could in the future have the C++ code return the files
// but ideally we have a rust implementation of hnswlib
const FILES: [&'static str; 4] = [
    "header.bin",
    "data_level0.bin",
    "length.bin",
    "link_lists.bin",
];

#[derive(Clone)]
pub struct HnswIndexProvider {
    cache: Cache<Uuid, Arc<RwLock<HnswIndex>>>,
    pub temporary_storage_path: PathBuf,
    storage: Storage,
}

impl Debug for HnswIndexProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HnswIndexProvider {{ temporary_storage_path: {:?} }}",
            self.temporary_storage_path,
        )
    }
}

#[async_trait]
impl Configurable<(HnswProviderConfig, Storage)> for HnswIndexProvider {
    async fn try_from_config(
        config: &(HnswProviderConfig, Storage),
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (hnsw_config, storage) = config;
        let cache = chroma_cache::from_config(&hnsw_config.hnsw_cache_config).await?;
        Ok(Self {
            cache,
            storage: storage.clone(),
            temporary_storage_path: PathBuf::from(&hnsw_config.hnsw_temporary_path),
        })
    }
}

impl HnswIndexProvider {
    pub fn new(
        storage: Storage,
        storage_path: PathBuf,
        cache: Cache<Uuid, Arc<RwLock<HnswIndex>>>,
    ) -> Self {
        Self {
            cache,
            storage,
            temporary_storage_path: storage_path,
        }
    }

    pub fn get(&self, id: &Uuid) -> Option<Arc<RwLock<HnswIndex>>> {
        match self.cache.get(id) {
            Some(index) => Some(index.clone()),
            None => None,
        }
    }

    fn format_key(&self, id: &Uuid, file: &str) -> String {
        format!("hnsw/{}/{}", id, file)
    }

    pub async fn fork(
        &self,
        source_id: &Uuid,
        segment: &Segment,
        dimensionality: i32,
    ) -> Result<Arc<RwLock<HnswIndex>>, Box<HnswIndexProviderForkError>> {
        let new_id = Uuid::new_v4();
        let new_storage_path = self.temporary_storage_path.join(new_id.to_string());
        match self.create_dir_all(&new_storage_path) {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(HnswIndexProviderForkError::FileError(*e)));
            }
        }

        match self
            .load_hnsw_segment_into_directory(source_id, &new_storage_path)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(HnswIndexProviderForkError::FileError(*e)));
            }
        }

        let index_config = IndexConfig::from_segment(&segment, dimensionality);

        let index_config = match index_config {
            Ok(index_config) => index_config,
            Err(e) => {
                return Err(Box::new(HnswIndexProviderForkError::IndexConfigError(*e)));
            }
        };

        let hnsw_config = HnswIndexConfig::from_segment(segment, &new_storage_path);
        match hnsw_config {
            Ok(hnsw_config) => hnsw_config,
            Err(e) => {
                return Err(Box::new(HnswIndexProviderForkError::HnswConfigError(*e)));
            }
        };

        let storage_path_str = match new_storage_path.to_str() {
            Some(storage_path_str) => storage_path_str,
            None => {
                return Err(Box::new(HnswIndexProviderForkError::PathToStringError(
                    new_storage_path,
                )));
            }
        };

        match HnswIndex::load(storage_path_str, &index_config, new_id) {
            Ok(index) => {
                let index = Arc::new(RwLock::new(index));
                self.cache.insert(new_id, index.clone());
                Ok(index)
            }
            Err(e) => Err(Box::new(HnswIndexProviderForkError::IndexLoadError(e))),
        }
    }

    async fn copy_bytes_to_local_file(
        &self,
        file_path: &PathBuf,
        buf: Arc<Vec<u8>>,
    ) -> Result<(), Box<HnswIndexProviderFileError>> {
        let file_handle = tokio::fs::File::create(&file_path).await;
        let mut file_handle = match file_handle {
            Ok(file) => file,
            Err(e) => {
                tracing::error!("Failed to create file: {}", e);
                return Err(Box::new(HnswIndexProviderFileError::IOError(e)));
            }
        };
        let res = file_handle.write_all(&buf).await;
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to copy file: {}", e);
                return Err(Box::new(HnswIndexProviderFileError::IOError(e)));
            }
        }
        match file_handle.flush().await {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("Failed to flush file: {}", e);
                Err(Box::new(HnswIndexProviderFileError::IOError(e)))
            }
        }
    }

    #[instrument]
    async fn load_hnsw_segment_into_directory(
        &self,
        source_id: &Uuid,
        index_storage_path: &Path,
    ) -> Result<(), Box<HnswIndexProviderFileError>> {
        // Fetch the files from storage and put them in the index storage path.
        for file in FILES.iter() {
            let key = self.format_key(source_id, file);
            tracing::info!("Loading hnsw index file: {}", key);
            let bytes_res = self.storage.get_parallel(&key).await;
            let bytes_read;
            let buf = match bytes_res {
                Ok(buf) => {
                    bytes_read = buf.len();
                    buf
                }
                Err(e) => {
                    tracing::error!("Failed to load hnsw index file from storage: {}", e);
                    return Err(Box::new(HnswIndexProviderFileError::StorageGetError(e)));
                }
            };
            let file_path = index_storage_path.join(file);
            // For now, we never evict from the cache, so if the index is being loaded, the file does not exist
            self.copy_bytes_to_local_file(&file_path, buf).instrument(tracing::info_span!(parent: Span::current(), "hnsw provider copy bytes to local file", file = file)).await?;
            tracing::info!(
                "Copied {} bytes from storage key: {} to file: {}",
                bytes_read,
                key,
                file_path.to_str().unwrap()
            );
            tracing::info!("Loaded hnsw index file: {}", file);
        }
        Ok(())
    }

    pub async fn open(
        &self,
        id: &Uuid,
        segment: &Segment,
        dimensionality: i32,
    ) -> Result<Arc<RwLock<HnswIndex>>, Box<HnswIndexProviderOpenError>> {
        let index_storage_path = self.temporary_storage_path.join(id.to_string());

        match self.create_dir_all(&index_storage_path) {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(HnswIndexProviderOpenError::FileError(*e)));
            }
        }

        match self
            .load_hnsw_segment_into_directory(id, &index_storage_path)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(HnswIndexProviderOpenError::FileError(*e)));
            }
        }

        let index_config = IndexConfig::from_segment(&segment, dimensionality);
        let index_config = match index_config {
            Ok(index_config) => index_config,
            Err(e) => {
                return Err(Box::new(HnswIndexProviderOpenError::IndexConfigError(*e)));
            }
        };

        let hnsw_config = HnswIndexConfig::from_segment(segment, &index_storage_path);
        let hnsw_config = match hnsw_config {
            Ok(hnsw_config) => hnsw_config,
            Err(e) => {
                return Err(Box::new(HnswIndexProviderOpenError::HnswConfigError(*e)));
            }
        };

        // TODO: don't unwrap path conv here
        match HnswIndex::load(index_storage_path.to_str().unwrap(), &index_config, *id) {
            Ok(index) => {
                let index = Arc::new(RwLock::new(index));
                self.cache.insert(*id, index.clone());
                Ok(index)
            }
            Err(e) => Err(Box::new(HnswIndexProviderOpenError::IndexLoadError(e))),
        }
    }

    // Compactor
    // Cases
    // A write comes in and no files are in the segment -> we know we need to create a new index
    // A write comes in and files are in the segment -> we know we need to load the index
    // If the writer drops, but we already have the index, the id will be in the cache and the next job will have files and not need to load the index

    // Query
    // Cases
    // A query comes in and the index is in the cache -> we can query the index based on segment files id (Same as compactor case 3 where we have the index)
    // A query comes in and the index is not in the cache -> we need to load the index from s3 based on the segment files id
    pub fn create(
        &self,
        // TODO: This should not take Segment. The index layer should not know about the segment concept
        segment: &Segment,
        dimensionality: i32,
    ) -> Result<Arc<RwLock<HnswIndex>>, Box<HnswIndexProviderCreateError>> {
        let id = Uuid::new_v4();
        let index_storage_path = self.temporary_storage_path.join(id.to_string());

        match self.create_dir_all(&index_storage_path) {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(HnswIndexProviderCreateError::FileError(*e)));
            }
        }

        let index_config = match IndexConfig::from_segment(&segment, dimensionality) {
            Ok(index_config) => index_config,
            Err(e) => {
                return Err(Box::new(HnswIndexProviderCreateError::IndexConfigError(*e)));
            }
        };

        let hnsw_config = match HnswIndexConfig::from_segment(segment, &index_storage_path) {
            Ok(hnsw_config) => hnsw_config,
            Err(e) => {
                return Err(Box::new(HnswIndexProviderCreateError::HnswConfigError(*e)));
            }
        };
        // HnswIndex init is not thread safe. We should not call it from multiple threads
        let index = match HnswIndex::init(&index_config, Some(&hnsw_config), id) {
            Ok(index) => index,
            Err(e) => {
                return Err(Box::new(HnswIndexProviderCreateError::IndexInitError(e)));
            }
        };
        let index = Arc::new(RwLock::new(index));
        self.cache.insert(id, index.clone());
        Ok(index)
    }

    pub fn commit(&self, index: Arc<RwLock<HnswIndex>>) -> Result<(), Box<dyn ChromaError>> {
        match index.write().save() {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(HnswIndexProviderCommitError::HnswSaveError(e)));
            }
        }

        Ok(())
    }

    pub async fn flush(&self, id: &Uuid) -> Result<(), Box<HnswIndexProviderFlushError>> {
        let index_storage_path = self.temporary_storage_path.join(id.to_string());
        for file in FILES.iter() {
            let file_path = index_storage_path.join(file);
            let key = self.format_key(id, file);
            let res = self
                .storage
                .put_file(&key, file_path.to_str().unwrap())
                .await;
            match res {
                Ok(_) => {
                    println!("Flushed hnsw index file: {}", file);
                }
                Err(e) => {
                    return Err(Box::new(HnswIndexProviderFlushError::StoragePutError(e)));
                }
            }
        }
        Ok(())
    }

    fn create_dir_all(&self, path: &PathBuf) -> Result<(), Box<HnswIndexProviderFileError>> {
        match std::fs::create_dir_all(path) {
            Ok(_) => Ok(()),
            Err(e) => return Err(Box::new(HnswIndexProviderFileError::IOError(e))),
        }
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderOpenError {
    #[error("Index configuration error")]
    IndexConfigError(#[from] IndexConfigFromSegmentError),
    #[error("Hnsw index file error")]
    FileError(#[from] HnswIndexProviderFileError),
    #[error("Hnsw config error")]
    HnswConfigError(#[from] HnswIndexFromSegmentError),
    #[error("Index load error")]
    IndexLoadError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for HnswIndexProviderOpenError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswIndexProviderOpenError::IndexConfigError(e) => e.code(),
            HnswIndexProviderOpenError::FileError(_) => ErrorCodes::Internal,
            HnswIndexProviderOpenError::HnswConfigError(e) => e.code(),
            HnswIndexProviderOpenError::IndexLoadError(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderForkError {
    #[error("Index configuration error")]
    IndexConfigError(#[from] IndexConfigFromSegmentError),
    #[error("Hnsw index file error")]
    FileError(#[from] HnswIndexProviderFileError),
    #[error("Hnsw config error")]
    HnswConfigError(#[from] HnswIndexFromSegmentError),
    #[error("Index load error")]
    IndexLoadError(#[from] Box<dyn ChromaError>),
    #[error("Path: {0} could not be converted to string")]
    PathToStringError(PathBuf),
}

impl ChromaError for HnswIndexProviderForkError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswIndexProviderForkError::IndexConfigError(e) => e.code(),
            HnswIndexProviderForkError::FileError(_) => ErrorCodes::Internal,
            HnswIndexProviderForkError::HnswConfigError(e) => e.code(),
            HnswIndexProviderForkError::IndexLoadError(e) => e.code(),
            HnswIndexProviderForkError::PathToStringError(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderCreateError {
    #[error("Index configuration error")]
    IndexConfigError(#[from] IndexConfigFromSegmentError),
    #[error("Hnsw index file error")]
    FileError(#[from] HnswIndexProviderFileError),
    #[error("Hnsw config error")]
    HnswConfigError(#[from] HnswIndexFromSegmentError),
    #[error("Index init error")]
    IndexInitError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for HnswIndexProviderCreateError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswIndexProviderCreateError::IndexConfigError(e) => e.code(),
            HnswIndexProviderCreateError::FileError(_) => ErrorCodes::Internal,
            HnswIndexProviderCreateError::HnswConfigError(e) => e.code(),
            HnswIndexProviderCreateError::IndexInitError(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderCommitError {
    #[error("No index found for id: {0}")]
    NoIndexFound(Uuid),
    #[error("HNSW Save Error")]
    HnswSaveError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for HnswIndexProviderCommitError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswIndexProviderCommitError::NoIndexFound(_) => ErrorCodes::NotFound,
            HnswIndexProviderCommitError::HnswSaveError(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderFlushError {
    #[error("No index found for id: {0}")]
    NoIndexFound(Uuid),
    #[error("HNSW Save Error")]
    HnswSaveError(#[from] Box<dyn ChromaError>),
    #[error("Storage Put Error")]
    StoragePutError(#[from] chroma_storage::PutError),
}

impl ChromaError for HnswIndexProviderFlushError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswIndexProviderFlushError::NoIndexFound(_) => ErrorCodes::NotFound,
            HnswIndexProviderFlushError::HnswSaveError(e) => e.code(),
            HnswIndexProviderFlushError::StoragePutError(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderFileError {
    #[error("IO Error")]
    IOError(#[from] std::io::Error),
    #[error("Storage Get Error")]
    StorageGetError(#[from] chroma_storage::GetError),
    #[error("Storage Put Error")]
    StoragePutError(#[from] chroma_storage::PutError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_cache::config::{CacheConfig, UnboundedCacheConfig};
    use chroma_storage::local::LocalStorage;
    use chroma_types::SegmentType;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_fork() {
        let storage_dir = tempfile::tempdir().unwrap().path().to_path_buf();
        let hnsw_tmp_path = storage_dir.join("hnsw");

        // Create the directories needed
        std::fs::create_dir_all(&hnsw_tmp_path).unwrap();

        let storage = Storage::Local(LocalStorage::new(storage_dir.to_str().unwrap()));
        let cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let provider = HnswIndexProvider::new(storage, hnsw_tmp_path, cache);
        let segment = Segment {
            id: Uuid::new_v4(),
            r#type: SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: Uuid::new_v4(),
            metadata: None,
            file_path: HashMap::new(),
        };

        let dimensionality = 128;
        let created_index = provider.create(&segment, dimensionality).unwrap();
        let created_index_id = created_index.read().id;

        let forked_index = provider
            .fork(&created_index_id, &segment, dimensionality)
            .await
            .unwrap();
        let forked_index_id = forked_index.read().id;

        assert_ne!(created_index_id, forked_index_id);
    }
}
