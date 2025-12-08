use crate::usearch_index::{USearchIndex, USearchIndexConfig, USearchIndexConfigError};
use crate::{Index, IndexConfig, IndexUuid};

use async_trait::async_trait;
use chroma_cache::AysncPartitionedMutex;
use chroma_cache::Cache;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use chroma_storage::{GetOptions, PutOptions, Storage};
use chroma_types::CollectionUuid;
use parking_lot::RwLock;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::{path::PathBuf, time::Instant};
use thiserror::Error;
use tracing::{Instrument, Span};
use usearch::ScalarKind;
use uuid::Uuid;

use super::config::USearchProviderConfig;

/// USearch index file name
pub const USEARCH_INDEX_FILE: &str = "index.usearch";

type CacheKey = CollectionUuid;

/// Reference to a distributed USearch index with internal locking
#[derive(Clone)]
pub struct USearchIndexRef {
    pub inner: Arc<RwLock<DistributedUSearchInner>>,
}

/// Inner struct containing the actual USearch index and metadata
pub struct DistributedUSearchInner {
    pub usearch_index: USearchIndex,
    pub prefix_path: String,
}

impl Debug for USearchIndexRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("USearchIndexRef")
            .field("id", &self.inner.read().usearch_index.id)
            .field(
                "dimensionality",
                &self.inner.read().usearch_index.dimensionality(),
            )
            .finish_non_exhaustive()
    }
}

impl chroma_cache::Weighted for USearchIndexRef {
    fn weight(&self) -> usize {
        let index = self.inner.read();
        if index.usearch_index.is_empty() {
            return 1;
        }
        let bytes = index.usearch_index.len()
            * std::mem::size_of::<f32>()
            * index.usearch_index.dimensionality() as usize;
        let as_mb = bytes / 1024 / 1024;
        if as_mb == 0 {
            1
        } else {
            as_mb
        }
    }
}

/// Provider for USearch indices with caching and storage integration
#[derive(Clone)]
pub struct USearchIndexProvider {
    cache: Arc<dyn Cache<CollectionUuid, USearchIndexRef>>,
    pub temporary_storage_path: PathBuf,
    storage: Storage,
    pub write_mutex: AysncPartitionedMutex<IndexUuid>,
    /// Whether to use quantization (f16) for memory efficiency
    pub use_quantization: bool,
    /// Quantization scalar kind to use
    pub quantization_kind: ScalarKind,
}

impl Debug for USearchIndexProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("USearchIndexProvider")
            .field("temporary_storage_path", &self.temporary_storage_path)
            .field("use_quantization", &self.use_quantization)
            .finish_non_exhaustive()
    }
}

/// Flusher for persisting USearch index to storage
pub struct USearchIndexFlusher {
    pub provider: USearchIndexProvider,
    pub prefix_path: String,
    pub index_id: IndexUuid,
    pub usearch_index: USearchIndexRef,
}

#[async_trait]
impl Configurable<(USearchProviderConfig, Storage)> for USearchIndexProvider {
    async fn try_from_config(
        config: &(USearchProviderConfig, Storage),
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (usearch_config, storage) = config;
        let cache = chroma_cache::from_config(&usearch_config.cache_config).await?;
        
        let quantization_kind = match usearch_config.quantization.as_deref() {
            Some("f16") => ScalarKind::F16,
            Some("i8") => ScalarKind::I8,
            Some("f32") | None => ScalarKind::F32,
            Some(other) => {
                tracing::warn!("Unknown quantization type '{}', using f32", other);
                ScalarKind::F32
            }
        };

        Ok(Self::new(
            storage.clone(),
            PathBuf::from(&usearch_config.temporary_path),
            cache,
            usearch_config.permitted_parallelism,
            usearch_config.use_quantization,
            quantization_kind,
        ))
    }
}

impl USearchIndexProvider {
    pub fn new(
        storage: Storage,
        storage_path: PathBuf,
        cache: Box<dyn Cache<CollectionUuid, USearchIndexRef>>,
        permitted_parallelism: u32,
        use_quantization: bool,
        quantization_kind: ScalarKind,
    ) -> Self {
        let cache: Arc<dyn Cache<CollectionUuid, USearchIndexRef>> = cache.into();
        Self {
            cache,
            storage,
            temporary_storage_path: storage_path,
            write_mutex: AysncPartitionedMutex::with_parallelism(
                permitted_parallelism as usize,
                (),
            ),
            use_quantization,
            quantization_kind,
        }
    }

    /// Get an index from cache if it exists and matches the expected ID
    pub async fn get(&self, index_id: &IndexUuid, cache_key: &CacheKey) -> Option<USearchIndexRef> {
        match self.cache.get(cache_key).await.ok().flatten() {
            Some(index) => {
                let index_with_lock = index.inner.read();
                if index_with_lock.usearch_index.id == *index_id {
                    Some(index.clone())
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Format the storage key for an index file
    pub fn format_key(prefix_path: &str, id: &IndexUuid) -> String {
        if prefix_path.is_empty() {
            return format!("usearch/{}/{}", id, USEARCH_INDEX_FILE);
        }
        format!("{}/usearch/{}/{}", prefix_path, id, USEARCH_INDEX_FILE)
    }

    /// Fork an existing index (create a copy with a new ID)
    pub async fn fork(
        &self,
        source_id: &IndexUuid,
        cache_key: &CacheKey,
        dimensionality: i32,
        distance_function: DistanceFunction,
        expansion_search: usize,
        prefix_path: &str,
    ) -> Result<USearchIndexRef, Box<USearchIndexProviderForkError>> {
        let new_id = IndexUuid(Uuid::new_v4());
        let index_config = IndexConfig::new(dimensionality, distance_function);

        // Check cache first
        if let Some(index) = self.get(&new_id, cache_key).await {
            return Ok(index);
        }

        // Fetch from storage
        let data = self
            .fetch_index_data(source_id, prefix_path)
            .await
            .map_err(|e| {
                Box::new(USearchIndexProviderForkError::FileError(
                    USearchIndexProviderFileError::StorageError(e),
                ))
            })?;

        // Check cache again after fetch
        if let Some(index) = self.get(&new_id, cache_key).await {
            return Ok(index);
        }

        // Load the index from fetched data
        let usearch_index = USearchIndex::load_from_bytes(&data, &index_config, expansion_search, new_id)
            .map_err(|e| Box::new(USearchIndexProviderForkError::IndexLoadError(e)))?;

        let index = USearchIndexRef {
            inner: Arc::new(RwLock::new(DistributedUSearchInner {
                usearch_index,
                prefix_path: prefix_path.to_string(),
            })),
        };

        self.cache.insert(*cache_key, index.clone()).await;
        Ok(index)
    }

    /// Fetch index data from storage
    async fn fetch_index_data(
        &self,
        source_id: &IndexUuid,
        prefix_path: &str,
    ) -> Result<Arc<Vec<u8>>, chroma_storage::StorageError> {
        let key = Self::format_key(prefix_path, source_id);
        let s3_fetch_span =
            tracing::trace_span!(parent: Span::current(), "Read usearch index from storage");
        
        let result = self
            .storage
            .get(&key, GetOptions::new(StorageRequestPriority::P0))
            .instrument(s3_fetch_span)
            .await?;
        
        Ok(result)
    }

    /// Open an existing index from storage
    pub async fn open(
        &self,
        id: &IndexUuid,
        cache_key: &CacheKey,
        dimensionality: i32,
        distance_function: DistanceFunction,
        expansion_search: usize,
        prefix_path: &str,
    ) -> Result<USearchIndexRef, Box<USearchIndexProviderOpenError>> {
        let index_config = IndexConfig::new(dimensionality, distance_function);

        // Check cache
        if let Some(index) = self.get(id, cache_key).await {
            return Ok(index);
        }

        // Fetch from storage
        let data = self
            .fetch_index_data(id, prefix_path)
            .await
            .map_err(|e| {
                Box::new(USearchIndexProviderOpenError::FileError(
                    USearchIndexProviderFileError::StorageError(e),
                ))
            })?;

        // Check cache again after fetch
        if let Some(index) = self.get(id, cache_key).await {
            return Ok(index);
        }

        // Load index
        let _guard = self.write_mutex.lock(id).await;
        
        // Final cache check
        if let Some(index) = self.get(id, cache_key).await {
            return Ok(index);
        }

        let usearch_index = USearchIndex::load_from_bytes(&data, &index_config, expansion_search, *id)
            .map_err(|e| Box::new(USearchIndexProviderOpenError::IndexLoadError(e)))?;

        let index = USearchIndexRef {
            inner: Arc::new(RwLock::new(DistributedUSearchInner {
                usearch_index,
                prefix_path: prefix_path.to_string(),
            })),
        };

        self.cache.insert(*cache_key, index.clone()).await;
        Ok(index)
    }

    /// Create a new empty index
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        &self,
        cache_key: &CacheKey,
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
        dimensionality: i32,
        distance_function: DistanceFunction,
        prefix_path: &str,
    ) -> Result<USearchIndexRef, Box<USearchIndexProviderCreateError>> {
        let id = IndexUuid(Uuid::new_v4());
        let index_config = IndexConfig::new(dimensionality, distance_function);

        let _guard = self.write_mutex.lock(&id).await;

        let usearch_config = if self.use_quantization {
            USearchIndexConfig::new_ephemeral_quantized(
                connectivity,
                expansion_add,
                expansion_search,
                self.quantization_kind.clone(),
            )
        } else {
            USearchIndexConfig::new_ephemeral(connectivity, expansion_add, expansion_search)
        };

        let usearch_index = USearchIndex::init(&index_config, Some(&usearch_config), id)
            .map_err(|e| Box::new(USearchIndexProviderCreateError::IndexInitError(e)))?;

        // Check cache
        if let Some(index) = self.get(&id, cache_key).await {
            return Ok(index);
        }

        let index = USearchIndexRef {
            inner: Arc::new(RwLock::new(DistributedUSearchInner {
                usearch_index,
                prefix_path: prefix_path.to_string(),
            })),
        };

        self.cache.insert(*cache_key, index.clone()).await;
        Ok(index)
    }

    /// Commit changes (no-op for in-memory USearch indices)
    pub fn commit(&self, _index: USearchIndexRef) -> Result<(), Box<dyn ChromaError>> {
        // USearch modifications are immediately reflected in memory
        // No explicit commit needed
        Ok(())
    }

    /// Flush index to storage
    pub async fn flush(
        &self,
        prefix_path: &str,
        id: &IndexUuid,
        usearch_index: &USearchIndexRef,
    ) -> Result<(), Box<USearchIndexProviderFlushError>> {
        // Serialize index to bytes
        let data = usearch_index
            .inner
            .read()
            .usearch_index
            .serialize_to_bytes()
            .map_err(|e| USearchIndexProviderFlushError::SerializeError(e.to_string()))?;

        // Upload to storage
        let key = Self::format_key(prefix_path, id);
        self.storage
            .put_bytes(
                &key,
                data,
                PutOptions::with_priority(StorageRequestPriority::P0),
            )
            .await
            .map_err(|e| {
                tracing::error!("Failed to flush usearch index: {}", e);
                Box::new(USearchIndexProviderFlushError::StoragePutError(e))
            })?;

        tracing::info!("Flushed usearch index: {}", id);
        Ok(())
    }

    /// Purge a local index directory
    pub async fn purge_one_id(path: &Path, id: IndexUuid) -> tokio::io::Result<()> {
        let index_storage_path = path.join(id.to_string());
        tracing::info!(
            "Purging USearch index ID: {}, path: {}, ts: {}",
            id,
            index_storage_path.to_str().unwrap_or("unknown"),
            Instant::now().elapsed().as_nanos()
        );
        match tokio::fs::remove_dir_all(&index_storage_path).await {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    tracing::warn!(
                        "USearch index ID: {} not found at path: {}",
                        id,
                        index_storage_path.to_str().unwrap_or("unknown")
                    );
                    Ok(())
                }
                _ => {
                    tracing::error!(
                        "Failed to remove USearch index ID: {} at path: {}. Error: {}",
                        id,
                        index_storage_path.to_str().unwrap_or("unknown"),
                        e
                    );
                    Err(e)
                }
            },
        }
    }
}

// Error types

#[derive(Error, Debug)]
pub enum USearchIndexProviderOpenError {
    #[error("USearch index file error")]
    FileError(#[from] USearchIndexProviderFileError),
    #[error("Index load error")]
    IndexLoadError(#[from] Box<dyn ChromaError>),
    #[error("Path: {0} could not be converted to string")]
    PathToStringError(PathBuf),
    #[error("Failed to cleanup files")]
    CleanupError(#[from] tokio::io::Error),
}

impl ChromaError for USearchIndexProviderOpenError {
    fn code(&self) -> ErrorCodes {
        match self {
            USearchIndexProviderOpenError::FileError(_) => ErrorCodes::Internal,
            USearchIndexProviderOpenError::IndexLoadError(e) => e.code(),
            USearchIndexProviderOpenError::PathToStringError(_) => ErrorCodes::InvalidArgument,
            USearchIndexProviderOpenError::CleanupError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum USearchIndexProviderForkError {
    #[error("USearch index file error")]
    FileError(#[from] USearchIndexProviderFileError),
    #[error("Index load error")]
    IndexLoadError(#[from] Box<dyn ChromaError>),
    #[error("Path: {0} could not be converted to string")]
    PathToStringError(PathBuf),
}

impl ChromaError for USearchIndexProviderForkError {
    fn code(&self) -> ErrorCodes {
        match self {
            USearchIndexProviderForkError::FileError(_) => ErrorCodes::Internal,
            USearchIndexProviderForkError::IndexLoadError(e) => e.code(),
            USearchIndexProviderForkError::PathToStringError(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Error, Debug)]
pub enum USearchIndexProviderCreateError {
    #[error("USearch index file error")]
    FileError(#[from] USearchIndexProviderFileError),
    #[error("USearch config error")]
    ConfigError(#[from] USearchIndexConfigError),
    #[error("Index init error")]
    IndexInitError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for USearchIndexProviderCreateError {
    fn code(&self) -> ErrorCodes {
        match self {
            USearchIndexProviderCreateError::FileError(_) => ErrorCodes::Internal,
            USearchIndexProviderCreateError::ConfigError(e) => e.code(),
            USearchIndexProviderCreateError::IndexInitError(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum USearchIndexProviderCommitError {
    #[error("No index found for id: {0}")]
    NoIndexFound(Uuid),
    #[error("USearch Save Error: {0}")]
    SaveError(String),
}

impl ChromaError for USearchIndexProviderCommitError {
    fn code(&self) -> ErrorCodes {
        match self {
            USearchIndexProviderCommitError::NoIndexFound(_) => ErrorCodes::NotFound,
            USearchIndexProviderCommitError::SaveError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum USearchIndexProviderFlushError {
    #[error("No index found for id: {0}")]
    NoIndexFound(Uuid),
    #[error("Storage Put Error")]
    StoragePutError(#[from] chroma_storage::StorageError),
    #[error("Failed to serialize USearch index: {0}")]
    SerializeError(String),
}

impl ChromaError for USearchIndexProviderFlushError {
    fn code(&self) -> ErrorCodes {
        match self {
            USearchIndexProviderFlushError::NoIndexFound(_) => ErrorCodes::NotFound,
            USearchIndexProviderFlushError::StoragePutError(e) => e.code(),
            USearchIndexProviderFlushError::SerializeError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum USearchIndexProviderFileError {
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Storage Error: {0}")]
    StorageError(#[from] chroma_storage::StorageError),
    #[error("Must provide full path to file")]
    InvalidFilePath,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_cache::new_non_persistent_cache_for_test;
    use chroma_storage::local::LocalStorage;

    #[tokio::test]
    async fn test_create_and_query() {
        let storage_dir = tempfile::tempdir().unwrap().path().to_path_buf();
        let usearch_tmp_path = storage_dir.join("usearch");
        tokio::fs::create_dir_all(&usearch_tmp_path).await.unwrap();

        let storage = Storage::Local(LocalStorage::new(storage_dir.to_str().unwrap()));
        let cache = new_non_persistent_cache_for_test();
        let provider = USearchIndexProvider::new(
            storage,
            usearch_tmp_path,
            cache,
            16,
            false,
            ScalarKind::F32,
        );
        let collection_id = CollectionUuid(Uuid::new_v4());

        let dimensionality = 3;
        let distance_function = DistanceFunction::Euclidean;
        let prefix_path = "";

        let index = provider
            .create(
                &collection_id,
                16,  // connectivity (M)
                128, // expansion_add (ef_construction)
                64,  // expansion_search (ef_search)
                dimensionality,
                distance_function,
                prefix_path,
            )
            .await
            .unwrap();

        // Add vectors
        {
            let guard = index.inner.write();
            guard.usearch_index.add(1, &[1.0, 0.0, 0.0]).unwrap();
            guard.usearch_index.add(2, &[0.0, 1.0, 0.0]).unwrap();
            guard.usearch_index.add(3, &[0.0, 0.0, 1.0]).unwrap();
        }

        // Query
        {
            let guard = index.inner.read();
            let (ids, distances) = guard
                .usearch_index
                .query(&[1.0, 0.0, 0.0], 1, &[], &[])
                .unwrap();
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], 1);
            assert!(distances[0] < 0.001);
        }
    }

    #[tokio::test]
    async fn test_flush_and_open() {
        let storage_dir = tempfile::tempdir().unwrap().path().to_path_buf();
        let usearch_tmp_path = storage_dir.join("usearch");
        tokio::fs::create_dir_all(&usearch_tmp_path).await.unwrap();

        let storage = Storage::Local(LocalStorage::new(storage_dir.to_str().unwrap()));
        let cache = new_non_persistent_cache_for_test();
        let provider = USearchIndexProvider::new(
            storage,
            usearch_tmp_path,
            cache,
            16,
            false,
            ScalarKind::F32,
        );
        let collection_id = CollectionUuid(Uuid::new_v4());

        let dimensionality = 3;
        let distance_function = DistanceFunction::Euclidean;
        let prefix_path = "";

        // Create and populate
        let index = provider
            .create(
                &collection_id,
                16,
                128,
                64,
                dimensionality,
                distance_function.clone(),
                prefix_path,
            )
            .await
            .unwrap();

        let index_id = index.inner.read().usearch_index.id;

        {
            let guard = index.inner.write();
            guard.usearch_index.add(1, &[1.0, 0.0, 0.0]).unwrap();
            guard.usearch_index.add(2, &[0.0, 1.0, 0.0]).unwrap();
        }

        // Flush
        provider
            .flush(prefix_path, &index_id, &index)
            .await
            .unwrap();

        // Clear cache and open
        provider.cache.clear().await.unwrap();

        let opened_index = provider
            .open(
                &index_id,
                &collection_id,
                dimensionality,
                distance_function,
                64,
                prefix_path,
            )
            .await
            .unwrap();

        // Verify data persisted
        {
            let guard = opened_index.inner.read();
            assert_eq!(guard.usearch_index.len(), 2);
            
            let (ids, _) = guard
                .usearch_index
                .query(&[1.0, 0.0, 0.0], 1, &[], &[])
                .unwrap();
            assert_eq!(ids[0], 1);
        }
    }

    #[tokio::test]
    async fn test_quantized_index() {
        let storage_dir = tempfile::tempdir().unwrap().path().to_path_buf();
        let usearch_tmp_path = storage_dir.join("usearch");
        tokio::fs::create_dir_all(&usearch_tmp_path).await.unwrap();

        let storage = Storage::Local(LocalStorage::new(storage_dir.to_str().unwrap()));
        let cache = new_non_persistent_cache_for_test();
        
        // Create provider with f16 quantization
        let provider = USearchIndexProvider::new(
            storage,
            usearch_tmp_path,
            cache,
            16,
            true,  // use_quantization
            ScalarKind::F16,
        );
        let collection_id = CollectionUuid(Uuid::new_v4());

        let index = provider
            .create(
                &collection_id,
                16,
                128,
                64,
                128,  // dimensionality
                DistanceFunction::Euclidean,
                "",
            )
            .await
            .unwrap();

        // Add and query with quantized index
        {
            let guard = index.inner.write();
            let vec: Vec<f32> = (0..128).map(|i| i as f32 / 128.0).collect();
            guard.usearch_index.add(1, &vec).unwrap();
        }

        {
            let guard = index.inner.read();
            assert_eq!(guard.usearch_index.len(), 1);
        }
    }
}

