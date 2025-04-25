use crate::{HnswIndexConfigError, PersistentIndex};

use super::config::HnswProviderConfig;
use super::{HnswIndex, HnswIndexConfig, Index, IndexConfig, IndexUuid};

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
use std::time::Instant;
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tracing::{instrument, Span};
use uuid::Uuid;

// These are the files hnswlib writes to disk. This is strong coupling, but we need to know
// what files to read from disk. We could in the future have the C++ code return the files
// but ideally we have a rust implementation of hnswlib
const FILES: [&str; 4] = [
    "header.bin",
    "data_level0.bin",
    "length.bin",
    "link_lists.bin",
];

type CacheKey = CollectionUuid;

// The key of the cache is the collection id and the value is
// the HNSW index for that collection. This restricts the cache to
// contain atmost one index per collection. Ideally, we would like
// this index to be the latest index for that collection but rn it
// is not guaranteed. For e.g. one case could be:
// 1. get index version v1
// 2. get index version v2 (> v1)
// 3. get index version v1 (can happen due to an inflight query
//    that started before compaction of v2 occured) -- this will
//    evict v2 even though it is more recent and will be used again in future.
// Once we have versioning propagated throughout the system we can make
// this better. We can also do a deferred eviction for such entries when
// their ref count goes to 0.
#[derive(Clone)]
pub struct HnswIndexProvider {
    cache: Arc<dyn Cache<CollectionUuid, HnswIndexRef>>,
    pub temporary_storage_path: PathBuf,
    storage: Storage,
    pub write_mutex: AysncPartitionedMutex<IndexUuid>,
}

#[derive(Clone)]
pub struct HnswIndexRef {
    pub inner: Arc<RwLock<HnswIndex>>,
}

impl Debug for HnswIndexRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIndexRef")
            .field("id", &self.inner.read().id)
            .field("dimensionality", &self.inner.read().dimensionality())
            .finish_non_exhaustive()
    }
}

impl Debug for HnswIndexProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIndexProvider")
            .field("temporary_storage_path", &self.temporary_storage_path)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Configurable<(HnswProviderConfig, Storage)> for HnswIndexProvider {
    async fn try_from_config(
        config: &(HnswProviderConfig, Storage),
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (hnsw_config, storage) = config;
        let cache = chroma_cache::from_config(&hnsw_config.hnsw_cache_config).await?;
        Ok(Self::new(
            storage.clone(),
            PathBuf::from(&hnsw_config.hnsw_temporary_path),
            cache,
            hnsw_config.permitted_parallelism,
        ))
    }
}

impl chroma_cache::Weighted for HnswIndexRef {
    fn weight(&self) -> usize {
        let index = self.inner.read();
        if index.len() == 0 {
            return 1;
        }
        let bytes = index.len() * std::mem::size_of::<f32>() * index.dimensionality() as usize;
        let as_mb = bytes / 1024 / 1024;
        if as_mb == 0 {
            1
        } else {
            as_mb
        }
    }
}

impl HnswIndexProvider {
    pub fn new(
        storage: Storage,
        storage_path: PathBuf,
        cache: Box<dyn Cache<CollectionUuid, HnswIndexRef>>,
        permitted_parallelism: u32,
    ) -> Self {
        let cache: Arc<dyn Cache<CollectionUuid, HnswIndexRef>> = cache.into();
        Self {
            cache,
            storage,
            temporary_storage_path: storage_path,
            write_mutex: AysncPartitionedMutex::with_parallelism(
                permitted_parallelism as usize,
                (),
            ),
        }
    }

    pub async fn get(&self, index_id: &IndexUuid, cache_key: &CacheKey) -> Option<HnswIndexRef> {
        match self.cache.get(cache_key).await.ok().flatten() {
            Some(index) => {
                let index_with_lock = index.inner.read();
                if index_with_lock.id == *index_id {
                    // Clone is cheap because we are just cloning the Arc.
                    Some(index.clone())
                } else {
                    None
                }
            }
            None => None,
        }
    }

    // TODO(rohitcp): Use HNSW_INDEX_S3_PREFIX.
    fn format_key(&self, id: &IndexUuid, file: &str) -> String {
        format!("hnsw/{}/{}", id, file)
    }

    pub async fn fork(
        &self,
        source_id: &IndexUuid,
        cache_key: &CacheKey,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
    ) -> Result<HnswIndexRef, Box<HnswIndexProviderForkError>> {
        // We take a lock here to synchronize concurrent forks of the same index.
        // Otherwise, we could end up with a corrupted index since the filesystem
        // operations are not guaranteed to be atomic.
        // The lock is a partitioned mutex to allow for higher concurrency across collections.
        let _guard = self.write_mutex.lock(source_id).await;
        let new_id = IndexUuid(Uuid::new_v4());
        let new_storage_path = self.temporary_storage_path.join(new_id.to_string());
        // This is ok to be called from multiple threads concurrently. See
        // the documentation of tokio::fs::create_dir_all to see why.
        match self.create_dir_all(&new_storage_path).await {
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

        let index_config = IndexConfig::new(dimensionality, distance_function);

        let storage_path_str = match new_storage_path.to_str() {
            Some(storage_path_str) => storage_path_str,
            None => {
                return Err(Box::new(HnswIndexProviderForkError::PathToStringError(
                    new_storage_path,
                )));
            }
        };

        // Check if the entry is in the cache, if it is, we assume
        // another thread has loaded the index and we return it.
        match self.get(&new_id, cache_key).await {
            Some(index) => Ok(index.clone()),
            None => match HnswIndex::load(storage_path_str, &index_config, ef_search, new_id) {
                Ok(index) => {
                    let index = HnswIndexRef {
                        inner: Arc::new(RwLock::new(index)),
                    };
                    self.cache.insert(*cache_key, index.clone()).await;
                    Ok(index)
                }
                Err(e) => Err(Box::new(HnswIndexProviderForkError::IndexLoadError(e))),
            },
        }
    }

    #[instrument(skip(self, buf))]
    async fn copy_bytes_to_local_file(
        &self,
        file_path: &Path,
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
                tracing::error!("Failed to flush temporary file: {}", e);
                return Err(Box::new(HnswIndexProviderFileError::IOError(e)));
            }
        }
    }

    #[instrument]
    async fn load_hnsw_segment_into_directory(
        &self,
        source_id: &IndexUuid,
        index_storage_path: &Path,
    ) -> Result<(), Box<HnswIndexProviderFileError>> {
        // Fetch the files from storage and put them in the index storage path.
        for file in FILES.iter() {
            let s3_fetch_span =
                tracing::trace_span!(parent: Span::current(), "Read bytes from s3", file = file);
            let buf = s3_fetch_span
                .in_scope(|| async {
                    let key = self.format_key(source_id, file);
                    tracing::info!("Loading hnsw index file: {} into directory", key);
                    let bytes_res = self
                        .storage
                        .get_parallel(&key, GetOptions::new(StorageRequestPriority::P0))
                        .await;
                    let bytes_read;
                    let buf = match bytes_res {
                        Ok(buf) => {
                            bytes_read = buf.len();
                            buf
                        }
                        Err(e) => {
                            tracing::error!("Failed to load hnsw index file from storage: {}", e);
                            return Err(Box::new(HnswIndexProviderFileError::StorageError(e)));
                        }
                    };
                    tracing::info!(
                        "Fetched {} bytes from s3 for storage key {:?}",
                        bytes_read,
                        key,
                    );
                    Ok(buf)
                })
                .await?;
            let file_path = index_storage_path.join(file);
            self.copy_bytes_to_local_file(&file_path, buf).await?;
        }
        Ok(())
    }

    pub async fn open(
        &self,
        id: &IndexUuid,
        cache_key: &CacheKey,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
    ) -> Result<HnswIndexRef, Box<HnswIndexProviderOpenError>> {
        // This is the double checked locking pattern. This avoids taking the
        // async mutex in the common case where the index is already in the cache.
        if let Some(index) = self.get(id, cache_key).await {
            return Ok(index);
        }
        // We take a lock here to synchronize concurrent forks of the same index.
        // Otherwise, we could end up with a corrupted index since the filesystem
        // operations are not guaranteed to be atomic.
        // The lock is a partitioned mutex to allow for higher concurrency across collections.
        let _guard = self.write_mutex.lock(id).await;
        if let Some(index) = self.get(id, cache_key).await {
            return Ok(index);
        }
        let index_storage_path = self.temporary_storage_path.join(id.to_string());

        match self.create_dir_all(&index_storage_path).await {
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

        let index_config = IndexConfig::new(dimensionality, distance_function);

        let index_storage_path_str = match index_storage_path.to_str() {
            Some(index_storage_path_str) => index_storage_path_str,
            None => {
                return Err(Box::new(HnswIndexProviderOpenError::PathToStringError(
                    index_storage_path,
                )));
            }
        };

        // Check if the entry is in the cache, if it is, we assume
        // another thread has loaded the index and we return it.
        let index = match self.get(id, cache_key).await {
            Some(index) => Ok(index.clone()),
            None => match HnswIndex::load(index_storage_path_str, &index_config, ef_search, *id) {
                Ok(index) => {
                    let index = HnswIndexRef {
                        inner: Arc::new(RwLock::new(index)),
                    };
                    self.cache.insert(*cache_key, index.clone()).await;
                    Ok(index)
                }
                Err(e) => Err(Box::new(HnswIndexProviderOpenError::IndexLoadError(e))),
            },
        };

        // Cleanup directory.
        // Readers don't modify the index, so we can delete the files on disk
        // once the index is fully loaded in memory.
        Self::purge_one_id(&self.temporary_storage_path, *id)
            .await
            .map_err(|e| {
                tracing::error!("Failed to cleanup files: {}", e);
                Box::new(HnswIndexProviderOpenError::CleanupError(e))
            })?;

        index
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
    pub async fn create(
        &self,
        cache_key: &CacheKey,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        dimensionality: i32,
        distance_function: DistanceFunction,
    ) -> Result<HnswIndexRef, Box<HnswIndexProviderCreateError>> {
        let id = IndexUuid(Uuid::new_v4());
        // We take a lock here to synchronize concurrent creates of the same index.
        // Otherwise, we could end up with a corrupted index since the filesystem
        // operations are not guaranteed to be atomic.
        // The lock is a partitioned mutex to allow for higher concurrency across collections.
        let _guard = self.write_mutex.lock(&id).await;
        let index_storage_path = self.temporary_storage_path.join(id.to_string());

        match self.create_dir_all(&index_storage_path).await {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(HnswIndexProviderCreateError::FileError(*e)));
            }
        }

        let index_config = IndexConfig::new(dimensionality, distance_function);

        let hnsw_config = match HnswIndexConfig::new_persistent(
            m,
            ef_construction,
            ef_search,
            &index_storage_path,
        ) {
            Ok(hnsw_config) => hnsw_config,
            Err(e) => {
                return Err(Box::new(HnswIndexProviderCreateError::HnswConfigError(*e)));
            }
        };

        // HnswIndex init is not thread safe. We should not call it from multiple threads
        let index = HnswIndex::init(&index_config, Some(&hnsw_config), id)
            .map_err(|e| Box::new(HnswIndexProviderCreateError::IndexInitError(e)))?;

        match self.get(&id, cache_key).await {
            Some(index) => Ok(index.clone()),
            None => {
                let index = HnswIndexRef {
                    inner: Arc::new(RwLock::new(index)),
                };
                self.cache.insert(*cache_key, index.clone()).await;
                Ok(index)
            }
        }
    }

    pub fn commit(&self, index: HnswIndexRef) -> Result<(), Box<dyn ChromaError>> {
        match index.inner.write().save() {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(HnswIndexProviderCommitError::HnswSaveError(e)));
            }
        }

        Ok(())
    }

    pub async fn flush(&self, id: &IndexUuid) -> Result<(), Box<HnswIndexProviderFlushError>> {
        let index_storage_path = self.temporary_storage_path.join(id.to_string());
        for file in FILES.iter() {
            let file_path = index_storage_path.join(file);
            let key = self.format_key(id, file);
            let res = self
                .storage
                .put_file(
                    &key,
                    file_path.to_str().unwrap(),
                    PutOptions::with_priority(StorageRequestPriority::P0),
                )
                .await;
            match res {
                Ok(_) => {
                    tracing::info!("Flushed hnsw index file: {}", file);
                }
                Err(e) => {
                    return Err(Box::new(HnswIndexProviderFlushError::StoragePutError(e)));
                }
            }
        }
        Ok(())
    }

    pub async fn purge_one_id(path: &Path, id: IndexUuid) -> tokio::io::Result<()> {
        let index_storage_path = path.join(id.to_string());
        tracing::info!(
            "Purging HNSW index ID: {}, path: {}, ts: {}",
            id,
            index_storage_path.to_str().unwrap(),
            Instant::now().elapsed().as_nanos()
        );
        match tokio::fs::remove_dir_all(&index_storage_path).await {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    tracing::warn!(
                        "HNSW index ID: {} not found at path: {}",
                        id,
                        index_storage_path.to_str().unwrap()
                    );
                    Ok(())
                }
                _ => {
                    tracing::error!(
                        "Failed to remove HNSW index ID: {} at path: {}. Error: {}",
                        id,
                        index_storage_path.to_str().unwrap(),
                        e
                    );
                    Err(e)
                }
            },
        }
    }

    async fn create_dir_all(&self, path: &PathBuf) -> Result<(), Box<HnswIndexProviderFileError>> {
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| Box::new(HnswIndexProviderFileError::IOError(e)))
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderOpenError {
    #[error("Hnsw index file error")]
    FileError(#[from] HnswIndexProviderFileError),
    #[error("Index load error")]
    IndexLoadError(#[from] Box<dyn ChromaError>),
    #[error("Path: {0} could not be converted to string")]
    PathToStringError(PathBuf),
    #[error("Failed to cleanup files")]
    CleanupError(#[from] tokio::io::Error),
}

impl ChromaError for HnswIndexProviderOpenError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswIndexProviderOpenError::FileError(_) => ErrorCodes::Internal,
            HnswIndexProviderOpenError::IndexLoadError(e) => e.code(),
            HnswIndexProviderOpenError::PathToStringError(_) => ErrorCodes::InvalidArgument,
            HnswIndexProviderOpenError::CleanupError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderForkError {
    #[error("Hnsw index file error")]
    FileError(#[from] HnswIndexProviderFileError),
    #[error("Index load error")]
    IndexLoadError(#[from] Box<dyn ChromaError>),
    #[error("Path: {0} could not be converted to string")]
    PathToStringError(PathBuf),
}

impl ChromaError for HnswIndexProviderForkError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswIndexProviderForkError::FileError(_) => ErrorCodes::Internal,
            HnswIndexProviderForkError::IndexLoadError(e) => e.code(),
            HnswIndexProviderForkError::PathToStringError(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Error, Debug)]
pub enum HnswIndexProviderCreateError {
    #[error("Hnsw index file error")]
    FileError(#[from] HnswIndexProviderFileError),
    #[error("Hnsw config error")]
    HnswConfigError(#[from] HnswIndexConfigError),
    #[error("Index init error")]
    IndexInitError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for HnswIndexProviderCreateError {
    fn code(&self) -> ErrorCodes {
        match self {
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
    StoragePutError(#[from] chroma_storage::StorageError),
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
    use chroma_types::InternalHnswConfiguration;

    #[tokio::test]
    async fn test_fork() {
        let storage_dir = tempfile::tempdir().unwrap().path().to_path_buf();
        let hnsw_tmp_path = storage_dir.join("hnsw");

        // Create the directories needed
        tokio::fs::create_dir_all(&hnsw_tmp_path).await.unwrap();

        let storage = Storage::Local(LocalStorage::new(storage_dir.to_str().unwrap()));
        let cache = new_non_persistent_cache_for_test();
        let provider = HnswIndexProvider::new(storage, hnsw_tmp_path, cache, 16);
        let collection_id = CollectionUuid(Uuid::new_v4());

        let dimensionality = 128;
        let distance_function = DistanceFunction::Euclidean;
        let default_hnsw_params = InternalHnswConfiguration::default();
        let created_index = provider
            .create(
                &collection_id,
                default_hnsw_params.max_neighbors,
                default_hnsw_params.ef_construction,
                default_hnsw_params.ef_search,
                dimensionality,
                distance_function.clone(),
            )
            .await
            .unwrap();
        let created_index_id = created_index.inner.read().id;

        let forked_index = provider
            .fork(
                &created_index_id,
                &collection_id,
                dimensionality,
                distance_function,
                default_hnsw_params.ef_search,
            )
            .await
            .unwrap();
        let forked_index_id = forked_index.inner.read().id;

        assert_ne!(created_index_id, forked_index_id);
    }

    #[tokio::test]
    async fn test_open() {
        let storage_dir = tempfile::tempdir().unwrap().path().to_path_buf();

        // Create the directories needed
        tokio::fs::create_dir_all(&storage_dir).await.unwrap();

        let storage = Storage::Local(LocalStorage::new(storage_dir.to_str().unwrap()));
        let cache = new_non_persistent_cache_for_test();
        let provider = HnswIndexProvider::new(storage, storage_dir.clone(), cache, 16);
        let collection_id = CollectionUuid(Uuid::new_v4());

        let dimensionality = 2;
        let distance_function = DistanceFunction::Euclidean;
        let default_hnsw_params = InternalHnswConfiguration::default();
        let created_index = provider
            .create(
                &collection_id,
                default_hnsw_params.max_neighbors,
                default_hnsw_params.ef_construction,
                default_hnsw_params.ef_search,
                dimensionality,
                distance_function.clone(),
            )
            .await
            .unwrap();
        created_index
            .inner
            .write()
            .add(1, &[1.0, 3.0])
            .expect("Expected to add");
        let created_index_id = created_index.inner.read().id;
        provider.commit(created_index).expect("Expected to commit");
        provider
            .flush(&created_index_id)
            .await
            .expect("Expected to flush");
        // clear the cache.
        provider
            .cache
            .clear()
            .await
            .expect("Expected to clear cache");
        let open_index = provider
            .open(
                &created_index_id,
                &collection_id,
                dimensionality,
                distance_function,
                default_hnsw_params.ef_search,
            )
            .await
            .expect("Expect open to succeed");
        let opened_index_id = open_index.inner.read().id;

        assert_eq!(opened_index_id, created_index_id);
        check_purge_successful(storage_dir.clone()).await;
    }

    pub async fn check_purge_successful(path: impl AsRef<Path>) {
        let mut entries = tokio::fs::read_dir(&path)
            .await
            .expect("Failed to read dir");

        while let Some(entry) = entries.next_entry().await.expect("Failed to read next dir") {
            let path = entry.path();
            let metadata = entry.metadata().await.expect("Failed to read metadata");

            if metadata.is_dir() {
                assert!(
                    path.ends_with("hnsw")
                        || path.ends_with("block")
                        || path.ends_with("sparse_index")
                );
            } else {
                panic!("Expected hnsw purge to be successful")
            }
        }
    }
}
