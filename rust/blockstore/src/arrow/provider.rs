use super::{
    block::{delta::types::Delta, Block, BlockLoadError},
    blockfile::{ArrowBlockfileReader, ArrowUnorderedBlockfileWriter},
    config::ArrowBlockfileProviderConfig,
    ordered_blockfile_writer::ArrowOrderedBlockfileWriter,
    root::{FromBytesError, RootReader, RootWriter},
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
};
use crate::{
    key::KeyWrapper,
    memory::storage::Readable,
    provider::{CreateError, OpenError},
    BlockfileReader, BlockfileWriter, BlockfileWriterMutationOrdering, BlockfileWriterOptions, Key,
    Value,
};
use async_trait::async_trait;
use chroma_cache::{AysncPartitionedMutex, CacheError, PersistentCache};
use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, PutOptions, Storage,
};
use chroma_tracing::util::Stopwatch;
use futures::{future::Shared, stream::FuturesUnordered, FutureExt, StreamExt};
use opentelemetry::global;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tracing::{Instrument, Span};
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ArrowBlockfileProviderPrefetchError {
    #[error("Error reading root for blockfile: {0}")]
    RootManager(#[from] Box<dyn ChromaError>),
    #[error("Error fetching block: {0}")]
    BlockManager(#[from] GetError),
}

impl ChromaError for ArrowBlockfileProviderPrefetchError {
    fn code(&self) -> ErrorCodes {
        match self {
            ArrowBlockfileProviderPrefetchError::RootManager(e) => e.code(),
            ArrowBlockfileProviderPrefetchError::BlockManager(e) => e.code(),
        }
    }
}

const PREFETCH_TTL_HOURS: u64 = 8;

/// A BlockFileProvider that creates ArrowBlockfiles (Arrow-backed blockfiles used for production).
/// For now, it keeps a simple local cache of blockfiles.
#[derive(Clone)]
pub struct ArrowBlockfileProvider {
    block_manager: BlockManager,
    root_manager: RootManager,
}

pub struct BlockfileReaderOptions {
    id: uuid::Uuid,
    prefix_path: String,
}

impl BlockfileReaderOptions {
    pub fn new(id: Uuid, prefix_path: String) -> Self {
        BlockfileReaderOptions { id, prefix_path }
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }
}

impl ArrowBlockfileProvider {
    pub fn new(
        storage: Storage,
        max_block_size_bytes: usize,
        block_cache: Box<dyn PersistentCache<Uuid, Block>>,
        root_cache: Box<dyn PersistentCache<Uuid, RootReader>>,
    ) -> Self {
        Self {
            block_manager: BlockManager::new(storage.clone(), max_block_size_bytes, block_cache),
            root_manager: RootManager::new(storage, root_cache),
        }
    }

    pub async fn read<
        'new,
        K: Key + Into<KeyWrapper> + ArrowReadableKey<'new> + 'new,
        V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        options: BlockfileReaderOptions,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        let root = self
            .root_manager
            .get::<K>(
                &options.id,
                &options.prefix_path,
                self.block_manager.default_max_block_size_bytes(),
            )
            .await;
        match root {
            Ok(Some(root)) => Ok(BlockfileReader::ArrowBlockfileReader(
                ArrowBlockfileReader::new(self.block_manager.clone(), root),
            )),
            Ok(None) => Err(Box::new(OpenError::NotFound)),
            Err(e) => Err(Box::new(OpenError::Other(Box::new(e)))),
        }
    }

    pub async fn prefetch(
        &self,
        id: &Uuid,
        prefix_path: &str,
    ) -> Result<usize, ArrowBlockfileProviderPrefetchError> {
        if !self.root_manager.should_prefetch(id) {
            return Ok(0);
        }
        // We call .get_all_block_ids() here instead of just reading the root because reading the root requires a concrete Key type.
        let block_ids = self
            .root_manager
            .get_all_block_ids(id, prefix_path)
            .await
            .map_err(|e| ArrowBlockfileProviderPrefetchError::RootManager(Box::new(e)))?;

        let mut futures = FuturesUnordered::new();
        for block_id in block_ids.iter() {
            // Don't prefetch if already cached.
            if !self.block_manager.cached(block_id).await {
                futures.push(self.block_manager.get(
                    prefix_path,
                    block_id,
                    StorageRequestPriority::P1,
                ));
            }
        }
        let count = futures.len();

        tracing::info!("Prefetching {} blocks for blockfile ID: {:?}", count, id);

        while let Some(result) = futures.next().await {
            result?;
        }

        tracing::info!("Prefetched {} blocks for blockfile ID: {:?}", count, id);
        Ok(count)
    }

    pub async fn write<
        'new,
        K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
        V: Value + ArrowWriteableValue + 'new,
    >(
        &self,
        options: BlockfileWriterOptions,
    ) -> Result<crate::BlockfileWriter, Box<CreateError>> {
        if let Some(fork_from) = options.fork_from {
            tracing::info!("Forking blockfile from {:?}", fork_from);
            let new_id = Uuid::new_v4();
            let new_root = self
                .root_manager
                .fork::<K>(
                    &fork_from,
                    new_id,
                    &options.prefix_path,
                    self.block_manager.default_max_block_size_bytes(),
                )
                .await
                .map_err(|e| {
                    tracing::error!("Error forking root: {:?}", e);
                    Box::new(CreateError::Other(Box::new(e)))
                })?;

            match options.mutation_ordering {
                BlockfileWriterMutationOrdering::Ordered => {
                    let file = ArrowOrderedBlockfileWriter::from_root(
                        new_id,
                        self.block_manager.clone(),
                        self.root_manager.clone(),
                        new_root,
                    );

                    Ok(BlockfileWriter::ArrowOrderedBlockfileWriter(file))
                }
                BlockfileWriterMutationOrdering::Unordered => {
                    let file = ArrowUnorderedBlockfileWriter::from_root(
                        new_id,
                        self.block_manager.clone(),
                        self.root_manager.clone(),
                        new_root,
                    );
                    Ok(BlockfileWriter::ArrowUnorderedBlockfileWriter(file))
                }
            }
        } else {
            let new_id = Uuid::new_v4();
            let max_block_size_bytes = options
                .max_block_size_bytes
                .unwrap_or(self.block_manager.default_max_block_size_bytes());

            match options.mutation_ordering {
                BlockfileWriterMutationOrdering::Ordered => {
                    let file = ArrowOrderedBlockfileWriter::new::<K, V>(
                        new_id,
                        &options.prefix_path,
                        self.block_manager.clone(),
                        self.root_manager.clone(),
                        max_block_size_bytes,
                    );

                    Ok(BlockfileWriter::ArrowOrderedBlockfileWriter(file))
                }
                BlockfileWriterMutationOrdering::Unordered => {
                    let file = ArrowUnorderedBlockfileWriter::new::<K, V>(
                        new_id,
                        &options.prefix_path,
                        self.block_manager.clone(),
                        self.root_manager.clone(),
                        max_block_size_bytes,
                    );
                    Ok(BlockfileWriter::ArrowUnorderedBlockfileWriter(file))
                }
            }
        }
    }

    pub async fn clear(&self) -> Result<(), CacheError> {
        self.block_manager.block_cache.clear().await?;
        self.root_manager.cache.clear().await?;
        self.root_manager.prefetched_roots.lock().clear();
        Ok(())
    }
}

#[async_trait]
impl Configurable<(ArrowBlockfileProviderConfig, Storage)> for ArrowBlockfileProvider {
    async fn try_from_config(
        config: &(ArrowBlockfileProviderConfig, Storage),
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (blockfile_config, storage) = config;
        let block_cache = match chroma_cache::from_config_persistent(
            &blockfile_config.block_manager_config.block_cache_config,
        )
        .await
        {
            Ok(cache) => cache,
            Err(e) => {
                return Err(e);
            }
        };
        let sparse_index_cache: Box<dyn PersistentCache<_, _>> =
            match chroma_cache::from_config_persistent(
                &blockfile_config.root_manager_config.root_cache_config,
            )
            .await
            {
                Ok(cache) => cache,
                Err(e) => {
                    return Err(e);
                }
            };
        Ok(ArrowBlockfileProvider::new(
            storage.clone(),
            blockfile_config.block_manager_config.max_block_size_bytes,
            block_cache,
            sparse_index_cache,
        ))
    }
}

#[derive(Error, Debug)]
pub enum GetError {
    #[error(transparent)]
    BlockLoadError(#[from] BlockLoadError),
    #[error(transparent)]
    StorageGetError(#[from] chroma_storage::StorageError),
}

impl Clone for GetError {
    fn clone(&self) -> Self {
        match self {
            GetError::BlockLoadError(e) => GetError::BlockLoadError(e.clone()),
            GetError::StorageGetError(e) => GetError::StorageGetError(e.clone()),
        }
    }
}

impl ChromaError for GetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetError::BlockLoadError(e) => e.code(),
            GetError::StorageGetError(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub(super) enum ForkError {
    #[error("Block not found")]
    BlockNotFound,
    #[error(transparent)]
    GetError(#[from] GetError),
}

impl ChromaError for ForkError {
    fn code(&self) -> ErrorCodes {
        match self {
            ForkError::BlockNotFound => ErrorCodes::NotFound,
            ForkError::GetError(e) => e.code(),
        }
    }
}

#[derive(Clone)]
pub struct BlockMetrics {
    pub commit_latency: opentelemetry::metrics::Histogram<u64>,
    pub num_blocks_flushed: opentelemetry::metrics::Histogram<u64>,
    pub flush_latency: opentelemetry::metrics::Histogram<u64>,
    pub num_get_requests: opentelemetry::metrics::Histogram<u64>,
}

impl Default for BlockMetrics {
    fn default() -> Self {
        let meter = global::meter("chroma");
        Self {
            commit_latency: meter
                .u64_histogram("block_commit_latency")
                .with_description("Commit latency")
                .with_unit("microseconds")
                .build(),
            num_blocks_flushed: meter
                .u64_histogram("block_num_blocks_flushed")
                .with_description("Number of blocks flushed")
                .with_unit("blocks")
                .build(),
            flush_latency: meter
                .u64_histogram("block_flush_latency")
                .with_description("Flush latency")
                .with_unit("milliseconds")
                .build(),
            num_get_requests: meter
                .u64_histogram("block_num_cold_get_requests")
                .with_description("Number of cold block get requests")
                .with_unit("requests")
                .build(),
        }
    }
}

/// A simple local cache of Arrow-backed blocks, the blockfile provider passes this
/// to the ArrowBlockfile when it creates a new blockfile. So that the blockfile can manage and access blocks
/// # Note
/// The implementation is currently very simple and not intended for robust production use. We should
/// introduce a more sophisticated cache that can handle tiered eviction and other features. This interface
/// is a placeholder for that.
#[derive(Clone)]
pub struct BlockManager {
    block_cache: Arc<dyn PersistentCache<Uuid, Block>>,
    storage: Storage,
    default_max_block_size_bytes: usize,
    block_metrics: BlockMetrics,
    cache_mutex: AysncPartitionedMutex<Uuid>,
    inflight_serde: Arc<
        tokio::sync::Mutex<
            HashMap<
                String,
                Shared<
                    Pin<Box<dyn Future<Output = Result<Option<Block>, GetError>> + Send + 'static>>,
                >,
            >,
        >,
    >,
}

impl BlockManager {
    pub(super) fn new(
        storage: Storage,
        default_max_block_size_bytes: usize,
        block_cache: Box<dyn PersistentCache<Uuid, Block>>,
    ) -> Self {
        let block_cache: Arc<dyn PersistentCache<Uuid, Block>> = block_cache.into();
        Self {
            block_cache,
            storage,
            default_max_block_size_bytes,
            block_metrics: BlockMetrics::default(),
            cache_mutex: AysncPartitionedMutex::new(()),
            inflight_serde: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn create<K: ArrowWriteableKey, V: ArrowWriteableValue, D: Delta>(&self) -> D {
        let new_block_id = Uuid::new_v4();
        D::new::<K, V>(new_block_id)
    }

    pub(super) async fn fork<K: ArrowWriteableKey, V: ArrowWriteableValue, D: Delta>(
        &self,
        block_id: &Uuid,
        prefix_path: &str,
    ) -> Result<D, ForkError> {
        let block = self
            .get(prefix_path, block_id, StorageRequestPriority::P0)
            .await;
        let block = match block {
            Ok(Some(block)) => block,
            Ok(None) => {
                return Err(ForkError::BlockNotFound);
            }
            Err(e) => {
                return Err(ForkError::GetError(e));
            }
        };
        let new_block_id = Uuid::new_v4();
        Ok(Delta::fork_block::<K, V>(new_block_id, &block))
    }

    pub(super) async fn commit<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        delta: impl Delta,
    ) -> Block {
        let _stopwatch = Stopwatch::new(
            &self.block_metrics.commit_latency,
            &[],
            chroma_tracing::util::StopWatchUnit::Micros,
        );
        let delta_id = delta.id();
        let record_batch = delta.finish::<K, V>(None);
        let block = Block::from_record_batch(delta_id, record_batch);
        self.block_cache.insert(delta_id, block.clone()).await;
        block
    }

    pub(super) async fn cached(&self, id: &Uuid) -> bool {
        self.block_cache
            .get(id)
            .await
            .map(|b| b.is_some())
            .unwrap_or(false)
    }

    pub fn format_key(prefix_path: &str, id: &Uuid) -> String {
        // For legacy collections, prefix_path is empty.
        if prefix_path.is_empty() {
            return format!("block/{}", id);
        }
        format!("{}/block/{}", prefix_path, id)
    }

    pub(super) async fn get(
        &self,
        prefix_path: &str,
        id: &Uuid,
        priority: StorageRequestPriority,
    ) -> Result<Option<Block>, GetError> {
        let block = self.block_cache.obtain(*id).await.ok().flatten();
        match block {
            Some(block) => Ok(Some(block)),
            None => {
                let key = Self::format_key(prefix_path, id);
                let future_to_await;

                {
                    let mut shared_future_lock = self.inflight_serde.lock().await;
                    let maybe_inflight = shared_future_lock.get(&key).cloned();
                    future_to_await = match maybe_inflight {
                        Some(future) => future,
                        None => {
                            let storage_clone = self.storage.clone();
                            let id_clone = id.clone();
                            let key_clone = key.clone();
                            let shared_future = async move {
                            let bytes_res = storage_clone
                                .get(&key_clone, GetOptions::new(priority))
                                .await;
                            match bytes_res {
                                Ok(bytes) => {
                                    // TODO: i removed the caching logic
                                    let deserialization_span = tracing::trace_span!(parent: Span::current(), "BlockManager deserialize block");
                                    let block =
                                        deserialization_span.in_scope(|| Block::from_bytes(&bytes, id_clone));
                                    block.map_err(GetError::BlockLoadError).map(Option::Some)
                                }
                                Err(e) => {
                                    tracing::error!("Error converting bytes to Block {:?}", e);
                                    Err(GetError::StorageGetError(e))
                                }
                            }
                    }.boxed().shared();
                            // Insert the future into the inflight serde map.
                            shared_future_lock.insert(key.clone(), shared_future.clone());
                            shared_future
                        }
                    };
                }

                let result = future_to_await.await;
                {
                    let mut requests = self.inflight_serde.lock().await;
                    requests.remove(&key);
                }

                result
            }
        }
    }

    pub(super) async fn flush(
        &self,
        block: &Block,
        prefix_path: &str,
    ) -> Result<(), Box<dyn ChromaError>> {
        let bytes = match block.to_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!("Failed to convert block to bytes");
                return Err(Box::new(e));
            }
        };
        let key = Self::format_key(prefix_path, &block.id);
        let _stopwatch = Stopwatch::new(
            &self.block_metrics.flush_latency,
            &[],
            chroma_tracing::util::StopWatchUnit::Millis,
        );
        let block_bytes_len = bytes.len();
        let res = self
            .storage
            .put_bytes(
                &key,
                bytes,
                PutOptions::with_priority(StorageRequestPriority::P0),
            )
            .await;
        match res {
            Ok(_) => {
                tracing::debug!(
                    "Block: {} written to storage ({}B)",
                    block.id,
                    block_bytes_len
                );
                self.block_metrics.num_blocks_flushed.record(1, &[]);
            }
            Err(e) => {
                tracing::info!("Error writing block to storage {}", e);
                return Err(Box::new(e));
            }
        }
        Ok(())
    }

    pub(super) fn default_max_block_size_bytes(&self) -> usize {
        self.default_max_block_size_bytes
    }
}

#[derive(Error, Debug)]
pub enum BlockFlushError {
    #[error("Not found")]
    NotFound,
}

impl ChromaError for BlockFlushError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockFlushError::NotFound => ErrorCodes::NotFound,
        }
    }
}

// ==============
// Root Manager
// ==============

#[derive(Error, Debug)]
pub enum RootManagerError {
    #[error("Not found")]
    NotFound,
    #[error(transparent)]
    BlockLoadError(#[from] BlockLoadError),
    #[error(transparent)]
    UUIDParseError(#[from] uuid::Error),
    #[error(transparent)]
    StorageGetError(#[from] chroma_storage::StorageError),
    #[error(transparent)]
    FromBytesError(#[from] FromBytesError),
}

impl ChromaError for RootManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            RootManagerError::NotFound => ErrorCodes::NotFound,
            RootManagerError::BlockLoadError(e) => e.code(),
            RootManagerError::StorageGetError(e) => e.code(),
            RootManagerError::UUIDParseError(_) => ErrorCodes::DataLoss,
            RootManagerError::FromBytesError(e) => e.code(),
        }
    }
}

#[derive(Clone)]
pub struct RootManager {
    cache: Arc<dyn PersistentCache<Uuid, RootReader>>,
    storage: Storage,
    // Sparse indexes that have already been prefetched and don't need to be prefetched again.
    prefetched_roots: Arc<parking_lot::Mutex<HashMap<Uuid, Duration>>>,
}

impl std::fmt::Debug for RootManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootManager")
            .field("cache", &self.cache)
            .field("storage", &self.storage)
            .finish()
    }
}

impl RootManager {
    pub fn new(storage: Storage, cache: Box<dyn PersistentCache<Uuid, RootReader>>) -> Self {
        let cache: Arc<dyn PersistentCache<Uuid, RootReader>> = cache.into();
        Self {
            cache,
            storage,
            prefetched_roots: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }
    }

    pub async fn get<'new, K: ArrowReadableKey<'new> + 'new>(
        &self,
        id: &Uuid,
        prefix_path: &str,
        max_block_size_bytes: usize,
    ) -> Result<Option<RootReader>, RootManagerError> {
        let index = self.cache.obtain(*id).await.ok().flatten();
        match index {
            Some(index) => Ok(Some(index)),
            None => {
                tracing::info!("Cache miss - fetching root from storage");
                let key = Self::get_storage_key(prefix_path, id);
                tracing::debug!("Reading root from storage with key: {}", key);
                match self
                    .storage
                    .get(&key, GetOptions::new(StorageRequestPriority::P0))
                    .await
                {
                    Ok(bytes) => match RootReader::from_bytes::<K>(
                        &bytes,
                        prefix_path,
                        *id,
                        max_block_size_bytes,
                    ) {
                        Ok(root) => {
                            self.cache.insert(*id, root.clone()).await;
                            Ok(Some(root))
                        }
                        Err(e) => {
                            tracing::error!("Error turning bytes into root: {}", e);
                            Err(RootManagerError::FromBytesError(e))
                        }
                    },
                    Err(e) => {
                        tracing::error!("Error reading root from storage: {}", e);
                        Err(RootManagerError::StorageGetError(e))
                    }
                }
            }
        }
    }

    pub async fn get_all_block_ids(
        &self,
        id: &Uuid,
        prefix_path: &str,
    ) -> Result<Vec<Uuid>, RootManagerError> {
        let key = Self::get_storage_key(prefix_path, id);
        tracing::debug!("Reading root from storage with key: {}", key);
        match self
            .storage
            .get(&key, GetOptions::new(StorageRequestPriority::P0))
            .await
        {
            Ok(bytes) => RootReader::get_all_block_ids_from_bytes(&bytes, *id)
                .map_err(RootManagerError::FromBytesError),
            Err(e) => {
                tracing::error!("Error reading root from storage: {}", e);
                Err(RootManagerError::StorageGetError(e))
            }
        }
    }

    pub async fn flush<'read, K: ArrowWriteableKey + 'read>(
        &self,
        root: &RootWriter,
    ) -> Result<(), Box<dyn ChromaError>> {
        let bytes = match root.to_bytes::<K>() {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!("Failed to convert root to bytes");
                return Err(Box::new(e));
            }
        };
        let key = Self::get_storage_key(&root.prefix_path, &root.id);
        let res = self
            .storage
            .put_bytes(
                &key,
                bytes,
                PutOptions::with_priority(StorageRequestPriority::P0),
            )
            .await;
        match res {
            Ok(_) => {
                tracing::info!("Root written to storage");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Error writing root to storage");
                Err(Box::new(e))
            }
        }
    }

    pub async fn fork<'key, K: ArrowWriteableKey + 'key>(
        &self,
        old_id: &Uuid,
        new_id: Uuid,
        prefix_path: &str,
        max_block_size_bytes: usize,
    ) -> Result<RootWriter, RootManagerError> {
        tracing::info!("Forking root from {:?}", old_id);
        let original = self
            .get::<K::ReadableKey<'key>>(old_id, prefix_path, max_block_size_bytes)
            .await?;
        match original {
            Some(original) => {
                let forked = original.fork(new_id);
                Ok(forked)
            }
            None => Err(RootManagerError::NotFound),
        }
    }

    pub fn get_storage_key(prefix_path: &str, id: &Uuid) -> String {
        // For legacy collections, prefix_path is empty.
        if prefix_path.is_empty() {
            return format!("sparse_index/{}", id);
        }
        format!("{}/root/{}", prefix_path, id)
    }

    fn should_prefetch(&self, id: &Uuid) -> bool {
        let mut lock_guard = self.prefetched_roots.lock();
        let expires_at = lock_guard.get(id);
        match expires_at {
            Some(expires_at) => {
                if SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Do not deploy before UNIX epoch")
                    < *expires_at
                {
                    false
                } else {
                    lock_guard.insert(
                        *id,
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Do not deploy before UNIX epoch")
                            + std::time::Duration::from_secs(PREFETCH_TTL_HOURS * 3600),
                    );
                    true
                }
            }
            None => {
                lock_guard.insert(
                    *id,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Do not deploy before UNIX epoch")
                        + std::time::Duration::from_secs(PREFETCH_TTL_HOURS * 3600),
                );
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;
    use crate::arrow::block::{self, delta::UnorderedBlockDelta};
    use chroma_cache::{new_cache_for_test, FoyerCacheConfig};
    use chroma_storage::test_storage;
    use chroma_types::SpannPostingList;

    #[tokio::test]
    async fn test_cached() {
        let (_temp_dir, storage) = test_storage();
        let manager = BlockManager::new(storage, 100, new_cache_for_test());
        assert!(!manager.cached(&Uuid::new_v4()).await);

        let delta = manager.create::<&str, String, UnorderedBlockDelta>();
        let block = manager.commit::<&str, String>(delta).await;
        assert!(manager.cached(&block.id).await, "should be write-through");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 12)]
    async fn test_cache_stress() {
        let (_temp_dir, storage) = test_storage();

        let cache = FoyerCacheConfig {
            dir: Some("./hammad/test_cache".to_string()),
            capacity: 6000, // 6000 MB = 6 GB
            mem: 6000,      // 6000 MB = 6 GB
            ..Default::default()
        };
        let cache = cache.build_hybrid().await.expect("Failed to create cache");
        let manager = BlockManager::new(storage, 8 * 1024 * 1024, cache);
        assert!(!manager.cached(&Uuid::new_v4()).await);

        let n_req = 10;
        let n_tasks_per_req = 64;
        let n_block_fetches_per_task = 140; // ~88,000 cache accesses

        let mut block_ids = Vec::new();
        // Add ~5MB of data to the block
        let value_size = 1024 * 1024; // 1MB per value
        for d in 0..n_block_fetches_per_task {
            let delta = manager.create::<&str, String, UnorderedBlockDelta>();
            for i in 0..5 {
                delta.add("", format!("key_{}", i).as_str(), "".repeat(value_size));
            }
            let block = manager.commit::<&str, String>(delta).await;
            let block_id = block.id.clone();
            block_ids.push(block_id);
        }

        for block_id in block_ids.iter() {
            assert!(manager.cached(block_id).await, "should be cached yet");
        }

        let counter = Arc::new(AtomicU32::new(0));
        let mut tasks = Vec::new();
        for c in 0..n_req {
            for t in 0..n_tasks_per_req {
                let manager_clone = manager.clone();
                let counter_clone = counter.clone();
                let block_ids = block_ids.clone();
                let task = tokio::spawn(async move {
                    let futures_unorded = FuturesUnordered::new();
                    for i in 0..n_block_fetches_per_task {
                        let block = manager_clone.get(
                            "",
                            &block_ids[i % block_ids.len()],
                            StorageRequestPriority::P0,
                        );
                        futures_unorded.push(block);
                    }
                    futures_unorded
                        .for_each_concurrent(n_tasks_per_req, |result| async {
                            match result {
                                Ok(Some(_)) => {
                                    counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                }
                                Ok(None) => {
                                    panic!("Block not found in cache");
                                }
                                Err(e) => {
                                    panic!("Error fetching block: {:?}", e);
                                }
                            }
                        })
                        .await;
                });
                tasks.push(task);
            }
        }
        let mut futures = FuturesUnordered::new();
        for task in tasks {
            futures.push(task);
        }
        let start_time = std::time::Instant::now();
        while let Some(result) = futures.next().await {
            result.expect("Task failed");
        }
        let elapsed = start_time.elapsed();
        println!("Finished in {} seconds", elapsed.as_secs_f64());
        println!(
            "Total cache accesses: {}",
            counter.load(std::sync::atomic::Ordering::SeqCst)
        );
        // assert!(manager.cached(&block_id).await, "should still be cached");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 12)]
    async fn test_serde_stress() {
        let (_temp_dir, storage) = test_storage();

        let cache = FoyerCacheConfig {
            dir: Some("./hammad/test_cache".to_string()),
            capacity: 10, // 500 MB
            mem: 10,      // 500 MB
            ..Default::default()
        };
        let cache = cache.build_hybrid().await.expect("Failed to create cache");
        let manager = BlockManager::new(storage, 8 * 1024 * 1024, cache);
        assert!(!manager.cached(&Uuid::new_v4()).await);

        let n_req = 10;
        let n_tasks_per_req = 64;
        let n_block_fetches_per_task = 140; // ~88,000 cache accesses

        let mut block_ids = Vec::new();
        // Add ~5MB of data to the block
        let offset_ids = (0..100).collect::<Vec<_>>();
        let versions = (0..100).collect::<Vec<_>>();
        let embeddings = (0..100 * 1536)
            .collect::<Vec<_>>()
            .iter()
            .map(|x| *x as f32)
            .collect::<Vec<_>>();
        for d in 0..n_block_fetches_per_task {
            let delta = manager.create::<&str, &SpannPostingList, UnorderedBlockDelta>();
            for i in 0..5 {
                let pl = SpannPostingList {
                    doc_offset_ids: &offset_ids,
                    doc_versions: &versions,
                    doc_embeddings: &embeddings,
                };
                delta.add("", format!("key_{}", i).as_str(), &pl);
            }
            let block = manager.commit::<&str, String>(delta).await;
            manager
                .flush(&block, "test")
                .await
                .expect("Failed to flush block");
            let block_id = block.id.clone();
            block_ids.push(block_id);
        }

        let counter = Arc::new(AtomicU32::new(0));
        let mut tasks = Vec::new();
        for c in 0..n_req {
            for t in 0..n_tasks_per_req {
                let manager_clone = manager.clone();
                let counter_clone = counter.clone();
                let block_ids = block_ids.clone();
                let task = tokio::spawn(async move {
                    let futures_unorded = FuturesUnordered::new();
                    for i in 0..n_block_fetches_per_task {
                        let block = manager_clone.get(
                            "test",
                            &block_ids[i % block_ids.len()],
                            StorageRequestPriority::P0,
                        );
                        futures_unorded.push(block);
                    }
                    futures_unorded
                        .for_each_concurrent(n_tasks_per_req, |result| async {
                            match result {
                                Ok(Some(_)) => {
                                    counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                }
                                Ok(None) => {
                                    panic!("Block not found in cache");
                                }
                                Err(e) => {
                                    panic!("Error fetching block: {:?}", e);
                                }
                            }
                        })
                        .await;
                });
                tasks.push(task);
            }
        }
        let mut futures = FuturesUnordered::new();
        for task in tasks {
            futures.push(task);
        }
        let start_time = std::time::Instant::now();
        while let Some(result) = futures.next().await {
            result.expect("Task failed");
        }
        let elapsed = start_time.elapsed();
        println!("Finished in {} seconds", elapsed.as_secs_f64());
        println!(
            "Total cache accesses: {}",
            counter.load(std::sync::atomic::Ordering::SeqCst)
        );
        // assert!(manager.cached(&block_id).await, "should still be cached");
    }
}
