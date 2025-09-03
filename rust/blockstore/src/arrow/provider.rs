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
use chroma_cache::{CacheError, PersistentCache};
use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, PutOptions, Storage, StorageError,
};
use chroma_tracing::util::{LogSlowOperation, Stopwatch};
use futures::{stream::FuturesUnordered, StreamExt};
use opentelemetry::global;
use std::{
    collections::HashMap,
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
        num_concurrent_block_flushes: usize,
    ) -> Self {
        Self {
            block_manager: BlockManager::new(
                storage.clone(),
                max_block_size_bytes,
                block_cache,
                num_concurrent_block_flushes,
            ),
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

#[derive(Error, Debug)]
pub enum ArrowBlockfileProviderError {
    #[error("Invalid config")]
    ConfigValidationError,
}

impl ChromaError for ArrowBlockfileProviderError {
    fn code(&self) -> ErrorCodes {
        match self {
            ArrowBlockfileProviderError::ConfigValidationError => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Configurable<(ArrowBlockfileProviderConfig, Storage)> for ArrowBlockfileProvider {
    async fn try_from_config(
        config: &(ArrowBlockfileProviderConfig, Storage),
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (blockfile_config, storage) = config;
        blockfile_config
            .block_manager_config
            .validate()
            .then_some(())
            .ok_or(ArrowBlockfileProviderError::ConfigValidationError)
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
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
            blockfile_config
                .block_manager_config
                .num_concurrent_block_flushes,
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
    storage: Arc<Storage>,
    default_max_block_size_bytes: usize,
    block_metrics: BlockMetrics,
    num_concurrent_block_flushes: usize,
}

impl BlockManager {
    pub(super) fn new(
        storage: Storage,
        default_max_block_size_bytes: usize,
        block_cache: Box<dyn PersistentCache<Uuid, Block>>,
        num_concurrent_block_flushes: usize,
    ) -> Self {
        let block_cache: Arc<dyn PersistentCache<Uuid, Block>> = block_cache.into();
        let storage = Arc::new(storage);
        Self {
            block_cache,
            storage,
            default_max_block_size_bytes,
            block_metrics: BlockMetrics::default(),
            num_concurrent_block_flushes,
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
        if let Some(block) = block {
            return Ok(Some(block));
        }

        // Closure cloning
        let key = Self::format_key(prefix_path, id);
        let id_clone = *id;
        let block_cache_clone = self.block_cache.clone();
        let key_clone = key.clone();
        let num_get_requests_metric_clone = self.block_metrics.num_get_requests.clone();

        let res = {
            let _slow_operation_log = LogSlowOperation::new(
                format!(
                    "Cold block fetch from storage and deserialize: {}",
                    key_clone
                ),
                Duration::from_millis(100),
            );
            self.storage
                .fetch(&key, GetOptions::new(priority), move |bytes| async move {
                    let bytes = match bytes {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            tracing::error!("Error loading block from storage: {:?}", e);
                            return Err(StorageError::Message {
                                message: "Error loading block".to_string(),
                            });
                        }
                    };
                    num_get_requests_metric_clone.record(1, &[]);
                    let block = Block::from_bytes(&bytes, id_clone);
                    match block {
                        Ok(block) => {
                            block_cache_clone.insert(id_clone, block.clone()).await;
                            Ok(block)
                        }
                        Err(e) => {
                            tracing::error!(
                                "Error converting bytes to Block {:?}/{:?}",
                                key_clone,
                                e
                            );
                            // TODO(hammadb): We should ideally use BlockLoadError here since that is what this level of the code expects,
                            // however that type is not trivially Clone. Since for all practical purposes this error results in the same upstream handling
                            // and observability properties we use a generic StorageError here.
                            Err(StorageError::Message {
                                message: "Error converting bytes to Block".to_string(),
                            })
                        }
                    }
                })
                .instrument(Span::current())
                .await
        };
        match res {
            Ok(block) => Ok(Some(block.0)),
            Err(e) => {
                tracing::error!("Error fetching block from storage: {:?}", e);
                Err(GetError::StorageGetError(e))
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

    pub(super) fn num_concurrent_block_flushes(&self) -> usize {
        self.num_concurrent_block_flushes
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
                let key = Self::get_storage_key(prefix_path, id);
                let _slow_operation_log = LogSlowOperation::new(
                    format!(
                        "Cold root fetch from storage and deserialize for key: {}",
                        key
                    ),
                    Duration::from_millis(50),
                );
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
    use super::*;
    use crate::arrow::{block::delta::UnorderedBlockDelta, config::BlockManagerConfig};
    use chroma_cache::new_cache_for_test;
    use chroma_storage::test_storage;

    #[tokio::test]
    async fn test_cached() {
        let (_temp_dir, storage) = test_storage();
        let manager = BlockManager::new(
            storage,
            100,
            new_cache_for_test(),
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        assert!(!manager.cached(&Uuid::new_v4()).await);

        let delta = manager.create::<&str, String, UnorderedBlockDelta>();
        let block = manager.commit::<&str, String>(delta).await;
        assert!(manager.cached(&block.id).await, "should be write-through");
    }
}
