use super::{
    block::{delta::BlockDelta, Block, BlockLoadError},
    blockfile::{ArrowBlockfileReader, ArrowBlockfileWriter},
    config::ArrowBlockfileProviderConfig,
    sparse_index::SparseIndex,
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
};
use crate::{
    key::KeyWrapper,
    memory::storage::Readable,
    provider::{CreateError, OpenError},
    BlockfileReader, BlockfileWriter, Key, Value,
};
use async_trait::async_trait;
use chroma_cache::cache::Cache;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use core::panic;
use futures::StreamExt;
use std::sync::Arc;
use thiserror::Error;
use tracing::{Instrument, Span};
use uuid::Uuid;

/// A BlockFileProvider that creates ArrowBlockfiles (Arrow-backed blockfiles used for production).
/// For now, it keeps a simple local cache of blockfiles.
#[derive(Clone)]
pub struct ArrowBlockfileProvider {
    block_manager: BlockManager,
    sparse_index_manager: SparseIndexManager,
}

impl ArrowBlockfileProvider {
    pub fn new(
        storage: Storage,
        max_block_size_bytes: usize,
        block_cache: Cache<Uuid, Block>,
        sparse_index_cache: Cache<Uuid, SparseIndex>,
    ) -> Self {
        Self {
            block_manager: BlockManager::new(storage.clone(), max_block_size_bytes, block_cache),
            sparse_index_manager: SparseIndexManager::new(storage, sparse_index_cache),
        }
    }

    pub async fn open<
        'new,
        K: Key + Into<KeyWrapper> + ArrowReadableKey<'new> + 'new,
        V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        let sparse_index = self.sparse_index_manager.get::<K>(id).await;
        match sparse_index {
            Ok(Some(sparse_index)) => Ok(BlockfileReader::ArrowBlockfileReader(
                ArrowBlockfileReader::new(*id, self.block_manager.clone(), sparse_index),
            )),
            Ok(None) => {
                return Err(Box::new(OpenError::NotFound));
            }
            Err(e) => {
                return Err(Box::new(OpenError::Other(Box::new(e))));
            }
        }
    }

    pub fn create<
        'new,
        K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
        V: Value + crate::memory::storage::Writeable + ArrowWriteableValue + 'new,
    >(
        &self,
    ) -> Result<crate::BlockfileWriter, Box<CreateError>> {
        // Create a new blockfile and return a writer
        let new_id = Uuid::new_v4();
        let file = ArrowBlockfileWriter::new::<K, V>(
            new_id,
            self.block_manager.clone(),
            self.sparse_index_manager.clone(),
        );
        Ok(BlockfileWriter::ArrowBlockfileWriter(file))
    }

    pub fn clear(&self) {
        self.block_manager.block_cache.clear();
        self.sparse_index_manager.cache.clear();
    }

    pub async fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<crate::BlockfileWriter, Box<CreateError>> {
        tracing::info!("Forking blockfile from {:?}", id);
        let new_id = Uuid::new_v4();
        let new_sparse_index = self
            .sparse_index_manager
            .fork::<K>(id, new_id)
            .await
            .map_err(|e| {
                tracing::error!("Error forking sparse index: {:?}", e);
                Box::new(CreateError::Other(Box::new(e)))
            })?;
        let file = ArrowBlockfileWriter::from_sparse_index(
            new_id,
            self.block_manager.clone(),
            self.sparse_index_manager.clone(),
            new_sparse_index,
        );
        Ok(BlockfileWriter::ArrowBlockfileWriter(file))
    }
}

#[async_trait]
impl Configurable<(ArrowBlockfileProviderConfig, Storage)> for ArrowBlockfileProvider {
    async fn try_from_config(
        config: &(ArrowBlockfileProviderConfig, Storage),
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (blockfile_config, storage) = config;
        let block_cache = match chroma_cache::from_config(
            &blockfile_config.block_manager_config.block_cache_config,
        )
        .await
        {
            Ok(cache) => cache,
            Err(e) => {
                return Err(e);
            }
        };
        let sparse_index_cache = match chroma_cache::from_config(
            &blockfile_config
                .sparse_index_manager_config
                .sparse_index_cache_config,
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
pub(super) enum GetError {
    #[error(transparent)]
    BlockLoadError(#[from] BlockLoadError),
    #[error(transparent)]
    StorageGetError(#[from] chroma_storage::GetError),
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

/// A simple local cache of Arrow-backed blocks, the blockfile provider passes this
/// to the ArrowBlockfile when it creates a new blockfile. So that the blockfile can manage and access blocks
/// # Note
/// The implementation is currently very simple and not intended for robust production use. We should
/// introduce a more sophisticated cache that can handle tiered eviction and other features. This interface
/// is a placeholder for that.
#[derive(Clone)]
pub(super) struct BlockManager {
    block_cache: Cache<Uuid, Block>,
    storage: Storage,
    max_block_size_bytes: usize,
    write_mutex: Arc<tokio::sync::Mutex<()>>,
}

impl BlockManager {
    pub(super) fn new(
        storage: Storage,
        max_block_size_bytes: usize,
        block_cache: Cache<Uuid, Block>,
    ) -> Self {
        Self {
            block_cache,
            storage,
            max_block_size_bytes,
            write_mutex: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(super) fn create<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self) -> BlockDelta {
        let new_block_id = Uuid::new_v4();
        let block = BlockDelta::new::<K, V>(new_block_id);
        block
    }

    pub(super) async fn fork<KeyWrite: ArrowWriteableKey, ValueWrite: ArrowWriteableValue>(
        &self,
        block_id: &Uuid,
    ) -> Result<BlockDelta, ForkError> {
        let block = self.get(block_id).await;
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
        let delta = BlockDelta::new::<KeyWrite, ValueWrite>(new_block_id);
        let populated_delta = self.fork_lifetime_scope::<KeyWrite, ValueWrite>(&block, delta);
        Ok(populated_delta)
    }

    fn fork_lifetime_scope<'new, KeyWrite, ValueWrite>(
        &self,
        block: &'new Block,
        delta: BlockDelta,
    ) -> BlockDelta
    where
        KeyWrite: ArrowWriteableKey,
        ValueWrite: ArrowWriteableValue,
    {
        block.to_block_delta::<KeyWrite::ReadableKey<'new>, ValueWrite::ReadableValue<'new>>(delta)
    }

    pub(super) fn commit<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        delta: BlockDelta,
    ) -> Block {
        let delta_id = delta.id;
        let record_batch = delta.finish::<K, V>();
        let block = Block::from_record_batch(delta_id, record_batch);
        block
    }

    pub(super) fn cached(&self, id: &Uuid) -> bool {
        self.block_cache.get(id).is_some()
    }

    pub(super) async fn get(&self, id: &Uuid) -> Result<Option<Block>, GetError> {
        let block = self.block_cache.get(id);
        match block {
            Some(block) => Ok(Some(block.clone())),
            None => async {
                let key = format!("block/{}", id);
                let bytes_res = self
                    .storage
                    .get(&key)
                    .instrument(
                        tracing::trace_span!(parent: Span::current(), "BlockManager storage get"),
                    )
                    .await;
                match bytes_res {
                    Ok(bytes) => {
                        let deserialization_span = tracing::trace_span!(parent: Span::current(), "BlockManager deserialize block");
                        let block =
                            deserialization_span.in_scope(|| Block::from_bytes(&bytes, *id));
                        match block {
                            Ok(block) => {
                                let _guard = self.write_mutex.lock().await;
                                match self.block_cache.get(id) {
                                    Some(b) => {
                                        return Ok(Some(b));
                                    }
                                    None => {
                                        self.block_cache.insert(*id, block.clone());
                                        Ok(Some(block))
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Error converting bytes to Block {:?}/{:?}",
                                    key,
                                    e
                                );
                                return Err(GetError::BlockLoadError(e));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error converting bytes to Block {:?}", e);
                        return Err(GetError::StorageGetError(e));
                    }
                }
            }.instrument(tracing::trace_span!(parent: Span::current(), "BlockManager get cold")).await
        }
    }

    pub(super) async fn flush(&self, block: &Block) -> Result<(), Box<dyn ChromaError>> {
        let bytes = match block.to_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!("Failed to convert block to bytes");
                return Err(Box::new(e));
            }
        };
        let key = format!("block/{}", block.id);
        let res = self.storage.put_bytes(&key, bytes).await;
        match res {
            Ok(_) => {
                tracing::info!("Block: {} written to storage", block.id);
            }
            Err(e) => {
                tracing::info!("Error writing block to storage {}", e);
                return Err(Box::new(e));
            }
        }
        Ok(())
    }

    pub(super) fn max_block_size_bytes(&self) -> usize {
        self.max_block_size_bytes
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

#[derive(Error, Debug)]
pub(super) enum SparseIndexManagerError {
    #[error("Not found")]
    NotFound,
    #[error(transparent)]
    BlockLoadError(#[from] BlockLoadError),
    #[error(transparent)]
    UUIDParseError(#[from] uuid::Error),
    #[error(transparent)]
    StorageGetError(#[from] chroma_storage::GetError),
}

impl ChromaError for SparseIndexManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            SparseIndexManagerError::NotFound => ErrorCodes::NotFound,
            SparseIndexManagerError::BlockLoadError(e) => e.code(),
            SparseIndexManagerError::StorageGetError(e) => e.code(),
            SparseIndexManagerError::UUIDParseError(_) => ErrorCodes::DataLoss,
        }
    }
}

#[derive(Clone)]
pub(super) struct SparseIndexManager {
    cache: Cache<Uuid, SparseIndex>,
    storage: Storage,
}

impl SparseIndexManager {
    pub fn new(storage: Storage, cache: Cache<Uuid, SparseIndex>) -> Self {
        Self { cache, storage }
    }

    pub async fn get<'new, K: ArrowReadableKey<'new> + 'new>(
        &self,
        id: &Uuid,
    ) -> Result<Option<SparseIndex>, SparseIndexManagerError> {
        let index = self.cache.get(id);
        match index {
            Some(index) => Ok(Some(index)),
            None => {
                tracing::info!("Cache miss - fetching sparse index from storage");
                let key = format!("sparse_index/{}", id);
                tracing::debug!("Reading sparse index from storage with key: {}", key);
                // TODO: This should pass through NAC as well.
                let stream = self.storage.get_stream(&key).await;
                let mut buf: Vec<u8> = Vec::new();
                match stream {
                    Ok(mut bytes) => {
                        while let Some(res) = bytes.next().await {
                            match res {
                                Ok(chunk) => {
                                    buf.extend(chunk);
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Error reading sparse index from storage: {}",
                                        e
                                    );
                                    return Err(SparseIndexManagerError::StorageGetError(e));
                                }
                            }
                        }
                        let block = Block::from_bytes(&buf, *id);
                        match block {
                            Ok(block) => {
                                let block_ref = &block;
                                // Use unsafe to promote the liftimes using unsafe, we know block lives as long as it needs to
                                // it only needs to live as long as the SparseIndex is created in from_block
                                // the sparse index copies the block so it can live as long as it needs to independently
                                let promoted_block: &'new Block =
                                    unsafe { std::mem::transmute(block_ref) };
                                let index = SparseIndex::from_block::<K>(promoted_block);
                                match index {
                                    Ok(index) => {
                                        self.cache.insert(*id, index.clone());
                                        return Ok(Some(index));
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Error turning block into sparse index: {}",
                                            e
                                        );
                                        return Err(SparseIndexManagerError::UUIDParseError(e));
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error turning bytes into block: {}", e);
                                return Err(SparseIndexManagerError::BlockLoadError(e));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading sparse index from storage: {}", e);
                        return Err(SparseIndexManagerError::StorageGetError(e));
                    }
                }
            }
        }
    }

    pub async fn flush<'read, K: ArrowWriteableKey + 'read>(
        &self,
        index: &SparseIndex,
    ) -> Result<(), Box<dyn ChromaError>> {
        let as_block = index.to_block::<K>();
        match as_block {
            Ok(block) => {
                let bytes = match block.to_bytes() {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        tracing::error!("Failed to convert sparse index to bytes");
                        return Err(Box::new(e));
                    }
                };
                let key = format!("sparse_index/{}", index.id);
                let res = self.storage.put_bytes(&key, bytes).await;
                match res {
                    Ok(_) => {
                        tracing::info!("Sparse index written to storage");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!("Error writing sparse index to storage");
                        Err(Box::new(e))
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to convert sparse index to block");
                Err(e)
            }
        }
    }

    pub async fn fork<'key, K: ArrowWriteableKey + 'key>(
        &self,
        old_id: &Uuid,
        new_id: Uuid,
    ) -> Result<SparseIndex, SparseIndexManagerError> {
        tracing::info!("Forking sparse index from {:?}", old_id);
        let original = self.get::<K::ReadableKey<'key>>(old_id).await?;
        match original {
            Some(original) => {
                let forked = original.fork(new_id);
                Ok(forked)
            }
            None => Err(SparseIndexManagerError::NotFound),
        }
    }
}

#[derive(Error, Debug)]
pub enum SparseIndexFlushError {
    #[error("Not found")]
    NotFound,
}

impl ChromaError for SparseIndexFlushError {
    fn code(&self) -> ErrorCodes {
        match self {
            SparseIndexFlushError::NotFound => ErrorCodes::NotFound,
        }
    }
}
