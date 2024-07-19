use super::{
    block::{delta::BlockDelta, Block},
    blockfile::{ArrowBlockfileReader, ArrowBlockfileWriter},
    config::ArrowBlockfileProviderConfig,
    sparse_index::SparseIndex,
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
};
use crate::cache::cache::Cache;
use crate::{
    blockstore::{
        key::KeyWrapper,
        memory::storage::Readable,
        provider::{CreateError, OpenError},
        BlockfileReader, BlockfileWriter, Key, Value,
    },
    config::Configurable,
    errors::{ChromaError, ErrorCodes},
    storage::Storage,
};
use async_trait::async_trait;
use core::panic;
use futures::{future::join_all, StreamExt};
use thiserror::Error;
use tracing::{Instrument, Span};
use uuid::Uuid;

/// A BlockFileProvider that creates ArrowBlockfiles (Arrow-backed blockfiles used for production).
/// For now, it keeps a simple local cache of blockfiles.
#[derive(Clone)]
pub(crate) struct ArrowBlockfileProvider {
    block_manager: BlockManager,
    sparse_index_manager: SparseIndexManager,
}

impl ArrowBlockfileProvider {
    pub(crate) fn new(
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

    pub(crate) async fn open<
        'new,
        K: Key + Into<KeyWrapper> + ArrowReadableKey<'new> + 'new,
        V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        let sparse_index = self.sparse_index_manager.get::<K>(id).await;
        match sparse_index {
            Some(sparse_index) => Ok(BlockfileReader::ArrowBlockfileReader(
                ArrowBlockfileReader::new(*id, self.block_manager.clone(), sparse_index),
            )),
            None => {
                return Err(Box::new(OpenError::NotFound));
            }
        }
    }

    pub(crate) fn create<
        'new,
        K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
        V: Value + crate::blockstore::memory::storage::Writeable + ArrowWriteableValue + 'new,
    >(
        &self,
    ) -> Result<crate::blockstore::BlockfileWriter, Box<CreateError>> {
        // Create a new blockfile and return a writer
        let new_id = Uuid::new_v4();
        let file = ArrowBlockfileWriter::new::<K, V>(
            new_id,
            self.block_manager.clone(),
            self.sparse_index_manager.clone(),
        );
        Ok(BlockfileWriter::ArrowBlockfileWriter(file))
    }

    pub(crate) async fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<crate::blockstore::BlockfileWriter, Box<CreateError>> {
        tracing::info!("Forking blockfile from {:?}", id);
        let new_id = Uuid::new_v4();
        let new_sparse_index = self.sparse_index_manager.fork::<K>(id, new_id).await;
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
        let block_cache = match crate::cache::from_config(
            &blockfile_config.block_manager_config.block_cache_config,
        )
        .await
        {
            Ok(cache) => cache,
            Err(e) => {
                return Err(e);
            }
        };
        let sparse_index_cache = match crate::cache::from_config(
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
    ) -> BlockDelta {
        let block = self.get(block_id).await;
        let block = match block {
            Some(block) => block,
            None => {
                // TODO: Err - tried to fork a block not owned by this manager
                panic!("Tried to fork a block not owned by this manager")
            }
        };
        let new_block_id = Uuid::new_v4();
        let delta = BlockDelta::new::<KeyWrite, ValueWrite>(new_block_id);
        let populated_delta = self.fork_lifetime_scope::<KeyWrite, ValueWrite>(&block, delta);
        populated_delta
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
        delta: &BlockDelta,
    ) -> Block {
        let record_batch = delta.finish::<K, V>();
        let block = Block::from_record_batch(delta.id, record_batch);
        block
    }

    pub(super) async fn get(&self, id: &Uuid) -> Option<Block> {
        let block = self.block_cache.get(id);
        match block {
            Some(block) => Some(block.clone()),
            None => {
                async {
                    let key = format!("block/{}", id);
                    let stream = self.storage.get(&key).instrument(
                        tracing::trace_span!(parent: Span::current(), "BlockManager storage get"),
                    ).await;
                    match stream {
                        Ok(mut bytes) => {
                            let read_block_span = tracing::trace_span!(parent: Span::current(), "BlockManager read bytes to end for block get");
                            let buf = read_block_span.in_scope(|| async {
                                let mut buf: Vec<u8> = Vec::new();
                                while let Some(res) = bytes.next().await {
                                    match res {
                                        Ok(chunk) => {
                                            buf.extend(chunk);
                                        }
                                        Err(e) => {
                                            tracing::error!("Error reading block from storage: {}", e);
                                            return None;
                                        }
                                    }
                                }
                                Some(buf)
                            }
                            ).await;
                            let buf =  match buf {
                                Some(buf) => {
                                    buf
                                }
                                None => {
                                    return None;
                                }
                            };
                            tracing::info!("Read {:?} bytes from s3 for block get", buf.len());
                            let deserialization_span = tracing::trace_span!(parent: Span::current(), "BlockManager deserialize block");
                            let block = deserialization_span.in_scope(|| Block::from_bytes(&buf, *id));
                            match block {
                                Ok(block) => {
                                    self.block_cache.insert(*id, block.clone());
                                    Some(block)
                                }
                                Err(e) => {
                                    // TODO: Return an error to callsite instead of None.
                                    tracing::error!(
                                        "Error converting bytes to Block {:?}/{:?}",
                                        key,
                                        e
                                    );
                                    None
                                }
                            }
                        },
                        Err(e) => {
                            tracing::error!("Error reading block from storage: {}", e);
                            None
                        }
                    }
                }
                .instrument(tracing::trace_span!(parent: Span::current(), "BlockManager get cold"))
                .await
            }
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
    ) -> Option<SparseIndex> {
        let index = self.cache.get(id);
        match index {
            Some(index) => Some(index),
            None => {
                // TODO: move this to a separate function
                tracing::info!("Cache miss - fetching sparse index from storage");
                let key = format!("sparse_index/{}", id);
                tracing::debug!("Reading sparse index from storage with key: {}", key);
                let stream = self.storage.get(&key).await;
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
                                    return None;
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
                                        return Some(index);
                                    }
                                    Err(e) => {
                                        // TODO: return error
                                        tracing::error!(
                                            "Error turning block into sparse index: {}",
                                            e
                                        );
                                        return None;
                                    }
                                }
                            }
                            Err(e) => {
                                // TODO: return error
                                tracing::error!("Error turning bytes into block: {}", e);
                                return None;
                            }
                        }
                    }
                    Err(e) => {
                        // TODO: return error
                        tracing::error!("Error reading sparse index from storage: {}", e);
                        return None;
                    }
                }
            }
        }
    }

    pub fn create(&self, id: &Uuid) -> SparseIndex {
        let index = SparseIndex::new(*id);
        index
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
    ) -> SparseIndex {
        // TODO: error handling
        tracing::info!("Forking sparse index from {:?}", old_id);
        let original = self.get::<K::ReadableKey<'key>>(old_id).await.unwrap();
        let forked = original.fork(new_id);
        forked
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
