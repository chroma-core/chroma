use super::{
    block::{delta::BlockDelta, Block},
    blockfile::{ArrowBlockfileReader, ArrowBlockfileWriter},
    config::ArrowBlockfileProviderConfig,
    sparse_index::SparseIndex,
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
};
use crate::{
    blockstore::{
        key::KeyWrapper,
        memory::storage::Readable,
        provider::{CreateError, OpenError},
        BlockfileReader, BlockfileWriter, Key, Value,
    },
    config::Configurable,
    errors::{ChromaError, ErrorCodes},
    storage::{config::StorageConfig, Storage},
};
use async_trait::async_trait;
use core::panic;
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

/// A BlockFileProvider that creates ArrowBlockfiles (Arrow-backed blockfiles used for production).
/// For now, it keeps a simple local cache of blockfiles.
#[derive(Clone)]
pub(crate) struct ArrowBlockfileProvider {
    block_manager: BlockManager,
    sparse_index_manager: SparseIndexManager,
}

impl ArrowBlockfileProvider {
    pub(crate) fn new(storage: Storage, max_block_size_bytes: usize) -> Self {
        Self {
            block_manager: BlockManager::new(storage.clone(), max_block_size_bytes),
            sparse_index_manager: SparseIndexManager::new(storage),
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
        Ok(ArrowBlockfileProvider::new(
            storage.clone(),
            blockfile_config.max_block_size_bytes,
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
    read_cache: Arc<RwLock<HashMap<Uuid, Block>>>,
    storage: Storage,
    max_block_size_bytes: usize,
}

impl BlockManager {
    pub(super) fn new(storage: Storage, max_block_size_bytes: usize) -> Self {
        Self {
            read_cache: Arc::new(RwLock::new(HashMap::new())),
            storage,
            max_block_size_bytes,
        }
    }

    pub(super) fn create<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self) -> BlockDelta {
        let new_block_id = Uuid::new_v4();
        let block = BlockDelta::new::<K, V>(new_block_id);
        block
    }

    pub(super) fn fork<KeyWrite: ArrowWriteableKey, ValueWrite: ArrowWriteableValue>(
        &self,
        id: &Uuid,
    ) -> BlockDelta {
        let cache_guard = self.read_cache.read();
        let block = cache_guard.get(id);
        let block = match block {
            Some(block) => block,
            None => {
                // TODO: Err - tried to fork a block not owned by this manager
                panic!("Tried to fork a block not owned by this manager")
            }
        };
        let new_id = Uuid::new_v4();
        let delta = BlockDelta::new::<KeyWrite, ValueWrite>(new_id);
        let populated_delta = self.fork_lifetime_scope::<KeyWrite, ValueWrite>(block, delta);
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

    pub(super) fn commit<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self, delta: &BlockDelta) {
        let record_batch = delta.finish::<K, V>();
        let block = Block::from_record_batch(delta.id, record_batch);
        self.read_cache.write().insert(block.id, block);
    }

    pub(super) async fn get(&self, id: &Uuid) -> Option<Block> {
        let block = {
            let cache = self.read_cache.read();
            cache.get(id).cloned()
        };
        match block {
            Some(block) => Some(block),
            None => {
                let key = format!("block/{}", id);
                let bytes = self.storage.get(&key).await;
                let mut buf: Vec<u8> = Vec::new();
                match bytes {
                    Ok(mut bytes) => {
                        let res = bytes.read_to_end(&mut buf).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("Error reading block {:?} from s3 {:?}", key, e);
                                return None;
                            }
                        }
                        let block = Block::from_bytes(&buf, *id);
                        match block {
                            Ok(block) => {
                                self.read_cache.write().insert(*id, block.clone());
                                Some(block)
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Error converting bytes to Block {:?}/{:?}",
                                    key,
                                    e
                                );
                                None
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading block {:?} from s3 {:?}", key, e);
                        None
                    }
                }
            }
        }
    }

    pub(super) async fn flush(&self, id: &Uuid) -> Result<(), Box<dyn ChromaError>> {
        let block = self.get(id).await;

        match block {
            Some(block) => {
                let bytes = match block.to_bytes() {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };

                let key = format!("block/{}", id);
                let res = self.storage.put_bytes(&key, bytes).await;
                match res {
                    Ok(_) => {
                        println!("Block: {} written to storage", id);
                        Ok(())
                    }
                    Err(e) => {
                        println!("Error writing block to storage {}", e);
                        Err(Box::new(e))
                    }
                }
            }
            None => {
                return Err(Box::new(BlockFlushError::NotFound));
            }
        }
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
    cache: Arc<RwLock<HashMap<Uuid, SparseIndex>>>,
    storage: Storage,
}

impl SparseIndexManager {
    pub fn new(storage: Storage) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            storage,
        }
    }

    pub async fn get<'new, K: ArrowReadableKey<'new> + 'new>(
        &self,
        id: &Uuid,
    ) -> Option<SparseIndex> {
        let read = match self.cache.read().get(id) {
            Some(index) => Some(index.clone()),
            None => None,
        };
        match read {
            Some(index) => Some(index),
            None => {
                println!("Cache miss - fetching sparse index from storage");
                // TODO: move this to a separate function
                let key = format!("sparse_index/{}", id);
                let bytes = self.storage.get(&key).await;
                let mut buf: Vec<u8> = Vec::new();
                match bytes {
                    Ok(mut bytes) => {
                        let res = bytes.read_to_end(&mut buf).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                // TODO: return error
                                println!("Error reading sparse index from storage: {}", e);
                                return None;
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
                                        self.cache.write().insert(*id, index.clone());
                                        return Some(index);
                                    }
                                    Err(e) => {
                                        // TODO: return error
                                        println!("Error turning block into sparse index: {}", e);
                                        return None;
                                    }
                                }
                            }
                            Err(e) => {
                                // TODO: return error
                                println!("Error turning bytes into block: {}", e);
                                return None;
                            }
                        }
                    }
                    Err(e) => {
                        // TODO: return error
                        println!("Error reading sparse index from storage: {}", e);
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

    pub fn commit(&self, index: SparseIndex) {
        self.cache.write().insert(index.id, index);
    }

    pub async fn flush<'read, K: ArrowWriteableKey + 'read>(
        &self,
        id: &Uuid,
    ) -> Result<(), Box<dyn ChromaError>> {
        let index = self.get::<K::ReadableKey<'read>>(id).await;
        match index {
            Some(index) => {
                let as_block = index.to_block::<K>();
                match as_block {
                    Ok(block) => {
                        let bytes = match block.to_bytes() {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                return Err(Box::new(e));
                            }
                        };

                        let key = format!("sparse_index/{}", id);
                        let res = self.storage.put_bytes(&key, bytes).await;
                        match res {
                            Ok(_) => {
                                println!("Sparse index id {:?} written to storage", id);
                                Ok(())
                            }
                            Err(e) => {
                                println!("Error writing sparse index id {:?} to storage", id);
                                Err(Box::new(e))
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to convert sparse index to block");
                        Err(e)
                    }
                }
            }
            None => {
                println!("Tried to flush a sparse index that doesn't exist");
                return Err(Box::new(SparseIndexFlushError::NotFound));
            }
        }
    }

    pub async fn fork<'key, K: ArrowWriteableKey + 'key>(
        &self,
        old_id: &Uuid,
        new_id: Uuid,
    ) -> SparseIndex {
        // TODO: error handling
        println!("Forking sparse index from {:?}", old_id);
        let original = self.get::<K::ReadableKey<'key>>(old_id).await.unwrap();
        let forked = original.fork(new_id);
        self.cache.write().insert(new_id, forked.clone());
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
