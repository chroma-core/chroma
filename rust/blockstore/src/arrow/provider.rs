use super::{
    block::{delta::BlockDelta, Block, BlockLoadError},
    blockfile::{ArrowBlockfileReader, ArrowBlockfileWriter},
    config::ArrowBlockfileProviderConfig,
    root::{FromBytesError, RootReader, RootWriter},
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
};
use crate::{
    key::KeyWrapper,
    memory::storage::Readable,
    provider::{CreateError, OpenError},
    BlockfileReader, BlockfileWriter, Key, Value,
};
use async_trait::async_trait;
use chroma_cache::{CacheError, PersistentCache};
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
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
    root_manager: RootManager,
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

    pub async fn open<
        'new,
        K: Key + Into<KeyWrapper> + ArrowReadableKey<'new> + 'new,
        V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        let root = self.root_manager.get::<K>(id).await;
        match root {
            Ok(Some(root)) => Ok(BlockfileReader::ArrowBlockfileReader(
                ArrowBlockfileReader::new(self.block_manager.clone(), root),
            )),
            Ok(None) => Err(Box::new(OpenError::NotFound)),
            Err(e) => Err(Box::new(OpenError::Other(Box::new(e)))),
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
            self.root_manager.clone(),
        );
        Ok(BlockfileWriter::ArrowBlockfileWriter(file))
    }

    pub async fn clear(&self) -> Result<(), CacheError> {
        self.block_manager.block_cache.clear().await?;
        self.root_manager.cache.clear().await?;
        Ok(())
    }

    pub async fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<crate::BlockfileWriter, Box<CreateError>> {
        tracing::info!("Forking blockfile from {:?}", id);
        let new_id = Uuid::new_v4();
        let new_root = self.root_manager.fork::<K>(id, new_id).await.map_err(|e| {
            tracing::error!("Error forking root: {:?}", e);
            Box::new(CreateError::Other(Box::new(e)))
        })?;
        let file = ArrowBlockfileWriter::from_root(
            new_id,
            self.block_manager.clone(),
            self.root_manager.clone(),
            new_root,
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
    block_cache: Arc<dyn PersistentCache<Uuid, Block>>,
    storage: Storage,
    max_block_size_bytes: usize,
    write_mutex: Arc<tokio::sync::Mutex<()>>,
}

impl BlockManager {
    pub(super) fn new(
        storage: Storage,
        max_block_size_bytes: usize,
        block_cache: Box<dyn PersistentCache<Uuid, Block>>,
    ) -> Self {
        let block_cache: Arc<dyn PersistentCache<Uuid, Block>> = block_cache.into();
        Self {
            block_cache,
            storage,
            max_block_size_bytes,
            write_mutex: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(super) fn create<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self) -> BlockDelta {
        let new_block_id = Uuid::new_v4();
        BlockDelta::new::<K, V>(new_block_id)
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
        let record_batch = delta.finish::<K, V>(None);
        Block::from_record_batch(delta_id, record_batch)
    }

    pub(super) async fn cached(&self, id: &Uuid) -> bool {
        self.block_cache.get(id).await.ok().is_some()
    }

    pub(super) async fn get(&self, id: &Uuid) -> Result<Option<Block>, GetError> {
        let block = self.block_cache.get(id).await.ok().flatten();
        match block {
            Some(block) => Ok(Some(block)),
            None => async {
                let key = format!("block/{}", id);
                let bytes_res = self
                    .storage
                    .get(&key)
                    .instrument(
                        tracing::trace_span!(parent: Span::current(), "BlockManager storage get", id = id.to_string()),
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
                                match self.block_cache.get(id).await {
                                    Ok(Some(b)) => {
                                        Ok(Some(b))
                                    }
                                    Ok(None) => {
                                        self.block_cache.insert(*id, block.clone()).await;
                                        Ok(Some(block))
                                    }
                                    Err(e) => {
                                        tracing::error!("Error getting block from cache {:?}", e);
                                        Err(GetError::BlockLoadError(e.into()))
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Error converting bytes to Block {:?}/{:?}",
                                    key,
                                    e
                                );
                                Err(GetError::BlockLoadError(e))
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error converting bytes to Block {:?}", e);
                        Err(GetError::StorageGetError(e))
                    }
                }
            }.instrument(tracing::trace_span!(parent: Span::current(), "BlockManager get cold", block_id = id.to_string())).await
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
        let block_bytes_len = bytes.len();
        let res = self.storage.put_bytes(&key, bytes).await;
        match res {
            Ok(_) => {
                tracing::info!(
                    "Block: {} written to storage ({}B)",
                    block.id,
                    block_bytes_len
                );
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

// ==============
// Root Manager
// ==============

#[derive(Error, Debug)]
pub(super) enum RootManagerError {
    #[error("Not found")]
    NotFound,
    #[error(transparent)]
    BlockLoadError(#[from] BlockLoadError),
    #[error(transparent)]
    UUIDParseError(#[from] uuid::Error),
    #[error(transparent)]
    StorageGetError(#[from] chroma_storage::GetError),
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
pub(super) struct RootManager {
    cache: Arc<dyn PersistentCache<Uuid, RootReader>>,
    storage: Storage,
}

impl RootManager {
    pub fn new(storage: Storage, cache: Box<dyn PersistentCache<Uuid, RootReader>>) -> Self {
        let cache: Arc<dyn PersistentCache<Uuid, RootReader>> = cache.into();
        Self { cache, storage }
    }

    pub async fn get<'new, K: ArrowReadableKey<'new> + 'new>(
        &self,
        id: &Uuid,
    ) -> Result<Option<RootReader>, RootManagerError> {
        let index = self.cache.get(id).await.ok().flatten();
        match index {
            Some(index) => Ok(Some(index)),
            None => {
                tracing::info!("Cache miss - fetching root from storage");
                // TODO(hammadb): For legacy and temporary development purposes, we are reading the file
                // from a fixed location. The path is sparse_index/ for legacy reasons.
                // This will be replaced with a full prefix-based storage shortly
                let key = format!("sparse_index/{}", id);
                tracing::debug!("Reading root from storage with key: {}", key);
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
                                    tracing::error!("Error reading root from storage: {}", e);
                                    return Err(RootManagerError::StorageGetError(e));
                                }
                            }
                        }
                        let root = RootReader::from_bytes::<K>(&buf, *id);
                        match root {
                            Ok(root) => {
                                self.cache.insert(*id, root.clone()).await;
                                Ok(Some(root))
                            }
                            Err(e) => {
                                tracing::error!("Error turning bytes into root: {}", e);
                                Err(RootManagerError::FromBytesError(e))
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading root from storage: {}", e);
                        Err(RootManagerError::StorageGetError(e))
                    }
                }
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
        let key = format!("sparse_index/{}", root.id);
        let res = self.storage.put_bytes(&key, bytes).await;
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
    ) -> Result<RootWriter, RootManagerError> {
        tracing::info!("Forking root from {:?}", old_id);
        let original = self.get::<K::ReadableKey<'key>>(old_id).await?;
        match original {
            Some(original) => {
                let forked = original.fork(new_id);
                Ok(forked)
            }
            None => Err(RootManagerError::NotFound),
        }
    }
}
