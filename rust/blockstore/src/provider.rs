use super::arrow::block::Block;
use super::arrow::provider::ArrowBlockfileProvider;
use super::arrow::sparse_index::SparseIndex;
use super::arrow::types::{
    ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
};
use super::config::BlockfileProviderConfig;
use super::key::KeyWrapper;
use super::memory::provider::MemoryBlockfileProvider;
use super::memory::storage::{Readable, Writeable};
use super::types::BlockfileWriter;
use super::{BlockfileReader, Key, Value};
use async_trait::async_trait;
use chroma_cache::cache::Cache;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use core::fmt::{self, Debug};
use std::fmt::Formatter;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone)]
pub enum BlockfileProvider {
    HashMapBlockfileProvider(MemoryBlockfileProvider),
    ArrowBlockfileProvider(ArrowBlockfileProvider),
}

impl Debug for BlockfileProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(_provider) => {
                write!(f, "HashMapBlockfileProvider")
            }
            BlockfileProvider::ArrowBlockfileProvider(_provider) => {
                write!(f, "ArrowBlockfileProvider")
            }
        }
    }
}

impl BlockfileProvider {
    pub fn new_memory() -> Self {
        BlockfileProvider::HashMapBlockfileProvider(MemoryBlockfileProvider::new())
    }

    pub fn new_arrow(
        storage: Storage,
        max_block_size_bytes: usize,
        block_cache: Cache<Uuid, Block>,
        sparse_index_cache: Cache<Uuid, SparseIndex>,
    ) -> Self {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            storage,
            max_block_size_bytes,
            block_cache,
            sparse_index_cache,
        ))
    }

    pub async fn open<
        'new,
        K: Key
            + Into<KeyWrapper>
            + TryFrom<&'new KeyWrapper, Error = &'static str>
            + ArrowReadableKey<'new>
            + 'new,
        V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.open::<K, V>(id),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider.open::<K, V>(id).await,
        }
    }

    pub fn create<
        'new,
        K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
        V: Value + Writeable + ArrowWriteableValue + 'new,
    >(
        &self,
    ) -> Result<BlockfileWriter, Box<CreateError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.create(),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider.create::<K, V>(),
        }
    }

    pub fn clear(&self) {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.clear(),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider.clear(),
        }
    }

    pub async fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileWriter, Box<CreateError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.fork(id),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider.fork::<K, V>(id).await,
        }
    }
}

// =================== Configurable ===================

#[async_trait]
impl Configurable<(BlockfileProviderConfig, Storage)> for BlockfileProvider {
    async fn try_from_config(
        config: &(BlockfileProviderConfig, Storage),
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (blockfile_config, storage) = config;
        match blockfile_config {
            BlockfileProviderConfig::Arrow(blockfile_config) => {
                Ok(BlockfileProvider::ArrowBlockfileProvider(
                    ArrowBlockfileProvider::try_from_config(&(
                        blockfile_config.clone(),
                        storage.clone(),
                    ))
                    .await?,
                ))
            }
            BlockfileProviderConfig::Memory => Ok(BlockfileProvider::HashMapBlockfileProvider(
                MemoryBlockfileProvider::new(),
            )),
        }
    }
}

// =================== Errors ===================
#[derive(Error, Debug)]
pub enum OpenError {
    #[error("Blockfile not found")]
    NotFound,
    #[error(transparent)]
    Other(#[from] Box<dyn ChromaError>),
}

impl ChromaError for OpenError {
    fn code(&self) -> ErrorCodes {
        match self {
            OpenError::NotFound => ErrorCodes::NotFound,
            OpenError::Other(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum CreateError {
    #[error("Blockfile already exists")]
    AlreadyExists,
    #[error(transparent)]
    Other(#[from] Box<dyn ChromaError>),
}

impl ChromaError for CreateError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateError::AlreadyExists => ErrorCodes::AlreadyExists,
            CreateError::Other(e) => e.code(),
        }
    }
}
