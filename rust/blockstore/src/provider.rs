use crate::arrow::provider::BlockfileReaderOptions;
use crate::arrow::root::RootReader;
use crate::BlockfileWriterOptions;

use super::arrow::block::Block;
use super::arrow::provider::ArrowBlockfileProvider;
use super::arrow::types::{
    ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
};
use super::config::BlockfileProviderConfig;
use super::key::{InvalidKeyConversion, KeyWrapper};
use super::memory::provider::MemoryBlockfileProvider;
use super::memory::storage::Readable;
use super::types::BlockfileWriter;
use super::{BlockfileReader, Key, Value};
use async_trait::async_trait;
use chroma_cache::PersistentCache;
use chroma_config::registry::Registry;
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
                f.debug_struct("HashMapBlockfileProvider").finish()
            }
            BlockfileProvider::ArrowBlockfileProvider(_provider) => {
                f.debug_struct("ArrowBlockfileProvider").finish()
            }
        }
    }
}
pub trait ReadKey<'a>:
    Key
    + Into<KeyWrapper>
    + TryFrom<&'a KeyWrapper, Error = InvalidKeyConversion>
    + ArrowReadableKey<'a>
    + Sync
    + 'a
{
}

pub trait ReadValue<'a>: Value + Readable<'a> + ArrowReadableValue<'a> + Sync + 'a {}

impl<'a, T> ReadKey<'a> for T where
    T: Key
        + Into<KeyWrapper>
        + TryFrom<&'a KeyWrapper, Error = InvalidKeyConversion>
        + ArrowReadableKey<'a>
        + Sync
        + 'a
{
}

impl<'a, T> ReadValue<'a> for T where T: Value + Readable<'a> + ArrowReadableValue<'a> + Sync + 'a {}

impl BlockfileProvider {
    pub fn new_memory() -> Self {
        BlockfileProvider::HashMapBlockfileProvider(MemoryBlockfileProvider::new())
    }

    pub fn new_arrow(
        storage: Storage,
        max_block_size_bytes: usize,
        block_cache: Box<dyn PersistentCache<Uuid, Block>>,
        root_cache: Box<dyn PersistentCache<Uuid, RootReader>>,
        num_concurrent_block_flushes: usize,
    ) -> Self {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            storage,
            max_block_size_bytes,
            block_cache,
            root_cache,
            num_concurrent_block_flushes,
        ))
    }

    pub async fn read<'new, K: ReadKey<'new>, V: ReadValue<'new>>(
        &self,
        options: BlockfileReaderOptions,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => {
                provider.read::<K, V>(options.id())
            }
            BlockfileProvider::ArrowBlockfileProvider(provider) => {
                provider.read::<K, V>(options).await
            }
        }
    }

    pub async fn write<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
        &self,
        options: BlockfileWriterOptions,
    ) -> Result<BlockfileWriter, Box<CreateError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.write(options),
            BlockfileProvider::ArrowBlockfileProvider(provider) => {
                provider.write::<K, V>(options).await
            }
        }
    }

    pub async fn clear(&self) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.clear(),
            BlockfileProvider::ArrowBlockfileProvider(provider) => {
                provider.clear().await.map_err(|e| e.boxed())?
            }
        };
        Ok(())
    }

    pub async fn prefetch(
        &self,
        id: &uuid::Uuid,
        prefix_path: &str,
    ) -> Result<usize, Box<dyn ChromaError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(_) => unimplemented!(),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider
                .prefetch(id, prefix_path)
                .await
                .map_err(|e| Box::new(e) as _),
        }
    }
}

// =================== Configurable ===================

#[async_trait]
impl Configurable<(BlockfileProviderConfig, Storage)> for BlockfileProvider {
    async fn try_from_config(
        config: &(BlockfileProviderConfig, Storage),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (blockfile_config, storage) = config;
        match blockfile_config {
            BlockfileProviderConfig::Arrow(blockfile_config) => {
                Ok(BlockfileProvider::ArrowBlockfileProvider(
                    ArrowBlockfileProvider::try_from_config(
                        &(*blockfile_config.clone(), storage.clone()),
                        registry,
                    )
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
