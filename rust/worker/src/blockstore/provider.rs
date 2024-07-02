use super::arrow::provider::ArrowBlockfileProvider;
use super::arrow::types::{
    ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
};
use super::config::BlockfileProviderConfig;
use super::key::KeyWrapper;
use super::memory::provider::MemoryBlockfileProvider;
use super::memory::storage::{Readable, Writeable};
use super::types::BlockfileWriter;
use super::{BlockfileReader, Key, Value};
use crate::config::Configurable;
use crate::errors::ChromaError;
use crate::storage::config::StorageConfig;
use crate::storage::Storage;
use async_trait::async_trait;
use core::fmt::{self, Debug};
use std::fmt::Formatter;
use thiserror::Error;

#[derive(Clone)]
pub(crate) enum BlockfileProvider {
    HashMapBlockfileProvider(MemoryBlockfileProvider),
    ArrowBlockfileProvider(ArrowBlockfileProvider),
}

impl Debug for BlockfileProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => {
                write!(f, "HashMapBlockfileProvider")
            }
            BlockfileProvider::ArrowBlockfileProvider(provider) => {
                write!(f, "ArrowBlockfileProvider")
            }
        }
    }
}

impl BlockfileProvider {
    pub(crate) fn new_memory() -> Self {
        BlockfileProvider::HashMapBlockfileProvider(MemoryBlockfileProvider::new())
    }

    pub(crate) fn new_arrow(storage: Storage, max_block_size_bytes: usize) -> Self {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            storage,
            max_block_size_bytes,
        ))
    }

    pub(crate) async fn open<
        'new,
        K: Key + Into<KeyWrapper> + From<&'new KeyWrapper> + ArrowReadableKey<'new> + 'new,
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

    pub(crate) fn create<
        'new,
        K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
        V: Value + Writeable + ArrowWriteableValue + 'new,
    >(
        &self,
    ) -> Result<BlockfileWriter, Box<CreateError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.create::<K, V>(),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider.create::<K, V>(),
        }
    }

    pub(crate) async fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileWriter, Box<CreateError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.fork::<K, V>(id),
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
pub(crate) enum OpenError {
    #[error("Blockfile not found")]
    NotFound,
}

impl ChromaError for OpenError {
    fn code(&self) -> crate::errors::ErrorCodes {
        crate::errors::ErrorCodes::NotFound
    }
}

#[derive(Error, Debug)]
pub(crate) enum CreateError {
    #[error("Blockfile already exists")]
    AlreadyExists,
}

impl ChromaError for CreateError {
    fn code(&self) -> crate::errors::ErrorCodes {
        crate::errors::ErrorCodes::AlreadyExists
    }
}
