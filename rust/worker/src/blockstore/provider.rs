use super::arrow::provider::ArrowBlockfileProvider;
use super::arrow::types::{
    ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
};
use super::key::KeyWrapper;
use super::memory::provider::HashMapBlockfileProvider;
use super::memory::storage::{Readable, Writeable};
use super::types::BlockfileWriter;
use super::{BlockfileReader, Key, Value};
use crate::errors::ChromaError;
use core::fmt::{self, Debug};
use std::fmt::Formatter;
use thiserror::Error;

#[derive(Clone)]
pub(crate) enum BlockfileProvider {
    HashMapBlockfileProvider(HashMapBlockfileProvider),
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
        BlockfileProvider::HashMapBlockfileProvider(HashMapBlockfileProvider::new())
    }

    pub(crate) fn new_arrow() -> Self {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new())
    }

    pub(crate) fn open<
        'new,
        K: Key + Into<KeyWrapper> + ArrowReadableKey<'new> + 'new,
        V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.open::<K, V>(id),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider.open::<K, V>(id),
        }
    }

    pub(crate) fn create<
        'new,
        K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
        V: Value + Writeable + ArrowWriteableValue + 'new,
    >(
        &self,
    ) -> Result<BlockfileWriter<K, V>, Box<CreateError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.create::<K, V>(),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider.create::<K, V>(),
        }
    }

    pub(crate) fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileWriter<K, V>, Box<CreateError>> {
        match self {
            BlockfileProvider::HashMapBlockfileProvider(provider) => provider.fork::<K, V>(id),
            BlockfileProvider::ArrowBlockfileProvider(provider) => provider.fork::<K, V>(id),
        }
    }
}

// =================== Interfaces ===================

/// A trait for opening and creating blockfiles
/// # Methods
/// - new: Create a new instance of the blockfile provider. A blockfile provider returns a Box<dyn Blockfile> of a given type.
/// Currently, we support HashMap and Arrow-backed blockfiles.
/// - open: Open a blockfile with the given id, returning a Box<dyn Blockfile> and error if it does not exist
/// - create: Create a new blockfile. returning a Box<dyn Blockfile> and error if it already exists
/// - fork: Fork the blockfile with the given id, returning a Box<dyn Blockfile> and error if it does not exist
/// # Example
/// ```ignore (TODO: This example is not runnable from outside the crate it seems. Fix this. Ignore for now.)
/// use crate::blockstore::provider::HashMapBlockfileProvider;
/// let mut provider = HashMapBlockfileProvider::new();
/// let blockfile = provider.create("test")
// /// ```
// pub(crate) trait BlockfileProvider {
//     fn open<
//         'new,
//         K: Key + Into<KeyWrapper> + ArrowReadableKey<'new> + 'new,
//         V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
//     >(
//         &self,
//         id: &uuid::Uuid,
//     ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>>;
//     fn create<
//         'new,
//         K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
//         V: Value + Writeable + ArrowWriteableValue + 'new,
//     >(
//         &self,
//     ) -> Result<BlockfileWriter<K, V>, Box<CreateError>>;
//     fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
//         &self,
//         id: &uuid::Uuid,
//     ) -> Result<BlockfileWriter<K, V>, Box<CreateError>>;
// }

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
