use super::key::KeyWrapper;
use super::memory::storage::{Readable, Writeable};
use super::types::BlockfileWriter;
use super::{BlockfileReader, Key, Value};
use crate::errors::ChromaError;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

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
/// ```
pub(crate) trait BlockfileProvider {
    fn new() -> Self;
    fn open<'new, K: Key + Into<KeyWrapper> + 'new, V: Value + Readable<'new> + 'new>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<K, V>, Box<OpenError>>;
    fn create<'new, K: Key + Into<KeyWrapper> + 'new, V: Value + Writeable + 'new>(
        &self,
    ) -> Result<BlockfileWriter<K, V>, Box<CreateError>>;
    fn fork<K: Key, V: Value>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileWriter<K, V>, Box<CreateError>>;
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
