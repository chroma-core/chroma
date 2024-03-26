use super::types::Blockfile;
use super::types::{HashMapBlockfile, KeyType, ValueType};
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
/// - open: Open a blockfile at the given path, returning a Box<dyn Blockfile> and error if it does not exist
/// - create: Create a new blockfile at the given path, returning a Box<dyn Blockfile> and error if it already exists
/// # Example
/// ```ignore (TODO: This example is not runnable from outside the crate it seems. Fix this. Ignore for now.)
/// use crate::blockstore::provider::HashMapBlockfileProvider;
/// use crate::blockstore::types::{KeyType, ValueType};
/// let mut provider = HashMapBlockfileProvider::new();
/// let blockfile = provider.create("test", KeyType::String, ValueType::Int32Array);
/// ```
pub(crate) trait BlockfileProvider {
    fn open(&self, path: &str) -> Result<Box<dyn Blockfile>, Box<OpenError>>;
    fn create(
        &mut self,
        path: &str,
        key_type: KeyType,
        value_type: ValueType,
    ) -> Result<Box<dyn Blockfile>, Box<CreateError>>;
}

/// A BlockFileProvider that creates HashMapBlockfiles (in-memory blockfiles used for testing).
/// It bookkeeps the blockfiles locally.
/// # Note
/// This is not intended for production use.
pub(crate) struct HashMapBlockfileProvider {
    files: Arc<RwLock<HashMap<String, Box<dyn Blockfile>>>>,
}

impl HashMapBlockfileProvider {
    pub(crate) fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl BlockfileProvider for HashMapBlockfileProvider {
    fn open(&self, path: &str) -> Result<Box<dyn Blockfile>, Box<OpenError>> {
        match self.files.read().get(path) {
            Some(file) => Ok(file.clone()),
            None => Err(Box::new(OpenError::NotFound)),
        }
    }

    fn create(
        &mut self,
        path: &str,
        key_type: KeyType,
        value_type: ValueType,
    ) -> Result<Box<dyn Blockfile>, Box<CreateError>> {
        let mut files = self.files.write();
        match files.get(path) {
            Some(_) => Err(Box::new(CreateError::AlreadyExists)),
            None => {
                let blockfile = Box::new(HashMapBlockfile::new());
                files.insert(path.to_string(), blockfile);
                Ok(files.get(path).unwrap().clone())
            }
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
