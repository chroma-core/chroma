pub mod reader_writer;
pub mod storage;

use crate::arrow::types::{ArrowReadableKey, ArrowReadableValue};
use crate::key::{InvalidKeyConversion, KeyWrapper};
use crate::provider::{CreateError, OpenError};
use crate::{BlockfileReader, BlockfileWriter, BlockfileWriterOptions, Key, Value};

use reader_writer::{DashMapBlockfileReader, DashMapBlockfileWriter};
use storage::{FromStoredValue, StorageManager, ToStoredValue};

/// A BlockfileProvider backed by DashMap for concurrent in-memory operations.
/// Intended for testing purposes.
#[derive(Clone)]
pub struct DashMapProvider {
    storage_manager: StorageManager,
}

impl DashMapProvider {
    pub fn new() -> Self {
        Self {
            storage_manager: StorageManager::new(),
        }
    }

    pub fn read<
        'new,
        K: Key
            + Into<KeyWrapper>
            + TryFrom<&'new KeyWrapper, Error = InvalidKeyConversion>
            + ArrowReadableKey<'new>
            + 'new,
        V: Value + FromStoredValue<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        let reader = DashMapBlockfileReader::open(*id, self.storage_manager.clone());
        match reader {
            Some(r) => Ok(BlockfileReader::DashMapBlockfileReader(r)),
            None => Err(Box::new(OpenError::NotFound)),
        }
    }

    pub fn write<K: Key, V: Value + ToStoredValue>(
        &self,
        options: BlockfileWriterOptions,
    ) -> Result<BlockfileWriter, Box<CreateError>> {
        let writer = if let Some(fork_from) = options.fork_from {
            // Fork from existing blockfile
            DashMapBlockfileWriter::fork_from(self.storage_manager.clone(), &fork_from)
                .ok_or_else(|| Box::new(CreateError::Other(Box::new(OpenError::NotFound))))?
        } else {
            // Create new empty blockfile
            DashMapBlockfileWriter::new(self.storage_manager.clone())
        };
        Ok(BlockfileWriter::DashMapBlockfileWriter(writer))
    }

    pub fn clear(&self) {
        self.storage_manager.clear();
    }
}

impl Default for DashMapProvider {
    fn default() -> Self {
        Self::new()
    }
}
