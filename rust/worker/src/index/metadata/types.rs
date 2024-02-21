use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

use crate::blockstore::{Blockfile, BlockfileKey, HashMapBlockfile, Key, Value};

use async_trait::async_trait;
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    ops::{BitOrAssign, SubAssign}
};

#[derive(Debug, Error)]
pub(crate) enum MetadataIndexError {
    #[error("Key not found")]
    NotFoundError,
    #[error("This operation cannot be done in a transaction")]
    InTransaction,
    #[error("This operation can only be done in a transaction")]
    NotInTransaction,
}

impl ChromaError for MetadataIndexError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataIndexError::NotFoundError => ErrorCodes::InvalidArgument,
            MetadataIndexError::InTransaction => ErrorCodes::InvalidArgument,
            MetadataIndexError::NotInTransaction => ErrorCodes::InvalidArgument,
        }
    }
}

pub(crate) trait StringMetadataIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;

    // Must be in a transaction to put or delete.
    fn set(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>>;
    // Can delete anything -- if it's not in committed state the delete will be silently discarded.
    fn delete(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>>;

    // Always reads from committed state.
    fn get(&self, key: &str, value: &str) -> Result<RoaringBitmap, Box<dyn ChromaError>>;
}

struct InMemoryStringMetadataIndex {
    blockfile: Box<dyn Blockfile>,
    in_transaction: bool,
    uncommitted_rbms: HashMap<BlockfileKey, RoaringBitmap>,
}

impl InMemoryStringMetadataIndex {
    pub fn new() -> Self {
        InMemoryStringMetadataIndex {
            blockfile: Box::new(HashMapBlockfile::open(&"in-memory").unwrap()),
            in_transaction: false,
            uncommitted_rbms: HashMap::new(),
        }
    }
}

impl StringMetadataIndex for InMemoryStringMetadataIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(MetadataIndexError::InTransaction));
        }
        self.blockfile.begin_transaction()?;
        self.in_transaction = true;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(MetadataIndexError::NotInTransaction));
        }
        for (key, rbm) in &self.uncommitted_rbms {
            // TODO we shouldn't clone.
            self.blockfile.set(key.clone(), Value::RoaringBitmapValue(rbm.clone()));
        }
        self.blockfile.commit_transaction()?;
        self.in_transaction = false;
        self.uncommitted_rbms.clear();
        Ok(())
    }

    fn set(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(MetadataIndexError::NotInTransaction));
        }
        let blockfilekey = BlockfileKey::new(key.to_string(), Key::String(value.to_string()));
        if !self.uncommitted_rbms.contains_key(&blockfilekey) {
            match self.blockfile.get(blockfilekey.clone()) {
                Ok(Value::RoaringBitmapValue(rbm)) => {
                    self.uncommitted_rbms.insert(blockfilekey.clone(), rbm);
                },
                _ => {
                    let rbm = RoaringBitmap::new();
                    self.uncommitted_rbms.insert(blockfilekey.clone(), rbm);
                },
            };
        }
        let mut rbm = self.uncommitted_rbms.get_mut(&blockfilekey).unwrap();
        rbm.insert(offset_id.try_into().unwrap());
        Ok(())
    }

    fn delete(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(MetadataIndexError::NotInTransaction));
        }
        let blockfilekey = BlockfileKey::new(key.to_string(), Key::String(value.to_string()));
        // TODO refactor this from set() and delete().
        if !self.uncommitted_rbms.contains_key(&blockfilekey) {
            match self.blockfile.get(blockfilekey.clone()) {
                Ok(Value::RoaringBitmapValue(rbm)) => {
                    self.uncommitted_rbms.insert(blockfilekey.clone(), rbm);
                },
                _ => {
                    let rbm = RoaringBitmap::new();
                    self.uncommitted_rbms.insert(blockfilekey.clone(), rbm);
                },
            };
        }
        let mut rbm = self.uncommitted_rbms.get_mut(&blockfilekey).unwrap();
        rbm.remove(offset_id.try_into().unwrap());
        Ok(()) 
    }

    fn get(&self, key: &str, value: &str) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(MetadataIndexError::InTransaction));
        }
        let prefix = key.to_string();
        let key = Key::String(value.to_string());
        let blockfilekey = BlockfileKey::new(prefix, key);
        match self.blockfile.get(blockfilekey) {
            Ok(Value::RoaringBitmapValue(rbm)) => Ok(rbm),
            _ => Err(Box::new(MetadataIndexError::NotFoundError)),
        }
    }
}

mod test {
    use super::*;

    #[test]
    fn test_in_memory_string_metadata_index_error_when_not_in_transaction() {
        let mut index = InMemoryStringMetadataIndex::new();
        let result = index.set("key", "value", 1);
        assert_eq!(result.is_err(), true);
        let result = index.delete("key", "value", 1);
        assert_eq!(result.is_err(), true);
        let result = index.commit_transaction();
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_in_memory_string_metadata_index_empty_transaction() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.commit_transaction().unwrap();
    }

    #[test]
    fn test_in_memory_string_metadata_index_set_get() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_in_memory_string_metadata_index_set_delete_get() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", "value", 1).unwrap();
        index.delete("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 0);
    }

    #[test]
    fn test_in_memory_string_metadata_index_set_delete_set_get() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", "value", 1).unwrap();
        index.delete("key", "value", 1).unwrap();
        index.set("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_in_memory_string_metadata_index_multiple_keys() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key1", "value", 1).unwrap();
        index.set("key2", "value", 2).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key1", "value").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);

        let bitmap = index.get("key2", "value").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(2), true);
    }

    #[test]
    fn test_in_memory_string_metadata_index_multiple_values() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", "value1", 1).unwrap();
        index.set("key", "value2", 2).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value1").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);

        let bitmap = index.get("key", "value2").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(2), true);
    }

    #[test]
    fn test_in_memory_string_metadata_index_delete_in_standalone_transaction() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();

        index.begin_transaction().unwrap();
        index.delete("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 0);
    }
}