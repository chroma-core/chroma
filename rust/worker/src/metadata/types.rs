use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

use crate::blockstore::HashMapBlockfile;

use async_trait::async_trait;
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    ops::{BitOrAssign, SubAssign}
};

#[derive(Debug, Error)]
pub(crate) enum GetError {
    #[error("Key not found")]
    NotFoundError,
    #[error("Transaction already started")]
    TransactionAlreadyStarted,
    #[error("Not in a transaction")]
    NotInTransaction,
}

impl ChromaError for GetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetError::NotFoundError => ErrorCodes::InvalidArgument,
            GetError::TransactionAlreadyStarted => ErrorCodes::InvalidArgument,
            GetError::NotInTransaction => ErrorCodes::InvalidArgument,
        }
    }
}

pub(crate) trait StringMetadataIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;

    // Must be in a transaction to put or delete.
    fn put(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>>;
    // Can delete anything -- if it's not in committed state the delete will be silently discarded.
    fn delete(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>>;

    // Always reads from committed state.
    fn get(&self, key: &str, value: &str) -> Result<&RoaringBitmap, Box<dyn ChromaError>>;
}

struct InMemoryStringMetadataIndex {
    index: std::collections::HashMap<String, HashMap<String, RoaringBitmap>>,
    in_txn: bool,
    uncommitted_adds: std::collections::HashMap<String, HashMap<String, RoaringBitmap>>,
    uncommitted_deletes: std::collections::HashMap<String, HashMap<String, RoaringBitmap>>,
}

impl InMemoryStringMetadataIndex {
    pub fn new() -> Self {
        InMemoryStringMetadataIndex {
            index: std::collections::HashMap::new(),
            in_txn: false,
            uncommitted_adds: std::collections::HashMap::new(),
            uncommitted_deletes: std::collections::HashMap::new(),
        }
    }
}

impl StringMetadataIndex for InMemoryStringMetadataIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if self.in_txn {
            return Err(Box::new(GetError::TransactionAlreadyStarted));
        }
        self.in_txn = true;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_txn {
            return Err(Box::new(GetError::NotInTransaction));
        }

        for (key, value) in self.uncommitted_adds.iter() {
            let key_map = self.index.entry(key.to_string()).or_insert(HashMap::new());
            for (value, bitmap) in value.iter() {
                let mut existing_bitmap = key_map.entry(value.to_string()).or_insert(RoaringBitmap::new());
                existing_bitmap.bitor_assign(bitmap);
            }
        }
        for (key, value) in self.uncommitted_deletes.iter() {
            let key_map = self.index.entry(key.to_string()).or_insert(HashMap::new());
            for (value, bitmap) in value.iter() {
                let mut existing_bitmap = key_map.entry(value.to_string()).or_insert(RoaringBitmap::new());
                existing_bitmap.sub_assign(bitmap);
            }
        }

        self.in_txn = false;
        self.uncommitted_adds.clear();
        self.uncommitted_deletes.clear();
        Ok(())
    }

    fn put(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_txn {
            return Err(Box::new(GetError::NotInTransaction));
        }

        let key_map = self.uncommitted_adds.entry(key.to_string()).or_insert(HashMap::new());
        let bitmap = key_map.entry(value.to_string()).or_insert(RoaringBitmap::new());
        bitmap.insert(offset_id as u32);

        let key_map = self.uncommitted_deletes.entry(key.to_string()).or_insert(HashMap::new());
        let mut bitmap = key_map.entry(value.to_string()).or_insert(RoaringBitmap::new());
        bitmap.remove(offset_id as u32);

        Ok(())
    }

    fn delete(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_txn {
            return Err(Box::new(GetError::NotInTransaction));
        }

        let key_map = self.uncommitted_deletes.entry(key.to_string()).or_insert(HashMap::new());
        let bitmap = key_map.entry(value.to_string()).or_insert(RoaringBitmap::new());
        bitmap.insert(offset_id as u32);

        let key_map = self.uncommitted_adds.entry(key.to_string()).or_insert(HashMap::new());
        let mut bitmap = key_map.entry(value.to_string()).or_insert(RoaringBitmap::new());
        bitmap.remove(offset_id as u32);

        Ok(())
    }

    fn get(&self, key: &str, value: &str) -> Result<&RoaringBitmap, Box<dyn ChromaError>> {
        match self.index.get(key) {
            Some(key_map) => match key_map.get(value) {
                Some(bitmap) => return Ok(bitmap),
                None => return Err(Box::new(GetError::NotFoundError)),
            },
            None => return Err(Box::new(GetError::NotFoundError)),
        };
    }
}

mod test {
    use super::*;

    #[test]
    fn test_in_memory_string_metadata_index_error_when_not_in_transaction() {
        let mut index = InMemoryStringMetadataIndex::new();
        let result = index.put("key", "value", 1);
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
    fn test_in_memory_string_metadata_index_put_get() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.put("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();
        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_in_memory_string_metadata_index_put_delete_get() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.put("key", "value", 1).unwrap();
        index.delete("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();
        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 0);
    }

    #[test]
    fn test_in_memory_string_metadata_index_put_delete_put_get() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.put("key", "value", 1).unwrap();
        index.delete("key", "value", 1).unwrap();
        index.put("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();
        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_in_memory_string_metadata_index_multiple_keys() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.put("key1", "value", 1).unwrap();
        index.put("key2", "value", 2).unwrap();
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
        index.put("key", "value1", 1).unwrap();
        index.put("key", "value2", 2).unwrap();
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
        index.put("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();
        index.begin_transaction().unwrap();
        index.delete("key", "value", 1).unwrap();
        index.commit_transaction().unwrap();
        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 0);
    }
}