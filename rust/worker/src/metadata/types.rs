use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

use async_trait::async_trait;
use roaring::RoaringBitmap;
use std::collections::HashMap;

#[derive(Debug, Error)]
pub(crate) enum GetError {
    #[error("Error getting metadata")]
    NotFoundError,
}

impl ChromaError for GetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetError::NotFoundError => ErrorCodes::InvalidArgument,
        }
    }
}

pub(crate) trait StringMetadataIndex {
    fn put(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>>;
    fn get(&self, key: &str, value: &str) -> Result<&RoaringBitmap, Box<dyn ChromaError>>;
}

struct InMemoryStringMetadataIndex {
    index: std::collections::HashMap<String, HashMap<String, RoaringBitmap>>,
}

impl InMemoryStringMetadataIndex {
    pub fn new() -> Self {
        InMemoryStringMetadataIndex {
            index: std::collections::HashMap::new(),
        }
    }
}

impl StringMetadataIndex for InMemoryStringMetadataIndex {
    fn put(&mut self, key: &str, value: &str, offset_id: usize) -> Result<(), Box<dyn ChromaError>> {
        let key_map = self.index.entry(key.to_string()).or_insert(HashMap::new());
        let bitmap = key_map.entry(value.to_string()).or_insert(RoaringBitmap::new());
        bitmap.insert(offset_id as u32);
        Ok(())
    }

    fn get(&self, key: &str, value: &str) -> Result<&RoaringBitmap, Box<dyn ChromaError>> {
        match self.index.get(key) {
            Some(key_map) => match key_map.get(value) {
                Some(bitmap) => Ok(&bitmap),
                None => Err(Box::new(GetError::NotFoundError)),
            },
            None => Err(Box::new(GetError::NotFoundError)),
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_in_memory_string_metadata_index_single_value() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.put("key", "value", 0).unwrap();
        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(0));
    }

    #[test]
    fn test_in_memory_string_metadata_index_multiple_values() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.put("key", "value", 0).unwrap();
        index.put("key", "value", 1).unwrap();
        let bitmap = index.get("key", "value").unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(0));
        assert!(bitmap.contains(1));
    }

    #[test]
    fn test_in_memory_string_metadata_index_does_not_contain_key() {
        let index = InMemoryStringMetadataIndex::new();
        let result = index.get("key", "value");
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_string_metadata_index_does_not_contain_value() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.put("key", "value", 0).unwrap();
        let result = index.get("key", "value2");
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_string_metadata_index_multiple_values_for_key() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.put("key", "value1", 0).unwrap();
        index.put("key", "value2", 1).unwrap();
        let bitmap = index.get("key", "value1").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(0));
    }

    #[test]
    fn test_in_memory_string_metadata_index_multiple_keys_and_values() {
        let mut index = InMemoryStringMetadataIndex::new();
        index.put("key1", "value1", 0).unwrap();
        index.put("key1", "value1", 4).unwrap();
        index.put("key1", "value2", 1).unwrap();
        index.put("key2", "value1", 2).unwrap();
        index.put("key2", "value2", 3).unwrap();
        let bitmap = index.get("key1", "value1").unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(0));
        assert!(bitmap.contains(4));
        let bitmap = index.get("key2", "value1").unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(2));
    }
}