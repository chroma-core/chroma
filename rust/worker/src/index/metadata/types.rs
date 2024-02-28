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

pub(crate) enum MetadataIndexValue {
    String(String),
    Float(f32),
    Bool(bool),
}

pub(crate) trait MetadataIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;

    // Must be in a transaction to put or delete.
    fn set(&mut self, key: &str, value: MetadataIndexValue, offset_id: usize) -> Result<(), Box<dyn ChromaError>>;
    // Can delete anything -- if it's not in committed state the delete will be silently discarded.
    fn delete(&mut self, key: &str, value: MetadataIndexValue, offset_id: usize) -> Result<(), Box<dyn ChromaError>>;

    // Always reads from committed state.
    fn get(&self, key: &str, value: MetadataIndexValue) -> Result<RoaringBitmap, Box<dyn ChromaError>>;
}

struct BlockfileMetadataIndex {
    blockfile: Box<dyn Blockfile>,
    in_transaction: bool,
    uncommitted_rbms: HashMap<BlockfileKey, RoaringBitmap>,
}

impl BlockfileMetadataIndex {
    pub fn new() -> Self {
        BlockfileMetadataIndex {
            blockfile: Box::new(HashMapBlockfile::open(&"in-memory").unwrap()),
            in_transaction: false,
            uncommitted_rbms: HashMap::new(),
        }
    }

    fn look_up_key_and_populate_uncommitted_rbms(&mut self, key: &BlockfileKey) -> Result<(), Box<dyn ChromaError>> {
        if !self.uncommitted_rbms.contains_key(&key) {
            match self.blockfile.get(key.clone()) {
                Ok(Value::RoaringBitmapValue(rbm)) => {
                    self.uncommitted_rbms.insert(key.clone(), rbm);
                },
                _ => {
                    let rbm = RoaringBitmap::new();
                    self.uncommitted_rbms.insert(key.clone(), rbm);
                },
            };
        }
        Ok(())
    }
}

impl MetadataIndex for BlockfileMetadataIndex {
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
        for (key, rbm) in self.uncommitted_rbms.drain() {
            self.blockfile.set(key.clone(), Value::RoaringBitmapValue(rbm.clone()));
        }
        self.blockfile.commit_transaction()?;
        self.in_transaction = false;
        self.uncommitted_rbms.clear();
        Ok(())
    }

    fn set(&mut self, key: &str, value: MetadataIndexValue, offset_id: usize) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(MetadataIndexError::NotInTransaction));
        }
        let blockfilekey = kv_to_blockfile_key(key, value);
        self.look_up_key_and_populate_uncommitted_rbms(&blockfilekey)?;
        let mut rbm = self.uncommitted_rbms.get_mut(&blockfilekey).unwrap();
        rbm.insert(offset_id.try_into().unwrap());
        Ok(())
    }

    fn delete(&mut self, key: &str, value: MetadataIndexValue, offset_id: usize) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(MetadataIndexError::NotInTransaction));
        }
        let blockfilekey = kv_to_blockfile_key(key, value);
        self.look_up_key_and_populate_uncommitted_rbms(&blockfilekey)?;
        let mut rbm = self.uncommitted_rbms.get_mut(&blockfilekey).unwrap();
        rbm.remove(offset_id.try_into().unwrap());
        Ok(()) 
    }

    fn get(&self, key: &str, value: MetadataIndexValue) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(MetadataIndexError::InTransaction));
        }
        let blockfilekey = kv_to_blockfile_key(key, value);
        match self.blockfile.get(blockfilekey) {
            Ok(Value::RoaringBitmapValue(rbm)) => Ok(rbm),
            _ => Err(Box::new(MetadataIndexError::NotFoundError)),
        }
    }
}

fn kv_to_blockfile_key(key: &str, value: MetadataIndexValue) -> BlockfileKey {
    let blockfilekey_key = match value {
        MetadataIndexValue::String(s) => Key::String(s),
        MetadataIndexValue::Float(f) => Key::Float(f),
        MetadataIndexValue::Bool(b) => Key::Bool(b),
    };
    BlockfileKey::new(key.to_string(), blockfilekey_key)
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
    use system_under_test::MyHeap;

    #[test]
    fn test_string_value_metadata_index_error_when_not_in_transaction() {
        let mut index = BlockfileMetadataIndex::new();
        let result = index.set("key", MetadataIndexValue::String("value".to_string()), 1);
        assert_eq!(result.is_err(), true);
        let result = index.delete("key", MetadataIndexValue::String("value".to_string()), 1);
        assert_eq!(result.is_err(), true);
        let result = index.commit_transaction();
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_string_value_metadata_index_empty_transaction() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.commit_transaction().unwrap();
    }

    #[test]
    fn test_string_value_metadata_index_set_get() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", MetadataIndexValue::String("value".to_string())).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_float_value_metadata_index_set_get() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", MetadataIndexValue::Float(1.0), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", MetadataIndexValue::Float(1.0)).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_bool_value_metadata_index_set_get() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", MetadataIndexValue::Bool(true), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", MetadataIndexValue::Bool(true)).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_string_value_metadata_index_set_delete_get() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.delete("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", MetadataIndexValue::String("value".to_string())).unwrap();
        assert_eq!(bitmap.len(), 0);
    }

    #[test]
    fn test_string_value_metadata_index_set_delete_set_get() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.delete("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.set("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", MetadataIndexValue::String("value".to_string())).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_string_value_metadata_index_multiple_keys() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key1", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.set("key2", MetadataIndexValue::String("value".to_string()), 2).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key1", MetadataIndexValue::String("value".to_string())).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);

        let bitmap = index.get("key2", MetadataIndexValue::String("value".to_string())).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(2), true);
    }

    #[test]
    fn test_string_value_metadata_index_multiple_values() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", MetadataIndexValue::String("value1".to_string()), 1).unwrap();
        index.set("key", MetadataIndexValue::String("value2".to_string()), 2).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", MetadataIndexValue::String("value1".to_string())).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);

        let bitmap = index.get("key", MetadataIndexValue::String("value2".to_string())).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(2), true);
    }

    #[test]
    fn test_string_value_metadata_index_delete_in_standalone_transaction() {
        let mut index = BlockfileMetadataIndex::new();
        index.begin_transaction().unwrap();
        index.set("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.commit_transaction().unwrap();

        index.begin_transaction().unwrap();
        index.delete("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", MetadataIndexValue::String("value".to_string())).unwrap();
        assert_eq!(bitmap.len(), 0);
    }

    pub struct MetadataIndexStateMachine;

    #[derive(Clone, Debug)]
    pub enum Transition {
        BeginTransaction,
        CommitTransaction,
        Set(String, MetadataIndexValue, usize),
        Delete(String, MetadataIndexValue, usize),
        Get(String, MetadataIndexValue),
    }

    impl ReferenceStateMachine for MetadataIndexStateMachine {
        type State = (
            // Because MetadataIndex is parametrized across different metadata types
            // with the MetadataIndexValue enum, we need to store the type
            // of the index the test is running. We can put any value in the first
            // element of the tuple as long as its the correct type.
            MetadataIndexValue, 
            // {metadata key: {metadata value: offset id bitmap}}
            HashMap<String, HashMap<MetadataIndexValue, RoaringBitmap>>
        );
        type Transition = Transition;

        fn init_state() -> BoxedStrategy<Self::State> {
            // Pick a value type.
            prop_oneof![
                Just((MetadataIndexValue::String("".to_string()), HashMap::new())),
                Just((MetadataIndexValue::Float(0.0), HashMap::new())),
                Just((MetadataIndexValue::Bool(false), HashMap::new())),
            ]
            .boxed()
        }
    }

    proptest! {
        #[test]
        fn test_string_value_metadata_index_proptest(_v in "[1-9][0-9]{0,8}") {
            let mut index = BlockfileMetadataIndex::new();
            index.begin_transaction().unwrap();
            index.set("key", MetadataIndexValue::String("value".to_string()), 1).unwrap();
            index.commit_transaction().unwrap();

            let bitmap = index.get("key", MetadataIndexValue::String("value".to_string())).unwrap();
            assert_eq!(bitmap.len(), 1);
            assert_eq!(bitmap.contains(1), true);
        }
    }
}