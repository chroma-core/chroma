use super::super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};
use super::block::{BlockError, BlockState};
use super::provider::ArrowBlockProvider;
use super::sparse_index::SparseIndex;
use crate::blockstore::arrow_blockfile::block::delta::BlockDelta;
use crate::blockstore::BlockfileError;
use crate::errors::ChromaError;
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

pub(super) const MAX_BLOCK_SIZE: usize = 16384;

#[derive(Clone)]
pub(crate) struct ArrowBlockfile {
    key_type: KeyType,
    value_type: ValueType,
    block_provider: ArrowBlockProvider,
    sparse_index: Arc<Mutex<SparseIndex>>,
    transaction_state: Option<Arc<TransactionState>>,
}

struct TransactionState {
    block_delta: Mutex<Vec<BlockDelta>>,
    new_sparse_index: Mutex<Option<Arc<Mutex<SparseIndex>>>>,
}

impl TransactionState {
    fn new() -> Self {
        Self {
            block_delta: Mutex::new(Vec::new()),
            new_sparse_index: Mutex::new(None),
        }
    }

    fn add_delta(&self, delta: BlockDelta) {
        let mut block_delta = self.block_delta.lock();
        block_delta.push(delta);
    }

    fn get_delta_for_block(&self, search_id: &Uuid) -> Option<BlockDelta> {
        let block_delta = self.block_delta.lock();
        for delta in &*block_delta {
            if delta.source_block.get_id() == *search_id {
                return Some(delta.clone());
            }
        }
        None
    }
}

#[derive(Error, Debug)]
pub(crate) enum ArrowBlockfileError {
    #[error("Block not found")]
    BlockNotFoundError,
    #[error("Block Error")]
    BlockError(#[from] BlockError),
    #[error("No split key found")]
    NoSplitKeyFound,
}

impl ChromaError for ArrowBlockfileError {
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            ArrowBlockfileError::BlockNotFoundError => crate::errors::ErrorCodes::NotFound,
            ArrowBlockfileError::BlockError(err) => err.code(),
            ArrowBlockfileError::NoSplitKeyFound => crate::errors::ErrorCodes::Internal,
        }
    }
}

impl Blockfile for ArrowBlockfile {
    fn get(&self, key: BlockfileKey) -> Result<Value, Box<dyn ChromaError>> {
        let target_block_id = self.sparse_index.lock().get_target_block_id(&key);
        let target_block = match self.block_provider.get_block(&target_block_id) {
            None => return Err(Box::new(ArrowBlockfileError::BlockNotFoundError)),
            Some(block) => block,
        };
        let value = target_block.get(&key);
        match value {
            None => return Err(Box::new(BlockfileError::NotFoundError)),
            Some(value) => Ok(value),
        }
    }

    fn get_by_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn get_gt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn get_gte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn get_lt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn get_lte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(BlockfileKey, Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn set(
        &mut self,
        key: BlockfileKey,
        value: Value,
    ) -> Result<(), Box<dyn crate::errors::ChromaError>> {
        // TODO: value must be smaller than the block size except for position lists, which are a special case
        // where we split the value across multiple blocks
        if !self.in_transaction() {
            return Err(Box::new(BlockfileError::TransactionNotInProgress));
        }

        // Validate key type
        match key.key {
            Key::String(_) => {
                if self.key_type != KeyType::String {
                    return Err(Box::new(BlockfileError::InvalidKeyType));
                }
            }
            Key::Float(_) => {
                if self.key_type != KeyType::Float {
                    return Err(Box::new(BlockfileError::InvalidKeyType));
                }
            }
            Key::Bool(_) => {
                if self.key_type != KeyType::Bool {
                    return Err(Box::new(BlockfileError::InvalidKeyType));
                }
            }
        }

        // Validate value type
        match value {
            Value::Int32ArrayValue(_) => {
                if self.value_type != ValueType::Int32Array {
                    return Err(Box::new(BlockfileError::InvalidValueType));
                }
            }
            Value::StringValue(_) => {
                if self.value_type != ValueType::String {
                    return Err(Box::new(BlockfileError::InvalidValueType));
                }
            }
            Value::Int32Value(_) => {
                if self.value_type != ValueType::Int32 {
                    return Err(Box::new(BlockfileError::InvalidValueType));
                }
            }
            Value::PositionalPostingListValue(_) => {
                if self.value_type != ValueType::PositionalPostingList {
                    return Err(Box::new(BlockfileError::InvalidValueType));
                }
            }
            Value::RoaringBitmapValue(_) => {
                if self.value_type != ValueType::RoaringBitmap {
                    return Err(Box::new(BlockfileError::InvalidValueType));
                }
            }
        }

        let transaction_state = match &self.transaction_state {
            None => return Err(Box::new(BlockfileError::TransactionNotInProgress)),
            Some(transaction_state) => transaction_state,
        };

        let mut transaction_sparse_index = transaction_state.new_sparse_index.lock();
        let target_block_id = match *transaction_sparse_index {
            None => self.sparse_index.lock().get_target_block_id(&key),
            Some(ref index) => index.lock().get_target_block_id(&key),
        };

        let delta = match transaction_state.get_delta_for_block(&target_block_id) {
            None => {
                let target_block = match self.block_provider.get_block(&target_block_id) {
                    None => return Err(Box::new(ArrowBlockfileError::BlockNotFoundError)),
                    Some(block) => block,
                };
                let delta = BlockDelta::from(target_block);
                transaction_state.add_delta(delta.clone());
                delta
            }
            Some(delta) => delta,
        };

        if delta.can_add(&key, &value) {
            delta.add(key, value);
        } else {
            let (split_key, new_delta) = delta.split(&self.block_provider);
            match *transaction_sparse_index {
                None => {
                    let new_sparse_index =
                        Arc::new(Mutex::new(SparseIndex::from(&self.sparse_index.lock())));
                    new_sparse_index
                        .lock()
                        .add_block(split_key, new_delta.source_block.get_id());
                    *transaction_sparse_index = Some(new_sparse_index);
                }
                Some(ref index) => {
                    index
                        .lock()
                        .add_block(split_key, new_delta.source_block.get_id());
                }
            }
            transaction_state.add_delta(new_delta);
            drop(transaction_sparse_index);
            self.set(key, value)?
        }
        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), Box<dyn crate::errors::ChromaError>> {
        if self.in_transaction() {
            return Err(Box::new(BlockfileError::TransactionInProgress));
        }
        self.transaction_state = Some(Arc::new(TransactionState::new()));
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn crate::errors::ChromaError>> {
        if !self.in_transaction() {
            return Err(Box::new(BlockfileError::TransactionNotInProgress));
        }

        let transaction_state = match self.transaction_state {
            None => return Err(Box::new(BlockfileError::TransactionNotInProgress)),
            Some(ref transaction_state) => transaction_state,
        };

        for delta in &*transaction_state.block_delta.lock() {
            // Blocks are WORM, so if the block is uninitialized or initialized we can update it directly, if its registered, meaning the broader system is aware of it,
            // we need to create a new block and update the sparse index to point to the new block

            match delta.source_block.get_state() {
                BlockState::Uninitialized => {
                    match delta.source_block.apply_delta(&delta) {
                        Ok(_) => {}
                        Err(err) => {
                            return Err(Box::new(ArrowBlockfileError::BlockError(*err)));
                        }
                    }
                    match delta.source_block.commit() {
                        Ok(_) => {}
                        Err(err) => {
                            return Err(Box::new(ArrowBlockfileError::BlockError(*err)));
                        }
                    }
                }
                BlockState::Initialized => {
                    match delta.source_block.apply_delta(&delta) {
                        Ok(_) => {}
                        Err(err) => {
                            return Err(Box::new(ArrowBlockfileError::BlockError(*err)));
                        }
                    }
                    match delta.source_block.commit() {
                        Ok(_) => {}
                        Err(err) => {
                            return Err(Box::new(ArrowBlockfileError::BlockError(*err)));
                        }
                    }
                }
                BlockState::Commited | BlockState::Registered => {
                    // If the block is commited or registered, we need to create a new block and update the sparse index
                    let new_block = self
                        .block_provider
                        .create_block(self.key_type, self.value_type);
                    match new_block.apply_delta(&delta) {
                        Ok(_) => {}
                        Err(err) => {
                            return Err(Box::new(ArrowBlockfileError::BlockError(*err)));
                        }
                    }
                    let new_min_key = match delta.get_min_key() {
                        // This should never happen. We don't panic here because we want to return a proper error
                        None => return Err(Box::new(ArrowBlockfileError::NoSplitKeyFound)),
                        Some(key) => key,
                    };
                    let mut transaction_sparse_index = transaction_state.new_sparse_index.lock();
                    match *transaction_sparse_index {
                        None => {
                            let new_sparse_index =
                                Arc::new(Mutex::new(SparseIndex::from(&self.sparse_index.lock())));
                            new_sparse_index.lock().replace_block(
                                delta.source_block.get_id(),
                                new_block.get_id(),
                                new_min_key,
                            );
                            *transaction_sparse_index = Some(new_sparse_index);
                        }
                        Some(ref index) => {
                            index.lock().replace_block(
                                delta.source_block.get_id(),
                                new_block.get_id(),
                                new_min_key,
                            );
                        }
                    }
                    match new_block.commit() {
                        Ok(_) => {}
                        Err(err) => {
                            return Err(Box::new(ArrowBlockfileError::BlockError(*err)));
                        }
                    }
                }
            }
        }

        // update the sparse index
        let mut transaction_state_sparse_index = transaction_state.new_sparse_index.lock();
        if transaction_state_sparse_index.is_some() {
            self.sparse_index = transaction_state_sparse_index.take().unwrap();
            // unwrap is safe because we just checked it
        }

        // Reset the transaction state
        drop(transaction_state_sparse_index);
        self.transaction_state = None;
        Ok(())
    }
}

impl ArrowBlockfile {
    pub(super) fn new(
        key_type: KeyType,
        value_type: ValueType,
        block_provider: ArrowBlockProvider,
    ) -> Self {
        let initial_block = block_provider.create_block(key_type.clone(), value_type.clone());
        Self {
            sparse_index: Arc::new(Mutex::new(SparseIndex::new(initial_block.get_id()))),
            transaction_state: None,
            block_provider,
            key_type,
            value_type,
        }
    }

    fn in_transaction(&self) -> bool {
        self.transaction_state.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn test_blockfile() {
        let block_provider = ArrowBlockProvider::new();
        let mut blockfile =
            ArrowBlockfile::new(KeyType::String, ValueType::Int32Array, block_provider);

        blockfile.begin_transaction().unwrap();
        let key1 = BlockfileKey::new("key".to_string(), Key::String("zzzz".to_string()));
        blockfile
            .set(
                key1.clone(),
                Value::Int32ArrayValue(Int32Array::from(vec![1, 2, 3])),
            )
            .unwrap();
        let key2 = BlockfileKey::new("key".to_string(), Key::String("aaaa".to_string()));
        blockfile
            .set(
                key2,
                Value::Int32ArrayValue(Int32Array::from(vec![4, 5, 6])),
            )
            .unwrap();
        blockfile.commit_transaction().unwrap();

        let value = blockfile.get(key1).unwrap();
        match value {
            Value::Int32ArrayValue(array) => {
                assert_eq!(array.values(), &[1, 2, 3]);
            }
            _ => panic!("Unexpected value type"),
        }
    }

    #[test]
    fn test_splitting() {
        let block_provider = ArrowBlockProvider::new();
        let mut blockfile =
            ArrowBlockfile::new(KeyType::String, ValueType::Int32Array, block_provider);

        blockfile.begin_transaction().unwrap();
        let n = 1200;
        for i in 0..n {
            let string_key = format!("{:04}", i);
            let key = BlockfileKey::new("key".to_string(), Key::String(string_key));
            blockfile
                .set(key, Value::Int32ArrayValue(Int32Array::from(vec![i])))
                .unwrap();
        }
        blockfile.commit_transaction().unwrap();

        for i in 0..n {
            let string_key = format!("{:04}", i);
            let key = BlockfileKey::new("key".to_string(), Key::String(string_key));
            let res = blockfile.get(key).unwrap();
            match res {
                Value::Int32ArrayValue(array) => {
                    assert_eq!(array.values(), &[i]);
                }
                _ => panic!("Unexpected value type"),
            }
        }

        // Sparse index should have 3 blocks
        assert_eq!(blockfile.sparse_index.lock().len(), 3);
        assert!(blockfile.sparse_index.lock().is_valid());

        // Add 5 new entries to the first block
        blockfile.begin_transaction().unwrap();
        for i in 0..5 {
            let new_key = format! {"{:05}", i};
            let key = BlockfileKey::new("key".to_string(), Key::String(new_key));
            blockfile
                .set(key, Value::Int32ArrayValue(Int32Array::from(vec![i])))
                .unwrap();
        }
        blockfile.commit_transaction().unwrap();

        // Sparse index should still have 3 blocks
        assert_eq!(blockfile.sparse_index.lock().len(), 3);
        assert!(blockfile.sparse_index.lock().is_valid());

        // Add 1200 more entries, causing splits
        blockfile.begin_transaction().unwrap();
        for i in n..n * 2 {
            let new_key = format! {"{:04}", i};
            let key = BlockfileKey::new("key".to_string(), Key::String(new_key));
            blockfile
                .set(key, Value::Int32ArrayValue(Int32Array::from(vec![i])))
                .unwrap();
        }
        blockfile.commit_transaction().unwrap();
    }

    #[test]
    fn test_string_value() {
        let block_provider = ArrowBlockProvider::new();
        let mut blockfile = ArrowBlockfile::new(KeyType::String, ValueType::String, block_provider);

        blockfile.begin_transaction().unwrap();
        let n = 2000;

        for i in 0..n {
            let string_key = format!("{:04}", i);
            let key = BlockfileKey::new("key".to_string(), Key::String(string_key.clone()));
            blockfile
                .set(key, Value::StringValue(string_key.clone()))
                .unwrap();
        }
        blockfile.commit_transaction().unwrap();

        for i in 0..n {
            let string_key = format!("{:04}", i);
            let key = BlockfileKey::new("key".to_string(), Key::String(string_key.clone()));
            let res = blockfile.get(key).unwrap();
            match res {
                Value::StringValue(string) => {
                    assert_eq!(string, string_key);
                }
                _ => panic!("Unexpected value type"),
            }
        }
    }

    #[test]
    fn test_int_key() {
        let block_provider = ArrowBlockProvider::new();
        let mut blockfile = ArrowBlockfile::new(KeyType::Float, ValueType::String, block_provider);

        blockfile.begin_transaction().unwrap();
        let n = 2000;
        for i in 0..n {
            let key = BlockfileKey::new("key".to_string(), Key::Float(i as f32));
            blockfile
                .set(key, Value::StringValue(format!("{:04}", i)))
                .unwrap();
        }
        blockfile.commit_transaction().unwrap();

        for i in 0..n {
            let key = BlockfileKey::new("key".to_string(), Key::Float(i as f32));
            let res = blockfile.get(key).unwrap();
            match res {
                Value::StringValue(string) => {
                    assert_eq!(string, format!("{:04}", i));
                }
                _ => panic!("Unexpected value type"),
            }
        }
    }

    #[test]
    fn test_roaring_bitmap_value() {
        let block_provider = ArrowBlockProvider::new();
        let mut blockfile =
            ArrowBlockfile::new(KeyType::String, ValueType::RoaringBitmap, block_provider);

        blockfile.begin_transaction().unwrap();
        let n = 2000;
        for i in 0..n {
            let key = BlockfileKey::new("key".to_string(), Key::String(format!("{:04}", i)));
            blockfile
                .set(
                    key,
                    Value::RoaringBitmapValue(roaring::RoaringBitmap::from_iter(
                        (0..i).map(|x| x as u32),
                    )),
                )
                .unwrap();
        }
        blockfile.commit_transaction().unwrap();

        for i in 0..n {
            let key = BlockfileKey::new("key".to_string(), Key::String(format!("{:04}", i)));
            let res = blockfile.get(key).unwrap();
            match res {
                Value::RoaringBitmapValue(bitmap) => {
                    assert_eq!(bitmap.len(), i as u64);
                    assert_eq!(
                        bitmap.iter().collect::<Vec<u32>>(),
                        (0..i).collect::<Vec<u32>>()
                    );
                }
                _ => panic!("Unexpected value type"),
            }
        }
    }
}
