use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

use super::super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};
use super::block::{Block, BlockBuilderOptions, BlockData, BlockDataBuilder, BlockState};
use super::delta::BlockDelta;
use super::provider::ArrowBlockProvider;
use super::sparse_index::SparseIndex;

pub(super) const MAX_BLOCK_SIZE: usize = 16384;

pub(crate) struct ArrowBlockfile {
    sparse_index: SparseIndex,
    key_type: KeyType,
    value_type: ValueType,
    transaction_state: Option<TransactionState>,
    block_provider: ArrowBlockProvider,
}

struct TransactionState {
    block_delta: Vec<BlockDelta>,
    new_sparse_index: Option<SparseIndex>,
}

impl TransactionState {
    fn new() -> Self {
        Self {
            block_delta: Vec::new(),
            new_sparse_index: None,
        }
    }

    fn add_delta(&mut self, delta: BlockDelta) {
        self.block_delta.push(delta);
    }

    fn get_delta_for_block(&self, search_id: &Uuid) -> Option<BlockDelta> {
        for delta in &self.block_delta {
            if delta.source_block.get_id() == *search_id {
                return Some(delta.clone());
            }
        }
        None
    }
}

impl Blockfile for ArrowBlockfile {
    fn get(&self, key: BlockfileKey) -> Result<Value, Box<dyn crate::errors::ChromaError>> {
        let target_block_id = self.sparse_index.get_target_block_id(&key);
        let target_block = match self.block_provider.get_block(&target_block_id) {
            None => panic!("Block not found"), // TODO: This should not panic tbh
            Some(block) => block,
        };
        let value = target_block.get(&key);
        match value {
            None => panic!("Key not found"), // TODO: This should not panic tbh
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
            panic!("Transaction not in progress");
        }

        // Validate key type
        match key.key {
            Key::String(_) => {
                if self.key_type != KeyType::String {
                    panic!("Invalid key type");
                }
            }
            Key::Float(_) => {
                if self.key_type != KeyType::Float {
                    panic!("Invalid key type");
                }
            }
        }

        // Validate value type
        match value {
            Value::Int32ArrayValue(_) => {
                if self.value_type != ValueType::Int32Array {
                    panic!("Invalid value type");
                }
            }
            Value::StringValue(_) => {
                if self.value_type != ValueType::String {
                    panic!("Invalid value type");
                }
            }
            Value::PositionalPostingListValue(_) => {
                if self.value_type != ValueType::PositionalPostingList {
                    panic!("Invalid value type");
                }
            }
            Value::RoaringBitmapValue(_) => {
                if self.value_type != ValueType::RoaringBitmap {
                    panic!("Invalid value type");
                }
            }
        }

        let transaction_state = match self.transaction_state {
            None => panic!("Transaction not in progress"),
            Some(ref mut state) => state,
        };

        let target_block_id = match transaction_state.new_sparse_index {
            None => self.sparse_index.get_target_block_id(&key),
            Some(ref index) => index.get_target_block_id(&key),
        };

        // for debugging
        let target_block_id_string = target_block_id.to_string();

        let delta = match transaction_state.get_delta_for_block(&target_block_id) {
            None => {
                println!("Creating new block delta");
                let target_block = match self.block_provider.get_block(&target_block_id) {
                    None => panic!("Block not found"), // TODO: This should not panic tbh
                    Some(block) => block,
                };
                let delta = BlockDelta::from(target_block);
                println!("New delta has size: {}", delta.get_size());
                transaction_state.add_delta(delta.clone());
                delta
            }
            Some(delta) => delta,
        };

        if delta.can_add(&key, &value) {
            delta.add(key, value);
        } else {
            let (split_key, new_delta) = delta.split(&self.block_provider);
            match transaction_state.new_sparse_index {
                None => {
                    let mut new_sparse_index = SparseIndex::from(&self.sparse_index);
                    new_sparse_index.add_block(split_key, new_delta.source_block.get_id());
                    transaction_state.new_sparse_index = Some(new_sparse_index);
                }
                Some(ref mut index) => {
                    index.add_block(split_key, new_delta.source_block.get_id());
                }
            }
            transaction_state.add_delta(new_delta);
            self.set(key, value)?
        }
        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), Box<dyn crate::errors::ChromaError>> {
        if self.in_transaction() {
            // TODO: return error
            panic!("Transaction already in progress");
        }
        self.transaction_state = Some(TransactionState::new());
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn crate::errors::ChromaError>> {
        if !self.in_transaction() {
            panic!("Transaction not in progress");
        }

        let transaction_state = match self.transaction_state {
            None => panic!("Transaction not in progress"), // TODO: make error
            Some(ref mut state) => state,
        };

        for delta in &transaction_state.block_delta {
            // build a new block and replace the blockdata in the block
            // TOOO: the data capacities need to include the offsets and padding, not just the raw data size
            let new_block_data = BlockData::from(delta);

            // TODO: thinking about an edge case here: someone could register while we are in a transaction, and then we would have to handle that
            // in that case, update_data() can fail, since the block is registered, and we would have to create a new block and update the sparse index

            // Blocks are WORM, so if the block is uninitialized or initialized we can update it directly, if its registered, meaning the broader system is aware of it,
            // we need to create a new block and update the sparse index to point to the new block

            match delta.source_block.get_state() {
                BlockState::Uninitialized => {
                    delta.source_block.update_data(new_block_data);
                    delta.source_block.commit();
                    println!(
                        "Size of commited block in bytes: {} with len {}",
                        delta.source_block.get_size(),
                        delta.source_block.len()
                    );
                }
                BlockState::Initialized => {
                    delta.source_block.update_data(new_block_data);
                    delta.source_block.commit();
                    println!(
                        "Size of commited block in bytes: {} with len {}",
                        delta.source_block.get_size(),
                        delta.source_block.len()
                    );
                }
                BlockState::Commited | BlockState::Registered => {
                    // If the block is commited or registered, we need to create a new block and update the sparse index
                    let new_block = self
                        .block_provider
                        .create_block(self.key_type, self.value_type);
                    new_block.update_data(new_block_data);
                    let new_min_key = match delta.get_min_key() {
                        None => panic!("No start key"),
                        Some(key) => key,
                    };
                    match transaction_state.new_sparse_index {
                        None => {
                            let mut new_sparse_index = SparseIndex::from(&self.sparse_index);
                            new_sparse_index.replace_block(
                                delta.source_block.get_id(),
                                new_block.get_id(),
                                new_min_key,
                            );
                            transaction_state.new_sparse_index = Some(new_sparse_index);
                        }
                        Some(ref mut index) => {
                            index.replace_block(
                                delta.source_block.get_id(),
                                new_block.get_id(),
                                new_min_key,
                            );
                        }
                    }
                    new_block.commit();
                    println!(
                        "Size of commited block in bytes: {} with len {}",
                        new_block.get_size(),
                        new_block.len()
                    );
                }
            }
        }

        // update the sparse index
        if transaction_state.new_sparse_index.is_some() {
            self.sparse_index = transaction_state.new_sparse_index.take().unwrap();
            // unwrap is safe because we just checked it
        }
        println!("New sparse index after commit: {:?}", self.sparse_index);

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
            sparse_index: SparseIndex::new(initial_block.get_id()),
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
        assert_eq!(blockfile.sparse_index.len(), 3);
        assert!(blockfile.sparse_index.is_valid());

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
        assert_eq!(blockfile.sparse_index.len(), 3);
        assert!(blockfile.sparse_index.is_valid());

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
}
