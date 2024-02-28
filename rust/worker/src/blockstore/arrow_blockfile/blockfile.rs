use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

use super::super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};
use super::block::Block;
use super::provider::ArrowBlockProvider;
use super::sparse_index::SparseIndex;
use super::utils::{get_key_size, get_value_size};

const MAX_BLOCK_SIZE: usize = 1024;

pub(crate) struct ArrowBlockfile {
    sparse_index: SparseIndex,
    key_type: KeyType,
    value_type: ValueType,
    transaction_state: Option<TransactionState>,
    block_provider: ArrowBlockProvider,
}

struct TransactionState {
    block_delta: Vec<Arc<BlockDelta>>,
    new_sparse_index: Option<SparseIndex>,
}

impl TransactionState {
    fn new() -> Self {
        Self {
            block_delta: Vec::new(),
            new_sparse_index: None,
        }
    }

    fn add_delta(&mut self, delta: Arc<BlockDelta>) {
        self.block_delta.push(delta);
    }

    fn get_delta_for_block(&self, search_id: &Uuid) -> Option<Arc<BlockDelta>> {
        for delta in &self.block_delta {
            match delta.source_block {
                None => continue,
                Some(ref source_block) => {
                    if source_block.id == *search_id {
                        return Some(delta.clone());
                    }
                }
            }
        }
        None
    }
}

struct BlockDeltaInner {
    adds: BTreeMap<BlockfileKey, Value>,
    deletes: Vec<BlockfileKey>,
    split_into: Option<Vec<BlockDelta>>,
}

impl BlockDeltaInner {
    fn add(&mut self, key: BlockfileKey, value: Value) {
        if self.deletes.contains(&key) {
            self.deletes.retain(|x| x != &key);
        }
        self.adds.insert(key, value);
    }

    fn delete(&mut self, key: BlockfileKey) {
        if self.adds.contains_key(&key) {
            self.adds.remove(&key);
        }
        self.deletes.push(key);
    }

    fn can_add(&self, curr_data_size: usize, bytes: usize) -> bool {
        let curr_adds_size = self.adds.iter().fold(0, |acc, (key, value)| {
            acc + get_key_size(key) + get_value_size(value)
        });
        println!("Current adds size: {}", curr_adds_size);
        let curr_deletes_size = self
            .deletes
            .iter()
            .fold(0, |acc, key| acc + get_key_size(key));
        let total_size = curr_data_size + curr_adds_size - curr_deletes_size;
        total_size + bytes <= MAX_BLOCK_SIZE
    }
}

#[derive(Clone)]
struct BlockDelta {
    source_block: Option<Arc<Block>>,
    inner: Arc<RwLock<BlockDeltaInner>>,
}

impl BlockDelta {
    fn new() -> Self {
        Self {
            source_block: None,
            inner: Arc::new(RwLock::new(BlockDeltaInner {
                adds: BTreeMap::new(),
                deletes: Vec::new(),
                split_into: None,
            })),
        }
    }

    fn from(block: Arc<Block>) -> Self {
        Self {
            source_block: Some(block),
            inner: Arc::new(RwLock::new(BlockDeltaInner {
                adds: BTreeMap::new(),
                deletes: Vec::new(),
                split_into: None,
            })),
        }
    }

    fn is_new(&self) -> bool {
        self.source_block.is_none()
    }

    fn was_split(&self) -> bool {
        self.inner.read().split_into.is_some()
    }

    fn can_add(&self, bytes: usize) -> bool {
        // TODO: this should perform the rounding and padding to estimate the correct block size
        // TODO: the source block size includes the padding and rounding, but we want the actual data size so we can compute
        // what writing out a new block would look like
        let curr_data_size = match &self.source_block {
            None => 0,
            Some(block) => block.get_size(),
        };

        let inner = self.inner.read();
        inner.can_add(curr_data_size, bytes)
    }

    fn add(&self, key: BlockfileKey, value: Value) {
        let mut inner = self.inner.write();
        inner.add(key, value);
    }

    fn delete(&self, key: BlockfileKey) {
        let mut inner = self.inner.write();
        inner.delete(key);
    }
}

impl Blockfile for ArrowBlockfile {
    fn get(&self, key: BlockfileKey) -> Result<Value, Box<dyn crate::errors::ChromaError>> {
        // match &self.root {
        //     None => {
        //         // TODO: error instead
        //         panic!("Blockfile is empty");
        //     }
        //     Some(RootBlock::BlockData(block_data)) => {
        //         // TODO: don't unwrap
        //         // TODO: handle match on key type
        //         // TODO: binary search instead of scanning
        //         let prefixes = block_data.data.column_by_name("prefix").unwrap();
        //         let prefixes = prefixes.as_any().downcast_ref::<StringArray>().unwrap();
        //         let target_prefix_index = prefixes.iter().position(|x| x == Some(&key.prefix));
        //         let keys = block_data.data.column_by_name("key").unwrap();
        //         let keys = keys.as_any().downcast_ref::<StringArray>().unwrap();
        //         // Start at the index of the prefix and scan until we find the key
        //         let mut index = target_prefix_index.unwrap();
        //         while prefixes.value(index) == &key.prefix && index < keys.len() {
        //             match &key.key {
        //                 Key::String(key) => {
        //                     if keys.value(index) == key {
        //                         let values = block_data.data.column_by_name("value").unwrap();
        //                         let values = values.as_any().downcast_ref::<ListArray>().unwrap();
        //                         let value = values
        //                             .value(index)
        //                             .as_any()
        //                             .downcast_ref::<Int32Array>()
        //                             .unwrap()
        //                             .clone();
        //                         return Ok(Value::Int32ArrayValue(value));
        //                     } else {
        //                         index += 1;
        //                     }
        //                 }
        //                 _ => panic!("Unsupported key type"),
        //             }
        //         }

        //         unimplemented!("Need to implement get for block data");
        //     }
        //     Some(RootBlock::SparseIndex(sparse_index)) => {
        //         unimplemented!("Need to implement get for sparse index");
        //     }
        // }
        unimplemented!();
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
        if !self.in_transaction() {
            panic!("Transaction not in progress");
        }

        let transaction_state = match self.transaction_state {
            None => panic!("Transaction not in progress"),
            Some(ref mut state) => state,
        };

        let entry_size = get_key_size(&key) + get_value_size(&value);
        let target_block_id = self.sparse_index.get_target_block_id(&key);

        let delta = match transaction_state.get_delta_for_block(&target_block_id) {
            None => {
                println!("Creating new block delta");
                let target_block = match self.block_provider.get_block(&target_block_id) {
                    None => panic!("Block not found"), // TODO: This should not panic tbh
                    Some(block) => block,
                };
                let delta = Arc::new(BlockDelta::from(target_block));
                transaction_state.add_delta(delta.clone());
                delta
            }
            Some(delta) => delta,
        };

        if delta.can_add(entry_size) {
            println!("Adding to existing block");
            delta.add(key, value);
        } else {
            println!("Splitting block");
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
        // First determine the root block type
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
        let initial_block = block_provider.create_block();
        Self {
            sparse_index: SparseIndex::new(initial_block.id),
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
        let mut block_provider = ArrowBlockProvider::new();
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
        println!("GOT VALUE {:?}", value);
        match value {
            Value::Int32ArrayValue(array) => {
                assert_eq!(array.values(), &[1, 2, 3]);
            }
            _ => panic!("Unexpected value type"),
        }
    }

    #[test]
    fn test_splitting() {
        let mut block_provider = ArrowBlockProvider::new();
        let mut blockfile =
            ArrowBlockfile::new(KeyType::String, ValueType::Int32Array, block_provider);

        // Add one block worth of data
        blockfile.begin_transaction().unwrap();
        let n = 200;
        for i in 0..n {
            let key = BlockfileKey::new("key".to_string(), Key::String(i.to_string()));
            blockfile
                .set(key, Value::Int32ArrayValue(Int32Array::from(vec![i])))
                .unwrap();
        }
        // blockfile.commit_transaction().unwrap();

        // blockfile.begin_transaction().unwrap();
        // let bytes_per_entry = 8;
        // let start_i = n;
        // let entries_per_block = 1024 / bytes_per_entry;
        // println!("Entries per block: {}", entries_per_block);
        // for i in start_i..entries_per_block * 2 {
        //     let key = BlockfileKey::new("key".to_string(), Key::String(i.to_string()));
        //     blockfile
        //         .set(key, Value::Int32ArrayValue(Int32Array::from(vec![i])))
        //         .unwrap();
        // }
        // blockfile.commit_transaction().unwrap();
    }
}
