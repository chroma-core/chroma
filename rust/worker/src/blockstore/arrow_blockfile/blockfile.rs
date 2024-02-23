use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

use super::super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};
use super::arrow_utils::{get_key_size, get_value_size};
use super::block::BlockData;
use super::sparse_index::SparseIndex;

const MAX_BLOCK_SIZE: usize = 1024;

struct ArrowBlockfile {
    sparse_index: SparseIndex,
    key_type: KeyType,
    value_type: ValueType,
    transaction_state: Option<TransactionState>,
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

    fn get_delta_for_block(&self, block: &Arc<BlockData>) -> Option<Arc<BlockDelta>> {
        for delta in &self.block_delta {
            match delta.source_block {
                None => continue,
                Some(ref source_block) => {
                    if Arc::ptr_eq(source_block, &block) {
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
    source_block: Option<Arc<BlockData>>,
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

    fn from(block: Arc<BlockData>) -> Self {
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

    fn split(&self) {
        let mut inner = self.inner.write();
        // use std::mem::take to move the adds out since we are going to clear it
        let adds = std::mem::take(&mut inner.adds);
        let block_1 = BlockDelta::new();
        let block_2 = BlockDelta::new();
        let mut overall_size = 0;
        // Case 1: There is a source_block and we need to account for the data in it
        // Case 2: There is no source_block and we only need to account for the data in the adds
        // Ignore case 1 for now

        if self.source_block.is_some() {
            return unimplemented!();
        } else {
            // Incrementally add until we reach as close to 50% of the total size as possible
            for (key, value) in adds {
                let key_size = get_key_size(&key);
                let value_size = get_value_size(&value);
                let entry_size = key_size + value_size;
                if overall_size + entry_size <= MAX_BLOCK_SIZE / 2 {
                    overall_size += entry_size;
                    block_1.add(key, value);
                } else {
                    block_2.add(key, value);
                }
            }
        }

        let mut split_into = Vec::new();
        split_into.push(block_1);
        split_into.push(block_2);
        self.inner.write().split_into = Some(split_into);
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

    // TODO: have open vs create, we need create in order to define the type
    // of the value
    // the open method should be able to infer the type of the value based
    // on the record batch schema
    fn open(path: &str) -> Result<Self, Box<dyn crate::errors::ChromaError>>
    where
        Self: Sized,
    {
        // Fetch the sparse index
        unimplemented!();
    }

    fn create(
        path: &str,
        key_type: KeyType,
        value_type: ValueType,
    ) -> Result<Self, Box<dyn crate::errors::ChromaError>>
    where
        Self: Sized,
    {
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

        // Get target dirty block
        // let delta = match &self.root {
        //     None => {
        //         // Check if the transaction state has a root block delta, if not create one
        //         match &transaction_state.root_block_delta {
        //             None => {
        //                 let delta = Arc::new(BlockDelta::new());
        //                 transaction_state.root_block_delta = Some(delta.clone());
        //                 delta
        //             }
        //             Some(delta) => delta.clone(),
        //         }
        //     }
        //     Some(RootBlock::BlockData(block_data)) => {
        //         match transaction_state.get_delta_for_block(&block_data) {
        //             None => {
        //                 // TODO: BlockDelta is a Arc-inner pattern and doesn't need to be wrapped in an Arc again
        //                 let delta = Arc::new(BlockDelta::from(block_data.clone()));
        //                 transaction_state.add_delta(delta.clone());
        //                 delta
        //             }
        //             Some(delta) => delta,
        //         }
        //     }
        //     Some(RootBlock::SparseIndex(sparse_index)) => {
        //         unimplemented!();
        //     }
        // };

        // if delta.can_add(entry_size) {
        //     println!("Adding to existing delta");
        //     delta.add(key, value);
        // } else {
        //     println!("Splitting delta");
        //     delta.split();
        //     // let new_deltas = delta.inner.read().split_into.as_ref().unwrap();

        //     // Add the new deltas to the transaction state
        //     // for new_delta in new_deltas {
        //     //     // TODO: this doesn't need to be arc wrapped
        //     //     transaction_state.add_delta(Arc::new(new_delta.clone()));
        //     // }
        // }

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
    fn new(key_type: KeyType, value_type: ValueType) -> Self {
        Self {
            sparse_index: SparseIndex::new(0), // TODO: sparse index init id should be allocated
            transaction_state: None,
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
        let mut blockfile = ArrowBlockfile::new(KeyType::String, ValueType::Int32Array);

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
        let mut blockfile = ArrowBlockfile::new(KeyType::String, ValueType::Int32Array);

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
