use std::collections::BTreeMap;
use std::sync::Arc;

use super::arrow_utils::{get_key_size, get_value_size};
use super::block::{BlockBuilder, BlockData};
use super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};
use arrow::array::{Array, Int32Array, ListArray, RecordBatch, StringArray};
// arrow backed blockfile

// arrow backed sparse index is a blockfile where keys are the start key of a block and the value is
// the block id. The block id is used to look up the block in the blockfile. The blockfile is a
// columnar data store that is optimized for reading and writing large amounts of data.
// It is a bit confusing that a blockfile internally uses a blockfile to store the data - recursive

// A sparse index is a blockfile that contains a sparse index of the data in the blockfile when it
// is split into blocks. The sparse index is a list of the keys and the key ranges that are in each
// block. The sparse index is used to quickly find the block that contains a key or key range.
struct SparseIndex {
    data_blockfile: Box<dyn Blockfile>,
}

impl SparseIndex {
    fn new() -> Self {
        return Self {
            data_blockfile: Box::new(ArrowBlockfile::new(
                None,
                1024,
                KeyType::String,
                ValueType::String,
            )),
        };
    }
}

enum RootBlock {
    SparseIndex(SparseIndex),
    BlockData(BlockData),
}

struct ArrowBlockfile {
    root: Option<RootBlock>,
    max_block_size: usize,
    key_type: KeyType,
    value_type: ValueType,
    in_transaction: bool,
    transaction_state: TransactionState,
}

struct TransactionState {
    block_delta: Vec<BlockDelta>,
}

impl TransactionState {
    fn new() -> Self {
        Self {
            block_delta: Vec::new(),
        }
    }
}

struct BlockDelta {
    source_block: Option<Arc<BlockData>>,
    adds: BTreeMap<BlockfileKey, Value>,
    deletes: Vec<BlockfileKey>,
    split_into: Option<Vec<BlockDelta>>,
    max_block_size: usize, // TODO: this shouldn't be redundantly stored in every struct
}

impl BlockDelta {
    fn new(source_block: Arc<BlockData>) -> Self {
        Self {
            source_block: Some(source_block),
            adds: BTreeMap::new(),
            deletes: Vec::new(),
            split_into: None,
            max_block_size: 1024,
        }
    }

    fn is_new(&self) -> bool {
        self.source_block.is_none()
    }

    fn was_split(&self) -> bool {
        self.split_into.is_some()
    }

    fn split(&self) -> Vec<BlockDelta> {
        unimplemented!();
    }

    fn can_add(&self, bytes: usize) -> bool {
        // TODO: this should perform the rounding and padding to estimate the correct block size
        let curr_data_size = match &self.source_block {
            None => 0,
            Some(block) => block.get_size(),
        };
        let curr_adds_size = self.adds.iter().fold(0, |acc, (key, value)| {
            acc + get_key_size(key) + get_value_size(value)
        });
        let curr_deletes_size = self
            .deletes
            .iter()
            .fold(0, |acc, key| acc + get_key_size(key));
        let total_size = curr_data_size + curr_adds_size + curr_deletes_size;
        total_size + bytes <= self.max_block_size
    }
}

struct BlockCache {
    cache: std::collections::HashMap<u64, RecordBatch>,
}

impl BlockCache {
    fn new() -> Self {
        unimplemented!();
    }

    // A block is a record batch with the schema (key, value) and is sorted by key
    fn get_block(&self, block_id: u64) -> Result<RecordBatch, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }
}

impl Blockfile for ArrowBlockfile {
    fn get(&self, key: BlockfileKey) -> Result<Value, Box<dyn crate::errors::ChromaError>> {
        match &self.root {
            None => {
                // TODO: error instead
                panic!("Blockfile is empty");
            }
            Some(RootBlock::BlockData(block_data)) => {
                // TODO: don't unwrap
                // TODO: handle match on key type
                // TODO: binary search instead of scanning
                let prefixes = block_data.data.column_by_name("prefix").unwrap();
                let prefixes = prefixes.as_any().downcast_ref::<StringArray>().unwrap();
                let target_prefix_index = prefixes.iter().position(|x| x == Some(&key.prefix));
                let keys = block_data.data.column_by_name("key").unwrap();
                let keys = keys.as_any().downcast_ref::<StringArray>().unwrap();
                // Start at the index of the prefix and scan until we find the key
                let mut index = target_prefix_index.unwrap();
                while prefixes.value(index) == &key.prefix && index < keys.len() {
                    match &key.key {
                        Key::String(key) => {
                            if keys.value(index) == key {
                                let values = block_data.data.column_by_name("value").unwrap();
                                let values = values.as_any().downcast_ref::<ListArray>().unwrap();
                                let value = values
                                    .value(index)
                                    .as_any()
                                    .downcast_ref::<Int32Array>()
                                    .unwrap()
                                    .clone();
                                return Ok(Value::Int32ArrayValue(value));
                            } else {
                                index += 1;
                            }
                        }
                        _ => panic!("Unsupported key type"),
                    }
                }

                unimplemented!("Need to implement get for block data");
            }
            Some(RootBlock::SparseIndex(sparse_index)) => {
                unimplemented!("Need to implement get for sparse index");
            }
        }
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
        if !self.in_transaction {
            panic!("Transaction not in progress");
        }
        if self.transaction_state.adds.contains_key(&key) {
            panic!("Key already exists");
        }

        let mut entry_size = 0;
        match &value {
            Value::Int32ArrayValue(array) => {
                entry_size += array.get_buffer_memory_size();
            }
            _ => panic!("Unsupported value type"),
        }

        // compute the size of the prefix + key
        let prefix_size = key.prefix.len();
        let key_size = match &key.key {
            Key::String(key) => key.len(),
            _ => panic!("Unsupported"),
        };
        entry_size += prefix_size + key_size;
        println!("Entry size: {}", entry_size);

        match &self.root {
            None => {
                // let mut delta = BlockDelta::new(None);
                // if delta.can_add(entry_size) {
                //     delta.add_bytes(entry_size);
                // } else {
                //     // We need to split this block
                //     // Create a new sparse index
                //     // Create new data blocks
                //     // Move the data from the current block to the new blocks
                //     // Add the changes to the new blocks
                //     // Commit the new blocks
                // }
            }
            Some(RootBlock::BlockData(block_data)) => {
                let curr_size = block_data.get_size();
                if curr_size + entry_size > self.max_block_size {
                    // Split the block
                    // Create a new sparse index
                    // Create new data blocks
                    // Move the data from the current block to the new blocks
                    // Add the changes to the new blocks
                    // Commit the new blocks
                    unimplemented!();
                } else {
                    // Add the changes to the current block
                    // let mut delta = BlockDelta::new(Some(block_data.id));
                    // delta.add_bytes(entry_size);
                }
            }
            Some(RootBlock::SparseIndex(sparse_index)) => {
                unimplemented!();
            }
        }

        self.transaction_state.adds.insert(key, value);

        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), Box<dyn crate::errors::ChromaError>> {
        if self.in_transaction {
            // TODO: return error
            panic!("Transaction already in progress");
        }
        self.in_transaction = true;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn crate::errors::ChromaError>> {
        // First determine the root block type
        match &self.root {
            None => {
                let change_size = self.compute_changes_size();
                if change_size <= self.max_block_size {
                    // We can just add the changes to the block, no splitting is needed, and there is no current block
                    let mut block_builder =
                        BlockBuilder::new(self.key_type.clone(), self.value_type.clone(), None);
                    // TODO: drain this vec so we don't have to clone
                    for (key, value) in self.transaction_state.adds.iter() {
                        block_builder.add(key.clone(), value.clone());
                    }
                    let block_data = block_builder.build();
                    println!(
                        "After comitting single block size is: {:?}",
                        block_data.get_size()
                    );
                    self.root = Some(RootBlock::BlockData(block_data));
                } else {
                    unimplemented!("Need to split the block");
                }
            }
            Some(RootBlock::BlockData(root_block_data)) => {
                // Read the current block and see if we need to split based on its size
                // if we need to split, create a sparse index block and the needed number of data blocks
                // and move the data from the current block to the new blocks
                // if we don't need to split, just add the changes to the current block
                let curr_size = root_block_data.get_size();
                let change_size = self.compute_changes_size();
                if curr_size + change_size > self.max_block_size {
                    // Split the block
                    // Create a new sparse index
                    // Create new data blocks
                    // Move the data from the current block to the new blocks
                    // Add the changes to the new blocks
                    // Commit the new blocks
                    println!(
                        "curr_size: {:?} change_size: {:?} max_block_size: {:?}",
                        curr_size, change_size, self.max_block_size
                    );
                    let new_blocks_needed = (curr_size + change_size) / self.max_block_size;
                    println!("New blocks needed: {}", new_blocks_needed);
                    // let new_blocks = Vec::new();
                    for _ in 0..new_blocks_needed {
                        // Create a new block
                        // Add the changes to the new block
                        // Commit the new block
                        let mut builder =
                            BlockBuilder::new(self.key_type.clone(), self.value_type.clone(), None);
                        let mut added = 0;
                        for (key, value) in self.transaction_state.adds.iter() {
                            if added < self.max_block_size {
                                builder.add(key.clone(), value.clone());
                                match value {
                                    Value::Int32ArrayValue(array) => {
                                        added += array.get_buffer_memory_size();
                                    }
                                    _ => panic!("Unsupported value type"),
                                }
                            } else {
                                println!("Block is full, creating new block");
                                added = 0;
                                // Create a new block
                                // Add the changes to the new block
                                // Commit the new block
                            }
                        }
                    }
                } else {
                    // Add the changes to the current block
                }
            }
            Some(RootBlock::SparseIndex(sparse_index)) => {}
        }
        self.in_transaction = false;
        Ok(())
    }
}

impl ArrowBlockfile {
    fn new(
        root: Option<RootBlock>,
        max_block_size: usize,
        key_type: KeyType,
        value_type: ValueType,
    ) -> Self {
        Self {
            root,
            transaction_state: TransactionState {
                adds: BTreeMap::new(),
                deletes: Vec::new(),
                // block_delta: Vec::new(),
            },
            in_transaction: false,
            max_block_size,
            key_type,
            value_type,
        }
    }

    fn compute_changes_size(&self) -> usize {
        println!("=== ARROW BLOCKFILE CHANGESET SIZE ===");
        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut value_size = 0;
        let mut size = 0;
        for (key, value) in self.transaction_state.adds.iter() {
            size += key.prefix.len();
            prefix_size += key.prefix.len();
            match &key.key {
                Key::String(key) => {
                    size += key.len();
                    key_size += key.len();
                }
                _ => panic!("Unsupported key type"),
            }
            match value {
                Value::Int32ArrayValue(array) => {
                    size += array.get_buffer_memory_size();
                    value_size += array.get_buffer_memory_size();
                }
                _ => panic!("Unsupported value type"),
            }
        }
        // round all sizes to multiple of the arrow blockfile alignemnet - 64 bytes
        // add the size of the offset buffer for string arrays, we assume the offset buffer is 4 bytes per int
        // the prefix is always a string, the key may be a float and then there is no offset buffer
        let align = 64;
        prefix_size = prefix_size + (align - (prefix_size % align));
        println!("Size of prefixes value buffer: {}", prefix_size);
        let mut prefix_offset_buffer_size = 4 * self.transaction_state.adds.len();
        prefix_offset_buffer_size =
            prefix_offset_buffer_size + (align - (prefix_offset_buffer_size % align));
        println!(
            "Size of prefix offset buffer: {}",
            prefix_offset_buffer_size
        );
        prefix_size += prefix_offset_buffer_size;
        key_size = key_size + (align - (key_size % align));
        value_size = value_size + (align - (value_size % align));
        size = size + (align - (size % align));
        println!("Size of prefixes in changeset: {}", prefix_size);
        println!("Size of keys in changeset: {}", key_size);
        println!("Size of values in changeset: {}", value_size);
        println!("Size of changes in changeset: {}", size);
        size
    }

    fn commit_sparse_index(&self) {
        unimplemented!();
    }

    fn add_changes_to_block(&self, block_id: u64) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn test_blockfile() {
        let mut blockfile = ArrowBlockfile::new(None, 1024, KeyType::String, ValueType::Int32Array);

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
        let mut blockfile = ArrowBlockfile::new(None, 1024, KeyType::String, ValueType::Int32Array);

        // Add one block worth of data
        blockfile.begin_transaction().unwrap();
        let n = 42;
        for i in 0..n {
            let key = BlockfileKey::new("key".to_string(), Key::String(i.to_string()));
            blockfile
                .set(key, Value::Int32ArrayValue(Int32Array::from(vec![i])))
                .unwrap();
        }
        blockfile.commit_transaction().unwrap();

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
