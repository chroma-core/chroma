use std::collections::BTreeMap;
use std::{collections::HashMap, hash::Hash, sync::Arc};

use super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};
use arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, Int32Builder, ListArray, ListBuilder, RecordBatch,
    StringArray, StringBuilder, StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field};
use arrow::ipc::List;

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

// impl SparseIndex<K> {}

// TODO: this should be an arrow struct array
struct BlockInfo<K> {
    start_key: K,
    end_key: K,
    // TODO: make this uuid
    id: u64,
}

struct BlockData {
    // Arrow array of keys in one column and the corresponding data in another column
    data: RecordBatch,
}

impl BlockData {
    fn new(data: RecordBatch) -> Self {
        Self { data }
    }

    fn get_size(&self) -> usize {
        self.data.get_array_memory_size()
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
    adds: BTreeMap<BlockfileKey, Value>,
    deletes: Vec<BlockfileKey>,
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
    fn get(&self, key: BlockfileKey) -> Result<&Value, Box<dyn crate::errors::ChromaError>> {
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
                while prefixes.value(index) == &key.prefix {
                    if keys.value(index) == &key.key {}
                    index += 1;
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
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn get_gt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn get_gte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn get_lt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn get_lte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn crate::errors::ChromaError>> {
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
                if change_size < self.max_block_size {
                    // We can just add the changes to the block, no splitting is needed, and there is no current block
                    match (&self.key_type, &self.value_type) {
                        (KeyType::String, ValueType::Int32Array) => {
                            // TODO: non-nullable
                            let mut prefix_builder = StringBuilder::new();
                            let mut key_builder = StringBuilder::new();
                            let mut value_builder = ListBuilder::new(Int32Builder::new());
                            for (key, value) in self.transaction_state.adds.iter() {
                                // TODO: figure out how to iterate such that we don't have to clone the key
                                prefix_builder.append_value(key.prefix.clone());
                                match &key.key {
                                    Key::String(key) => {
                                        key_builder.append_value(key);
                                    }
                                    _ => panic!("Unsupported key type"),
                                }

                                match value {
                                    Value::Int32ArrayValue(array) => {
                                        value_builder.append_value(array);
                                    }
                                    _ => panic!("Unsupported value type"),
                                }
                            }
                            let prefix = prefix_builder.finish();
                            let key = key_builder.finish();
                            let value = value_builder.finish();
                            // TODO: figure out how to deal with nullable, we don't need nullable by the StringBuilder forces it
                            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                                Field::new("prefix", DataType::Utf8, true),
                                Field::new("key", DataType::Utf8, true),
                                Field::new(
                                    "value",
                                    DataType::List(Arc::new(Field::new(
                                        "item",
                                        DataType::Int32,
                                        true,
                                    ))),
                                    true,
                                ),
                            ]));
                            let record_batch = RecordBatch::try_new(
                                schema,
                                vec![Arc::new(prefix), Arc::new(key), Arc::new(value)],
                            );
                            println!("{:?}", &record_batch);
                            let block_data = BlockData::new(record_batch.unwrap());
                            self.root = Some(RootBlock::BlockData(block_data));
                        }
                        _ => panic!("Unsupported key and value type"),
                    };
                }
            }
            Some(RootBlock::BlockData(root_block_data)) => {}
            Some(RootBlock::SparseIndex(sparse_index)) => {}
        }
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
            },
            in_transaction: false,
            max_block_size,
            key_type,
            value_type,
        }
    }

    fn compute_changes_size(&self) -> usize {
        let mut size = 0;
        for (key, value) in self.transaction_state.adds.iter() {
            // TODO: add key size
            match value {
                Value::Int32ArrayValue(array) => {
                    size += array.get_buffer_memory_size();
                }
                _ => panic!("Unsupported value type"),
            }
        }
        println!("Size of changes: {}", size);
        size
    }

    fn commit_sparse_index(&self) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int32Builder};

    #[test]
    fn test_blockfile() {
        let mut blockfile = ArrowBlockfile::new(None, 1024, KeyType::String, ValueType::Int32Array);

        blockfile.begin_transaction().unwrap();
        let key1 = BlockfileKey::new("key".to_string(), Key::String("zzzz".to_string()));
        blockfile
            .set(
                key1,
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
    }
}
