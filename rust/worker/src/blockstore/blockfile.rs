use std::{collections::HashMap, hash::Hash, sync::Arc};

use super::types::{Blockfile, BlockfileKey, Key, Value};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int32Builder, ListArray, ListBuilder, RecordBatch, StringBuilder,
    StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field};

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
    // TODO: can we preserve typing here?
    data: ArrayRef,
}

impl BlockData {
    fn new(data: ArrayRef) -> Self {
        Self { data }
    }

    fn get_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }
}

enum RootBlock {
    SparseIndex(SparseIndex),
    BlockData(BlockData),
}

struct ArrowBlockfile {
    // Values are BlockInfo
    root: RootBlock,
    adds: HashMap<BlockfileKey, Value>,
    deletes: Vec<BlockfileKey>,
    in_transaction: bool,
    max_block_size: usize,
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

// TODO: Is Array too restrictive a type here for the value?
impl Blockfile for ArrowBlockfile {
    fn get(&self, key: BlockfileKey) -> Result<&Value, Box<dyn crate::errors::ChromaError>> {
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
        if self.adds.contains_key(&key) {
            panic!("Key already exists");
        }
        self.adds.insert(key, value);
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
            RootBlock::BlockData(root_block_data) => {
                let change_size = self.compute_changes_size();
                let new_size = change_size + root_block_data.get_size();
                if new_size < self.max_block_size {
                    // We can just add the changes to the block, no splitting is needed
                    // TODO: this key type inference is hacky
                    // let mut block_builder = match self.adds.keys().next().unwrap().key {
                    //     Key::String(_) => StructBuilder::new(
                    //         vec![
                    //             Field::new("prefix", DataType::Utf8, false),
                    //             Field::new("key", DataType::Utf8, false),
                    //             Field::new("value", DataType::Int32, false),
                    //         ],
                    //         vec![
                    //             Box::new(StringBuilder::new()),
                    //             Box::new(Int32Builder::new()),
                    //         ],
                    //     ),
                    //     Key::Float(_) => StructBuilder::new(
                    //         vec![
                    //             Field::new("prefix", DataType::Utf8, false),
                    //             Field::new("key", DataType::Float32, false),
                    //             Field::new("value", DataType::Int32, false),
                    //         ],
                    //         vec![
                    //             Box::new(StringBuilder::new()),
                    //             Box::new(Int32Builder::new()),
                    //         ],
                    //     ),
                    // };
                    let first_key = self.adds.keys().next().unwrap();
                    let first_value = self.adds.get(first_key).unwrap();
                    // let mut block_builder = match (first_key.key, first_value) {
                    //     (Key::String(_), Value::ArrowInt32Array(_)) => StructBuilder::new(
                    //         vec![
                    //             Field::new("prefix", DataType::Utf8, false),
                    //             Field::new("key", DataType::Utf8, false),
                    //             Field::new("value", DataType::Int32, false),
                    //         ],
                    //         vec![
                    //             Box::new(StringBuilder::new()),
                    //             Box::new(StringBuilder::new()),
                    //             Box::new(Int32Builder::new()),
                    //         ],
                    //     ),
                    //     (Key::Float(_), Value::ArrowInt32Array(_)) => StructBuilder::new(
                    //         vec![
                    //             Field::new("prefix", DataType::Utf8, false),
                    //             Field::new("key", DataType::Float32, false),
                    //             Field::new("value", DataType::Int32, false),
                    //         ],
                    //         vec![
                    //             Box::new(StringBuilder::new()),
                    //             Box::new(Int32Builder::new()),
                    //         ],
                    //     ),
                    //     (Key::Float(_), Value::ArrowStructArray(_)) => StructBuilder::new(
                    //         vec![
                    //             Field::new("prefix", DataType::Utf8, false),
                    //             Field::new("key", DataType::Float32, false),
                    //             Field::new("value", DataType::Struct(vec![]), false),
                    //         ],
                    //         vec![
                    //             Box::new(StringBuilder::new()),
                    //             Box::new(StructBuilder::new(vec![], vec![])),
                    //         ],
                    //     ),
                    //     (Key::String(_), Value::ArrowStructArray(_)) => StructBuilder::new(
                    //         vec![
                    //             Field::new("prefix", DataType::Utf8, false),
                    //             Field::new("key", DataType::Utf8, false),
                    //             Field::new("value", DataType::Struct(vec![]), false),
                    //         ],
                    //         vec![
                    //             Box::new(StringBuilder::new()),
                    //             Box::new(StructBuilder::new(vec![], vec![])),
                    //         ],
                    //     ),
                    //     _ => panic!("Unsupported key and value type"),
                    // };
                } else {
                    // We need to split the block, and define a new sparse index
                    let num_blocks = new_size / self.max_block_size;
                    let new_sparse_index = SparseIndex {
                        data_blockfile: Box::new(ArrowBlockfile::new(
                            RootBlock::BlockData(BlockData {
                                data: Arc::new(Int32Array::from(vec![1, 2, 3])),
                            }),
                            self.max_block_size,
                        )),
                    };
                    self.commit_sparse_index();
                }
            }
            RootBlock::SparseIndex(_) => {}
        }
        Ok(())
    }
}

impl ArrowBlockfile {
    fn new(root: RootBlock, max_block_size: usize) -> Self {
        Self {
            root,
            adds: HashMap::new(),
            deletes: Vec::new(),
            in_transaction: false,
            max_block_size,
        }
    }

    fn compute_changes_size(&self) -> usize {
        let mut size = 0;
        for (key, value) in self.adds.iter() {
            // TODO: Figure out the generics here but this
            // match value {
            //     Value::ArrowInt32Array(array) => {
            //         size += array.get_buffer_memory_size();
            //     }
            //     Value::ArrowStructArray(array) => {
            //         size += array.get_buffer_memory_size();
            //     }
            //     _ => {
            //         panic!("Unsupported value type");
            //     }
            // }
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
        let mut blockfile = ArrowBlockfile {
            root: RootBlock::BlockData(BlockData {
                data: Arc::new(Int32Array::from(vec![1, 2, 3])),
            }),
            adds: HashMap::new(),
            deletes: Vec::new(),
            in_transaction: false,
            max_block_size: 1024,
        };

        blockfile.begin_transaction().unwrap();
        let key1 = BlockfileKey::new("key".to_string(), Key::String("1".to_string()));
        blockfile
            .set(
                key1,
                Value::Int32ArrayValue(Int32Array::from(vec![1, 2, 3])),
            )
            .unwrap();
        blockfile.commit_transaction().unwrap();
    }
}
