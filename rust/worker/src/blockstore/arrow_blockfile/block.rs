use std::sync::Arc;

use arrow::{
    array::{Array, ArrayBuilder, Int32Builder, ListBuilder, StringBuilder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use uuid::Uuid;

use super::super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};

enum BlockState {
    Uninitialized,
    Initialized,
}

pub(super) struct Block {
    pub(super) id: Uuid,
    data: Option<BlockData>,
    state: BlockState,
}

impl Block {
    pub(super) fn new(id: Uuid) -> Self {
        Self {
            id,
            data: None,
            state: BlockState::Uninitialized,
        }
    }

    pub(super) fn get_size(&self) -> usize {
        match &self.data {
            Some(data) => data.get_size(),
            None => 0,
        }
    }
}

struct BlockData {
    // Arrow record batch with the schema (prefix, key, value)
    data: RecordBatch,
}

enum KeyBuilder {
    StringBuilder(StringBuilder),
}

enum ValueBuilder {
    Int32ArrayBuilder(ListBuilder<Int32Builder>),
}

impl BlockData {
    pub(crate) fn new(data: RecordBatch) -> Self {
        Self { data }
    }

    pub(crate) fn get_size(&self) -> usize {
        println!("==== BLOCK GET_SIZE ====");
        let mut total_size = 0;
        println!(
            "Size of batch overall: {}",
            self.data.get_array_memory_size()
        );
        println!("Length of columns: {}", self.data.column(0).len());
        for column in self.data.columns() {
            println!("Size of column: {}", column.get_buffer_memory_size());
            total_size += column.get_buffer_memory_size();
        }
        total_size
    }
}

pub(super) struct BlockBuilder {
    prefix_builder: StringBuilder,
    key_builder: KeyBuilder,
    value_builder: ValueBuilder,
}

pub(super) struct BlockBuilderOptions {
    pub(super) item_capacity: usize,
    pub(super) prefix_data_capacity: usize,
    pub(super) key_data_capacity: usize,
}

impl BlockBuilderOptions {
    pub(super) fn new(
        item_capacity: usize,
        prefix_data_capacity: usize,
        key_data_capacity: usize,
    ) -> Self {
        Self {
            item_capacity,
            prefix_data_capacity,
            key_data_capacity,
        }
    }

    pub(super) fn default() -> Self {
        Self {
            item_capacity: 1024,
            prefix_data_capacity: 1024,
            key_data_capacity: 1024,
        }
    }
}

impl BlockBuilder {
    pub(super) fn new(
        key_type: KeyType,
        value_type: ValueType,
        options: Option<BlockBuilderOptions>,
    ) -> Self {
        let options = options.unwrap_or(BlockBuilderOptions::default());
        match (key_type, value_type) {
            (KeyType::String, ValueType::Int32Array) => Self {
                prefix_builder: StringBuilder::with_capacity(
                    options.item_capacity,
                    options.prefix_data_capacity,
                ),
                key_builder: KeyBuilder::StringBuilder(StringBuilder::with_capacity(
                    options.item_capacity,
                    options.key_data_capacity,
                )),
                value_builder: ValueBuilder::Int32ArrayBuilder(ListBuilder::new(
                    Int32Builder::with_capacity(options.item_capacity),
                )),
            },
            _ => unimplemented!(),
        }
    }

    pub(super) fn add(&mut self, key: BlockfileKey, value: Value) {
        // TODO: you must add in sorted order, error if not
        self.prefix_builder.append_value(key.prefix);
        match self.key_builder {
            KeyBuilder::StringBuilder(ref mut builder) => match key.key {
                Key::String(key) => {
                    builder.append_value(key);
                }
                _ => unimplemented!(),
            },
        }

        match self.value_builder {
            ValueBuilder::Int32ArrayBuilder(ref mut builder) => match value {
                Value::Int32ArrayValue(array) => {
                    builder.append_value(&array);
                }
                _ => unimplemented!(),
            },
        }
    }

    pub(super) fn build(&mut self) -> BlockData {
        println!("==== BLOCK BUILDER BUILD ====");
        let prefix = self.prefix_builder.finish();
        let prefix_field = Field::new("prefix", DataType::Utf8, true);
        // TODO: figure out how to get rid of nullable, the builders turn it on by default but we don't want it
        let key_field;
        let key = match self.key_builder {
            KeyBuilder::StringBuilder(ref mut builder) => {
                key_field = Field::new("key", DataType::Utf8, true);
                builder.finish()
            }
        };

        println!(
            "Size of prefix in builder: {}",
            prefix.get_buffer_memory_size()
        );

        let value_field;
        let value = match self.value_builder {
            ValueBuilder::Int32ArrayBuilder(ref mut builder) => {
                value_field = Field::new(
                    "value",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                );
                builder.finish()
            }
        };

        println!("Size of key in builder: {}", key.get_buffer_memory_size());
        // println!(
        //     "Size of value in builder: {}",
        //     value.get_buffer_memory_size()
        // );
        // println!(
        //     "Total size of prefix, key, value: {}",
        //     prefix.get_buffer_memory_size()
        //         + key.get_buffer_memory_size()
        //         + value.get_buffer_memory_size()
        // );

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            value_field,
        ]));
        let record_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(prefix), Arc::new(key), Arc::new(value)],
        );
        BlockData::new(record_batch.unwrap())
    }

    pub(super) fn get_size(&self) -> usize {
        let size = 0;
        size
    }
}

#[cfg(test)]
mod test {
    use crate::blockstore::types::Key;
    use arrow::util::bit_util;

    use arrow::array::Int32Array;

    use super::*;

    #[test]
    fn test_block_builder() {
        let num_entries = 1000;

        let mut keys = Vec::new();
        let mut key_bytes = 0;
        for i in 0..num_entries {
            keys.push(Key::String(i.to_string()));
            key_bytes += i.to_string().len();
        }

        let prefix = "key".to_string();
        let prefix_bytes = prefix.len() * num_entries;
        let mut block_builder = BlockBuilder::new(
            KeyType::String,
            ValueType::Int32Array,
            Some(BlockBuilderOptions::new(
                num_entries,
                prefix_bytes,
                key_bytes,
            )),
        );

        for i in 0..num_entries {
            block_builder.add(
                BlockfileKey::new(prefix.clone(), keys[i].clone()),
                Value::Int32ArrayValue(Int32Array::from(vec![i as i32])),
            );
        }
        // NOTE: RESUME BY SPLITTING THE SIZE OF THE BLOCKS OFFSETS AND ROUNDING THE DATA AND THE OFFSET SEPERATELY
        let prefix_total_bytes = bit_util::round_upto_multiple_of_64(prefix_bytes)
            + bit_util::round_upto_multiple_of_64(4 * num_entries);
        let key_total_bytes = bit_util::round_upto_multiple_of_64(key_bytes)
            + bit_util::round_upto_multiple_of_64(4 * num_entries);

        println!("Expected prefix total size: {}", prefix_total_bytes);
        println!("Expected key total size: {}", key_total_bytes);
        let block_data = block_builder.build();
    }
}
