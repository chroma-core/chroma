use arrow::{
    array::{Array, Int32Array, Int32Builder, ListArray, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use parking_lot::RwLock;
use std::sync::Arc;
use uuid::Uuid;

use super::super::types::{BlockfileKey, Key, KeyType, Value, ValueType};

#[derive(Clone, Copy)]
pub(super) enum BlockState {
    Uninitialized,
    Initialized,
    Commited,
    Registered,
}

struct Inner {
    id: Uuid,
    data: Option<BlockData>,
    state: BlockState,
    key_type: KeyType,
    value_type: ValueType,
}

#[derive(Clone)]
pub(super) struct Block {
    inner: Arc<RwLock<Inner>>,
}

impl Block {
    pub(super) fn new(id: Uuid, key_type: KeyType, value_type: ValueType) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                id,
                data: None,
                state: BlockState::Uninitialized,
                key_type,
                value_type,
            })),
        }
    }

    pub(super) fn get(&self, query_key: &BlockfileKey) -> Option<Value> {
        match &self.inner.read().data {
            Some(data) => {
                let prefix = data.data.column(0);
                let key = data.data.column(1);
                let value = data.data.column(2);
                // TODO: binary search
                // TODO: clean this up
                for i in 0..prefix.len() {
                    if prefix
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(i)
                        == query_key.prefix
                    {
                        match &query_key.key {
                            Key::String(inner_key) => {
                                let curr_key =
                                    key.as_any().downcast_ref::<StringArray>().unwrap().value(i);
                                // println!("Current key: {}", curr_key);
                                if inner_key
                                    == key.as_any().downcast_ref::<StringArray>().unwrap().value(i)
                                {
                                    return Some(Value::Int32ArrayValue(
                                        value
                                            .as_any()
                                            .downcast_ref::<ListArray>()
                                            .unwrap()
                                            .value(i)
                                            .as_any()
                                            .downcast_ref::<Int32Array>()
                                            .unwrap()
                                            .clone(),
                                    ));
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }
                }
                None
            }
            None => None,
        }
    }

    pub(super) fn get_size(&self) -> usize {
        match &self.inner.read().data {
            Some(data) => data.get_size(),
            None => 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        match &self.inner.read().data {
            Some(data) => data.data.column(0).len(),
            None => 0,
        }
    }

    pub(super) fn get_id(&self) -> Uuid {
        self.inner.read().id
    }

    pub(super) fn get_key_type(&self) -> KeyType {
        self.inner.read().key_type
    }

    pub(super) fn get_value_type(&self) -> ValueType {
        self.inner.read().value_type
    }

    pub(super) fn get_state(&self) -> BlockState {
        self.inner.read().state
    }

    pub(super) fn update_data(&self, data: BlockData) {
        let mut inner = self.inner.write();
        match inner.state {
            BlockState::Uninitialized => {
                inner.data = Some(data);
                inner.state = BlockState::Initialized;
            }
            BlockState::Initialized => {
                inner.data = Some(data);
                inner.state = BlockState::Initialized;
            }
            BlockState::Commited => {
                panic!("Block is already commited, and cannot get updated");
            }
            BlockState::Registered => {
                // TODO: this should error
                panic!("Block is already registered, and cannot get updated")
            }
        }
    }

    pub(super) fn commit(&self) {
        let mut inner = self.inner.write();
        // TODO: switch to errors
        match inner.state {
            BlockState::Uninitialized => {
                panic!("Block is uninitialized, and cannot get commited");
            }
            BlockState::Initialized => {
                inner.state = BlockState::Commited;
            }
            BlockState::Commited => {
                panic!("Block is already commited, and cannot get commited again");
            }
            BlockState::Registered => {
                panic!("Block is already registered, and cannot get commited");
            }
        }
    }

    pub(super) fn iter(&self) -> BlockIterator {
        BlockIterator {
            block: self.clone(),
            index: 0,
            key_type: self.inner.read().key_type,
            value_type: self.inner.read().value_type,
        }
    }
}

#[derive(Clone)]
pub(super) struct BlockData {
    // Arrow record batch with the schema (prefix, key, value)
    data: RecordBatch,
}

pub(super) struct BlockIterator {
    block: Block,
    index: usize,
    key_type: KeyType,
    value_type: ValueType,
}

impl Iterator for BlockIterator {
    type Item = (BlockfileKey, Value);

    // TODO: should the iter not clone? need to futz with lifetimes
    fn next(&mut self) -> Option<Self::Item> {
        let data = &self.block.inner.read().data;
        if data.is_none() {
            return None;
        }

        // TODO: clean up unwraps
        let prefix = data.as_ref().unwrap().data.column(0);
        let key = data.as_ref().unwrap().data.column(1);
        let value = data.as_ref().unwrap().data.column(2);

        if self.index >= prefix.len() {
            return None;
        }

        let prefix = match prefix.as_any().downcast_ref::<StringArray>() {
            Some(prefix) => prefix.value(self.index).to_owned(),
            None => return None,
        };

        let key = match self.key_type {
            KeyType::String => match key.as_any().downcast_ref::<StringArray>() {
                Some(key) => Key::String(key.value(self.index).to_string()),
                None => return None,
            },
            KeyType::Float => match key.as_any().downcast_ref::<Int32Array>() {
                Some(key) => Key::Float(key.value(self.index) as f32),
                None => return None,
            },
        };

        let value = match self.value_type {
            ValueType::Int32Array => match value.as_any().downcast_ref::<ListArray>() {
                Some(value) => {
                    let value = match value
                        .value(self.index)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                    {
                        Some(value) => {
                            // An arrow array, if nested in a larger structure, when cloned may clone the entire larger buffer.
                            // This leads to a memory overhead and also breaks our sizing assumptions. In order to work around this,
                            // we have to manuallly create a new array and copy the data over rather than relying on clone.

                            // Note that we use a vector here to avoid the overhead of the builder. The from() method for primitive
                            // types uses unsafe code to wrap the vecs underlying buffer in an arrow array.

                            // There are more performant ways to do this, but this is the most straightforward.

                            let mut new_vec = Vec::with_capacity(value.len());
                            for i in 0..value.len() {
                                new_vec.push(value.value(i));
                            }
                            let value = Int32Array::from(new_vec);
                            Value::Int32ArrayValue(value)
                        }
                        None => return None,
                    };
                    value
                }
                None => return None,
            },
            _ => unimplemented!(),
        };
        self.index += 1;
        Some((BlockfileKey::new(prefix, key), value))
    }
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
        let mut total_size = 0;
        let prefix_size = self.data.column(0).get_buffer_memory_size();
        let key_size = self.data.column(1).get_buffer_memory_size();
        let value_column = self
            .data
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let value_size = value_column.get_buffer_memory_size();
        println!("Actual Prefix size: {}", prefix_size);
        println!("Actual Key size: {}", key_size);
        println!("Actual Value size: {}", value_size);
        for column in self.data.columns() {
            total_size += column.get_buffer_memory_size();
        }
        total_size
    }
}

pub(super) struct BlockDataBuilder {
    prefix_builder: StringBuilder,
    key_builder: KeyBuilder,
    value_builder: ValueBuilder,
}

pub(super) struct BlockBuilderOptions {
    pub(super) item_capacity: usize,
    pub(super) prefix_data_capacity: usize,
    pub(super) key_data_capacity: usize,
    // The capacity of the value in the case of nested types is a total capacity.
    // I.E. if you have a list of lists, the capacity is the total number of lists
    // times the total number of items in each list.
    // TODO: rethink the naming here. Data capacity vs number of items capacity
    pub(super) total_value_capacity: usize,
}

impl BlockBuilderOptions {
    pub(super) fn new(
        item_capacity: usize,
        prefix_data_capacity: usize,
        key_data_capacity: usize,
        total_value_capacity: usize,
    ) -> Self {
        Self {
            item_capacity,
            prefix_data_capacity,
            key_data_capacity,
            total_value_capacity,
        }
    }

    pub(super) fn default() -> Self {
        Self {
            item_capacity: 1024,
            prefix_data_capacity: 1024,
            key_data_capacity: 1024,
            total_value_capacity: 1024,
        }
    }
}

impl BlockDataBuilder {
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
                value_builder: ValueBuilder::Int32ArrayBuilder(ListBuilder::with_capacity(
                    Int32Builder::with_capacity(options.total_value_capacity),
                    options.item_capacity,
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
        println!("Value size in builder: {}", value.get_buffer_memory_size());

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
        let mut block_builder = BlockDataBuilder::new(
            KeyType::String,
            ValueType::Int32Array,
            Some(BlockBuilderOptions::new(
                num_entries,
                prefix_bytes,
                key_bytes,
                num_entries * 2, // 2 int32s per entry
            )),
        );

        for i in 0..num_entries {
            block_builder.add(
                BlockfileKey::new(prefix.clone(), keys[i].clone()),
                Value::Int32ArrayValue(Int32Array::from(vec![i as i32, (i + 1) as i32])),
            );
        }
        // let prefix_total_bytes = bit_util::round_upto_multiple_of_64(prefix_bytes)
        //     + bit_util::round_upto_multiple_of_64(4 * num_entries);
        // let key_total_bytes = bit_util::round_upto_multiple_of_64(key_bytes)
        //     + bit_util::round_upto_multiple_of_64(4 * num_entries);
        // let value_bytes = bit_util::round_upto_multiple_of_64(4 * num_entries)
        //     + bit_util::round_upto_multiple_of_64(4 * num_entries);

        // println!("Expected prefix total size: {}", prefix_total_bytes);
        // println!("Expected key total size: {}", key_total_bytes);
        // let block_data = block_builder.build();
        // let size = block_data.get_size();
        // println!(
        //     "Predicted size: {}: Actual size: {}",
        //     size,
        //     block_data.get_size()
        // );
    }
}
