use super::delta::BlockDelta;
use super::iterator::BlockIterator;
use crate::blockstore::types::{BlockfileKey, Key, KeyType, Value, ValueType};
use crate::errors::{ChromaError, ErrorCodes};
use arrow::array::{Float32Array, Float32Builder};
use arrow::{
    array::{Array, Int32Array, Int32Builder, ListArray, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use parking_lot::RwLock;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(in crate::blockstore::arrow_blockfile) enum BlockState {
    Uninitialized,
    Initialized,
    Commited,
    Registered,
}

pub(super) struct Inner {
    pub(super) id: Uuid,
    pub(super) data: Option<BlockData>,
    pub(super) state: BlockState,
    pub(super) key_type: KeyType,
    pub(super) value_type: ValueType,
}

#[derive(Clone)]
pub(in crate::blockstore::arrow_blockfile) struct Block {
    pub(super) inner: Arc<RwLock<Inner>>,
}

#[derive(Error, Debug)]
pub enum BlockError {
    #[error("Invalid state transition")]
    InvalidStateTransition,
}

impl ChromaError for BlockError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockError::InvalidStateTransition => ErrorCodes::Internal,
        }
    }
}

impl Block {
    pub fn new(id: Uuid, key_type: KeyType, value_type: ValueType) -> Self {
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

    pub(in crate::blockstore::arrow_blockfile) fn get(
        &self,
        query_key: &BlockfileKey,
    ) -> Option<Value> {
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
                        let key_matches = match &query_key.key {
                            Key::String(inner_key) => {
                                inner_key
                                    == key.as_any().downcast_ref::<StringArray>().unwrap().value(i)
                            }
                            Key::Float(inner_key) => {
                                *inner_key
                                    == key
                                        .as_any()
                                        .downcast_ref::<Float32Array>()
                                        .unwrap()
                                        .value(i)
                            }
                        };
                        if key_matches {
                            match self.get_value_type() {
                                ValueType::Int32Array => {
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
                                    ))
                                }
                                ValueType::String => {
                                    return Some(Value::StringValue(
                                        value
                                            .as_any()
                                            .downcast_ref::<StringArray>()
                                            .unwrap()
                                            .value(i)
                                            .to_string(),
                                    ))
                                }
                                _ => unimplemented!(),
                            }
                        }
                    }
                }
                None
            }
            None => None,
        }
    }

    pub(in crate::blockstore::arrow_blockfile) fn get_size(&self) -> usize {
        match &self.inner.read().data {
            Some(data) => data.get_size(),
            None => 0,
        }
    }

    pub(in crate::blockstore::arrow_blockfile) fn len(&self) -> usize {
        match &self.inner.read().data {
            Some(data) => data.data.column(0).len(),
            None => 0,
        }
    }

    pub(in crate::blockstore::arrow_blockfile) fn get_id(&self) -> Uuid {
        self.inner.read().id
    }

    pub(in crate::blockstore::arrow_blockfile) fn get_key_type(&self) -> KeyType {
        self.inner.read().key_type
    }

    pub(in crate::blockstore::arrow_blockfile) fn get_value_type(&self) -> ValueType {
        self.inner.read().value_type
    }

    pub(in crate::blockstore::arrow_blockfile) fn get_state(&self) -> BlockState {
        self.inner.read().state
    }

    pub(in crate::blockstore::arrow_blockfile) fn apply_delta(
        &self,
        delta: &BlockDelta,
    ) -> Result<(), Box<BlockError>> {
        let data = BlockData::from(delta);
        let mut inner = self.inner.write();
        match inner.state {
            BlockState::Uninitialized => {
                inner.data = Some(data);
                inner.state = BlockState::Initialized;
                Ok(())
            }
            BlockState::Initialized => {
                inner.data = Some(data);
                inner.state = BlockState::Initialized;
                Ok(())
            }
            BlockState::Commited | BlockState::Registered => {
                Err(Box::new(BlockError::InvalidStateTransition))
            }
        }
    }

    pub(in crate::blockstore::arrow_blockfile) fn commit(&self) -> Result<(), Box<BlockError>> {
        let mut inner = self.inner.write();
        match inner.state {
            BlockState::Uninitialized => Ok(()),
            BlockState::Initialized => {
                inner.state = BlockState::Commited;
                Ok(())
            }
            BlockState::Commited | BlockState::Registered => {
                Err(Box::new(BlockError::InvalidStateTransition))
            }
        }
    }

    pub(super) fn iter(&self) -> BlockIterator {
        BlockIterator::new(
            self.clone(),
            self.inner.read().key_type,
            self.inner.read().value_type,
        )
    }
}

#[derive(Clone)]
pub(super) struct BlockData {
    // Arrow record batch with the schema (prefix, key, value)
    pub(super) data: RecordBatch,
}

enum KeyBuilder {
    StringBuilder(StringBuilder),
    FloatBuilder(Float32Builder),
}

enum ValueBuilder {
    Int32ArrayValueBuilder(ListBuilder<Int32Builder>),
    StringValueBuilder(StringBuilder),
}

impl BlockData {
    pub(crate) fn new(data: RecordBatch) -> Self {
        Self { data }
    }

    pub(crate) fn get_size(&self) -> usize {
        let mut total_size = 0;
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
    pub(super) item_count: usize,
    pub(super) prefix_data_capacity: usize,
    pub(super) key_data_capacity: usize,
    // The capacity of the value in the case of nested types is a total capacity.
    // I.E. if you have a list of lists, the capacity is the total number of lists
    // times the total number of items in each list.
    // TODO: rethink the naming here. Data capacity vs number of items capacity
    pub(super) total_value_count: usize,
    pub(super) total_value_capacity: usize,
}

impl BlockBuilderOptions {
    pub(super) fn new(
        item_count: usize,
        prefix_data_capacity: usize,
        key_data_capacity: usize,
        total_value_count: usize,
        total_value_capacity: usize,
    ) -> Self {
        Self {
            item_count,
            prefix_data_capacity,
            key_data_capacity,
            total_value_count,
            total_value_capacity,
        }
    }

    pub(super) fn default() -> Self {
        Self {
            item_count: 1024,
            prefix_data_capacity: 1024,
            key_data_capacity: 1024,
            total_value_count: 1024,
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
        let prefix_builder =
            StringBuilder::with_capacity(options.item_count, options.prefix_data_capacity);
        let key_builder = match key_type {
            KeyType::String => KeyBuilder::StringBuilder(StringBuilder::with_capacity(
                options.item_count,
                options.key_data_capacity,
            )),
            KeyType::Float => {
                KeyBuilder::FloatBuilder(Float32Builder::with_capacity(options.item_count))
            }
        };
        let value_builder = match value_type {
            ValueType::Int32Array => {
                ValueBuilder::Int32ArrayValueBuilder(ListBuilder::with_capacity(
                    Int32Builder::with_capacity(options.total_value_count),
                    options.item_count,
                ))
            }
            ValueType::String => ValueBuilder::StringValueBuilder(StringBuilder::with_capacity(
                options.item_count,
                options.total_value_capacity,
            )),
            _ => unimplemented!(),
        };
        Self {
            prefix_builder,
            key_builder,
            value_builder,
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
                _ => unreachable!("Invalid key type for block"),
            },
            KeyBuilder::FloatBuilder(ref mut builder) => match key.key {
                Key::Float(key) => {
                    builder.append_value(key);
                }
                _ => unreachable!("Invalid key type for block"),
            },
        }

        match self.value_builder {
            ValueBuilder::Int32ArrayValueBuilder(ref mut builder) => match value {
                Value::Int32ArrayValue(array) => {
                    builder.append_value(&array);
                }
                _ => unimplemented!(),
            },
            ValueBuilder::StringValueBuilder(ref mut builder) => match value {
                Value::StringValue(string) => {
                    builder.append_value(string);
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
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            KeyBuilder::FloatBuilder(ref mut builder) => {
                key_field = Field::new("key", DataType::Float32, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
        };

        let value_field;
        let value = match self.value_builder {
            ValueBuilder::Int32ArrayValueBuilder(ref mut builder) => {
                value_field = Field::new(
                    "value",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                );
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            ValueBuilder::StringValueBuilder(ref mut builder) => {
                value_field = Field::new("value", DataType::Utf8, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
        };

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            value_field,
        ]));
        let record_batch =
            RecordBatch::try_new(schema, vec![Arc::new(prefix), Arc::new(key), value]);
        BlockData::new(record_batch.unwrap())
    }

    pub(super) fn get_size(&self) -> usize {
        let size = 0;
        size
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::blockstore::types::Key;
    use arrow::array::Int32Array;

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
                num_entries * 2,     // 2 int32s per entry
                num_entries * 2 * 4, // 2 int32s per entry
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
