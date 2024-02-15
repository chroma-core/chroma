use std::sync::Arc;

use arrow::{
    array::{Int32Builder, ListBuilder, StringBuilder},
    datatypes::{DataType, Field},
    ipc::List,
    record_batch::RecordBatch,
};

use super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};

// TODO: this should be an arrow struct array
struct BlockInfo<K> {
    start_key: K,
    end_key: K,
    // TODO: make this uuid
    id: u64,
}

pub(crate) struct BlockData {
    // Arrow record batch with the schema (prefix, key, value)
    pub(crate) data: RecordBatch,
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
        for column in self.data.columns() {
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

impl BlockBuilder {
    pub(super) fn new(key_type: KeyType, value_type: ValueType) -> Self {
        match (key_type, value_type) {
            (KeyType::String, ValueType::Int32Array) => Self {
                prefix_builder: StringBuilder::new(),
                key_builder: KeyBuilder::StringBuilder(StringBuilder::new()),
                value_builder: ValueBuilder::Int32ArrayBuilder(ListBuilder::new(
                    Int32Builder::new(),
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
        //TODO: figure out how to get rid of nullable, the builders turn it on by default but we don't want it
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
}
