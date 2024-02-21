use arrow::array::Array;

use super::types::{Blockfile, BlockfileKey, Key, KeyType, Value, ValueType};

pub(super) fn get_key_size(blockfile_key: &BlockfileKey) -> usize {
    let prefix_size = blockfile_key.prefix.len();
    let key_size = match blockfile_key.key {
        Key::String(key) => key.len(),
        Key::Float(_) => 4,
    };
    prefix_size + key_size
}

pub(super) fn get_value_size(value: &Value) -> usize {
    match value {
        // TODO: This should return the minimum data size, not the buffer capacity, which is what is returned by get_buffer_memory_size
        Value::Int32ArrayValue(array) => array.get_buffer_memory_size(),
        _ => unimplemented!("Value type not supported"),
    }
}
