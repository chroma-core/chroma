use super::types::Block;
use crate::blockstore::types::{BlockfileKey, Key, KeyType, Value, ValueType};
use arrow::array::{Array, BooleanArray, Int32Array, ListArray, StringArray, UInt32Array};

/// An iterator over the contents of a block.
/// This is a simple wrapper around the Arrow array data that is stored in the block.
/// For now, it clones the data in the Block, since it is only used to populate BlockDeltas.
pub(super) struct BlockIterator {
    block: Block,
    index: usize,
    length: usize,
    key_type: KeyType,
    value_type: ValueType,
}

impl BlockIterator {
    pub fn new(block: Block, key_type: KeyType, value_type: ValueType) -> Self {
        let len = block.len();
        Self {
            block,
            index: 0,
            length: len,
            key_type,
            value_type,
        }
    }
}

impl Iterator for BlockIterator {
    type Item = (BlockfileKey, Value);

    fn next(&mut self) -> Option<Self::Item> {
        let data = &self.block.inner.read().data;
        if data.is_none() {
            return None;
        }
        if self.index >= self.length {
            return None;
        }

        // Arrow requires us to downcast the array to the specific type we want to work with.
        // This is a bit awkward, but it's the way Arrow works to allow for dynamic typing.
        // We match and return None if the downcast fails, since we can't continue without the correct type.
        // In practice, this should never happen, since we control the types of the data we store in the block and
        // maintain the invariant that the data is always of the correct type.

        let prefix = match data
            .as_ref()
            .unwrap()
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
        {
            Some(prefix) => prefix.value(self.index).to_owned(),
            None => return None,
        };

        let key = match data.as_ref() {
            Some(data) => data.data.column(1),
            None => return None,
        };

        let value = match data.as_ref() {
            Some(data) => data.data.column(2),
            None => return None,
        };

        if self.index >= prefix.len() {
            return None;
        }

        let key = match self.key_type {
            KeyType::String => match key.as_any().downcast_ref::<StringArray>() {
                Some(key) => Key::String(key.value(self.index).to_string()),
                None => return None,
            },
            KeyType::Float => match key.as_any().downcast_ref::<Int32Array>() {
                Some(key) => Key::Float(key.value(self.index) as f32),
                None => return None,
            },
            KeyType::Bool => match key.as_any().downcast_ref::<BooleanArray>() {
                Some(key) => Key::Bool(key.value(self.index)),
                None => return None,
            },
            KeyType::Uint => match key.as_any().downcast_ref::<UInt32Array>() {
                Some(key) => Key::Uint(key.value(self.index) as u32),
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
            // TODO: Implement the rest of the value types
            _ => unimplemented!(),
        };
        self.index += 1;
        Some((BlockfileKey::new(prefix, key), value))
    }
}
