use super::{
    block::{Block, BlockBuilderOptions, BlockData, BlockDataBuilder},
    blockfile::MAX_BLOCK_SIZE,
    provider::ArrowBlockProvider,
};
use crate::blockstore::types::{BlockfileKey, KeyType, Value, ValueType};
use arrow::util::bit_util;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
pub(super) struct BlockDelta {
    pub(super) source_block: Arc<Block>,
    inner: Arc<RwLock<BlockDeltaInner>>,
}

impl BlockDelta {
    pub(super) fn can_add(&self, key: &BlockfileKey, value: &Value) -> bool {
        let inner = self.inner.read();
        inner.can_add(key, value)
    }

    pub(super) fn add(&self, key: BlockfileKey, value: Value) {
        let mut inner = self.inner.write();
        inner.add(key, value);
    }

    pub(super) fn delete(&self, key: BlockfileKey) {
        let mut inner = self.inner.write();
        inner.delete(key);
    }

    pub(super) fn get_min_key(&self) -> Option<BlockfileKey> {
        let inner = self.inner.read();
        let first_key = inner.new_data.keys().next();
        first_key.cloned()
    }

    fn get_prefix_size(&self) -> usize {
        let inner = self.inner.read();
        inner.get_prefix_size()
    }

    fn get_key_size(&self) -> usize {
        let inner = self.inner.read();
        inner.get_key_size()
    }

    fn get_value_size(&self) -> usize {
        let inner = self.inner.read();
        inner.get_value_size()
    }

    fn get_value_count(&self) -> usize {
        let inner = self.inner.read();
        inner.get_value_count()
    }

    pub(super) fn get_size(&self) -> usize {
        let inner = self.inner.read();
        inner.get_size(
            self.source_block.get_key_type(),
            self.source_block.get_value_type(),
        )
    }

    pub(super) fn len(&self) -> usize {
        let inner = self.inner.read();
        inner.new_data.len()
    }

    pub(super) fn split(&self, provider: &ArrowBlockProvider) -> (BlockfileKey, BlockDelta) {
        let new_block = provider.create_block(
            self.source_block.get_key_type(),
            self.source_block.get_value_type(),
        );
        let mut inner = self.inner.write();
        let (split_key, new_adds) = inner.split(
            self.source_block.get_key_type(),
            self.source_block.get_value_type(),
        );
        (
            split_key,
            BlockDelta {
                source_block: new_block,
                inner: Arc::new(RwLock::new(BlockDeltaInner { new_data: new_adds })),
            },
        )
    }
}

struct BlockDeltaInner {
    new_data: BTreeMap<BlockfileKey, Value>,
}

impl BlockDeltaInner {
    fn add(&mut self, key: BlockfileKey, value: Value) {
        self.new_data.insert(key, value);
    }

    fn delete(&mut self, key: BlockfileKey) {
        if self.new_data.contains_key(&key) {
            self.new_data.remove(&key);
        }
    }

    fn get_block_size(
        &self,
        item_count: usize,
        prefix_size: usize,
        key_size: usize,
        value_size: usize,
        key_type: KeyType,
        value_type: ValueType,
    ) -> usize {
        let prefix_total_bytes = bit_util::round_upto_multiple_of_64(prefix_size);
        let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);

        // https://docs.rs/arrow/latest/arrow/array/array/struct.GenericListArray.html
        let key_total_bytes = bit_util::round_upto_multiple_of_64(key_size);
        let key_offset_bytes = match key_type {
            KeyType::String => bit_util::round_upto_multiple_of_64((item_count + 1) * 4),
            KeyType::Float => bit_util::round_upto_multiple_of_64(item_count * 4),
        };

        let value_total_bytes = bit_util::round_upto_multiple_of_64(value_size);
        let value_offset_bytes = match value_type {
            ValueType::Int32Array | ValueType::String => {
                bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
            }
            _ => unimplemented!("Value type not implemented"),
        };

        println!(
            "Predicted prefix bytes: {}",
            prefix_total_bytes + prefix_offset_bytes
        );
        println!(
            "Predicted key bytes: {}",
            key_total_bytes + key_offset_bytes
        );
        println!(
            "Predicted value bytes: {}",
            value_total_bytes + value_offset_bytes
        );

        prefix_total_bytes
            + prefix_offset_bytes
            + key_total_bytes
            + key_offset_bytes
            + value_total_bytes
            + value_offset_bytes
    }

    fn get_size(&self, key_type: KeyType, value_type: ValueType) -> usize {
        let prefix_data_size = self.get_prefix_size();
        let key_data_size = self.get_key_size();
        let value_data_size = self.get_value_size();

        self.get_block_size(
            self.new_data.len(),
            prefix_data_size,
            key_data_size,
            value_data_size,
            key_type,
            value_type,
        )
    }

    fn get_prefix_size(&self) -> usize {
        self.new_data
            .iter()
            .fold(0, |acc, (key, _)| acc + key.get_prefix_size())
    }

    fn get_key_size(&self) -> usize {
        self.new_data
            .iter()
            .fold(0, |acc, (key, _)| acc + key.key.get_size())
    }

    fn get_value_size(&self) -> usize {
        self.new_data
            .iter()
            .fold(0, |acc, (_, value)| acc + value.get_size())
    }

    fn get_value_count(&self) -> usize {
        self.new_data.iter().fold(0, |acc, (_, value)| match value {
            Value::Int32ArrayValue(arr) => acc + arr.len(),
            Value::StringValue(s) => acc + s.len(),
            _ => unimplemented!("Value type not implemented"),
        })
    }

    fn can_add(&self, key: &BlockfileKey, value: &Value) -> bool {
        // TODO: move this into add with an error
        let additional_prefix_size = key.get_prefix_size();
        let additional_key_size = key.key.get_size();
        let additional_value_size = value.get_size();

        let prefix_data_size = self.get_prefix_size() + additional_prefix_size;
        let key_data_size = self.get_key_size() + additional_key_size;
        let value_data_size = self.get_value_size() + additional_value_size;
        if value_data_size > MAX_BLOCK_SIZE {
            println!("Can add, value data size: {}", value_data_size);
        }
        // TODO: use the same offset matching as in get_block_size
        let prefix_offset_size = (self.new_data.len() + 1) * 4;
        let key_offset_size = (self.new_data.len() + 1) * 4;
        let value_offset_size = (self.new_data.len() + 1) * 4;

        let prefix_total_bytes = bit_util::round_upto_multiple_of_64(prefix_data_size)
            + bit_util::round_upto_multiple_of_64(prefix_offset_size);
        let key_total_bytes = bit_util::round_upto_multiple_of_64(key_data_size)
            + bit_util::round_upto_multiple_of_64(key_offset_size);
        let value_total_bytes = bit_util::round_upto_multiple_of_64(value_data_size)
            + bit_util::round_upto_multiple_of_64(value_offset_size);
        let total_future_size = prefix_total_bytes + key_total_bytes + value_total_bytes;

        total_future_size <= MAX_BLOCK_SIZE
    }

    fn split(
        &mut self,
        key_type: KeyType,
        value_type: ValueType,
    ) -> (BlockfileKey, BTreeMap<BlockfileKey, Value>) {
        let half_size = MAX_BLOCK_SIZE / 2;
        let mut running_prefix_size = 0;
        let mut running_key_size = 0;
        let mut running_value_size = 0;
        let mut running_count = 0;
        let mut split_key = None;
        // The split key will be the last key that pushes the block over the half size. Not the first key that pushes it over
        for (key, value) in self.new_data.iter() {
            running_prefix_size += key.get_prefix_size();
            running_key_size += key.key.get_size();
            running_value_size += value.get_size();
            running_count += 1;
            let current_size = self.get_block_size(
                running_count,
                running_prefix_size,
                running_key_size,
                running_value_size,
                key_type,
                value_type,
            );
            if half_size < current_size {
                break;
            }
            split_key = Some(key.clone());
        }

        match &split_key {
            None => panic!("No split point found"),
            Some(split_key) => {
                let split_after = self.new_data.split_off(split_key);
                return (split_key.clone(), split_after);
            }
        }
    }
}

impl From<&BlockDelta> for BlockData {
    fn from(delta: &BlockDelta) -> Self {
        let mut builder = BlockDataBuilder::new(
            delta.source_block.get_key_type(),
            delta.source_block.get_value_type(),
            Some(BlockBuilderOptions::new(
                delta.len(),
                delta.get_prefix_size(),
                delta.get_key_size(),
                delta.get_value_count(),
            )),
        );
        for (key, value) in delta.inner.read().new_data.iter() {
            builder.add(key.clone(), value.clone());
        }
        builder.build()
    }
}

impl From<Arc<Block>> for BlockDelta {
    fn from(source_block: Arc<Block>) -> Self {
        // Read the exising block and put it into adds. We only create these
        // when we have a write to this block, so we don't care about the cost of
        // reading the block. Since we know we will have to do that no matter what.
        let mut adds = BTreeMap::new();
        let source_block_iter = source_block.iter();
        for (key, value) in source_block_iter {
            adds.insert(key, value);
        }
        BlockDelta {
            source_block,
            inner: Arc::new(RwLock::new(BlockDeltaInner { new_data: adds })),
        }
    }
}

#[cfg(test)]
mod test {
    use arrow::array::{Array, Int32Array};
    use rand::{random, Rng};

    use crate::blockstore::types::{Key, KeyType, ValueType};

    use super::*;

    #[test]
    fn test_sizing() {
        let block_provider = ArrowBlockProvider::new();
        let block = block_provider.create_block(KeyType::String, ValueType::Int32Array);
        let delta = BlockDelta::from(block.clone());

        let n = 2000;
        for i in 0..n {
            let key = BlockfileKey::new("prefix".to_string(), Key::String(format!("key{}", i)));
            let value_len: usize = rand::thread_rng().gen_range(1..100);
            let mut new_vec = Vec::with_capacity(value_len);
            for _ in 0..value_len {
                new_vec.push(random::<i32>());
            }
            delta.add(key, Value::Int32ArrayValue(Int32Array::from(new_vec)));
        }

        let size = delta.get_size();
        let block_data = BlockData::from(&delta);
        assert_eq!(size, block_data.get_size());
    }
}
