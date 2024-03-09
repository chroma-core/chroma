use super::{Block, BlockBuilderOptions, BlockData, BlockDataBuilder};
use crate::blockstore::{
    arrow_blockfile::{blockfile::MAX_BLOCK_SIZE, provider::ArrowBlockProvider},
    types::{BlockfileKey, KeyType, Value, ValueType},
};
use arrow::util::bit_util;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

/// A block delta tracks a source block and represents the new state of a block. Blocks are
/// immutable, so when a write is made to a block, a new block is created with the new state.
/// A block delta is a temporary representation of the new state of a block. A block delta
/// can be converted to a block data, which is then used to create a new block. A block data
/// can be converted into a block delta for new writes.
/// # Methods
/// - can_add: checks if a key value pair can be added to the block delta and still be within the
///  max block size.
/// - add: adds a key value pair to the block delta.
/// - delete: deletes a key from the block delta.
/// - get_min_key: gets the minimum key in the block delta.
/// - get_size: gets the size of the block delta.
/// - split: splits the block delta into two block deltas.
#[derive(Clone)]
pub struct BlockDelta {
    pub source_block: Arc<Block>,
    inner: Arc<RwLock<BlockDeltaInner>>,
}

impl BlockDelta {
    /// Checks if a key value pair can be added to the block delta and still be within the
    /// max block size.
    pub fn can_add(&self, key: &BlockfileKey, value: &Value) -> bool {
        let inner = self.inner.read();
        inner.can_add(key, value)
    }

    /// Adds a key value pair to the block delta.
    pub fn add(&self, key: BlockfileKey, value: Value) {
        let mut inner = self.inner.write();
        inner.add(key, value);
    }

    /// Deletes a key from the block delta.
    pub fn delete(&self, key: BlockfileKey) {
        let mut inner = self.inner.write();
        inner.delete(key);
    }

    /// Gets the minimum key in the block delta.
    pub fn get_min_key(&self) -> Option<BlockfileKey> {
        let inner = self.inner.read();
        let first_key = inner.new_data.keys().next();
        first_key.cloned()
    }

    /// Gets the size of the block delta as it would be in a block. This includes
    /// the size of the prefix, key, and value data and the size of the offsets
    /// where applicable. The size is rounded up to the nearest 64 bytes as per
    /// the arrow specification. When a block delta is converted into a block data
    /// the same sizing is used to allocate the memory for the block data.
    pub fn get_size(&self) -> usize {
        let inner = self.inner.read();
        inner.get_size(
            self.source_block.get_key_type(),
            self.source_block.get_value_type(),
        )
    }

    /// Splits the block delta into two block deltas. The split point is the last key
    /// that pushes the block over the half size.
    /// # Arguments
    /// - provider: the arrow block provider to create the new block.
    /// # Returns
    /// A tuple containing the the key of the split point and the new block delta.
    /// The new block delta contains all the key value pairs after, but not including the
    /// split point.
    /// # Panics
    /// This function will panic if their is no split point found. This should never happen
    /// as we should only call this function if can_add returns false.
    pub fn split(&self, provider: &ArrowBlockProvider) -> (BlockfileKey, BlockDelta) {
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

    fn len(&self) -> usize {
        let inner = self.inner.read();
        inner.new_data.len()
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
        let key_offset_bytes = self.offset_size_for_key_type(item_count, key_type);

        let value_total_bytes = bit_util::round_upto_multiple_of_64(value_size);
        let value_offset_bytes = self.offset_size_for_value_type(item_count, value_type);

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
            Value::RoaringBitmapValue(bitmap) => acc + bitmap.serialized_size(),
            _ => unimplemented!("Value type not implemented"),
        })
    }

    fn can_add(&self, key: &BlockfileKey, value: &Value) -> bool {
        let additional_prefix_size = key.get_prefix_size();
        let additional_key_size = key.key.get_size();
        let additional_value_size = value.get_size();

        let prefix_data_size = self.get_prefix_size() + additional_prefix_size;
        let key_data_size = self.get_key_size() + additional_key_size;
        let value_data_size = self.get_value_size() + additional_value_size;

        let prefix_offset_size = bit_util::round_upto_multiple_of_64((self.new_data.len() + 1) * 4);
        let key_offset_size = self.offset_size_for_key_type(self.new_data.len(), key.into());
        let value_offset_size = self.offset_size_for_value_type(self.new_data.len(), value.into());

        let prefix_total_bytes =
            bit_util::round_upto_multiple_of_64(prefix_data_size) + prefix_offset_size;
        let key_total_bytes = bit_util::round_upto_multiple_of_64(key_data_size) + key_offset_size;
        let value_total_bytes =
            bit_util::round_upto_multiple_of_64(value_data_size) + value_offset_size;
        let total_future_size = prefix_total_bytes + key_total_bytes + value_total_bytes;

        if total_future_size > MAX_BLOCK_SIZE {
            return false;
        }

        total_future_size <= MAX_BLOCK_SIZE
    }

    fn offset_size_for_value_type(&self, item_count: usize, value_type: ValueType) -> usize {
        match value_type {
            ValueType::Int32Array | ValueType::String | ValueType::RoaringBitmap => {
                bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
            }
            _ => unimplemented!("Value type not implemented"),
        }
    }

    fn offset_size_for_key_type(&self, item_count: usize, key_type: KeyType) -> usize {
        match key_type {
            KeyType::String => bit_util::round_upto_multiple_of_64((item_count + 1) * 4),
            KeyType::Float => 0,
            _ => unimplemented!("Key type not implemented"),
        }
    }

    /// Splits the block delta into two block deltas. The split point is the last key
    /// that pushes the block over the half size.
    /// # Arguments
    /// - key_type: the key type of the block.
    /// - value_type: the value type of the block.
    /// # Returns
    ///
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

        // The split key will be the last key that pushes the block over the half size. Not the first key that pushes it over
        let mut split_key = None;
        let mut iter = self.new_data.iter();
        while let Some((key, value)) = iter.next() {
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
            if current_size > half_size {
                let next = iter.next();
                match next {
                    Some((next_key, _)) => split_key = Some(next_key.clone()),
                    None => split_key = Some(key.clone()),
                }
                break;
            }
        }

        match &split_key {
            // Note: Consider returning a Result instead of panicking
            // This should never happen as we should only call this
            // function if can_add returns false. But it may be worth making
            // this compile time safe.
            None => panic!("No split point found"),
            Some(split_key) => {
                let split_after = self.new_data.split_off(split_key);
                return (split_key.clone(), split_after);
            }
        }
    }
}

impl TryFrom<&BlockDelta> for BlockData {
    type Error = super::BlockDataBuildError;

    fn try_from(delta: &BlockDelta) -> Result<BlockData, Self::Error> {
        let mut builder = BlockDataBuilder::new(
            delta.source_block.get_key_type(),
            delta.source_block.get_value_type(),
            Some(BlockBuilderOptions::new(
                delta.len(),
                delta.get_prefix_size(),
                delta.get_key_size(),
                delta.get_value_count(),
                delta.get_value_size(),
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
    use super::*;
    use crate::blockstore::types::{Key, KeyType, ValueType};
    use arrow::array::Int32Array;
    use rand::{random, Rng};

    #[test]
    fn test_sizing_int_arr_val() {
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
        let block_data = BlockData::try_from(&delta).unwrap();
        assert_eq!(size, block_data.get_size());
    }

    #[test]
    fn test_sizing_string_val() {
        let block_provider = ArrowBlockProvider::new();
        let block = block_provider.create_block(KeyType::String, ValueType::String);
        let delta = BlockDelta::from(block.clone());

        let n = 2000;
        for i in 0..n {
            let key = BlockfileKey::new("prefix".to_string(), Key::String(format!("key{}", i)));
            let value = Value::StringValue(format!("value{}", i));
            delta.add(key, value);
        }
        let size = delta.get_size();
        let block_data = BlockData::try_from(&delta).unwrap();
        assert_eq!(size, block_data.get_size());
    }

    #[test]
    fn test_sizing_int_key() {
        let block_provider = ArrowBlockProvider::new();
        let block = block_provider.create_block(KeyType::Float, ValueType::String);
        let delta = BlockDelta::from(block.clone());

        let n = 2000;
        for i in 0..n {
            let key = BlockfileKey::new("prefix".to_string(), Key::Float(i as f32));
            let value = Value::StringValue(format!("value{}", i));
            delta.add(key, value);
        }

        let size = delta.get_size();
        let block_data = BlockData::try_from(&delta).unwrap();
        assert_eq!(size, block_data.get_size());
    }

    #[test]
    fn test_sizing_roaring_bitmap_val() {
        let block_provider = ArrowBlockProvider::new();
        let block = block_provider.create_block(KeyType::String, ValueType::RoaringBitmap);
        let delta = BlockDelta::from(block.clone());

        let n = 2000;
        for i in 0..n {
            let key = BlockfileKey::new("key".to_string(), Key::String(format!("{:04}", i)));
            let value = Value::RoaringBitmapValue(roaring::RoaringBitmap::from_iter(
                (0..i).map(|x| x as u32),
            ));
            delta.add(key, value);
        }

        let size = delta.get_size();
        let block_data = BlockData::try_from(&delta).unwrap();
        assert_eq!(size, block_data.get_size());

        let (split_key, delta) = delta.split(&block_provider);
    }
}
