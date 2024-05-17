use super::delta_storage::{BlockKeyArrowBuilder, BlockStorage, StringValueStorage};
use crate::blockstore::{
    arrow::{
        blockfile::MAX_BLOCK_SIZE,
        types::{ArrowWriteableKey, ArrowWriteableValue},
    },
    key::{CompositeKey, KeyWrapper},
};
use arrow::{
    array::{RecordBatch, StringBuilder},
    util::bit_util,
};
use uuid::Uuid;

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
    pub(super) builder: BlockStorage,
    pub id: Uuid,
}

impl BlockDelta {
    /// Creates a new block delta from a block.
    pub fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(id: Uuid) -> Self {
        BlockDelta {
            builder: V::get_delta_builder(),
            id,
        }
    }
}

impl BlockDelta {
    /// Checks if a key value pair can be added to the block delta and still be within the
    /// max block size.
    pub fn can_add<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: &K,
        value: &V,
    ) -> bool {
        let additional_prefix_size = prefix.len();
        let additional_key_size = key.get_size();
        let additional_value_size = value.get_size();

        let prefix_data_size = self.builder.get_prefix_size(0, self.len()) + additional_prefix_size;
        let key_data_size = self.builder.get_key_size(0, self.len()) + additional_key_size;
        let value_data_size = self.builder.get_value_size(0, self.len()) + additional_value_size;

        let prefix_offset_size = bit_util::round_upto_multiple_of_64((self.builder.len() + 1) * 4);
        let key_offset_size = K::offset_size(self.builder.len() + 1);
        let value_offset_size = V::offset_size(self.builder.len() + 1);

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

    /// Adds a key value pair to the block delta.
    pub fn add<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) {
        // TODO: errors?
        V::add(prefix, key.into(), value, self)
    }

    /// Deletes a key from the block delta.
    pub fn delete<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self, prefix: &str, key: K) {
        V::delete(prefix, key.into(), self)
    }

    /// Gets the minimum key in the block delta.
    pub fn get_min_key(&self) -> Option<CompositeKey> {
        if self.builder.len() == 0 {
            return None;
        }
        Some(self.builder.get_key(0))
    }

    ///  Gets the size of the block delta as it would be in a block. This includes
    ///  the size of the prefix, key, and value data and the size of the offsets
    ///  where applicable. The size is rounded up to the nearest 64 bytes as per
    ///  the arrow specification. When a block delta is converted into a block data
    ///  the same sizing is used to allocate the memory for the block data.
    fn get_size<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self) -> usize {
        let prefix_data_size = self.builder.get_prefix_size(0, self.len());
        let key_data_size = self.builder.get_key_size(0, self.len());
        let value_data_size = self.builder.get_value_size(0, self.len());

        self.get_block_size::<K, V>(
            self.builder.len(),
            prefix_data_size,
            key_data_size,
            value_data_size,
        )
    }

    fn get_block_size<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        item_count: usize,
        prefix_size: usize,
        key_size: usize,
        value_size: usize,
    ) -> usize {
        let prefix_total_bytes = bit_util::round_upto_multiple_of_64(prefix_size);
        let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);

        // https://docs.rs/arrow/latest/arrow/array/array/struct.GenericListArray.html
        let key_total_bytes = bit_util::round_upto_multiple_of_64(key_size);
        let key_offset_bytes = K::offset_size(item_count);

        let value_total_bytes = bit_util::round_upto_multiple_of_64(value_size);
        let value_offset_bytes = V::offset_size(item_count);

        prefix_total_bytes
            + prefix_offset_bytes
            + key_total_bytes
            + key_offset_bytes
            + value_total_bytes
            + value_offset_bytes
    }

    pub fn finish<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self) -> RecordBatch {
        self.builder.to_record_batch::<K>()
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
    pub fn split<'referred_data, K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &'referred_data self,
    ) -> (CompositeKey, BlockDelta) {
        let half_size = MAX_BLOCK_SIZE / 2;
        let mut running_prefix_size = 0;
        let mut running_key_size = 0;
        let mut running_value_size = 0;
        let mut running_count = 0;

        println!("(Sanket-temp) Tree before split {:?}", self.builder);

        // The split key will be the last key that pushes the block over the half size. Not the first key that pushes it over
        let mut split_key = None;
        for i in 1..self.len() {
            // TODO: change this interface to be more ergo
            running_prefix_size += self.builder.get_prefix_size(i - 1, i);
            running_key_size += self.builder.get_key_size(i - 1, i);
            running_value_size += self.builder.get_value_size(i - 1, i);
            running_count += 1;

            let current_size = self.get_block_size::<K, V>(
                running_count,
                running_prefix_size,
                running_key_size,
                running_value_size,
            );

            if current_size > half_size {
                if i + 1 < self.len() {
                    split_key = Some(self.builder.get_key(i));
                } else {
                    split_key = Some(self.builder.get_key(i - 1));
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
                // TODO: standardize on composite key vs split key
                let split_after = self.builder.split(&split_key.prefix, split_key.key.clone());
                let new_delta = BlockDelta {
                    builder: split_after,
                    id: Uuid::new_v4(),
                };
                println!("(Sanket-temp) New split tree {:?} ", new_delta.builder);
                return (split_key.clone(), new_delta);
            }
        }
    }

    // fn get_value_count(&self) -> usize {
    //     let inner = self.inner.read();
    //     inner.get_value_count()
    // }

    fn len(&self) -> usize {
        self.builder.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        blockstore::{
            arrow::{block::Block, provider::BlockManager},
            types::Key,
        },
        segment::DataRecord,
        storage::{local::LocalStorage, Storage},
        types::MetadataValue,
    };
    use arrow::array::Int32Array;
    use rand::{random, Rng};
    use roaring::RoaringBitmap;
    use std::{collections::HashMap, hash::Hash};

    #[tokio::test]
    async fn test_sizing_int_arr_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_manager = BlockManager::new(storage);
        let delta = block_manager.create::<&str, &Int32Array>();

        let n = 2000;
        for i in 0..n {
            let prefix = "prefix";
            let key = format!("key{}", i);
            let value_len: usize = rand::thread_rng().gen_range(1..100);
            let mut new_vec = Vec::with_capacity(value_len);
            for _ in 0..value_len {
                new_vec.push(random::<i32>());
            }
            delta.add::<&str, &Int32Array>(prefix, &key, &Int32Array::from(new_vec));
        }

        let size = delta.get_size::<&str, &Int32Array>();
        // TODO: should commit take ownership of delta?
        // Semantically, that makes sense, since a delta is unsuable after commit
        block_manager.commit::<&str, &Int32Array>(&delta);
        let block = block_manager.get(&delta.id).await.unwrap();
        assert_eq!(size, block.get_size());
    }

    #[tokio::test]
    async fn test_sizing_string_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_manager = BlockManager::new(storage);
        let delta = block_manager.create::<&str, &str>();
        let delta_id = delta.id.clone();

        let n = 2000;
        for i in 0..n {
            let prefix = "prefix";
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            delta.add(prefix, key.as_str(), value.as_str());
        }
        let size = delta.get_size::<&str, &str>();
        block_manager.commit::<&str, &str>(&delta);
        let block = block_manager.get(&delta_id).await.unwrap();
        assert_eq!(size, block.get_size());
        for i in 0..n {
            let key = format!("key{}", i);
            let read = block.get::<&str, &str>("prefix", &key);
            assert_eq!(read, Some(format!("value{}", i).as_str()));
        }

        // test save/load
        block.save("test.arrow").unwrap();
        let loaded = Block::load("test.arrow", delta_id).unwrap();
        assert_eq!(loaded.id, delta_id);
        // TODO: make this sizing work
        // assert_eq!(block.get_size(), loaded.get_size());
        for i in 0..n {
            let key = format!("key{}", i);
            let read = loaded.get::<&str, &str>("prefix", &key);
            assert_eq!(read, Some(format!("value{}", i).as_str()));
        }

        // test fork
        let forked_block = block_manager.fork::<&str, &str>(&delta_id);
        let new_id = forked_block.id.clone();
        block_manager.commit::<&str, &str>(&forked_block);
        let forked_block = block_manager.get(&new_id).await.unwrap();
        for i in 0..n {
            let key = format!("key{}", i);
            let read = forked_block.get::<&str, &str>("prefix", &key);
            assert_eq!(read, Some(format!("value{}", i).as_str()));
        }
    }

    #[tokio::test]
    async fn test_sizing_float_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_manager = BlockManager::new(storage);
        let delta = block_manager.create::<f32, &str>();

        let n = 2000;
        for i in 0..n {
            let prefix = "prefix";
            let key = i as f32;
            let value = format!("value{}", i);
            delta.add(prefix, key, value.as_str());
        }

        let size = delta.get_size::<f32, &str>();
        block_manager.commit::<f32, &str>(&delta);
        let block = block_manager.get(&delta.id).await.unwrap();
        assert_eq!(size, block.get_size());
    }

    #[tokio::test]
    async fn test_sizing_roaring_bitmap_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_manager = BlockManager::new(storage);
        let delta = block_manager.create::<&str, &RoaringBitmap>();

        let n = 2000;
        for i in 0..n {
            let prefix = "prefix";
            let key = format!("{:04}", i);
            let value = RoaringBitmap::from_iter((0..i).map(|x| x as u32));
            delta.add(prefix, key.as_str(), &value);
        }

        let size = delta.get_size::<&str, &RoaringBitmap>();
        block_manager.commit::<&str, &RoaringBitmap>(&delta);
        let block = block_manager.get(&delta.id).await.unwrap();
        assert_eq!(size, block.get_size());

        for i in 0..n {
            let key = format!("{:04}", i);
            let read = block.get::<&str, RoaringBitmap>("prefix", &key);
            let expected = RoaringBitmap::from_iter((0..i).map(|x| x as u32));
            assert_eq!(read, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_data_record() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_manager = BlockManager::new(storage);
        let ids = vec!["embedding_id_2", "embedding_id_0", "embedding_id_1"];
        let embeddings = vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
            vec![7.0, 8.0, 9.0],
        ];
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), MetadataValue::Str("value1".to_string()));
        let metadata = Some(metadata);
        let metadatas = vec![None, metadata.clone(), None];
        let documents = vec![None, Some("test document"), None];
        let delta = block_manager.create::<&str, &DataRecord>();

        //TODO: Option<&T> as opposed to &Option<T>
        let data = vec![
            DataRecord {
                id: ids[0],
                embedding: &embeddings[0],
                metadata: metadatas[0].clone(),
                document: documents[0],
            },
            DataRecord {
                id: ids[1],
                embedding: &embeddings[1],
                metadata: metadatas[1].clone(),
                document: documents[1],
            },
            DataRecord {
                id: ids[2],
                embedding: &embeddings[2],
                metadata: metadatas[2].clone(),
                document: documents[2],
            },
        ];

        for record in data {
            delta.add("", record.id, &record);
        }

        let size = delta.get_size::<&str, &DataRecord>();
        block_manager.commit::<&str, &DataRecord>(&delta);
        let block = block_manager.get(&delta.id).await.unwrap();
        for i in 0..3 {
            let read = block.get::<&str, DataRecord>("", ids[i]).unwrap();
            assert_eq!(read.id, ids[i]);
            assert_eq!(read.embedding, &embeddings[i]);
            assert_eq!(read.metadata, metadatas[i]);
            assert_eq!(read.document, documents[i]);
        }
        assert_eq!(size, block.get_size());
    }

    // #[test]
    // fn test_sizing_uint_key_val() {
    //     let block_provider = ArrowBlockProvider::new();
    //     let block = block_provider.create_block(KeyType::Uint, ValueType::Uint);
    //     let delta = BlockDelta::from(block.clone());

    //     let n = 2000;
    //     for i in 0..n {
    //         let key = BlockfileKey::new("prefix".to_string(), Key::Uint(i as u32));
    //         let value = Value::UintValue(i as u32);
    //         delta.add(key, value);
    //     }

    //     let size = delta.get_size();
    //     let block_data = BlockData::try_from(&delta).unwrap();
    //     assert_eq!(size, block_data.get_size());
    // }
}
