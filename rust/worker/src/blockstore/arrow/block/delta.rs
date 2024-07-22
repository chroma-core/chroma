use super::delta_storage::BlockStorage;
use crate::blockstore::{
    arrow::types::{ArrowWriteableKey, ArrowWriteableValue},
    key::CompositeKey,
};
use arrow::{array::RecordBatch, util::bit_util};
use uuid::Uuid;

/// A block delta tracks a source block and represents the new state of a block. Blocks are
/// immutable, so when a write is made to a block, a new block is created with the new state.
/// A block delta is a temporary representation of the new state of a block. A block delta
/// can be converted to a block data, which is then used to create a new block. A block data
/// can be converted into a block delta for new writes.
/// # Methods
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
    /// # Arguments
    /// - id: the id of the block delta.
    pub fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(id: Uuid) -> Self {
        BlockDelta {
            builder: V::get_delta_builder(),
            id,
        }
    }
}

impl BlockDelta {
    /// Adds a key value pair to the block delta.
    pub fn add<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) {
        // TODO: errors?
        V::add(prefix, key.into(), value, self);
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
    pub(in crate::blockstore::arrow) fn get_size<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
    ) -> usize {
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
        let value_validity_bytes = V::validity_size(item_count);

        prefix_total_bytes
            + prefix_offset_bytes
            + key_total_bytes
            + key_offset_bytes
            + value_total_bytes
            + value_offset_bytes
            + value_validity_bytes
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
    pub fn split<'referred_data, K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &'referred_data self,
        max_block_size_bytes: usize,
    ) -> Vec<(CompositeKey, BlockDelta)> {
        let half_size = max_block_size_bytes / 2;

        let mut blocks_to_split = Vec::new();
        blocks_to_split.push(self.clone());
        let mut output = Vec::new();
        let mut first_iter = true;
        // iterate over all blocks to split until its empty
        while !blocks_to_split.is_empty() {
            let curr_block = blocks_to_split.pop().unwrap();
            let mut curr_split_index = 0;
            let mut curr_running_prefix_size = 0;
            let mut curr_running_key_size = 0;
            let mut curr_running_value_size = 0;
            let mut curr_running_count = 0;
            for i in 1..curr_block.len() {
                curr_running_prefix_size += curr_block.builder.get_prefix_size(i - 1, i);
                curr_running_key_size += curr_block.builder.get_key_size(i - 1, i);
                curr_running_value_size += curr_block.builder.get_value_size(i - 1, i);
                curr_running_count += 1;

                let current_size = curr_block.get_block_size::<K, V>(
                    curr_running_count,
                    curr_running_prefix_size,
                    curr_running_key_size,
                    curr_running_value_size,
                );

                if current_size > half_size {
                    break;
                }
                curr_split_index = i;
            }

            // The split() method is exclusive of the split index. Meaning
            // the new block will contain the key at the split index. So we increment
            // the split index by 1 to get the correct split point.
            curr_split_index = std::cmp::min(curr_split_index + 1, curr_block.len() - 1);

            let split_key = curr_block.builder.get_key(curr_split_index);
            let new_delta = curr_block
                .builder
                .split(&split_key.prefix, split_key.key.clone());
            let new_block = BlockDelta {
                builder: new_delta,
                id: Uuid::new_v4(),
            };
            if first_iter {
                first_iter = false;
            } else {
                output.push((curr_block.builder.get_key(0).clone(), curr_block));
            }
            if new_block.get_size::<K, V>() > max_block_size_bytes {
                blocks_to_split.push(new_block);
            } else {
                output.push((split_key.clone(), new_block));
            }
        }

        return output;
    }

    pub(crate) fn len(&self) -> usize {
        self.builder.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cache::cache::Cache;
    use crate::cache::config::CacheConfig;
    use crate::cache::config::UnboundedCacheConfig;
    use crate::{
        blockstore::arrow::{
            block::Block, config::TEST_MAX_BLOCK_SIZE_BYTES, provider::BlockManager,
        },
        segment::DataRecord,
        storage::{local::LocalStorage, Storage},
        types::MetadataValue,
    };
    use arrow::array::Int32Array;
    use rand::{random, Rng};
    use roaring::RoaringBitmap;
    use std::collections::HashMap;

    /// Saves a block to a random file under the given path, then loads the block
    /// and validates that the loaded block has the same size as the original block.
    /// ### Returns
    /// - The loaded block
    /// ### Notes
    /// - Assumes that path will be cleaned up by the caller
    fn test_save_load_size(path: &str, block: &Block) -> Block {
        let save_path = format!("{}/{}", path, random::<u32>());
        block.save(&save_path).unwrap();
        let loaded = Block::load_with_validation(&save_path, block.id).unwrap();
        assert_eq!(loaded.id, block.id);
        assert_eq!(block.get_size(), loaded.get_size());
        loaded
    }

    #[tokio::test]
    async fn test_sizing_int_arr_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().to_str().unwrap();
        let storage = Storage::Local(LocalStorage::new(path));
        let cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let block_manager = BlockManager::new(storage, TEST_MAX_BLOCK_SIZE_BYTES, cache);
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

        let block = block_manager.commit::<&str, &Int32Array>(&delta);
        let mut values_before_flush = vec![];
        for i in 0..n {
            let key = format!("key{}", i);
            let read = block.get::<&str, Int32Array>("prefix", &key).unwrap();
            values_before_flush.push(read);
        }
        block_manager.flush(&block).await.unwrap();
        let block = block_manager.get(&block.clone().id).await.unwrap();
        for i in 0..n {
            let key = format!("key{}", i);
            let read = block.get::<&str, Int32Array>("prefix", &key).unwrap();
            assert_eq!(read, values_before_flush[i]);
        }
        test_save_load_size(path, &block);
        assert_eq!(size, block.get_size());
    }

    #[tokio::test]
    async fn test_sizing_string_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().to_str().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let block_manager = BlockManager::new(storage, TEST_MAX_BLOCK_SIZE_BYTES, cache);
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
        let block = block_manager.commit::<&str, &str>(&delta);
        let mut values_before_flush = vec![];
        for i in 0..n {
            let key = format!("key{}", i);
            let read = block.get::<&str, &str>("prefix", &key);
            values_before_flush.push(read.unwrap().to_string());
        }
        block_manager.flush(&block).await.unwrap();

        let block = block_manager.get(&delta_id).await.unwrap();
        // TODO: enable this assertion after the sizing is fixed
        assert_eq!(size, block.get_size());
        for i in 0..n {
            let key = format!("key{}", i);
            let read = block.get::<&str, &str>("prefix", &key);
            assert_eq!(read.unwrap().to_string(), values_before_flush[i]);
        }

        // test save/load
        let loaded = test_save_load_size(path, &block);
        for i in 0..n {
            let key = format!("key{}", i);
            let read = loaded.get::<&str, &str>("prefix", &key);
            assert_eq!(read, Some(format!("value{}", i).as_str()));
        }

        // test fork
        let forked_block = block_manager.fork::<&str, &str>(&delta_id).await;
        let new_id = forked_block.id.clone();
        let block = block_manager.commit::<&str, &str>(&forked_block);
        block_manager.flush(&block).await.unwrap();
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
        let path = tmp_dir.path().to_str().unwrap();
        let storage = Storage::Local(LocalStorage::new(path));
        let cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let block_manager = BlockManager::new(storage, TEST_MAX_BLOCK_SIZE_BYTES, cache);
        let delta = block_manager.create::<f32, &str>();

        let n = 2000;
        for i in 0..n {
            let prefix = "prefix";
            let key = i as f32;
            let value = format!("value{}", i);
            delta.add(prefix, key, value.as_str());
        }

        let size = delta.get_size::<f32, &str>();
        let block = block_manager.commit::<f32, &str>(&delta);
        let mut values_before_flush = vec![];
        for i in 0..n {
            let key = i as f32;
            let read = block.get::<f32, &str>("prefix", key).unwrap();
            values_before_flush.push(read);
        }
        block_manager.flush(&block).await.unwrap();
        let block = block_manager.get(&delta.id).await.unwrap();
        assert_eq!(size, block.get_size());
        for i in 0..n {
            let key = i as f32;
            let read = block.get::<f32, &str>("prefix", key).unwrap();
            assert_eq!(read, values_before_flush[i]);
        }
        // test save/load
        test_save_load_size(path, &block);
    }

    #[tokio::test]
    async fn test_sizing_roaring_bitmap_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().to_str().unwrap();
        let storage = Storage::Local(LocalStorage::new(path));
        let cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let block_manager = BlockManager::new(storage, TEST_MAX_BLOCK_SIZE_BYTES, cache);
        let delta = block_manager.create::<&str, &RoaringBitmap>();

        let n = 2000;
        for i in 0..n {
            let prefix = "prefix";
            let key = format!("{:04}", i);
            let value = RoaringBitmap::from_iter((0..i).map(|x| x as u32));
            delta.add(prefix, key.as_str(), &value);
        }

        let size = delta.get_size::<&str, &RoaringBitmap>();
        let block = block_manager.commit::<&str, &RoaringBitmap>(&delta);
        block_manager.flush(&block).await.unwrap();
        let block = block_manager.get(&delta.id).await.unwrap();
        // TODO: enable this assertion after the sizing is fixed
        assert_eq!(size, block.get_size());

        for i in 0..n {
            let key = format!("{:04}", i);
            let read = block.get::<&str, RoaringBitmap>("prefix", &key);
            let expected = RoaringBitmap::from_iter((0..i).map(|x| x as u32));
            assert_eq!(read, Some(expected));
        }

        // test save/load
        test_save_load_size(path, &block);
    }

    #[tokio::test]
    async fn test_data_record() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().to_str().unwrap();
        let storage = Storage::Local(LocalStorage::new(path));
        let cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let block_manager = BlockManager::new(storage, TEST_MAX_BLOCK_SIZE_BYTES, cache);
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
        let block = block_manager.commit::<&str, &DataRecord>(&delta);
        block_manager.flush(&block).await.unwrap();
        let block = block_manager.get(&delta.id).await.unwrap();
        for i in 0..3 {
            let read = block.get::<&str, DataRecord>("", ids[i]).unwrap();
            assert_eq!(read.id, ids[i]);
            assert_eq!(read.embedding, &embeddings[i]);
            assert_eq!(read.metadata, metadatas[i]);
            assert_eq!(read.document, documents[i]);
        }
        assert_eq!(size, block.get_size());

        // test save/load
        test_save_load_size(path, &block);
    }

    #[tokio::test]
    async fn test_sizing_uint_key_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().to_str().unwrap();
        let storage = Storage::Local(LocalStorage::new(path));
        let cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let block_manager = BlockManager::new(storage, TEST_MAX_BLOCK_SIZE_BYTES, cache);
        let delta = block_manager.create::<u32, &str>();

        let n = 2000;
        for i in 0..n {
            let prefix = "prefix";
            let key = i as u32;
            let value = format!("value{}", i);
            delta.add(prefix, key, value.as_str());
        }

        let size = delta.get_size::<u32, &str>();
        let block = block_manager.commit::<u32, &str>(&delta);
        block_manager.flush(&block).await.unwrap();
        let block = block_manager.get(&delta.id).await.unwrap();
        assert_eq!(size, block.get_size());

        // test save/load
        test_save_load_size(path, &block);
    }
}
