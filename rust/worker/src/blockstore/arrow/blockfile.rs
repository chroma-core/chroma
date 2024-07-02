use super::{block::delta::BlockDelta, provider::BlockManager};
use super::{
    block::Block,
    flusher::ArrowBlockfileFlusher,
    provider::SparseIndexManager,
    sparse_index::SparseIndex,
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
};
use crate::blockstore::key::KeyWrapper;
use crate::blockstore::BlockfileError;
use crate::errors::ErrorCodes;
use crate::{blockstore::key::CompositeKey, errors::ChromaError};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use std::{collections::HashSet, mem::transmute};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct ArrowBlockfileWriter {
    block_manager: BlockManager,
    sparse_index_manager: SparseIndexManager,
    block_deltas: Arc<Mutex<HashMap<Uuid, BlockDelta>>>,
    sparse_index: SparseIndex,
    id: Uuid,
    write_mutex: Arc<tokio::sync::Mutex<()>>,
}
// TODO: method visibility should not be pub(crate)

#[derive(Error, Debug)]
pub enum ArrowBlockfileError {
    #[error("Block not found")]
    BlockNotFound,
}

impl ChromaError for ArrowBlockfileError {
    fn code(&self) -> ErrorCodes {
        match self {
            ArrowBlockfileError::BlockNotFound => ErrorCodes::Internal,
        }
    }
}

impl ArrowBlockfileWriter {
    pub(super) fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        id: Uuid,
        block_manager: BlockManager,
        sparse_index_manager: SparseIndexManager,
    ) -> Self {
        let initial_block = block_manager.create::<K, V>();
        // TODO: we can update the constructor to take the initial block instead of having a seperate method
        let sparse_index = SparseIndex::new(id);
        sparse_index.add_initial_block(initial_block.id);
        let block_deltas = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut block_deltas_map = block_deltas.lock();
            block_deltas_map.insert(initial_block.id, initial_block);
        }
        Self {
            block_manager,
            sparse_index_manager,
            block_deltas: block_deltas,
            sparse_index: sparse_index,
            id,
            write_mutex: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(super) fn from_sparse_index(
        id: Uuid,
        block_manager: BlockManager,
        sparse_index_manager: SparseIndexManager,
        new_sparse_index: SparseIndex,
    ) -> Self {
        let block_deltas = Arc::new(Mutex::new(HashMap::new()));
        Self {
            block_manager,
            sparse_index_manager,
            block_deltas: block_deltas,
            sparse_index: new_sparse_index,
            id,
            write_mutex: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) fn commit<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        self,
    ) -> Result<ArrowBlockfileFlusher, Box<dyn ChromaError>> {
        let mut delta_ids = HashSet::new();
        for delta in self.block_deltas.lock().values() {
            // TODO: might these error?
            self.block_manager.commit::<K, V>(delta);
            delta_ids.insert(delta.id);
        }
        self.sparse_index_manager.commit(self.sparse_index.clone());

        let flusher = ArrowBlockfileFlusher::new(
            self.block_manager,
            self.sparse_index_manager,
            delta_ids,
            self.sparse_index,
            self.id,
        );

        // TODO: we need to update the sparse index with the new min keys?
        Ok(flusher)
    }

    pub(crate) async fn set<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) -> Result<(), Box<dyn ChromaError>> {
        // TODO: for now the BF writer locks the entire write operation
        let _guard = self.write_mutex.lock().await;

        // TODO: value must be smaller than the block size except for position lists, which are a special case
        //  where we split the value across multiple blocks

        // Get the target block id for the key
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let target_block_id = self.sparse_index.get_target_block_id(&search_key);

        // See if a delta for the target block already exists, if not create a new one and add it to the transaction state
        // Creating a delta loads the block entirely into memory

        // TODO: replace with R/W lock
        let delta = {
            let deltas = self.block_deltas.lock();
            let delta = match deltas.get(&target_block_id) {
                None => None,
                Some(delta) => Some(delta.clone()),
            };
            delta
        };

        let delta = match delta {
            None => {
                let block = self.block_manager.get(&target_block_id).await.unwrap();
                let new_delta = self.block_manager.fork::<K, V>(&block.id);
                let new_id = new_delta.id;
                self.sparse_index.replace_block(
                    target_block_id,
                    new_delta.id,
                    new_delta
                        .get_min_key()
                        .expect("Block should never be empty when forked"),
                );
                {
                    let mut deltas = self.block_deltas.lock();
                    deltas.insert(new_id, new_delta.clone());
                }
                new_delta
            }
            Some(delta) => delta,
        };

        // Add the key, value pair to delta.
        // Then check if its over size and split as needed
        delta.add(prefix, key, value);
        if delta.get_size::<K, V>() > self.block_manager.max_block_size_bytes() {
            let new_blocks = delta.split::<K, V>(self.block_manager.max_block_size_bytes());
            for (split_key, new_delta) in new_blocks {
                self.sparse_index.add_block(split_key, new_delta.id);
                let mut deltas = self.block_deltas.lock();
                deltas.insert(new_delta.id, new_delta);
            }
        }

        Ok(())
    }

    pub(crate) async fn delete<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<(), Box<dyn ChromaError>> {
        let _guard = self.write_mutex.lock().await;
        // Get the target block id for the key
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let target_block_id = self.sparse_index.get_target_block_id(&search_key);

        // TODO: clean this up as its redudant with the set method
        let delta = {
            let deltas = self.block_deltas.lock();
            let delta = match deltas.get(&target_block_id) {
                None => None,
                Some(delta) => Some(delta.clone()),
            };
            delta
        };

        let delta = match delta {
            None => {
                let block = self.block_manager.get(&target_block_id).await.unwrap();
                let new_delta = self.block_manager.fork::<K, V>(&block.id);
                let new_id = new_delta.id;
                self.sparse_index.replace_block(
                    target_block_id,
                    new_delta.id,
                    new_delta
                        .get_min_key()
                        .expect("Block should never be empty when forked"),
                );
                {
                    let mut deltas = self.block_deltas.lock();
                    deltas.insert(new_id, new_delta.clone());
                }
                new_delta
            }
            Some(delta) => delta,
        };
        delta.delete::<K, V>(prefix, key);
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}

#[derive(Clone)]
pub(crate) struct ArrowBlockfileReader<
    'me,
    K: ArrowReadableKey<'me> + Into<KeyWrapper>,
    V: ArrowReadableValue<'me>,
> {
    block_manager: BlockManager,
    pub(super) sparse_index: SparseIndex,
    loaded_blocks: Arc<Mutex<HashMap<Uuid, Box<Block>>>>,
    marker: std::marker::PhantomData<(K, V, &'me ())>,
    id: Uuid,
}

impl<'me, K: ArrowReadableKey<'me> + Into<KeyWrapper>, V: ArrowReadableValue<'me>>
    ArrowBlockfileReader<'me, K, V>
{
    pub(super) fn new(id: Uuid, block_manager: BlockManager, sparse_index: SparseIndex) -> Self {
        Self {
            block_manager,
            sparse_index,
            loaded_blocks: Arc::new(Mutex::new(HashMap::new())),
            marker: std::marker::PhantomData,
            id,
        }
    }

    pub(super) async fn get_block(&self, block_id: Uuid) -> Option<&Block> {
        if !self.loaded_blocks.lock().contains_key(&block_id) {
            let block = self.block_manager.get(&block_id).await?;
            self.loaded_blocks.lock().insert(block_id, Box::new(block));
        }

        if let Some(block) = self.loaded_blocks.lock().get(&block_id) {
            // https://github.com/mitsuhiko/memo-map/blob/a5db43853b2561145d7778dc2a5bd4b861fbfd75/src/lib.rs#L163
            // This is safe because we only ever insert Box<Block> into the HashMap
            // We never remove the Box<Block> from the HashMap, so the reference is always valid
            // We never mutate the Box<Block> after inserting it into the HashMap
            // We never share the Box<Block> with other threads - readers are single-threaded
            // We never drop the Box<Block> while the HashMap is still alive
            // We never drop the Box<Block> while the reference is still alive
            // We never drop the HashMap while the reference is still alive
            // We never drop the HashMap while the Box<Block> is still alive
            return Some(unsafe { transmute(&**block) });
        }

        None
    }

    pub(crate) async fn get(&'me self, prefix: &str, key: K) -> Result<V, Box<dyn ChromaError>> {
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let target_block_id = self.sparse_index.get_target_block_id(&search_key);
        let block = self.get_block(target_block_id).await;
        let res = match block {
            Some(block) => block.get(prefix, key.clone()),
            None => {
                tracing::error!("Block with id {:?} not found", target_block_id);
                return Err(Box::new(ArrowBlockfileError::BlockNotFound));
            }
        };
        match res {
            Some(value) => Ok(value),
            None => {
                tracing::error!(
                    "Key {:?}/{:?} not found in block {:?}",
                    prefix,
                    key,
                    target_block_id
                );
                return Err(Box::new(BlockfileError::NotFoundError));
            }
        }
    }

    pub(crate) async fn get_at_index(
        &'me self,
        index: usize,
    ) -> Result<(&'me str, K, V), Box<dyn ChromaError>> {
        let mut block_offset = 0;
        let mut block = None;
        let sparse_index_len = self.sparse_index.len();
        for i in 0..sparse_index_len {
            let uuid = {
                let sparse_index_forward = self.sparse_index.forward.lock();
                *sparse_index_forward.iter().nth(i).unwrap().1
            };
            block = self.get_block(uuid).await;
            match block {
                Some(b) => {
                    if block_offset + b.len() > index {
                        break;
                    }
                    block_offset += b.len();
                }
                None => {
                    tracing::error!("Block id {:?} not found", uuid);
                    return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                }
            }
        }
        let block = block.unwrap();
        let res = block.get_at_index::<'me, K, V>(index - block_offset);
        match res {
            Some((prefix, key, value)) => {
                return Ok((prefix, key, value));
            }
            _ => {
                tracing::error!(
                    "Value not found at index {:?} for block",
                    index - block_offset,
                );
                return Err(Box::new(BlockfileError::NotFoundError));
            }
        }
    }

    /// Returns all arrow records whose key > supplied key.
    pub(crate) async fn get_gt(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        // Get all block ids that contain keys > key from sparse index for this prefix.
        let block_ids = self.sparse_index.get_block_ids_gt(prefix, key.clone());
        let mut result: Vec<(&str, K, V)> = vec![];
        // Read all the blocks individually to get keys > key.
        for block_id in block_ids {
            let block_opt = self.get_block(block_id).await;
            let block = match block_opt {
                Some(b) => b,
                None => {
                    return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                }
            };
            match block.get_gt(prefix, key.clone()) {
                Some(data) => {
                    result.extend(data);
                }
                None => {
                    return Err(Box::new(BlockfileError::NotFoundError));
                }
            };
        }
        return Ok(result);
    }

    /// Returns all arrow records whose key < supplied key.
    pub(crate) async fn get_lt(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        // Get all block ids that contain keys < key from sparse index.
        let block_ids = self.sparse_index.get_block_ids_lt(prefix, key.clone());
        let mut result: Vec<(&str, K, V)> = vec![];
        // Read all the blocks individually to get keys < key.
        for block_id in block_ids {
            let block_opt = self.get_block(block_id).await;
            let block = match block_opt {
                Some(b) => b,
                None => {
                    return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                }
            };
            match block.get_lt(prefix, key.clone()) {
                Some(data) => {
                    result.extend(data);
                }
                None => {
                    return Err(Box::new(BlockfileError::NotFoundError));
                }
            };
        }
        return Ok(result);
    }

    /// Returns all arrow records whose key >= supplied key.
    pub(crate) async fn get_gte(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        // Get all block ids that contain keys >= key from sparse index.
        let block_ids = self.sparse_index.get_block_ids_gte(prefix, key.clone());
        let mut result: Vec<(&str, K, V)> = vec![];
        // Read all the blocks individually to get keys >= key.
        for block_id in block_ids {
            let block_opt = self.get_block(block_id).await;
            let block = match block_opt {
                Some(b) => b,
                None => {
                    return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                }
            };
            match block.get_gte(prefix, key.clone()) {
                Some(data) => {
                    result.extend(data);
                }
                None => {
                    return Err(Box::new(BlockfileError::NotFoundError));
                }
            };
        }
        return Ok(result);
    }

    /// Returns all arrow records whose key <= supplied key.
    pub(crate) async fn get_lte(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        // Get all block ids that contain keys <= key from sparse index.
        let block_ids = self.sparse_index.get_block_ids_lte(prefix, key.clone());
        let mut result: Vec<(&str, K, V)> = vec![];
        // Read all the blocks individually to get keys <= key.
        for block_id in block_ids {
            let block_opt = self.get_block(block_id).await;
            let block = match block_opt {
                Some(b) => b,
                None => {
                    return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                }
            };
            match block.get_lte(prefix, key.clone()) {
                Some(data) => {
                    result.extend(data);
                }
                None => {
                    return Err(Box::new(BlockfileError::NotFoundError));
                }
            };
        }
        return Ok(result);
    }

    /// Returns all arrow records whose prefix is same as supplied prefix.
    pub(crate) async fn get_by_prefix(
        &'me self,
        prefix: &str,
    ) -> Result<Vec<(&str, K, V)>, Box<dyn ChromaError>> {
        let block_ids = self.sparse_index.get_block_ids_prefix(prefix);
        let mut result: Vec<(&str, K, V)> = vec![];
        for block_id in block_ids {
            let block_opt = self.get_block(block_id).await;
            let block = match block_opt {
                Some(b) => b,
                None => {
                    return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                }
            };
            match block.get_prefix(prefix) {
                Some(data) => {
                    result.extend(data);
                }
                None => {
                    return Err(Box::new(BlockfileError::NotFoundError));
                }
            };
        }
        Ok(result)
    }

    pub(crate) async fn contains(&'me self, prefix: &str, key: K) -> bool {
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let target_block_id = self.sparse_index.get_target_block_id(&search_key);
        let block = self.get_block(target_block_id).await;
        let res: Option<V> = match block {
            Some(block) => block.get(prefix, key),
            None => {
                return false;
            }
        };
        match res {
            Some(_) => true,
            None => false,
        }
    }

    // Count the total number of records.
    pub(crate) async fn count(&self) -> Result<usize, Box<dyn ChromaError>> {
        let mut block_ids: Vec<Uuid> = vec![];
        {
            let lock_guard = self.sparse_index.forward.lock();
            let mut curr_iter = lock_guard.iter();
            while let Some((_, block_id)) = curr_iter.next() {
                block_ids.push(block_id.clone());
            }
        }
        let mut result: usize = 0;
        for block_id in block_ids {
            let block = self.get_block(block_id).await;
            match block {
                Some(b) => result = result + b.len(),
                None => {
                    return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                }
            }
        }
        Ok(result)
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        blockstore::arrow::provider::ArrowBlockfileProvider,
        segment::DataRecord,
        storage::{local::LocalStorage, Storage},
        types::MetadataValue,
    };
    use arrow::array::Int32Array;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use rand::seq::IteratorRandom;
    use std::collections::HashMap;
    use tokio::runtime::Runtime;

    const TEST_MAX_BLOCK_SIZE_BYTES: usize = 16384;

    #[tokio::test]
    async fn test_count() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let writer = blockfile_provider.create::<&str, &Int32Array>().unwrap();
        let id = writer.id();

        let prefix_1 = "key";
        let key1 = "zzzz";
        let value1 = Int32Array::from(vec![1, 2, 3]);
        writer.set(prefix_1, key1, &value1).await.unwrap();

        let prefix_2 = "key";
        let key2 = "aaaa";
        let value2 = Int32Array::from(vec![4, 5, 6]);
        writer.set(prefix_2, key2, &value2).await.unwrap();

        writer.commit::<&str, &Int32Array>().unwrap();

        let reader = blockfile_provider
            .open::<&str, Int32Array>(&id)
            .await
            .unwrap();

        let count = reader.count().await;
        match count {
            Ok(c) => assert_eq!(2, c),
            Err(_) => assert!(true, "Error getting count"),
        }
    }

    fn test_prefix(num_keys: u32, prefix_for_query: u32) {
        Runtime::new().unwrap().block_on(async {
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let blockfile_provider =
                ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
            let writer = blockfile_provider.create::<&str, u32>().unwrap();
            let id = writer.id();

            for j in 1..=5 {
                let prefix = format!("{}/{}", "prefix", j);
                for i in 1..=num_keys {
                    let key = format!("{}/{}", "key", i);
                    writer
                        .set(prefix.as_str(), key.as_str(), i as u32)
                        .await
                        .unwrap();
                }
            }
            // commit.
            writer.commit::<&str, u32>().unwrap();

            let reader = blockfile_provider.open::<&str, u32>(&id).await.unwrap();
            let prefix_query = format!("{}/{}", "prefix", prefix_for_query);
            println!("Query {}, num_keys {}", prefix_query, num_keys);
            let res = reader.get_by_prefix(prefix_query.as_str()).await;
            match res {
                Ok(c) => {
                    let mut kv_map = HashMap::new();
                    for entry in c {
                        kv_map.insert(format!("{}/{}", entry.0, entry.1), entry.2);
                    }
                    for j in 1..=5 {
                        let prefix = format!("{}/{}", "prefix", j);
                        for i in 1..=num_keys {
                            let key = format!("{}/{}", "key", i);
                            let map_key = format!("{}/{}", prefix, key);
                            if prefix == prefix_query {
                                assert!(
                                    kv_map.contains_key(&map_key),
                                    "{}",
                                    format!("Key {} should be present but not found", map_key)
                                );
                            } else {
                                assert!(
                                    !kv_map.contains_key(&map_key),
                                    "{}",
                                    format!("Key {} should not be present but found", map_key)
                                );
                            }
                        }
                    }
                }
                Err(_) => assert!(true, "Error running get by prefix"),
            }
        });
    }

    fn blockfile_comparisons(operation: ComparisonOperation, num_keys: u32, query_key: u32) {
        Runtime::new().unwrap().block_on(async {
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let blockfile_provider =
                ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
            let writer = blockfile_provider.create::<&str, u32>().unwrap();
            let id = writer.id();
            println!("Number of keys {}", num_keys);
            let prefix = "prefix";
            for i in 1..num_keys {
                let key = format!("{}/{}", "key", i);
                writer.set(prefix, key.as_str(), i as u32).await.unwrap();
            }
            // commit.
            writer.commit::<&str, u32>().unwrap();

            let reader = blockfile_provider.open::<&str, u32>(&id).await.unwrap();
            let query = format!("{}/{}", "key", query_key);
            println!("Query {}", query);
            println!("Operation {:?}", operation);
            let greater_than = match operation {
                ComparisonOperation::GreaterThan => reader.get_gt(prefix, query.as_str()).await,
                ComparisonOperation::GreaterThanOrEquals => {
                    reader.get_gte(prefix, query.as_str()).await
                }
                ComparisonOperation::LessThan => reader.get_lt(prefix, query.as_str()).await,
                ComparisonOperation::LessThanOrEquals => {
                    reader.get_lte(prefix, query.as_str()).await
                }
                _ => {
                    assert!(true, "Invalid operation");
                    // Won't reach here.
                    Ok(vec![])
                }
            };
            match greater_than {
                Ok(c) => {
                    let mut kv_map = HashMap::new();
                    for entry in c {
                        kv_map.insert(entry.1, entry.2);
                    }
                    for i in 1..num_keys {
                        let key = format!("{}/{}", "key", i);
                        let mut condition: bool = false;
                        match operation {
                            ComparisonOperation::GreaterThan => condition = key > query,
                            ComparisonOperation::GreaterThanOrEquals => condition = key >= query,
                            ComparisonOperation::LessThan => condition = key < query,
                            ComparisonOperation::LessThanOrEquals => condition = key <= query,
                            _ => assert!(true, "invalid input"),
                        }
                        if condition {
                            assert!(
                                kv_map.contains_key(key.as_str()),
                                "{}",
                                format!("Key {} should be present but not found", key)
                            );
                        } else {
                            assert!(
                                !kv_map.contains_key(key.as_str()),
                                "{}",
                                format!("Key {} should not be present but found", key)
                            );
                        }
                    }
                }
                Err(_) => assert!(true, "Error getting gt"),
            }
        });
    }

    #[derive(Debug)]
    pub(crate) enum ComparisonOperation {
        GreaterThan,
        LessThan,
        GreaterThanOrEquals,
        LessThanOrEquals,
    }

    proptest! {
        #![proptest_config(Config::with_cases(10))]
        #[test]
        fn test_get_gt(num_key in 1..10000u32, query_key in 1..10000u32) {
            blockfile_comparisons(ComparisonOperation::GreaterThan, num_key, query_key);
        }

        #[test]
        fn test_get_lt(num_key in 1..10000u32, query_key in 1..10000u32) {
            blockfile_comparisons(ComparisonOperation::LessThan, num_key, query_key);
        }

        #[test]
        fn test_get_gte(num_key in 1..10000u32, query_key in 1..10000u32) {
            blockfile_comparisons(ComparisonOperation::GreaterThanOrEquals, num_key, query_key);
        }

        #[test]
        fn test_get_lte(num_key in 1..10000u32, query_key in 1..10000u32) {
            blockfile_comparisons(ComparisonOperation::LessThanOrEquals, num_key, query_key);
        }

        #[test]
        fn test_get_by_prefix(num_key in 1..10000u32, prefix_query in 1..=5u32) {
            test_prefix(num_key, prefix_query);
        }
    }

    #[tokio::test]
    async fn test_blockfile() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let writer = blockfile_provider.create::<&str, &Int32Array>().unwrap();
        let id = writer.id();

        let prefix_1 = "key";
        let key1 = "zzzz";
        let value1 = Int32Array::from(vec![1, 2, 3]);
        writer.set(prefix_1, key1, &value1).await.unwrap();

        let prefix_2 = "key";
        let key2 = "aaaa";
        let value2 = Int32Array::from(vec![4, 5, 6]);
        writer.set(prefix_2, key2, &value2).await.unwrap();

        writer.commit::<&str, &Int32Array>().unwrap();

        let reader = blockfile_provider
            .open::<&str, Int32Array>(&id)
            .await
            .unwrap();

        let value = reader.get(prefix_1, key1).await.unwrap();
        assert_eq!(value.values(), &[1, 2, 3]);

        let value = reader.get(prefix_2, key2).await.unwrap();
        assert_eq!(value.values(), &[4, 5, 6]);
    }

    #[tokio::test]
    async fn test_splitting() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let writer = blockfile_provider.create::<&str, &Int32Array>().unwrap();
        let id_1 = writer.id();

        let n = 1200;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = Int32Array::from(vec![i]);
            writer.set("key", key.as_str(), &value).await.unwrap();
        }
        writer.commit::<&str, &Int32Array>().unwrap();

        let reader = blockfile_provider
            .open::<&str, Int32Array>(&id_1)
            .await
            .unwrap();

        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value.values(), &[i]);
        }

        // Sparse index should have 3 blocks
        match &reader {
            crate::blockstore::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.sparse_index.len(), 3);
                assert!(reader.sparse_index.is_valid());
            }
            _ => panic!("Unexpected reader type"),
        }

        // Add 5 new entries to the first block
        let writer = blockfile_provider
            .fork::<&str, &Int32Array>(&id_1)
            .await
            .unwrap();
        let id_2 = writer.id();
        for i in 0..5 {
            let key = format!("{:05}", i);
            let value = Int32Array::from(vec![i]);
            writer.set("key", key.as_str(), &value).await.unwrap();
        }
        writer.commit::<&str, &Int32Array>().unwrap();

        let reader = blockfile_provider
            .open::<&str, Int32Array>(&id_2)
            .await
            .unwrap();
        for i in 0..5 {
            let key = format!("{:05}", i);
            println!("Getting key: {}", key);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value.values(), &[i]);
        }

        // Sparse index should still have 3 blocks
        match &reader {
            crate::blockstore::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.sparse_index.len(), 3);
                assert!(reader.sparse_index.is_valid());
            }
            _ => panic!("Unexpected reader type"),
        }

        // Add 1200 more entries, causing splits
        let writer = blockfile_provider
            .fork::<&str, &Int32Array>(&id_2)
            .await
            .unwrap();
        let id_3 = writer.id();
        for i in n..n * 2 {
            let key = format!("{:04}", i);
            let value = Int32Array::from(vec![i]);
            writer.set("key", key.as_str(), &value).await.unwrap();
        }
        writer.commit::<&str, &Int32Array>().unwrap();

        let reader = blockfile_provider
            .open::<&str, Int32Array>(&id_3)
            .await
            .unwrap();
        for i in n..n * 2 {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value.values(), &[i]);
        }

        // Sparse index should have 6 blocks
        match &reader {
            crate::blockstore::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.sparse_index.len(), 6);
                assert!(reader.sparse_index.is_valid());
            }
            _ => panic!("Unexpected reader type"),
        }
    }

    #[tokio::test]
    async fn test_splitting_boundary() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let writer = blockfile_provider.create::<&str, &Int32Array>().unwrap();
        let id_1 = writer.id();

        // Add the larger keys first then smaller.
        let n = 1200;
        for i in n..n * 2 {
            let key = format!("{:04}", i);
            let value = Int32Array::from(vec![i]);
            writer.set("key", key.as_str(), &value).await.unwrap();
        }
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = Int32Array::from(vec![i]);
            writer.set("key", key.as_str(), &value).await.unwrap();
        }
        writer.commit::<&str, &Int32Array>().unwrap();

        let reader = blockfile_provider
            .open::<&str, Int32Array>(&id_1)
            .await
            .unwrap();

        for i in 0..n * 2 {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value.values(), &[i]);
        }
    }

    #[tokio::test]
    async fn test_string_value() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);

        let writer = blockfile_provider.create::<&str, &str>().unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = format!("{:04}", i);
            writer
                .set("key", key.as_str(), value.as_str())
                .await
                .unwrap();
        }

        writer.commit::<&str, &str>().unwrap();

        let reader = blockfile_provider.open::<&str, &str>(&id).await.unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value, format!("{:04}", i));
        }
    }

    #[tokio::test]
    async fn test_float_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);

        let writer = provider.create::<f32, &str>().unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = i as f32;
            let value = format!("{:04}", i);
            writer.set("key", key, value.as_str()).await.unwrap();
        }

        writer.commit::<f32, &str>().unwrap();

        let reader = provider.open::<f32, &str>(&id).await.unwrap();
        for i in 0..n {
            let key = i as f32;
            let value = reader.get("key", key).await.unwrap();
            assert_eq!(value, format!("{:04}", i));
        }
    }

    #[tokio::test]
    async fn test_roaring_bitmap_value() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);

        let writer = blockfile_provider
            .create::<&str, &roaring::RoaringBitmap>()
            .unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = format!("{:04}", i);
            println!("Setting key: {}", key);
            let value = roaring::RoaringBitmap::from_iter((0..i).map(|x| x as u32));
            writer.set("key", key.as_str(), &value).await.unwrap();
        }
        writer.commit::<&str, &roaring::RoaringBitmap>().unwrap();

        let reader = blockfile_provider
            .open::<&str, roaring::RoaringBitmap>(&id)
            .await
            .unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value.len(), i as u64);
            assert_eq!(
                value.iter().collect::<Vec<u32>>(),
                (0..i).collect::<Vec<u32>>()
            );
        }
    }

    #[tokio::test]
    async fn test_uint_key_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);

        let writer = blockfile_provider.create::<u32, u32>().unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = i as u32;
            let value = i as u32;
            writer.set("key", key, value).await.unwrap();
        }

        writer.commit::<u32, u32>().unwrap();

        let reader = blockfile_provider.open::<u32, u32>(&id).await.unwrap();
        for i in 0..n {
            let key = i as u32;
            let value = reader.get("key", key).await.unwrap();
            assert_eq!(value, i as u32);
        }
    }

    #[tokio::test]
    async fn test_data_record_val() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);

        let writer = blockfile_provider.create::<&str, &DataRecord>().unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = format!("{:04}", i);
            let mut metdata = HashMap::new();
            metdata.insert("key".to_string(), MetadataValue::Str("value".to_string()));
            let value = DataRecord {
                id: &key,
                embedding: &[i as f32],
                document: None,
                metadata: Some(metdata),
            };
            writer.set("key", key.as_str(), &value).await.unwrap();
        }

        writer.commit::<&str, &DataRecord>().unwrap();

        let reader = blockfile_provider
            .open::<&str, DataRecord>(&id)
            .await
            .unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value.id, key);
            assert_eq!(value.embedding, &[i as f32]);
            let metadata = value.metadata.unwrap();
            assert_eq!(metadata.len(), 1);
            assert_eq!(
                metadata.get("key").unwrap(),
                &MetadataValue::Str("value".to_string())
            );
        }
    }

    #[tokio::test]
    async fn test_large_split_value() {
        // Tests the case where a value is larger than half the block size
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);

        let writer = blockfile_provider.create::<&str, &str>().unwrap();
        let id = writer.id();

        let val_1_small = "a";
        let val_2_large = "a".repeat(TEST_MAX_BLOCK_SIZE_BYTES / 2 + 1);

        writer.set("key", "1", val_1_small).await.unwrap();
        writer.set("key", "2", val_2_large.as_str()).await.unwrap();
        writer.commit::<&str, &str>().unwrap();

        let reader = blockfile_provider.open::<&str, &str>(&id).await.unwrap();
        let val_1 = reader.get("key", "1").await.unwrap();
        let val_2 = reader.get("key", "2").await.unwrap();

        assert_eq!(val_1, val_1_small);
        assert_eq!(val_2, val_2_large);
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let writer = blockfile_provider.create::<&str, &str>().unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = format!("{:04}", i);
            writer
                .set("key", key.as_str(), value.as_str())
                .await
                .unwrap();
        }
        writer.commit::<&str, &str>().unwrap();

        let reader = blockfile_provider.open::<&str, &str>(&id).await.unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value, format!("{:04}", i));
        }

        let writer = blockfile_provider.fork::<&str, &str>(&id).await.unwrap();
        let id = writer.id();

        // Delete some keys
        let mut rng = rand::thread_rng();
        let deleted_keys = (0..n).choose_multiple(&mut rng, n / 2);
        for i in &deleted_keys {
            let key = format!("{:04}", *i);
            writer
                .delete::<&str, &str>("key", key.as_str())
                .await
                .unwrap();
        }
        writer.commit::<&str, &str>().unwrap();

        // Check that the deleted keys are gone
        let reader = blockfile_provider.open::<&str, &str>(&id).await.unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            if deleted_keys.contains(&i) {
                assert!(reader.get("key", &key).await.is_err());
            } else {
                let value = reader.get("key", &key).await.unwrap();
                assert_eq!(value, format!("{:04}", i));
            }
        }
    }

    #[tokio::test]
    async fn test_get_at_index() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let writer = blockfile_provider.create::<&str, &Int32Array>().unwrap();
        let id_1 = writer.id();

        let n = 1200;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = Int32Array::from(vec![i]);
            writer.set("key", key.as_str(), &value).await.unwrap();
        }
        writer.commit::<&str, &Int32Array>().unwrap();

        let reader = blockfile_provider
            .open::<&str, Int32Array>(&id_1)
            .await
            .unwrap();

        for i in 0..n {
            let expected_key = format!("{:04}", i);
            let expected_value = Int32Array::from(vec![i]);
            let res = reader.get_at_index(i as usize).await.unwrap();
            assert_eq!(res.0, "key");
            assert_eq!(res.1, expected_key);
            assert_eq!(res.2, expected_value);
        }
    }
}
