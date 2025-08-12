use super::block::delta::types::Delta;
use super::block::delta::OrderedBlockDelta;
use super::migrations::apply_migrations_to_blockfile;
use super::migrations::MigrationError;
use super::provider::BlockManager;
use super::provider::RootManager;
use super::root::RootWriter;
use super::sparse_index::SparseIndexDelimiter;
use super::{
    flusher::ArrowBlockfileFlusher,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};
use crate::arrow::root::CURRENT_VERSION;
use crate::arrow::sparse_index::SparseIndexWriter;
use crate::key::CompositeKey;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use itertools::Itertools;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use uuid::Uuid;

// The end key is exclusive, if the end key is None, then the block/delta is open-ended
type BlockIdAndEndKey = (Uuid, Option<CompositeKey>);
type CurrentDeltaAndEndKey = (OrderedBlockDelta, Option<CompositeKey>);

#[derive(Default)]
struct Inner {
    /// On construction, this contains all existing block IDs and the end of their key range ordered by end key (asc).
    /// As mutations are made, the writer pops from the front of this stack to determine which block to mutate.
    remaining_block_stack: VecDeque<BlockIdAndEndKey>,
    /// Holds the current block delta and its end key. When we receive a write past the end key, this delta is moved into `completed_block_deltas`.
    current_block_delta: Option<CurrentDeltaAndEndKey>,
    /// Deltas in this vec can no longer receive writes and are ready to be committed.
    completed_block_deltas: Vec<OrderedBlockDelta>,
}

#[derive(Clone)]
pub struct ArrowOrderedBlockfileWriter {
    block_manager: BlockManager,
    root_manager: RootManager,
    root: RootWriter,
    inner: Arc<Mutex<Inner>>,
    id: Uuid,
}

#[derive(Error, Debug)]
pub enum ArrowBlockfileError {
    #[error("Could not migrate blockfile to new version")]
    MigrationError(#[from] MigrationError),
}

impl ChromaError for ArrowBlockfileError {
    fn code(&self) -> ErrorCodes {
        match self {
            ArrowBlockfileError::MigrationError(e) => e.code(),
        }
    }
}

impl ArrowOrderedBlockfileWriter {
    pub(super) fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        id: Uuid,
        prefix_path: &str,
        block_manager: BlockManager,
        root_manager: RootManager,
        max_block_size_bytes: usize,
    ) -> Self {
        let initial_block = block_manager.create::<K, V, OrderedBlockDelta>();
        let sparse_index = SparseIndexWriter::new(initial_block.id);
        let root_writer = RootWriter::new(
            CURRENT_VERSION,
            id,
            sparse_index,
            prefix_path.to_string(),
            max_block_size_bytes,
        );

        Self {
            block_manager,
            root_manager,
            root: root_writer,
            id,
            inner: Arc::new(Mutex::new(Inner {
                current_block_delta: Some((initial_block, None)),
                completed_block_deltas: Vec::new(),
                remaining_block_stack: VecDeque::new(),
            })),
        }
    }

    pub(super) fn from_root(
        id: Uuid,
        block_manager: BlockManager,
        root_manager: RootManager,
        new_root: RootWriter,
    ) -> Self {
        let remaining_block_stack = {
            let root_forward = &new_root.sparse_index.data.lock().forward;

            root_forward
                .iter()
                .zip_longest(root_forward.iter().skip(1))
                .map(|zipped| match zipped {
                    itertools::EitherOrBoth::Both(block, next_block) => match next_block.0 {
                        SparseIndexDelimiter::Start => {
                            // The next block should never be a start block
                            panic!("Invariant violated: unexpected start delimiter.")
                        }
                        SparseIndexDelimiter::Key(end_key) => (*block.1, Some(end_key.clone())),
                    },
                    itertools::EitherOrBoth::Left(block) => (*block.1, None),
                    itertools::EitherOrBoth::Right(_) => {
                        unreachable!()
                    }
                })
                .collect::<VecDeque<_>>()
        };

        Self {
            block_manager,
            root_manager,
            root: new_root,
            id,
            inner: Arc::new(Mutex::new(Inner {
                current_block_delta: None,
                completed_block_deltas: Vec::new(),
                remaining_block_stack,
            })),
        }
    }

    pub(crate) async fn commit<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        mut self,
    ) -> Result<ArrowBlockfileFlusher, Box<dyn ChromaError>> {
        let mut inner = std::mem::take(&mut *self.inner.lock().await);

        Self::complete_current_delta::<K, V>(&mut inner);

        let mut split_block_deltas = Vec::new();
        for delta in inner.completed_block_deltas.drain(..) {
            // Don't we split on-mutation (.set() calls)?
            // Yes, but that is only a performance optimization. For correctness, we must also split on commit. Why?
            //
            // We need to defer copying old forked data until:
            // - we receive a set()/delete() for a later key
            // - we are committing the delta (it will receive no further writes)
            //
            // Because of this constraint, we cannot always effectively split on-mutation if the writer is over a forked blockfile. Imagine this scenario:
            // 1. There is 1 existing block whose size == limit.
            // 2. We receive a .set() for a key before the existing block's start key.
            // 3. We turn the existing block into a delta and add the new KV pair.
            // 4. At this point, the total size of the delta (materialized + pending forked data) is above the limit.
            // 5. We would like to split our delta into two immediately after the newly-added key. However, this means that the right half of the split is empty (there is no materialized data), which violates a fundamental assumption made by our blockstore code. And we cannot materialize only the first key in the right half from the pending forked data because that would violate the above constraint.
            //
            // Thus, we handle splitting in two places:
            //
            // 1. Split deltas in half on-mutation if the materialized size is over the limit (just a performance optimization).
            // 2. During the commit phase, after all deltas have been fully materialized, split if necessary.
            //
            // An alternative would be to create a fresh delta that does not fork from an existing block if we receive a .set() for a key that is not contained in any existing block key range, however this complicates writing logic and potentially increases fragmentation.
            if delta.get_size::<K, V>() > self.root.max_block_size_bytes {
                let split_blocks = delta.split::<K, V>(self.root.max_block_size_bytes);
                for (split_key, split_delta) in split_blocks {
                    self.root
                        .sparse_index
                        .add_block(split_key, split_delta.id)
                        .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
                    split_block_deltas.push(split_delta);
                }
            }
            split_block_deltas.push(delta);
        }

        let mut blocks = Vec::new();
        let mut new_block_ids = HashSet::new();
        for delta in split_block_deltas.drain(..) {
            new_block_ids.insert(delta.id());
            let mut removed = false;
            // Skip empty blocks. Also, remove from sparse index.
            if delta.len() == 0 {
                tracing::info!("Delta with id {:?} is empty", delta.id());
                removed = self.root.sparse_index.remove_block(&delta.id());
            }
            if !removed {
                self.root
                    .sparse_index
                    .set_count(delta.id(), delta.len() as u32)
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
                let block = self.block_manager.commit::<K, V>(delta).await;
                blocks.push(block);
            }
        }

        apply_migrations_to_blockfile(&mut self.root, &self.block_manager, &new_block_ids)
            .await
            .map_err(|e| {
                Box::new(ArrowBlockfileError::MigrationError(e)) as Box<dyn ChromaError>
            })?;

        let count = self
            .root
            .sparse_index
            .data
            .lock()
            .counts
            .values()
            .map(|&x| x as u64)
            .sum::<u64>();

        let flusher = ArrowBlockfileFlusher::new(
            self.block_manager,
            self.root_manager,
            blocks,
            self.root,
            self.id,
            count,
        );

        Ok(flusher)
    }

    fn complete_current_delta<K: ArrowWriteableKey, V: ArrowWriteableValue>(inner: &mut Inner) {
        if let Some((mut delta, _)) = inner.current_block_delta.take() {
            delta.copy_to_end::<K, V>();
            inner.completed_block_deltas.push(delta);
        }
    }

    async fn swap_current_delta<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        inner: &mut Inner,
        new_delta_block_id: &Uuid,
        new_delta_end_key: Option<CompositeKey>,
    ) -> Result<(), Box<dyn ChromaError>> {
        Self::complete_current_delta::<K, V>(inner);

        let new_delta = self
            .block_manager
            .fork::<K, V, OrderedBlockDelta>(new_delta_block_id, &self.root.prefix_path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;

        self.root
            .sparse_index
            .replace_block(*new_delta_block_id, new_delta.id);

        inner.current_block_delta = Some((new_delta, new_delta_end_key));

        Ok(())
    }

    async fn advance_current_delta_and_get_inner<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: &K,
    ) -> Result<MutexGuard<'_, Inner>, Box<dyn ChromaError>> {
        let mut inner = self.inner.lock().await;

        if let Some((_, end_key)) = inner.current_block_delta.as_ref() {
            if let Some(end_key) = end_key {
                if prefix < end_key.prefix.as_str()
                    || (prefix == end_key.prefix.as_str() && key.clone().into() < end_key.key)
                // todo: avoid cloning key
                {
                    // Provided prefix/key pair is less than the current delta's end key, so there's nothing to do
                    return Ok(inner);
                }
            } else {
                // Open-ended delta
                return Ok(inner);
            }
        }

        // Find the next block to rewrite
        loop {
            match inner.remaining_block_stack.pop_front() {
                Some((block_id, Some(end_key))) => {
                    if prefix < end_key.prefix.as_str()
                        || (prefix == end_key.prefix.as_str() && key.clone().into() < end_key.key)
                    // todo: avoid cloning key
                    {
                        self.swap_current_delta::<K, V>(&mut inner, &block_id, Some(end_key))
                            .await?;
                        break;
                    }
                }
                Some((block_id, None)) => {
                    self.swap_current_delta::<K, V>(&mut inner, &block_id, None)
                        .await?;
                    break;
                }
                None => {
                    panic!("Invariant violated: no blocks left in the stack.")
                }
            }
        }

        Ok(inner)
    }

    pub(crate) async fn set<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) -> Result<(), Box<dyn ChromaError>> {
        let inner = &mut self
            .advance_current_delta_and_get_inner::<K, V>(prefix, &key)
            .await?;
        let current_materialized_delta_size = {
            let delta = &mut inner.current_block_delta.as_mut().expect("Invariant violation: advance_current_delta_and_get_inner() did not populate current delta").0;
            delta.add(prefix, key, value);
            delta.get_size::<K, V>()
        };

        let max_block_size_bytes = self.root.max_block_size_bytes;
        if current_materialized_delta_size > max_block_size_bytes {
            let (mut current_delta, current_end_key) = inner
                .current_block_delta
                .take()
                .expect("We already checked above that there is a current delta");
            let new_delta = current_delta.split_off_half::<K, V>();

            self.root
                .sparse_index
                .add_block(
                    new_delta
                        .min_key()
                        .expect("the split delta should not be empty"),
                    new_delta.id,
                )
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;

            inner.completed_block_deltas.push(current_delta);
            inner.current_block_delta = Some((new_delta, current_end_key));
        }

        Ok(())
    }

    pub(crate) async fn delete<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<(), Box<dyn ChromaError>> {
        let inner = &mut self
            .advance_current_delta_and_get_inner::<K, V>(prefix, &key)
            .await?;
        let delta = &mut inner.current_block_delta.as_mut().expect("Invariant violation: advance_current_delta_and_get_inner() did not populate current delta").0;
        delta.skip::<K, V>(prefix, key);
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use crate::arrow::block::delta::types::Delta;
    use crate::arrow::block::delta::OrderedBlockDelta;
    use crate::arrow::block::Block;
    use crate::arrow::config::BlockManagerConfig;
    use crate::arrow::ordered_blockfile_writer::{ArrowOrderedBlockfileWriter, Inner};
    use crate::arrow::provider::{BlockManager, BlockfileReaderOptions, RootManager};
    use crate::arrow::root::{RootWriter, Version};
    use crate::arrow::sparse_index::SparseIndexWriter;
    use crate::key::CompositeKey;
    use crate::{
        arrow::config::TEST_MAX_BLOCK_SIZE_BYTES, arrow::provider::ArrowBlockfileProvider,
    };
    use crate::{BlockfileReader, BlockfileWriter, BlockfileWriterOptions};
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use rand::seq::IteratorRandom;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_reader_count() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let id = writer.id();

        let prefix_1 = "key";
        let key1 = "aaaa";
        let value1 = vec![1, 2, 3];
        writer.set(prefix_1, key1, value1.clone()).await.unwrap();

        let prefix_2 = "key";
        let key2 = "zzzz";
        let value2 = vec![4, 5, 6];
        writer.set(prefix_2, key2, value2).await.unwrap();

        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        let count = reader.count().await;
        match count {
            Ok(c) => assert_eq!(2, c),
            Err(_) => panic!("Error getting count"),
        }
    }

    #[tokio::test]
    async fn test_writer_count() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");

        // Test no keys
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();

        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        assert_eq!(0_u64, flusher.count());
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        // Test 2 keys
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();

        let prefix_1 = "key";
        let key1 = "zzzz";
        let value1 = vec![1, 2, 3];
        writer.set(prefix_1, key1, value1.clone()).await.unwrap();

        let prefix_2 = "key";
        let key2 = "aaaa";
        let value2 = vec![4, 5, 6];
        writer.set(prefix_2, key2, value2).await.unwrap();

        let flusher1 = writer.commit::<&str, Vec<u32>>().await.unwrap();
        assert_eq!(2_u64, flusher1.count());

        // Test add keys after commit, before flush
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();

        let prefix_3 = "key";
        let key3 = "yyyy";
        let value3 = vec![7, 8, 9];
        writer.set(prefix_3, key3, value3.clone()).await.unwrap();

        let prefix_4 = "key";
        let key4 = "bbbb";
        let value4 = vec![10, 11, 12];
        writer.set(prefix_4, key4, value4).await.unwrap();

        let flusher2 = writer.commit::<&str, Vec<u32>>().await.unwrap();
        assert_eq!(2_u64, flusher2.count());

        flusher1.flush::<&str, Vec<u32>>().await.unwrap();
        flusher2.flush::<&str, Vec<u32>>().await.unwrap();

        // Test count after flush
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path))
            .await
            .unwrap();
        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        assert_eq!(0_u64, flusher.count());
    }

    #[tokio::test]
    async fn test_blockfile() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let id = writer.id();

        let prefix_1 = "key";
        let key1 = "aaaa";
        let value1 = vec![1, 2, 3];
        writer.set(prefix_1, key1, value1).await.unwrap();

        let prefix_2 = "key";
        let key2 = "zzzz";
        let value2 = vec![4, 5, 6];
        writer.set(prefix_2, key2, value2).await.unwrap();

        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        let value = reader.get(prefix_1, key1).await.unwrap().unwrap();
        assert_eq!(value, [1, 2, 3]);

        let value = reader.get(prefix_2, key2).await.unwrap().unwrap();
        assert_eq!(value, [4, 5, 6]);
    }

    #[tokio::test]
    async fn test_splitting() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let id_1 = writer.id();

        let n = 1200;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = vec![i];
            writer.set("key", key.as_str(), value).await.unwrap();
        }

        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id_1, prefix_path.clone());
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, [i]);
        }

        // Sparse index should have 3 blocks
        match &reader {
            crate::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.root.sparse_index.len(), 3);
                assert!(reader.root.sparse_index.is_valid());
            }
            _ => panic!("Unexpected reader type"),
        }

        // Add 5 new entries to the first block
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .fork(id_1)
                    .ordered_mutations(),
            )
            .await
            .unwrap();
        let id_2 = writer.id();
        for i in 0..5 {
            let key = format!("{:05}", i);
            let value = vec![i];
            writer.set("key", key.as_str(), value).await.unwrap();
        }

        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id_2, prefix_path.clone());
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();
        for i in 0..5 {
            let key = format!("{:05}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, [i]);
        }

        // Sparse index should still have 3 blocks
        match &reader {
            crate::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.root.sparse_index.len(), 3);
                assert!(reader.root.sparse_index.is_valid());
            }
            _ => panic!("Unexpected reader type"),
        }

        // Add 1200 more entries, causing splits
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .fork(id_2)
                    .ordered_mutations(),
            )
            .await
            .unwrap();
        let id_3 = writer.id();
        for i in n..n * 2 {
            let key = format!("{:04}", i);
            let value = vec![i];
            writer.set("key", key.as_str(), value).await.unwrap();
        }
        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id_3, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();
        for i in n..n * 2 {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, [i]);
        }

        // Sparse index should have 6 blocks
        match &reader {
            crate::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.root.sparse_index.len(), 6);
                assert!(reader.root.sparse_index.is_valid());
            }
            _ => panic!("Unexpected reader type"),
        }
    }

    #[tokio::test]
    async fn test_large_split_value() {
        // Tests the case where a value is larger than half the block size
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");

        let writer = blockfile_provider
            .write::<&str, String>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let id = writer.id();

        let val_1_small = "a";
        let val_2_large = "a".repeat(TEST_MAX_BLOCK_SIZE_BYTES / 2 + 1);

        writer
            .set("key", "1", val_1_small.to_string())
            .await
            .unwrap();
        writer
            .set("key", "2", val_2_large.to_string())
            .await
            .unwrap();
        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &str>(read_options)
            .await
            .unwrap();
        let val_1 = reader.get("key", "1").await.unwrap().unwrap();
        let val_2 = reader.get("key", "2").await.unwrap().unwrap();

        assert_eq!(val_1, val_1_small);
        assert_eq!(val_2, val_2_large);
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let writer = blockfile_provider
            .write::<&str, String>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = format!("{:04}", i);
            writer
                .set("key", key.as_str(), value.to_string())
                .await
                .unwrap();
        }
        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path.clone());
        let reader = blockfile_provider
            .read::<&str, &str>(read_options)
            .await
            .unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap();
            assert_eq!(value, Some(format!("{:04}", i).as_str()));
        }

        let writer = blockfile_provider
            .write::<&str, String>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .fork(id)
                    .ordered_mutations(),
            )
            .await
            .unwrap();
        let id = writer.id();

        // Delete some keys
        let mut rng = rand::thread_rng();
        let mut deleted_keys = (0..n).choose_multiple(&mut rng, n / 2);
        deleted_keys.sort();
        for i in &deleted_keys {
            let key = format!("{:04}", *i);
            writer
                .delete::<&str, String>("key", key.as_str())
                .await
                .unwrap();
        }
        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        // Check that the deleted keys are gone
        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &str>(read_options)
            .await
            .unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            if deleted_keys.contains(&i) {
                assert!(matches!(reader.get("key", &key).await, Ok(None)));
            } else {
                let value = reader.get("key", &key).await.unwrap();
                assert_eq!(value, Some(format!("{:04}", i).as_str()));
            }
        }
    }

    #[tokio::test]
    async fn test_first_block_removal() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let id_1 = writer.id();

        // Add the larger keys first then smaller.
        let n = 2400;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = vec![i];
            writer.set("key", key.as_str(), value).await.unwrap();
        }
        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();
        // Create another writer.
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .fork(id_1)
                    .ordered_mutations(),
            )
            .await
            .expect("BlockfileWriter fork unsuccessful");
        // Delete everything but the last 10 keys.
        let delete_end = n - 10;
        for i in 0..delete_end {
            let key = format!("{:04}", i);
            writer
                .delete::<&str, Vec<u32>>("key", key.as_str())
                .await
                .expect("Delete failed");
        }
        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        let id_2 = flusher.id();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id_2, prefix_path.clone());
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        for i in 0..delete_end {
            let key = format!("{:04}", i);
            assert!(!reader.contains("key", &key).await.unwrap());
        }

        for i in delete_end..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, [i]);
        }

        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .fork(id_2)
                    .ordered_mutations(),
            )
            .await
            .expect("BlockfileWriter fork unsuccessful");
        // Add everything back.
        for i in 0..delete_end {
            let key = format!("{:04}", i);
            let value = vec![i];
            writer
                .set::<&str, Vec<u32>>("key", key.as_str(), value)
                .await
                .expect("Delete failed");
        }
        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        let id_3 = flusher.id();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id_3, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, &[i]);
        }
    }

    #[tokio::test]
    async fn test_v1_to_v1_1_migration_all_new() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let root_manager = RootManager::new(storage.clone(), root_cache);
        let block_manager = BlockManager::new(
            storage.clone(),
            8 * 1024 * 1024,
            block_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );

        // Manually create a v1 blockfile with no counts
        let initial_block = block_manager.create::<&str, String, OrderedBlockDelta>();
        let sparse_index = SparseIndexWriter::new(initial_block.id);
        let file_id = Uuid::new_v4();
        let prefix_path = "";
        let max_block_size_bytes = 8 * 1024 * 1024; // 8 MB
        let root_writer = RootWriter::new(
            Version::V1,
            file_id,
            sparse_index,
            prefix_path.to_string(),
            max_block_size_bytes,
        );

        let writer = ArrowOrderedBlockfileWriter {
            block_manager,
            root_manager: root_manager.clone(),
            root: root_writer,
            id: Uuid::new_v4(),
            inner: Arc::new(Mutex::new(Inner {
                remaining_block_stack: VecDeque::new(),
                current_block_delta: Some((initial_block, None)),
                completed_block_deltas: Vec::new(),
            })),
        };

        let n = 2000;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = format!("{:04}", i);
            writer
                .set("key", key.as_str(), value.to_string())
                .await
                .unwrap();
        }

        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        // Get the RootReader and verify the counts
        let root_reader = root_manager
            .get::<&str>(&file_id, prefix_path, max_block_size_bytes)
            .await
            .unwrap()
            .unwrap();
        let count_in_index: u32 = root_reader
            .sparse_index
            .data
            .forward
            .iter()
            .map(|x| x.1.count)
            .sum();
        assert_eq!(count_in_index, n);
    }

    #[tokio::test]
    async fn test_v1_to_v1_1_migration_partially_new() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let root_manager = RootManager::new(storage.clone(), root_cache);
        let block_manager = BlockManager::new(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );

        // This test is rather fragile, but it is the best way to test the migration
        // without a lot of logic duplication. We will create a v1 blockfile with
        // 2 blocks manually, then we will create a v1.1 blockfile with 1 block and 1 block delta
        // and verify that the counts are correct after the migration.
        // The test has 4 main steps
        // 1 - Create a v1 blockfile with 2 blocks and no counts in the root
        // 2 - Create a v1.1 blockfile reader and ensure it loads the data correctly
        // 3 - Create a v1 writer with the v1.1 code and add a new key to the block, dirtying only one block
        // 4 - Flush the block and verify that the counts are correct in the root with a v1.1 reader
        // This will test the migration from v1 to v1.1 on both paths - deltas and old undirty blocks

        ////////////////////////// STEP 1 //////////////////////////

        // Create two blocks with some data, we will make this conceptually a v1 block
        let mut old_block_delta_1 = block_manager.create::<&str, String, OrderedBlockDelta>();
        old_block_delta_1.add("prefix", "a", "value_a".to_string());
        let mut old_block_delta_2 = block_manager.create::<&str, String, OrderedBlockDelta>();
        old_block_delta_2.add("prefix", "f", "value_b".to_string());
        let old_block_id_1 = old_block_delta_1.id;
        let old_block_id_2 = old_block_delta_2.id;
        let sparse_index = SparseIndexWriter::new(old_block_id_1);
        sparse_index
            .add_block(
                CompositeKey::new("prefix".to_string(), "f"),
                old_block_delta_2.id,
            )
            .unwrap();
        let first_write_id = Uuid::new_v4();
        let prefix_path = "";
        let max_block_size_bytes = 8 * 1024 * 1024; // 8 MB
        let old_root_writer = RootWriter::new(
            Version::V1,
            first_write_id,
            sparse_index,
            prefix_path.to_string(),
            max_block_size_bytes,
        );

        // Flush the blocks and the root
        let old_block_1_record_batch = old_block_delta_1.finish::<&str, String>(None);
        let old_block_1 = Block::from_record_batch(old_block_id_1, old_block_1_record_batch);
        let old_block_2_record_batch = old_block_delta_2.finish::<&str, String>(None);
        let old_block_2 = Block::from_record_batch(old_block_id_2, old_block_2_record_batch);
        block_manager
            .flush(&old_block_1, prefix_path)
            .await
            .unwrap();
        block_manager
            .flush(&old_block_2, prefix_path)
            .await
            .unwrap();
        root_manager.flush::<&str>(&old_root_writer).await.unwrap();

        // We now have a v1 blockfile with 2 blocks and no counts in the root

        ////////////////////////// STEP 2 //////////////////////////

        // Ensure that a v1.1 compatible reader on a v1 blockfile will work as expected

        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            root_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );

        let read_options = BlockfileReaderOptions::new(first_write_id, prefix_path.to_string());
        let reader = blockfile_provider
            .read::<&str, &str>(read_options)
            .await
            .unwrap();
        let reader = match reader {
            BlockfileReader::ArrowBlockfileReader(reader) => reader,
            _ => panic!("Unexpected reader type"),
        };
        assert_eq!(reader.get("prefix", "a").await.unwrap(), Some("value_a"));
        assert_eq!(reader.get("prefix", "f").await.unwrap(), Some("value_b"));
        assert_eq!(reader.count().await.unwrap(), 2);
        assert_eq!(reader.root.version, Version::V1);

        ////////////////////////// STEP 3 //////////////////////////

        // Test that a v1.1 writer can read a v1 blockfile and dirty a block
        // successfully hydrating counts for ALL blocks it needs to set counts for
        let writer = blockfile_provider
            .write::<&str, String>(
                BlockfileWriterOptions::new(prefix_path.to_string())
                    .fork(first_write_id)
                    .ordered_mutations(),
            )
            .await
            .unwrap();
        let second_write_id = writer.id();
        let writer = match writer {
            BlockfileWriter::ArrowOrderedBlockfileWriter(writer) => writer,
            _ => panic!("Unexpected writer type"),
        };
        assert_eq!(writer.root.version, Version::V1);
        assert_eq!(writer.root.sparse_index.len(), 2);
        assert_eq!(writer.root.sparse_index.data.lock().counts.len(), 2);
        // We don't expect the v1.1 writer to have any values for counts
        assert_eq!(
            writer
                .root
                .sparse_index
                .data
                .lock()
                .counts
                .values()
                .sum::<u32>(),
            0
        );

        // Add some new data, we only want to dirty one block so we write the key "b"
        writer
            .set("prefix", "b", "value".to_string())
            .await
            .unwrap();

        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        ////////////////////////// STEP 4 //////////////////////////

        // Verify that the counts were correctly migrated

        let read_options = BlockfileReaderOptions::new(second_write_id, prefix_path.to_string());
        let blockfile_reader = blockfile_provider
            .read::<&str, &str>(read_options)
            .await
            .unwrap();

        let reader = match blockfile_reader {
            BlockfileReader::ArrowBlockfileReader(reader) => reader,
            _ => panic!("Unexpected reader type"),
        };

        assert_eq!(reader.root.version, Version::V1_2);
        assert_eq!(reader.root.sparse_index.len(), 2);

        // Manually verify sparse index counts
        let count_in_index: u32 = reader
            .root
            .sparse_index
            .data
            .forward
            .iter()
            .map(|x| x.1.count)
            .sum();
        assert_eq!(count_in_index, 3);
        assert_eq!(reader.count().await.unwrap(), 3);
    }
}
