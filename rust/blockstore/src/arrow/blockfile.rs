use super::migrations::{apply_migrations_to_blockfile, MigrationError};
use super::provider::{GetError, RootManager};
use super::root::{RootReader, RootWriter, Version};
use super::{block::delta::UnorderedBlockDelta, provider::BlockManager};
use super::{
    block::Block,
    flusher::ArrowBlockfileFlusher,
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
};
use crate::arrow::root::CURRENT_VERSION;
use crate::arrow::sparse_index::SparseIndexWriter;
use crate::key::CompositeKey;
use crate::key::KeyWrapper;
use chroma_cache::AysncPartitionedMutex;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_storage::admissioncontrolleds3::StorageRequestPriority;
use futures::future::{join_all, try_join_all};
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::mem::transmute;
use std::ops::RangeBounds;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tracing::{Instrument, Span};
use uuid::Uuid;

#[derive(Clone)]
pub struct ArrowUnorderedBlockfileWriter {
    block_manager: BlockManager,
    root_manager: RootManager,
    block_deltas: Arc<Mutex<HashMap<Uuid, UnorderedBlockDelta>>>,
    root: RootWriter,
    id: Uuid,
    deltas_mutex: Arc<AysncPartitionedMutex<Uuid>>,
}
// TODO: method visibility should not be pub(crate)

#[derive(Error, Debug)]
pub enum ArrowBlockfileError {
    #[error("Block not found")]
    BlockNotFound,
    #[error("Could not fetch block")]
    BlockFetchError(#[from] GetError),
    #[error("Could not migrate blockfile to new version")]
    MigrationError(#[from] MigrationError),
}

impl ChromaError for ArrowBlockfileError {
    fn code(&self) -> ErrorCodes {
        match self {
            ArrowBlockfileError::BlockNotFound => ErrorCodes::Internal,
            ArrowBlockfileError::BlockFetchError(_) => ErrorCodes::Internal,
            ArrowBlockfileError::MigrationError(e) => e.code(),
        }
    }
}

impl ArrowUnorderedBlockfileWriter {
    pub(super) fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        id: Uuid,
        prefix_path: &str,
        block_manager: BlockManager,
        root_manager: RootManager,
        max_block_size_bytes: usize,
    ) -> Self {
        let initial_block = block_manager.create::<K, V, UnorderedBlockDelta>();
        let sparse_index = SparseIndexWriter::new(initial_block.id);
        let root_writer = RootWriter::new(
            CURRENT_VERSION,
            id,
            sparse_index,
            prefix_path.to_string(),
            max_block_size_bytes,
        );

        let block_deltas = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut block_deltas_map = block_deltas.lock();
            block_deltas_map.insert(initial_block.id, initial_block);
        }
        tracing::debug!("Constructed blockfile writer with id {:?}", id);
        Self {
            block_manager,
            root_manager,
            block_deltas,
            root: root_writer,
            id,
            deltas_mutex: Arc::new(AysncPartitionedMutex::new(())),
        }
    }

    pub(super) fn from_root(
        id: Uuid,
        block_manager: BlockManager,
        root_manager: RootManager,
        new_root: RootWriter,
    ) -> Self {
        tracing::debug!("Constructed blockfile writer from existing root {:?}", id);
        let block_deltas = Arc::new(Mutex::new(HashMap::new()));

        Self {
            block_manager,
            root_manager,
            block_deltas,
            root: new_root,
            id,
            deltas_mutex: Arc::new(AysncPartitionedMutex::new(())),
        }
    }

    pub(crate) async fn commit<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        mut self,
    ) -> Result<ArrowBlockfileFlusher, Box<dyn ChromaError>> {
        let mut blocks = Vec::new();
        let mut new_block_ids = HashSet::new();
        let mut deltas_to_commit = Vec::new();
        for (_, delta) in self.block_deltas.lock().drain() {
            new_block_ids.insert(delta.id);
            let mut removed = false;
            // Skip empty blocks. Also, remove from sparse index.
            if delta.len() == 0 {
                tracing::info!("Delta with id {:?} is empty", delta.id);
                removed = self.root.sparse_index.remove_block(&delta.id);
            }
            if !removed {
                self.root
                    .sparse_index
                    .set_count(delta.id, delta.len() as u32)
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
                deltas_to_commit.push(delta);
            }
        }

        for delta in deltas_to_commit {
            let block = self.block_manager.commit::<K, V>(delta).await;
            blocks.push(block);
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

    // TODO: value must be smaller than the block size except for position lists, which are a special case
    // where we split the value across multiple blocks
    pub(crate) async fn set<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) -> Result<(), Box<dyn ChromaError>> {
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let (_guard, target_block_id) = loop {
            // Get the target block id for the key
            let target_block_id = self.root.sparse_index.get_target_block_id(&search_key);

            // Lock the delta for the target block id
            let delta_guard = self.deltas_mutex.lock(&target_block_id).await;

            // Recheck if the target block id is still the same. Otherwise, someone concurrently
            // modified the root and we need to restart.
            let new_target_block_id = self.root.sparse_index.get_target_block_id(&search_key);
            // Someone concurrently converted the block to delta and/or split it so restart all over.
            if new_target_block_id != target_block_id {
                continue;
            }
            break (delta_guard, target_block_id);
        };

        let delta = {
            let deltas = self.block_deltas.lock();
            deltas.get(&target_block_id).cloned()
        };

        if let Some(delta) = delta {
            // Add the key, value pair to delta.
            // Then check if its over size and split as needed
            delta.add(prefix, key, value);

            if delta.get_size::<K, V>() > self.root.max_block_size_bytes {
                let new_blocks = delta.split::<K, V>(self.root.max_block_size_bytes);
                // First add to deltas before making it visible through the sparse index.
                // This prevents dangling references.
                let blocks_to_add = {
                    let mut blocks_to_add = Vec::with_capacity(new_blocks.len());
                    let mut deltas = self.block_deltas.lock();
                    for (split_key, new_delta) in new_blocks {
                        blocks_to_add.push((split_key, new_delta.id));
                        deltas.insert(new_delta.id, new_delta);
                    }
                    blocks_to_add
                };
                // Update the sparse index atomically in one batch.
                self.root
                    .sparse_index
                    .apply_updates(vec![], blocks_to_add)
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
            }
        } else {
            // Fetch the block and convert to delta.
            let block = match self
                .block_manager
                .get(
                    &self.root.prefix_path,
                    &target_block_id,
                    StorageRequestPriority::P0,
                )
                .await
            {
                Ok(Some(block)) => block,
                Ok(None) => {
                    return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                }
                Err(e) => {
                    return Err(Box::new(e));
                }
            };
            let new_delta = match self
                .block_manager
                .fork::<K, V, UnorderedBlockDelta>(&block.id, &self.root.prefix_path)
                .await
            {
                Ok(delta) => delta,
                Err(e) => {
                    return Err(Box::new(e));
                }
            };

            // Add the key, value pair to delta.
            // Then check if its over size and split as needed
            new_delta.add(prefix, key, value);

            if new_delta.get_size::<K, V>() > self.root.max_block_size_bytes {
                // First add to deltas before making it visible through the sparse index.
                // This prevents dangling references.
                let new_blocks = new_delta.split::<K, V>(self.root.max_block_size_bytes);
                let (blocks_to_add, blocks_to_replace) = {
                    let mut blocks_to_add = Vec::with_capacity(new_blocks.len());
                    let mut deltas = self.block_deltas.lock();
                    for (split_key, new_delta) in new_blocks {
                        blocks_to_add.push((split_key, new_delta.id));
                        deltas.insert(new_delta.id, new_delta);
                    }
                    deltas.insert(new_delta.id, new_delta.clone());
                    (blocks_to_add, vec![(target_block_id, new_delta.id)])
                };
                // Update the sparse index atomically in one batch.
                self.root
                    .sparse_index
                    .apply_updates(blocks_to_replace, blocks_to_add)
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
            } else {
                let mut deltas = self.block_deltas.lock();
                deltas.insert(new_delta.id, new_delta.clone());
                self.root
                    .sparse_index
                    .replace_block(target_block_id, new_delta.id);
            }
        }

        Ok(())
    }

    pub async fn get_owned<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<Option<V::PreparedValue>, Box<dyn ChromaError>> {
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let (_guard, target_block_id) = loop {
            // Get the target block id for the key
            let target_block_id = self.root.sparse_index.get_target_block_id(&search_key);

            // Lock the delta for the target block id
            let delta_guard = self.deltas_mutex.lock(&target_block_id).await;

            // Recheck if the target block id is still the same. Otherwise, someone concurrently
            // modified the root and we need to restart.
            let new_target_block_id = self.root.sparse_index.get_target_block_id(&search_key);
            // Someone concurrently converted the block to delta and/or split it so restart all over.
            if new_target_block_id != target_block_id {
                continue;
            }
            break (delta_guard, target_block_id);
        };

        let delta = {
            let deltas = self.block_deltas.lock();
            deltas.get(&target_block_id).cloned()
        };

        Ok(match delta {
            None => {
                let block = match self
                    .block_manager
                    .get(
                        &self.root.prefix_path,
                        &target_block_id,
                        StorageRequestPriority::P0,
                    )
                    .await
                {
                    Ok(Some(block)) => block,
                    Ok(None) => {
                        return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                    }
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };
                let new_delta = match self
                    .block_manager
                    .fork::<K, V, UnorderedBlockDelta>(&block.id, &self.root.prefix_path)
                    .await
                {
                    Ok(delta) => delta,
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };
                // Read the value before making the delta visible through the sparse index.
                let value = V::get_owned_value_from_delta(prefix, key.into(), &new_delta);
                // Insert to delta first and then make it visible through the sparse index to
                // prevent dangling references.
                let mut deltas = self.block_deltas.lock();
                deltas.insert(new_delta.id, new_delta.clone());
                self.root
                    .sparse_index
                    .replace_block(target_block_id, new_delta.id);
                value
            }
            Some(delta) => V::get_owned_value_from_delta(prefix, key.into(), &delta),
        })
    }

    pub(crate) async fn delete<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<(), Box<dyn ChromaError>> {
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let (_guard, target_block_id) = loop {
            // Get the target block id for the key
            let target_block_id = self.root.sparse_index.get_target_block_id(&search_key);

            // Lock the delta for the target block id
            let delta_guard = self.deltas_mutex.lock(&target_block_id).await;

            // Recheck if the target block id is still the same. Otherwise, someone concurrently
            // modified the root and we need to restart.
            let new_target_block_id = self.root.sparse_index.get_target_block_id(&search_key);
            // Someone concurrently converted the block to delta and/or split it so restart all over.
            if new_target_block_id != target_block_id {
                continue;
            }
            break (delta_guard, target_block_id);
        };

        let delta = {
            let deltas = self.block_deltas.lock();
            deltas.get(&target_block_id).cloned()
        };

        match delta {
            None => {
                let block = match self
                    .block_manager
                    .get(
                        &self.root.prefix_path,
                        &target_block_id,
                        StorageRequestPriority::P0,
                    )
                    .await
                {
                    Ok(Some(block)) => block,
                    Ok(None) => {
                        return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                    }
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };
                let new_delta = match self
                    .block_manager
                    .fork::<K, V, UnorderedBlockDelta>(&block.id, &self.root.prefix_path)
                    .await
                {
                    Ok(delta) => delta,
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };
                // Delete the key before making the delta visible through the sparse index.
                new_delta.delete::<K, V>(prefix, key);
                // Insert to delta first and then make it visible through the sparse index to
                // prevent dangling references.
                let mut deltas = self.block_deltas.lock();
                deltas.insert(new_delta.id, new_delta.clone());
                self.root
                    .sparse_index
                    .replace_block(target_block_id, new_delta.id);
            }
            Some(delta) => {
                delta.delete::<K, V>(prefix, key);
            }
        };
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}

#[derive(Clone)]
pub struct ArrowBlockfileReader<
    'me,
    K: ArrowReadableKey<'me> + Into<KeyWrapper>,
    V: ArrowReadableValue<'me>,
> {
    block_manager: BlockManager,
    pub(super) root: RootReader,
    loaded_blocks: Arc<RwLock<HashMap<Uuid, Box<Block>>>>,
    marker: std::marker::PhantomData<(K, V, &'me ())>,
}

impl<'me, K: ArrowReadableKey<'me> + Into<KeyWrapper>, V: ArrowReadableValue<'me>>
    ArrowBlockfileReader<'me, K, V>
{
    pub(super) fn new(block_manager: BlockManager, root: RootReader) -> Self {
        Self {
            block_manager,
            root,
            loaded_blocks: Arc::new(RwLock::new(HashMap::new())),
            marker: std::marker::PhantomData,
        }
    }

    pub(super) async fn get_block(
        &self,
        block_id: Uuid,
        priority: StorageRequestPriority,
    ) -> Result<Option<&Block>, GetError> {
        // NOTE(rescrv):  This will complain with clippy, but we don't want to hold a reference to
        // the loaded_blocks map across a call to the block manager.
        #[allow(clippy::map_entry)]
        if !self.loaded_blocks.read().contains_key(&block_id) {
            let block = match self
                .block_manager
                .get(&self.root.prefix_path, &block_id, priority)
                .await
            {
                Ok(Some(block)) => block,
                Ok(None) => {
                    return Ok(None);
                }
                Err(e) => {
                    return Err(e);
                }
            };
            // Don't reinsert if someone else has already inserted it.
            // All existing references to the block would become invalid
            // causing a NPE.
            let mut write_guard = self.loaded_blocks.write();
            if let Some(block) = write_guard.get(&block_id) {
                return Ok(Some(unsafe { transmute::<&Block, &Block>(&**block) }));
            }
            write_guard.insert(block_id, Box::new(block));
        }

        if let Some(block) = self.loaded_blocks.read().get(&block_id) {
            // https://github.com/mitsuhiko/memo-map/blob/a5db43853b2561145d7778dc2a5bd4b861fbfd75/src/lib.rs#L163
            // This is safe because we only ever insert Box<Block> into the HashMap
            // We never remove the Box<Block> from the HashMap, so the reference is always valid
            // We never mutate the Box<Block> after inserting it into the HashMap
            // We never share the Box<Block> with other threads - readers are single-threaded
            // We never drop the Box<Block> while the HashMap is still alive
            // We never drop the Box<Block> while the reference is still alive
            // We never drop the HashMap while the reference is still alive
            // We never drop the HashMap while the Box<Block> is still alive
            return Ok(Some(unsafe { transmute::<&Block, &Block>(&**block) }));
        }
        Ok(None)
    }

    /// Load all required blocks into the underlying block manager so that
    /// they are available for subsequent reads.
    /// This is a no-op if the block is already cached.
    /// # Parameters
    /// - `block_ids`: A list of block ids to load.
    /// # Returns
    /// - `()`: Returns nothing.
    async fn load_blocks(&self, block_ids: &[Uuid]) {
        // TODO: These need to be separate tasks enqueued onto dispatcher.
        let mut futures = Vec::new();
        for block_id in block_ids {
            // Don't prefetch if already cached.
            // We do not dispatch if block is present in the block manager's cache
            // but not present in the reader's cache (i.e. loaded_blocks). The
            // next read for this block using this reader instance will populate it.
            if !self.block_manager.cached(block_id).await
                && !self.loaded_blocks.read().contains_key(block_id)
            {
                futures.push(self.get_block(*block_id, StorageRequestPriority::P1));
            }
        }
        join_all(futures).await;
    }

    pub(crate) async fn load_blocks_for_keys(&self, keys: impl IntoIterator<Item = (String, K)>) {
        let composite_keys = keys
            .into_iter()
            .map(|(prefix, key)| CompositeKey::new(prefix, key))
            .collect::<Vec<_>>();

        let target_block_ids = self
            .root
            .sparse_index
            .get_all_target_block_ids(composite_keys);
        self.load_blocks(&target_block_ids).await;
    }

    pub(crate) async fn load_blocks_for_prefixes(&self, prefixes: impl IntoIterator<Item = &str>) {
        let prefix_vec = prefixes.into_iter().collect();
        let target_block_ids = self
            .root
            .sparse_index
            .get_block_ids_for_prefixes(prefix_vec);
        self.load_blocks(&target_block_ids).await;
    }

    pub(crate) async fn get(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<Option<V>, Box<dyn ChromaError>> {
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let target_block_id = self.root.sparse_index.get_target_block_id(&search_key);
        let block = self
            .get_block(target_block_id, StorageRequestPriority::P0)
            .await;
        match block {
            Ok(Some(block)) => Ok(block.get(prefix, key.clone())),
            Ok(None) => {
                tracing::error!("Block with id {:?} not found", target_block_id);
                Ok(None)
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    /// Get all key-value pairs for a specific prefix
    /// Optimized for retrieving all entries under a single prefix
    pub(crate) async fn get_prefix(
        &'me self,
        prefix: &str,
    ) -> Result<impl Iterator<Item = (K, V)>, Box<dyn ChromaError>> {
        // Get all block IDs that might contain this prefix
        let block_ids = self.root.sparse_index.get_block_ids_range(prefix..=prefix);

        if block_ids.is_empty() {
            return Ok(Vec::new().into_iter().flatten());
        }

        // Fetch blocks AND extract prefix data in parallel
        let block_futures = block_ids.into_iter().map(|block_id| {
            async move {
                match self.get_block(block_id, StorageRequestPriority::P0).await {
                    Ok(Some(block)) => {
                        // Extract prefix data while we're in the async context
                        let prefix_data = block.get_prefix::<K, V>(prefix);
                        Ok(prefix_data)
                    }
                    Ok(None) => {
                        Err(Box::new(ArrowBlockfileError::BlockNotFound) as Box<dyn ChromaError>)
                    }
                    Err(e) => Err(Box::new(e) as Box<dyn ChromaError>),
                }
            }
            .instrument(Span::current())
        });

        let block_iters = try_join_all(block_futures).await?;

        Ok(block_iters.into_iter().flatten())
    }

    // Returns all Arrow records in the specified range.
    pub(crate) fn get_range_stream<'prefix, PrefixRange, KeyRange>(
        &'me self,
        prefix_range: PrefixRange,
        key_range: KeyRange,
    ) -> impl Stream<Item = Result<(&'me str, K, V), Box<dyn ChromaError>>> + Send + 'me
    where
        PrefixRange: RangeBounds<&'prefix str> + Clone + Send + 'me,
        KeyRange: RangeBounds<K> + Clone + Send + 'me,
        K: Sync,
        V: Sync,
    {
        futures::stream::iter(
            self.root
                .sparse_index
                .get_block_ids_range(prefix_range.clone())
                .into_iter()
                .map(Ok),
        )
        .try_filter_map(move |block_id| async move {
            match self.get_block(block_id, StorageRequestPriority::P0).await {
                Ok(Some(block)) => Ok(Some(block)),
                Ok(None) => Err(Box::new(ArrowBlockfileError::BlockNotFound)),
                Err(e) => Err(Box::new(ArrowBlockfileError::BlockFetchError(e))),
            }
        })
        .map(move |block| match block {
            Ok(block) => futures::stream::iter(
                block
                    .get_range::<K, V, _, _>(prefix_range.clone(), key_range.clone())
                    .map(Ok),
            )
            .boxed(),
            Err(e) => futures::stream::once(async { Err(e as Box<dyn ChromaError>) }).boxed(),
        })
        .flatten()
    }

    pub async fn get_range<'prefix, PrefixRange, KeyRange>(
        &'me self,
        prefix_range: PrefixRange,
        key_range: KeyRange,
    ) -> Result<impl Iterator<Item = (&'me str, K, V)> + 'me, Box<dyn ChromaError>>
    where
        PrefixRange: RangeBounds<&'prefix str> + Clone + 'me,
        KeyRange: RangeBounds<K> + Clone + 'me,
    {
        let block_ids = self
            .root
            .sparse_index
            .get_block_ids_range(prefix_range.clone());

        let block_futures = block_ids.into_iter().map(|block_id| {
            async move {
                match self.get_block(block_id, StorageRequestPriority::P0).await {
                    Ok(Some(block)) => Ok(block),
                    Ok(None) => {
                        Err(Box::new(ArrowBlockfileError::BlockNotFound) as Box<dyn ChromaError>)
                    }
                    Err(e) => Err(Box::new(e) as Box<dyn ChromaError>),
                }
            }
            .instrument(Span::current())
        });

        let blocks = try_join_all(block_futures).await?;
        Ok(blocks
            .into_iter()
            .flat_map(move |block| block.get_range(prefix_range.clone(), key_range.clone())))
    }

    pub(crate) async fn contains(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<bool, Box<dyn ChromaError>> {
        let search_key = CompositeKey::new(prefix.to_string(), key.clone());
        let target_block_id = self.root.sparse_index.get_target_block_id(&search_key);
        let block = match self
            .get_block(target_block_id, StorageRequestPriority::P0)
            .await
        {
            Ok(Some(block)) => block,
            Ok(None) => {
                return Ok(false);
            }
            Err(e) => {
                return Err(Box::new(e));
            }
        };
        match block.get::<K, V>(prefix, key) {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    // Count the total number of records.
    pub(crate) async fn count(&self) -> Result<usize, Box<dyn ChromaError>> {
        if self.root.version >= Version::V1_1 {
            // If the version is >=V1_1, we can use the count in the sparse index.
            let result = self
                .root
                .sparse_index
                .data
                .forward
                .iter()
                .map(|x| x.1.count)
                .sum::<u32>() as usize;
            Ok(result)
        } else {
            let mut block_ids: Vec<Uuid> = vec![];
            let curr_iter = self.root.sparse_index.data.forward.iter();
            for (_, block_id) in curr_iter {
                block_ids.push(block_id.id);
            }
            // Preload all blocks in parallel using the load_blocks helper
            self.load_blocks(&block_ids).await;
            let mut result: usize = 0;
            for block_id in block_ids {
                let block = match self.get_block(block_id, StorageRequestPriority::P0).await {
                    Ok(Some(block)) => block,
                    Ok(None) => {
                        return Err(Box::new(ArrowBlockfileError::BlockNotFound));
                    }
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };
                result += block.len();
            }
            Ok(result)
        }
    }

    pub(crate) fn id(&self) -> Uuid {
        self.root.id
    }

    /// Returns the number of elements strictly less than the given prefix-key pair in the blockfile
    /// In other words, the rank is the position where the given prefix-key pair can be inserted while maintaining the order of the blockfile
    pub(crate) async fn rank(
        &'me self,
        prefix: &'me str,
        key: K,
    ) -> Result<usize, Box<dyn ChromaError>> {
        let mut rank = 0;

        let last_block_id = self.root.sparse_index.get_target_block_id(&CompositeKey {
            prefix: prefix.to_string(),
            key: key.clone().into(),
        });
        let block_ids = self
            .root
            .sparse_index
            .get_block_ids_range(..=prefix)
            .into_iter()
            .take_while(|id| id != &last_block_id)
            .collect::<Vec<_>>();

        if self.root.version >= Version::V1_1 {
            rank += self
                .root
                .sparse_index
                .data
                .forward
                .values()
                .take(block_ids.len())
                .map(|meta| meta.count)
                .sum::<u32>() as usize;
        } else {
            self.load_blocks(&block_ids).await;
            for block_id in block_ids.iter().take(block_ids.len() - 1) {
                let block = self
                    .get_block(*block_id, StorageRequestPriority::P0)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?
                    .ok_or(Box::new(ArrowBlockfileError::BlockNotFound) as Box<dyn ChromaError>)?;
                rank += block.len();
            }
        }

        // The block that may contain the prefix-key pair
        let last_block = self
            .get_block(last_block_id, StorageRequestPriority::P0)
            .await
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?
            .ok_or(Box::new(ArrowBlockfileError::BlockNotFound) as Box<dyn ChromaError>)?;
        rank += last_block.binary_search_prefix_key(prefix, &key);

        Ok(rank)
    }

    /// Check if the blockfile is valid.
    /// Validates that the sparse index is valid and that no block exceeds the max block size.
    pub async fn is_valid(&self) -> bool {
        if !self.root.sparse_index.is_valid() {
            return false;
        }

        for (_, block_id) in self.root.sparse_index.data.forward.iter() {
            match self
                .get_block(block_id.id, StorageRequestPriority::P0)
                .await
            {
                Ok(Some(block)) => {
                    if block.get_size() > self.root.max_block_size_bytes {
                        return false;
                    }
                }
                Ok(None) => {
                    return false;
                }
                Err(_) => {
                    return false;
                }
            };
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::block::delta::types::Delta;
    use crate::arrow::block::delta::UnorderedBlockDelta;
    use crate::arrow::block::Block;
    use crate::arrow::blockfile::ArrowUnorderedBlockfileWriter;
    use crate::arrow::config::BlockManagerConfig;
    use crate::arrow::provider::{BlockManager, BlockfileReaderOptions, RootManager};
    use crate::arrow::root::{RootWriter, Version};
    use crate::arrow::sparse_index::SparseIndexWriter;
    use crate::key::CompositeKey;
    use crate::{
        arrow::config::TEST_MAX_BLOCK_SIZE_BYTES, arrow::provider::ArrowBlockfileProvider,
    };
    use crate::{BlockfileReader, BlockfileWriter, BlockfileWriterOptions};
    use chroma_cache::{new_cache_for_test, AysncPartitionedMutex};
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{CollectionUuid, DataRecord, DatabaseUuid, MetadataValue, SegmentUuid};
    use futures::{StreamExt, TryStreamExt};
    use parking_lot::Mutex;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use rand::seq::IteratorRandom;
    use std::collections::HashMap;
    use std::ops::Bound;
    use std::sync::Arc;
    use tokio::runtime::Runtime;
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
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id = writer.id();

        let prefix_1 = "key";
        let key1 = "zzzz";
        let value1 = vec![1, 2, 3];
        writer.set(prefix_1, key1, value1.clone()).await.unwrap();

        let prefix_2 = "key";
        let key2 = "aaaa";
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
    async fn test_new_prefix() {
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
        let tenant = "test_tenant";
        let db_id = DatabaseUuid::new();
        let coll_id = CollectionUuid::new();
        let segment_id = SegmentUuid::new();
        let prefix_path = format!(
            "tenant/{}/database/{}/collection/{}/segment/{}",
            tenant, db_id, coll_id, segment_id
        );
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id = writer.id();

        let prefix_1 = "key";
        let key1 = "zzzz";
        let value1 = vec![1, 2, 3];
        writer.set(prefix_1, key1, value1.clone()).await.unwrap();

        let prefix_2 = "key";
        let key2 = "aaaa";
        let value2 = vec![4, 5, 6];
        writer.set(prefix_2, key2, value2.clone()).await.unwrap();

        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path.clone());
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        let count = reader.count().await;
        match count {
            Ok(c) => assert_eq!(2, c),
            Err(_) => panic!("Error getting count"),
        }
        let value = reader
            .get(prefix_1, key1)
            .await
            .expect("Failed to get value for key1")
            .expect("Key1 not found");
        assert_eq!(value, value1);
        let value = reader
            .get(prefix_2, key2)
            .await
            .expect("Failed to get value for key2")
            .expect("Key2 not found");
        assert_eq!(value, value2);

        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()).fork(id))
            .await
            .unwrap();
        let id = writer.id();

        let prefix_3 = "key";
        let key3 = "bbbb";
        let value3 = vec![7, 8, 9];
        writer.set(prefix_3, key3, value3.clone()).await.unwrap();

        let prefix_4 = "key";
        let key4 = "cccc";
        let value4 = vec![10, 11, 12];
        writer.set(prefix_4, key4, value4.clone()).await.unwrap();

        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        let count = reader.count().await;
        match count {
            Ok(c) => assert_eq!(4, c),
            Err(_) => panic!("Error getting count"),
        }
        let value = reader
            .get(prefix_1, key1)
            .await
            .expect("Failed to get value for key1")
            .expect("Key1 not found");
        assert_eq!(value, value1);
        let value = reader
            .get(prefix_2, key2)
            .await
            .expect("Failed to get value for key2")
            .expect("Key2 not found");
        assert_eq!(value, value2);
        let value = reader
            .get(prefix_3, key3)
            .await
            .expect("Failed to get value for key3")
            .expect("Key3 not found");
        assert_eq!(value, value3);
        let value = reader
            .get(prefix_4, key4)
            .await
            .expect("Failed to get value for key4")
            .expect("Key4 not found");
        assert_eq!(value, value4);
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

    fn test_prefix(num_keys: u32, prefix_for_query: u32) {
        Runtime::new().unwrap().block_on(async {
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
                .write::<&str, u32>(BlockfileWriterOptions::new(prefix_path.clone()))
                .await
                .unwrap();
            let id = writer.id();

            for j in 1..=5 {
                let prefix = format!("{}/{}", "prefix", j);
                for i in 1..=num_keys {
                    let key = format!("{}/{}", "key", i);
                    writer.set(prefix.as_str(), key.as_str(), i).await.unwrap();
                }
            }
            // commit.
            let flusher = writer.commit::<&str, u32>().await.unwrap();
            flusher.flush::<&str, u32>().await.unwrap();

            let read_options = BlockfileReaderOptions::new(id, prefix_path);
            let reader = blockfile_provider
                .read::<&str, u32>(read_options)
                .await
                .unwrap();
            let prefix_query = format!("{}/{}", "prefix", prefix_for_query);
            println!("Query {}, num_keys {}", prefix_query, num_keys);
            let range_iter =
                reader.get_range_stream(prefix_query.as_str()..=prefix_query.as_str(), ..);
            let res = range_iter.try_collect::<Vec<_>>().await;
            match res {
                Ok(c) => {
                    let mut kv_map = HashMap::new();
                    for entry in c {
                        kv_map.insert(format!("{}/{}", prefix_query, entry.1), entry.2);
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
                Err(_) => panic!("Error running get by prefix"),
            }
        });
    }

    fn blockfile_comparisons(operation: ComparisonOperation, num_keys: u32, query_key: u32) {
        Runtime::new().unwrap().block_on(async {
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
                .write::<&str, u32>(BlockfileWriterOptions::new(prefix_path.clone()))
                .await
                .unwrap();
            let id = writer.id();
            println!("Number of keys {}", num_keys);
            let prefix = "prefix";
            for i in 1..num_keys {
                let key = format!("{}/{}", "key", i);
                writer.set(prefix, key.as_str(), i).await.unwrap();
            }
            // commit.
            let flusher = writer.commit::<&str, u32>().await.unwrap();
            flusher.flush::<&str, u32>().await.unwrap();

            let read_options = BlockfileReaderOptions::new(id, prefix_path);
            let reader = blockfile_provider
                .read::<&str, u32>(read_options)
                .await
                .unwrap();
            let query = format!("{}/{}", "key", query_key);
            println!("Query {}", query);
            println!("Operation {:?}", operation);

            let range_stream = match operation {
                ComparisonOperation::GreaterThan => reader
                    .get_range_stream(
                        prefix..=prefix,
                        (Bound::Excluded(query.as_str()), Bound::Unbounded),
                    )
                    .boxed_local(),
                ComparisonOperation::GreaterThanOrEquals => reader
                    .get_range_stream(prefix..=prefix, query.as_str()..)
                    .boxed_local(),
                ComparisonOperation::LessThan => reader
                    .get_range_stream(prefix..=prefix, ..query.as_str())
                    .boxed_local(),
                ComparisonOperation::LessThanOrEquals => reader
                    .get_range_stream(prefix..=prefix, ..=query.as_str())
                    .boxed_local(),
            };

            let materialized_range = match operation {
                ComparisonOperation::GreaterThan => reader
                    .get_range(
                        prefix..=prefix,
                        (Bound::Excluded(query.as_str()), Bound::Unbounded),
                    )
                    .await
                    .unwrap()
                    .collect::<Vec<_>>(),
                ComparisonOperation::GreaterThanOrEquals => reader
                    .get_range(
                        prefix..=prefix,
                        (Bound::Included(query.as_str()), Bound::Unbounded),
                    )
                    .await
                    .unwrap()
                    .collect(),
                ComparisonOperation::LessThan => reader
                    .get_range(
                        prefix..=prefix,
                        (Bound::Unbounded, Bound::Excluded(query.as_str())),
                    )
                    .await
                    .unwrap()
                    .collect(),
                ComparisonOperation::LessThanOrEquals => reader
                    .get_range(
                        prefix..=prefix,
                        (Bound::Unbounded, Bound::Included(query.as_str())),
                    )
                    .await
                    .unwrap()
                    .collect(),
            };

            let stream_result = range_stream.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(
                materialized_range, stream_result,
                ".get_range() and .get_range_stream() should return the same result"
            );

            let mut kv_map = HashMap::new();
            for entry in materialized_range {
                kv_map.insert(entry.1, entry.2);
            }
            for i in 1..num_keys {
                let key = format!("{}/{}", "key", i);
                let condition: bool = match operation {
                    ComparisonOperation::GreaterThan => key > query,
                    ComparisonOperation::GreaterThanOrEquals => key >= query,
                    ComparisonOperation::LessThan => key < query,
                    ComparisonOperation::LessThanOrEquals => key <= query,
                };
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
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id = writer.id();

        let prefix_1 = "key";
        let key1 = "zzzz";
        let value1 = vec![1, 2, 3];
        writer.set(prefix_1, key1, value1).await.unwrap();

        let prefix_2 = "key";
        let key2 = "aaaa";
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
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
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
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()).fork(id_1))
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
            println!("Getting key: {}", key);
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
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()).fork(id_2))
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
    async fn test_splitting_with_custom_blocksize() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        // Set a very small block size for the block manager
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            10,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let custom_block_size = 100 * 1024 * 1024; // 100 MiB
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .max_block_size_bytes(custom_block_size),
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

        blockfile_provider
            .clear()
            .await
            .expect("Expected to clear cache");
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

        // Sparse index should have 1 block, custom block size and at v1.2
        match &reader {
            crate::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.root.sparse_index.len(), 1);
                assert!(reader.root.sparse_index.is_valid());
                assert_eq!(reader.root.max_block_size_bytes, custom_block_size);
                assert_eq!(reader.root.version, Version::V1_2);
            }
            _ => panic!("Unexpected reader type"),
        }

        blockfile_provider
            .clear()
            .await
            .expect("expected to clear cache");
        // Add 5 new entries to the first block
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .fork(id_1)
                    .max_block_size_bytes(10),
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

        blockfile_provider
            .clear()
            .await
            .expect("expected to clear cache");
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

        // Sparse index should still have 1 block, custom_block_size and v1.2
        match &reader {
            crate::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.root.sparse_index.len(), 1);
                assert!(reader.root.sparse_index.is_valid());
                assert_eq!(reader.root.max_block_size_bytes, custom_block_size);
                assert_eq!(reader.root.version, Version::V1_2);
            }
            _ => panic!("Unexpected reader type"),
        }

        blockfile_provider
            .clear()
            .await
            .expect("expected to clear cache");
        // Add 1200 more entries, still 1 block
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .fork(id_2)
                    .max_block_size_bytes(10),
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

        blockfile_provider
            .clear()
            .await
            .expect("expected to clear cache");
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

        // Sparse index should have 1 block
        match &reader {
            crate::BlockfileReader::ArrowBlockfileReader(reader) => {
                assert_eq!(reader.root.sparse_index.len(), 1);
                assert!(reader.root.sparse_index.is_valid());
                assert_eq!(reader.root.max_block_size_bytes, custom_block_size);
                assert_eq!(reader.root.version, Version::V1_2);
            }
            _ => panic!("Unexpected reader type"),
        }
    }

    #[tokio::test]
    async fn test_splitting_boundary() {
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
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id_1 = writer.id();

        // Add the larger keys first then smaller.
        let n = 1200;
        for i in n..n * 2 {
            let key = format!("{:04}", i);
            let value = vec![i];
            writer.set("key", key.as_str(), value).await.unwrap();
        }
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = vec![i];
            writer.set("key", key.as_str(), value).await.unwrap();
        }
        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id_1, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        for i in 0..n * 2 {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, &[i]);
        }
    }

    #[tokio::test]
    async fn test_string_value() {
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
            .write::<&str, String>(BlockfileWriterOptions::new(prefix_path.clone()))
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

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &str>(read_options)
            .await
            .unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, format!("{:04}", i));
        }
    }

    #[tokio::test]
    async fn test_float_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");

        let writer = provider
            .write::<f32, String>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = i as f32;
            let value = format!("{:04}", i);
            writer.set("key", key, value).await.unwrap();
        }

        let flusher = writer.commit::<f32, String>().await.unwrap();
        flusher.flush::<f32, String>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = provider.read::<f32, &str>(read_options).await.unwrap();
        for i in 0..n {
            let key = i as f32;
            let value = reader.get("key", key).await.unwrap().unwrap();
            assert_eq!(value, format!("{:04}", i));
        }
    }

    #[tokio::test]
    async fn test_roaring_bitmap_value() {
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
            .write::<&str, roaring::RoaringBitmap>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = format!("{:04}", i);
            println!("Setting key: {}", key);
            let value = roaring::RoaringBitmap::from_iter(0..i);
            writer.set("key", key.as_str(), value).await.unwrap();
        }
        let flusher = writer
            .commit::<&str, roaring::RoaringBitmap>()
            .await
            .unwrap();
        flusher
            .flush::<&str, roaring::RoaringBitmap>()
            .await
            .unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, roaring::RoaringBitmap>(read_options)
            .await
            .unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
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
            .write::<u32, u32>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = i as u32;
            let value = i as u32;
            writer.set("key", key, value).await.unwrap();
        }

        let flusher = writer.commit::<u32, u32>().await.unwrap();
        flusher.flush::<u32, u32>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<u32, u32>(read_options)
            .await
            .unwrap();
        for i in 0..n {
            let key = i as u32;
            let value = reader.get("key", key).await.unwrap().unwrap();
            assert_eq!(value, i as u32);
        }
    }

    #[tokio::test]
    async fn test_data_record_val() {
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
            .write::<&str, &DataRecord>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
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

        let flusher = writer.commit::<&str, &DataRecord>().await.unwrap();
        flusher.flush::<&str, &DataRecord>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, DataRecord>(read_options)
            .await
            .unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
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
            .write::<&str, String>(BlockfileWriterOptions::new(prefix_path.clone()))
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
            .write::<&str, String>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id = writer.id();

        let n = 2000;
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = format!("{:04}", i);
            println!("Setting key: {}", key);
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
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, format!("{:04}", i));
        }

        let writer = blockfile_provider
            .write::<&str, String>(BlockfileWriterOptions::new(prefix_path.clone()).fork(id))
            .await
            .unwrap();
        let id = writer.id();

        // Delete some keys
        let mut rng = rand::thread_rng();
        let deleted_keys = (0..n).choose_multiple(&mut rng, n / 2);
        for i in &deleted_keys {
            let key = format!("{:04}", *i);
            writer
                .delete::<&str, String>("key", key.as_str())
                .await
                .unwrap();
        }
        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        // Check that the deleted keys are gone
        let reader = blockfile_provider
            .read::<&str, &str>(read_options)
            .await
            .unwrap();
        for i in 0..n {
            let key = format!("{:04}", i);
            if deleted_keys.contains(&i) {
                assert!(matches!(reader.get("key", &key).await, Ok(None)));
            } else {
                let value = reader.get("key", &key).await.unwrap().unwrap();
                assert_eq!(value, format!("{:04}", i));
            }
        }
    }

    #[tokio::test]
    async fn test_rank() {
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
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
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

        let read_options = BlockfileReaderOptions::new(id_1, prefix_path);
        let reader = blockfile_provider
            .read::<&str, &[u32]>(read_options)
            .await
            .unwrap();

        for i in 0..n {
            let rank_key = format!("{:04}", i);
            let rank = reader.rank("key", &rank_key).await.unwrap();
            assert_eq!(rank, i as usize);
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
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id_1 = writer.id();

        // Add the larger keys first then smaller.
        let n = 1200;
        for i in n..n * 2 {
            let key = format!("{:04}", i);
            let value = vec![i];
            writer.set("key", key.as_str(), value).await.unwrap();
        }
        for i in 0..n {
            let key = format!("{:04}", i);
            let value = vec![i];
            writer.set("key", key.as_str(), value).await.unwrap();
        }
        let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
        flusher.flush::<&str, Vec<u32>>().await.unwrap();
        // Create another writer.
        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()).fork(id_1))
            .await
            .expect("BlockfileWriter fork unsuccessful");
        // Delete everything but the last 10 keys.
        let delete_end = n * 2 - 10;
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

        for i in delete_end..n * 2 {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, [i]);
        }

        let writer = blockfile_provider
            .write::<&str, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()).fork(id_2))
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

        for i in 0..n * 2 {
            let key = format!("{:04}", i);
            let value = reader.get("key", &key).await.unwrap().unwrap();
            assert_eq!(value, &[i]);
        }
    }

    #[tokio::test]
    async fn test_write_to_same_key_many_times() {
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
            .write::<&str, u32>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let id = writer.id();

        let n = 20000;
        let fixed_key = "key";
        for i in 0..n {
            let value: u32 = i;
            writer.set("prefix", fixed_key, value).await.unwrap();
        }

        let flusher = writer.commit::<&str, u32>().await.unwrap();
        flusher.flush::<&str, u32>().await.unwrap();

        let read_options = BlockfileReaderOptions::new(id, prefix_path);
        let reader = blockfile_provider
            .read::<&str, u32>(read_options)
            .await
            .unwrap();
        let value = reader.get("prefix", fixed_key).await.unwrap().unwrap();
        assert_eq!(value, n - 1);
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
            16384,
            block_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );

        // Manually create a v1 blockfile with no counts
        let initial_block = block_manager.create::<&str, String, UnorderedBlockDelta>();
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

        let block_deltas = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut block_deltas_map = block_deltas.lock();
            block_deltas_map.insert(initial_block.id, initial_block);
        }

        let writer = ArrowUnorderedBlockfileWriter {
            block_manager,
            root_manager: root_manager.clone(),
            block_deltas,
            root: root_writer,
            id: Uuid::new_v4(),
            deltas_mutex: Arc::new(AysncPartitionedMutex::new(())),
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
        let old_block_delta_1 = block_manager.create::<&str, String, UnorderedBlockDelta>();
        old_block_delta_1.add("prefix", "a", "value_a".to_string());
        let old_block_delta_2 = block_manager.create::<&str, String, UnorderedBlockDelta>();
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
                BlockfileWriterOptions::new(prefix_path.to_string()).fork(first_write_id),
            )
            .await
            .unwrap();
        let second_write_id = writer.id();
        let writer = match writer {
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => writer,
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

    #[tokio::test]
    async fn test_v1_1_to_v1_2_migration_partially_new() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let root_manager = RootManager::new(storage.clone(), root_cache);
        // 8MiB blocks in V1.1.
        let max_block_size_bytes = 8 * 1024 * 1024;
        let block_manager = BlockManager::new(
            storage.clone(),
            max_block_size_bytes,
            block_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );

        ////////////////////////// STEP 1 //////////////////////////

        // Create two blocks with some data, we will make this conceptually a v1.1 block
        // Because the block size is 8MiB, these values should fit in their blocks
        let old_block_delta_1 = block_manager.create::<&str, String, UnorderedBlockDelta>();
        old_block_delta_1.add("prefix", "a", "value_a".to_string());
        let old_block_delta_2 = block_manager.create::<&str, String, UnorderedBlockDelta>();
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
        sparse_index
            .set_count(old_block_id_1, 1)
            .expect("Expected to set count");
        sparse_index
            .set_count(old_block_id_2, 1)
            .expect("Expected to set count");
        let first_write_id = Uuid::new_v4();
        let prefix_path = "";
        let old_root_writer = RootWriter::new(
            Version::V1_1,
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

        // We now have a v1.1 blockfile with 2 blocks.

        ////////////////////////// STEP 2 //////////////////////////

        // Ensure that a v1.2 compatible reader on a v1.1 blockfile will work as expected

        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            max_block_size_bytes,
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
        assert_eq!(reader.root.version, Version::V1_1);
        assert_eq!(reader.root.max_block_size_bytes, max_block_size_bytes);

        ////////////////////////// STEP 3 //////////////////////////
        // Test that a v1.2 writer can read a v1.1 blockfile and dirty a block.
        // We will explicitly try to fork this blockfile with a value for
        // max_block_size_bytes which is not big enough to fit
        // in a value. The provider should ignore this supplied param and instead
        // take the value of max_block_size_bytes from the block manager.
        // Thus later when we insert a (k, v) bigger than this value
        // (but smaller than the Block Manager's value) it should succeed.

        // Clear cache so that the blocks and root are deserialized again.
        blockfile_provider
            .clear()
            .await
            .expect("Expected to clear cache");
        // Fork with a very small value for max_block_size_bytes.
        let writer = blockfile_provider
            .write::<&str, String>(
                BlockfileWriterOptions::new(prefix_path.to_string())
                    .fork(first_write_id)
                    .max_block_size_bytes(10),
            )
            .await
            .unwrap();
        let second_write_id = writer.id();
        let writer = match writer {
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => writer,
            _ => panic!("Unexpected writer type"),
        };
        assert_eq!(writer.root.version, Version::V1_1);
        assert_eq!(writer.root.sparse_index.len(), 2);
        assert_eq!(writer.root.sparse_index.data.lock().counts.len(), 2);
        // max_block_size_bytes should be hydrated from the block manager and NOT
        // from the value passed in the BlockfileWriterOptions.
        assert_eq!(writer.root.max_block_size_bytes, max_block_size_bytes);

        // Writing a value > 10 bytes should succeed.
        let more_than_100_bytes_value = "v".repeat(500);
        writer
            .set("prefix", "b", more_than_100_bytes_value)
            .await
            .unwrap();

        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        ////////////////////////// STEP 4 //////////////////////////

        // Verify that the root version migration took place
        // and that all the data is intact.
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
        assert_eq!(reader.root.max_block_size_bytes, max_block_size_bytes);
        assert_eq!(reader.count().await.unwrap(), 3);

        ////////////////////////// STEP 5 //////////////////////////
        // If I create a V1.2 writer for a V1.2 blockfile, it should NOT use the block size of the block manager
        // but use the value that is persisted in its arrow metadata.
        // Thus writing a big value should succeed (similar to step 3).

        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        // Create a block manager with max_block_size_bytes = 10
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            10,
            block_cache,
            root_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );

        let writer = blockfile_provider
            .write::<&str, String>(
                BlockfileWriterOptions::new(prefix_path.to_string())
                    .fork(second_write_id)
                    .max_block_size_bytes(10),
            )
            .await
            .unwrap();
        let third_write_id = writer.id();
        let writer = match writer {
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => writer,
            _ => panic!("Unexpected writer type"),
        };
        assert_eq!(writer.root.version, Version::V1_2);
        assert_eq!(writer.root.sparse_index.len(), 2);
        assert_eq!(writer.root.sparse_index.data.lock().counts.len(), 2);
        // max_block_size_bytes from arrow metadata should be loaded.
        assert_eq!(writer.root.max_block_size_bytes, max_block_size_bytes);

        // Writing a value > 10 bytes should succeed.
        let more_than_100_bytes_value = "v".repeat(500);
        writer
            .set("prefix", "c", more_than_100_bytes_value)
            .await
            .unwrap();

        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        ////////////////////////// STEP 6 //////////////////////////

        // Verify that the data is correct

        let read_options = BlockfileReaderOptions::new(third_write_id, prefix_path.to_string());
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
        assert_eq!(reader.root.max_block_size_bytes, max_block_size_bytes);
        assert_eq!(reader.count().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn test_v1_to_v1_2_migration_partially_new() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let root_manager = RootManager::new(storage.clone(), root_cache);
        // 8MiB blocks in V1.
        let max_block_size_bytes = 8 * 1024 * 1024;
        let block_manager = BlockManager::new(
            storage.clone(),
            max_block_size_bytes,
            block_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );

        ////////////////////////// STEP 1 //////////////////////////

        // Create two blocks with some data, we will make this conceptually a v1.1 block
        // Because the block size is 8MiB, these values should fit in their blocks
        let old_block_delta_1 = block_manager.create::<&str, String, UnorderedBlockDelta>();
        old_block_delta_1.add("prefix", "a", "value_a".to_string());
        let old_block_delta_2 = block_manager.create::<&str, String, UnorderedBlockDelta>();
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
        sparse_index
            .set_count(old_block_id_1, 1)
            .expect("Expected to set count");
        sparse_index
            .set_count(old_block_id_2, 1)
            .expect("Expected to set count");
        let first_write_id = Uuid::new_v4();
        let prefix_path = "";
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

        // We now have a v1 blockfile with 2 blocks.

        ////////////////////////// STEP 2 //////////////////////////

        // Ensure that a v1.2 compatible reader on a v1 blockfile will work as expected

        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            max_block_size_bytes,
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
        assert_eq!(reader.root.max_block_size_bytes, max_block_size_bytes);

        ////////////////////////// STEP 3 //////////////////////////
        // Test that a v1.2 writer can read a v1 blockfile and dirty a block.
        // We will explicitly try to fork this blockfile with a value for
        // max_block_size_bytes which is not big enough to fit
        // in a value. The provider should ignore this supplied param and instead
        // take the value of max_block_size_bytes from the block manager.
        // Thus later when we insert a (k, v) bigger than this value
        // (but smaller than the Block Manager's value) it should succeed.

        // Clear cache so that the blocks and root are deserialized again.
        blockfile_provider
            .clear()
            .await
            .expect("Expected to clear cache");
        // Fork with a very small value for max_block_size_bytes.
        let writer = blockfile_provider
            .write::<&str, String>(
                BlockfileWriterOptions::new(prefix_path.to_string())
                    .fork(first_write_id)
                    .max_block_size_bytes(10),
            )
            .await
            .unwrap();
        let second_write_id = writer.id();
        let writer = match writer {
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => writer,
            _ => panic!("Unexpected writer type"),
        };
        assert_eq!(writer.root.version, Version::V1);
        assert_eq!(writer.root.sparse_index.len(), 2);
        assert_eq!(writer.root.sparse_index.data.lock().counts.len(), 2);
        // max_block_size_bytes should be hydrated from the block manager and NOT
        // from the value passed in the BlockfileWriterOptions.
        assert_eq!(writer.root.max_block_size_bytes, max_block_size_bytes);

        // Writing a value > 10 bytes should succeed.
        let more_than_100_bytes_value = "v".repeat(500);
        writer
            .set("prefix", "b", more_than_100_bytes_value)
            .await
            .unwrap();

        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        ////////////////////////// STEP 4 //////////////////////////

        // Verify that the root version migration took place
        // and that all the data is intact.
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
        assert_eq!(reader.root.max_block_size_bytes, max_block_size_bytes);
        assert_eq!(reader.count().await.unwrap(), 3);

        ////////////////////////// STEP 5 //////////////////////////
        // If I create a V1.2 writer for a V1.2 blockfile, it should NOT use the block size of the block manager
        // but use the value that is persisted in its arrow metadata.
        // Thus writing a big value should succeed (similar to step 3).

        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        // Create a block manager with max_block_size_bytes = 10
        let blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            10,
            block_cache,
            root_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );

        let writer = blockfile_provider
            .write::<&str, String>(
                BlockfileWriterOptions::new(prefix_path.to_string())
                    .fork(second_write_id)
                    .max_block_size_bytes(10),
            )
            .await
            .unwrap();
        let third_write_id = writer.id();
        let writer = match writer {
            BlockfileWriter::ArrowUnorderedBlockfileWriter(writer) => writer,
            _ => panic!("Unexpected writer type"),
        };
        assert_eq!(writer.root.version, Version::V1_2);
        assert_eq!(writer.root.sparse_index.len(), 2);
        assert_eq!(writer.root.sparse_index.data.lock().counts.len(), 2);
        // max_block_size_bytes from arrow metadata should be loaded.
        assert_eq!(writer.root.max_block_size_bytes, max_block_size_bytes);

        // Writing a value > 10 bytes should succeed.
        let more_than_100_bytes_value = "v".repeat(500);
        writer
            .set("prefix", "c", more_than_100_bytes_value)
            .await
            .unwrap();

        let flusher = writer.commit::<&str, String>().await.unwrap();
        flusher.flush::<&str, String>().await.unwrap();

        ////////////////////////// STEP 6 //////////////////////////

        // Verify that the data is correct

        let read_options = BlockfileReaderOptions::new(third_write_id, prefix_path.to_string());
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
        assert_eq!(reader.root.max_block_size_bytes, max_block_size_bytes);
        assert_eq!(reader.count().await.unwrap(), 4);
    }
}
