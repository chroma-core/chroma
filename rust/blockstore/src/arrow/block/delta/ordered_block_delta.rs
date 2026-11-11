use std::collections::HashMap;

use super::{storage::BlockStorage, types::Delta};
use crate::{
    arrow::{
        block::Block,
        types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::{CompositeKey, KeyWrapper},
};
use arrow::array::{BooleanArray, RecordBatch, StringArray};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("Could not compare keys while deleting from an ordered block")]
pub struct IncomparableDeleteKeys;

impl chroma_error::ChromaError for IncomparableDeleteKeys {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}

/// This delta type performs mutations more efficiently than the `UnorderedBlockDelta` type if the mutations are already in sorted order.
/// See rust/blockstore/src/arrow/block/delta/types.rs for more info about deltas.
pub struct OrderedBlockDelta {
    pub(in crate::arrow) builder: BlockStorage,
    pub(in crate::arrow) id: Uuid,
    copied_up_to_row_of_old_block: usize,
    old_block: Option<Block>,
    // Keep delete-only forks in Arrow form until an insertion requires a builder.
    deleted_rows: Option<Vec<usize>>,
    #[cfg(debug_assertions)]
    last_added_key: Option<(String, KeyWrapper)>,
}

impl Delta for OrderedBlockDelta {
    // NOTE(rescrv):  K is unused, but it is very conceptually easy to think of everything as
    // key-value pairs.  I started to refactor this to remove ArrowWriteableKey, but it was not
    // readable to tell whether I was operating on the key or value type.  Keeping both but
    // suppressing the clippy error is a reasonable alternative.
    #[allow(clippy::extra_unused_type_parameters)]
    fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(id: Uuid) -> Self {
        OrderedBlockDelta {
            builder: V::get_delta_builder(crate::BlockfileWriterMutationOrdering::Ordered),
            id,
            copied_up_to_row_of_old_block: 0,
            old_block: None,
            deleted_rows: None,
            #[cfg(debug_assertions)]
            last_added_key: None,
        }
    }

    fn fork_block<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        new_id: Uuid,
        old_block: &Block,
    ) -> Self {
        let mut delta = OrderedBlockDelta::new::<K, V>(new_id);
        delta.old_block = Some(old_block.clone());
        delta.deleted_rows = Some(Vec::new());
        delta
    }

    fn id(&self) -> Uuid {
        self.id
    }

    fn finish<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        self,
        metadata: Option<HashMap<String, String>>,
    ) -> RecordBatch {
        self.builder.into_record_batch::<K>(metadata)
    }
}

impl OrderedBlockDelta {
    pub fn add<K, V>(&mut self, prefix: &str, key: K, value: V)
    where
        K: ArrowWriteableKey,
        V: ArrowWriteableValue,
    {
        self.materialize_deletes::<K, V>();
        let wrapped_key: KeyWrapper = key.into();

        #[cfg(debug_assertions)]
        {
            let this_key = (prefix.to_string(), wrapped_key.clone());
            if let Some(ref last_key) = self.last_added_key {
                if *last_key > this_key {
                    panic!("Found unordered keys while adding to block delta. Last key: {:?}, current key: {:?}", last_key, this_key);
                }
            } else {
                self.last_added_key = Some(this_key);
            }
        }

        self.copy_up_to::<K::ReadableKey<'_>, V::ReadableValue<'_>>(prefix, &wrapped_key);

        // TODO: errors?
        V::add(prefix, wrapped_key, value, &self.builder);
    }

    pub fn skip<K, V>(&mut self, prefix: &str, key: K) -> Result<(), IncomparableDeleteKeys>
    where
        K: ArrowWriteableKey,
        V: ArrowWriteableValue,
    {
        let key = key.into();
        if let Some(deleted_rows) = self.deleted_rows.as_mut() {
            // Follow the existing writer's behavior of asserting internal state invariants.
            let block = self
                .old_block
                .as_ref()
                .expect("delete-only fork has old block");
            // Follow copy_up_to/copy_to_end: the prefix column must be a StringArray.
            let prefixes = block
                .data
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let mut lo = 0;
            let mut hi = block.data.num_rows();
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let old_key: KeyWrapper = K::ReadableKey::get(block.data.column(1), mid).into();
                let ordering = match prefixes.value(mid).cmp(prefix) {
                    std::cmp::Ordering::Equal => {
                        old_key.partial_cmp(&key).ok_or(IncomparableDeleteKeys)?
                    }
                    ordering => ordering,
                };
                if ordering.is_lt() {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo < block.data.num_rows()
                && prefixes.value(lo) == prefix
                && Into::<KeyWrapper>::into(K::ReadableKey::get(block.data.column(1), lo)) == key
                && deleted_rows.last() != Some(&lo)
            {
                deleted_rows.push(lo);
            }
            return Ok(());
        }
        self.copy_up_to::<K::ReadableKey<'_>, V::ReadableValue<'_>>(prefix, &key);
        Ok(())
    }

    /// Filter delete-only forks without allocating a Rust key/value for each survivor.
    /// Oversized legacy blocks use the usual builder path so they still split.
    pub(in crate::arrow) fn finish_deletions(
        &self,
        max_block_size_bytes: usize,
    ) -> Result<Option<Block>, arrow::error::ArrowError> {
        let (Some(rows), Some(old)) = (&self.deleted_rows, &self.old_block) else {
            return Ok(None);
        };
        if old.get_size() > max_block_size_bytes {
            return Ok(None);
        }
        let mut keep = vec![true; old.data.num_rows()];
        for &row in rows {
            keep[row] = false;
        }
        let data = arrow::compute::filter_record_batch(&old.data, &BooleanArray::from(keep))?;
        Ok(Some(Block::from_record_batch(self.id, data)))
    }

    fn materialize_deletes<K: ArrowWriteableKey, V: ArrowWriteableValue>(&mut self) {
        let Some(rows) = self.deleted_rows.take() else {
            return;
        };
        let Some(&last) = rows.last() else {
            return;
        };
        // Follow the existing writer's behavior of asserting internal state invariants.
        let old = self
            .old_block
            .as_ref()
            .expect("delete-only fork has old block");
        // Follow copy_up_to/copy_to_end: the prefix column must be a StringArray.
        let prefixes = old
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut deleted = rows.into_iter().peekable();
        for row in 0..=last {
            if deleted.peek() == Some(&row) {
                deleted.next();
                continue;
            }
            let key = K::ReadableKey::get(old.data.column(1), row);
            let value = V::ReadableValue::get(old.data.column(2), row);
            K::ReadableKey::add_to_delta(prefixes.value(row), key, value, &mut self.builder);
        }
        self.copied_up_to_row_of_old_block = last + 1;
    }

    pub fn copy_to_end<K: ArrowWriteableKey, V: ArrowWriteableValue>(&mut self) {
        self.materialize_deletes::<K, V>();
        // Copy remaining rows
        if let Some(old_block) = self.old_block.take() {
            #[cfg(debug_assertions)]
            let mut last_key = None;

            for i in self.copied_up_to_row_of_old_block..old_block.data.num_rows() {
                let old_prefix = old_block
                    .data
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i);
                let old_key = K::ReadableKey::get(old_block.data.column(1), i);

                #[cfg(debug_assertions)]
                {
                    if let Some(ref last_key) = last_key {
                        if *last_key > (old_prefix, old_key.clone()) {
                            panic!("Found unordered keys while copying to end of block. Last key: {:?}, current key: {:?}", last_key, (old_prefix, old_key));
                        }
                    } else {
                        last_key = Some((old_prefix, old_key.clone()));
                    }
                }

                let old_value = V::ReadableValue::get(old_block.data.column(2), i);
                K::ReadableKey::add_to_delta(old_prefix, old_key, old_value, &mut self.builder);
            }
        }
    }

    fn copy_up_to<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me mut self,
        excluded_prefix: &str,
        excluded_key: &KeyWrapper,
    ) {
        if let Some(old_block) = self.old_block.as_ref() {
            let prefix_arr = old_block
                .data
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let key_arr = old_block.data.column(1);

            #[cfg(debug_assertions)]
            let mut last_key = None;
            for i in self.copied_up_to_row_of_old_block..old_block.data.num_rows() {
                let old_prefix = prefix_arr.value(i);
                let old_key = K::get(key_arr, i);

                #[cfg(debug_assertions)]
                {
                    if let Some(ref last_key) = last_key {
                        if *last_key > (old_prefix, old_key.clone()) {
                            panic!("Found unordered keys while copying up to excluded key. Excluded key: {:?}, last key: {:?}, current key: {:?}", (excluded_prefix, excluded_key), last_key, (old_prefix, old_key));
                        }
                    } else {
                        last_key = Some((old_prefix, old_key.clone()));
                    }
                }

                match old_prefix.cmp(excluded_prefix) {
                    std::cmp::Ordering::Less => {}
                    std::cmp::Ordering::Equal => {
                        match old_key.clone().into().partial_cmp(excluded_key) {
                            Some(std::cmp::Ordering::Less) => {}
                            Some(std::cmp::Ordering::Equal) => {
                                self.copied_up_to_row_of_old_block += 1;
                                break;
                            }
                            Some(std::cmp::Ordering::Greater) => break,
                            None => panic!("Could not compare keys"),
                        }
                    }
                    std::cmp::Ordering::Greater => break,
                }

                let old_value = V::get(old_block.data.column(2), i);
                K::add_to_delta(old_prefix, old_key, old_value, &mut self.builder);
                self.copied_up_to_row_of_old_block += 1;
            }
        }
    }

    ///  Gets the size of the block delta as it would be in a block. This includes
    ///  the size of the prefix, key, and value data and the size of the offsets
    ///  where applicable. The size is rounded up to the nearest 64 bytes as per
    ///  the arrow specification. When a block delta is converted into a block data
    ///  the same sizing is used to allocate the memory for the block data.
    ///
    ///  If this delta was forked from an existing block, the size returned **does not include** any pending data from the old block. Call `.copy_to_end()` first if you want this to return the complete size.
    #[allow(clippy::extra_unused_type_parameters)]
    pub(in crate::arrow) fn get_size<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self) -> usize {
        self.builder.get_size::<K>()
    }

    /// Splits the block delta into two block deltas. The split point is the last key
    /// that pushes the block over the half size.
    /// # Arguments
    /// - max_block_size_bytes: the maximum size of a block in bytes.
    /// # Returns
    /// A tuple containing the the key of the split point and the new block delta.
    /// The new block deltas contains all the key value pairs after, but not including the
    /// split point.
    pub(crate) fn split<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &self,
        max_block_size_bytes: usize,
    ) -> Vec<(CompositeKey, OrderedBlockDelta)> {
        let half_size = max_block_size_bytes / 2;

        let mut blocks_to_split: Vec<OrderedBlockDelta> = Vec::new();

        // Special case for the first split (self) because it's an immutable borrow
        let (new_start_key, new_delta) = self.builder.split::<K>(half_size);
        let new_block = OrderedBlockDelta {
            builder: new_delta,
            id: Uuid::new_v4(),
            copied_up_to_row_of_old_block: 0,
            old_block: None,
            deleted_rows: None,
            #[cfg(debug_assertions)]
            last_added_key: None,
        };
        if new_block.get_size::<K, V>() > max_block_size_bytes {
            blocks_to_split.push(new_block);
        } else {
            return vec![(new_start_key, new_block)];
        }

        let mut output = Vec::new();
        // iterate over all blocks to split until its empty
        while let Some(curr_block) = blocks_to_split.pop() {
            let (new_start_key, new_delta) = curr_block.builder.split::<K>(half_size);
            let new_block = OrderedBlockDelta {
                builder: new_delta,
                id: Uuid::new_v4(),
                copied_up_to_row_of_old_block: 0,
                old_block: None,
                deleted_rows: None,
                #[cfg(debug_assertions)]
                last_added_key: None,
            };

            output.push((
                curr_block
                    .builder
                    .get_min_key()
                    .expect("Block must be non empty after split"),
                curr_block,
            ));

            if new_block.get_size::<K, V>() > max_block_size_bytes {
                blocks_to_split.push(new_block);
            } else {
                output.push((new_start_key, new_block));
            }
        }

        output
    }

    pub(crate) fn split_off_half<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        &mut self,
    ) -> OrderedBlockDelta {
        let half_size = self.get_size::<K, V>() / 2;
        let (_, new_delta) = self.builder.split::<K>(half_size);

        let old_block = self.old_block.take();

        let new_delta = OrderedBlockDelta {
            builder: new_delta,
            id: Uuid::new_v4(),
            copied_up_to_row_of_old_block: self.copied_up_to_row_of_old_block,
            old_block,
            deleted_rows: None,
            #[cfg(debug_assertions)]
            last_added_key: None,
        };

        self.copied_up_to_row_of_old_block = 0;

        new_delta
    }

    pub(crate) fn min_key(&self) -> Option<CompositeKey> {
        self.builder.get_min_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn incomparable_delete_keys_return_error_without_deleting_rows() {
        for (stored_key, deleted_key) in [(1.0f32, f32::NAN), (f32::NAN, 1.0f32)] {
            let mut seed = OrderedBlockDelta::new::<f32, Vec<u32>>(Uuid::new_v4());
            seed.add("p", stored_key, vec![42u32]);
            let block = Block::from_record_batch(seed.id, seed.finish::<f32, Vec<u32>>(None));
            let mut delta = OrderedBlockDelta::fork_block::<f32, Vec<u32>>(Uuid::new_v4(), &block);
            assert!(delta.skip::<f32, Vec<u32>>("p", deleted_key).is_err());
            assert!(delta.deleted_rows.as_ref().unwrap().is_empty());
            assert_eq!(
                delta
                    .finish_deletions(usize::MAX)
                    .unwrap()
                    .unwrap()
                    .data
                    .num_rows(),
                1
            );
        }
    }

    proptest! {
        #[test]
        fn proptest_delete_filter_and_mixed_fallback(
            old in prop::collection::btree_map(0u32..200, prop::collection::vec(any::<u32>(), 0..12), 0..100),
            updates in prop::collection::btree_map(0u32..220, prop::option::of(prop::collection::vec(any::<u32>(), 0..12)), 0..60),
            delete_only in any::<bool>(),
            force_split_path in any::<bool>(),
        ) {
            let mut seed = OrderedBlockDelta::new::<u32, Vec<u32>>(Uuid::new_v4());
            // Multiple prefixes share a block, including absent keys/prefixes.
            let prefix = |key: u32| format!("p{}", key / 50);
            for (&key, value) in &old {
                seed.add(&prefix(key), key, value.clone());
            }
            let block = Block::from_record_batch(seed.id, seed.finish::<u32, Vec<u32>>(None));
            let mut delta = OrderedBlockDelta::fork_block::<u32, Vec<u32>>(Uuid::new_v4(), &block);
            let mut expected = old;
            for (key, value) in updates {
                if !delete_only {
                    if let Some(value) = value {
                        delta.add(&prefix(key), key, value.clone());
                        expected.insert(key, value);
                        continue;
                    }
                }
                delta.skip::<u32, Vec<u32>>(&prefix(key), key).unwrap();
                // Repeated deletes must be idempotent.
                delta.skip::<u32, Vec<u32>>(&prefix(key), key).unwrap();
                expected.remove(&key);
            }
            let filtered = delta.finish_deletions(if force_split_path { 0 } else { usize::MAX }).unwrap();
            if delete_only && !force_split_path {
                prop_assert!(filtered.is_some());
            }
            let actual = if let Some(block) = filtered {
                block.data.0
            } else {
                delta.copy_to_end::<u32, Vec<u32>>();
                delta.finish::<u32, Vec<u32>>(None)
            };
            let mut oracle = OrderedBlockDelta::new::<u32, Vec<u32>>(Uuid::new_v4());
            for (key, value) in expected {
                oracle.add(&prefix(key), key, value);
            }
            let expected = oracle.finish::<u32, Vec<u32>>(None);
            prop_assert_eq!(actual, expected);
        }
    }
}
