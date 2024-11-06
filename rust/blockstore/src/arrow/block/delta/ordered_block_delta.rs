use std::collections::HashMap;

use super::{storage::BlockStorage, types::Delta};
use crate::{
    arrow::{
        block::Block,
        types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::{CompositeKey, KeyWrapper},
};
use arrow::array::{RecordBatch, StringArray};
use uuid::Uuid;

/// This delta type performs mutations more efficiently than the `UnorderedBlockDelta` type if the mutations are already in sorted order.
/// See rust/blockstore/src/arrow/block/delta/types.rs for more info about deltas.
pub struct OrderedBlockDelta {
    pub(in crate::arrow) builder: BlockStorage,
    pub(in crate::arrow) id: Uuid,
    copied_up_to_row_of_old_block: usize,
    old_block: Option<Block>,
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
        }
    }

    fn fork_block<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        new_id: Uuid,
        old_block: &Block,
    ) -> Self {
        let mut delta = OrderedBlockDelta::new::<K, V>(new_id);
        delta.old_block = Some(old_block.clone());
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
        let wrapped_key: KeyWrapper = key.into();
        self.copy_up_to::<K::ReadableKey<'_>, V::ReadableValue<'_>>(prefix, &wrapped_key);

        // TODO: errors?
        V::add(prefix, wrapped_key, value, &self.builder);
    }

    pub fn skip<K, V>(&mut self, prefix: &str, key: K)
    where
        K: ArrowWriteableKey,
        V: ArrowWriteableValue,
    {
        self.copy_up_to::<K::ReadableKey<'_>, V::ReadableValue<'_>>(prefix, &key.into());
    }

    pub fn copy_to_end<K: ArrowWriteableKey, V: ArrowWriteableValue>(&mut self) {
        // Copy remaining rows
        if let Some(old_block) = self.old_block.as_ref() {
            for i in self.copied_up_to_row_of_old_block..old_block.data.num_rows() {
                let old_prefix = old_block
                    .data
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i);
                let old_key = K::ReadableKey::get(old_block.data.column(1), i);
                let old_value = V::ReadableValue::get(old_block.data.column(2), i);
                K::ReadableKey::add_to_delta(old_prefix, old_key, old_value, &mut self.builder);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.builder.len()
    }

    fn copy_up_to<'a, K: ArrowReadableKey<'a>, V: ArrowReadableValue<'a>>(
        &'a mut self,
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

            for i in self.copied_up_to_row_of_old_block..old_block.data.num_rows() {
                let old_prefix = prefix_arr.value(i);
                let old_key = K::get(key_arr, i);

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
        };

        self.copied_up_to_row_of_old_block = 0;

        new_delta
    }

    pub(crate) fn min_key(&self) -> Option<CompositeKey> {
        self.builder.get_min_key()
    }
}
