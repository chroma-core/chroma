use std::collections::HashMap;

use super::{storage::BlockStorage, types::DeltaCommon};
use crate::{
    arrow::{
        block::Block,
        types::{
            ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
            BuilderMutationOrderHint,
        },
    },
    key::{CompositeKey, KeyWrapper},
};
use arrow::array::{RecordBatch, StringArray};
use uuid::Uuid;

/// A block delta tracks a source block and represents the new state of a block. Blocks are
/// immutable, so when a write is made to a block, a new block is created with the new state.
/// A block delta is a temporary representation of the new state of a block. A block delta
/// can be converted to a block data, which is then used to create a new block. A block data
/// can be converted into a block delta for new writes.
/// # Methods
/// - add: adds a key value pair to the block delta.
/// - delete: deletes a key from the block delta.
/// - get_size: gets the size of the block delta.
/// - split: splits the block delta into new block deltas based on a max block size.
pub struct OrderedBlockDelta {
    pub(in crate::arrow) builder: BlockStorage,
    pub(in crate::arrow) id: Uuid,
    copied_up_to_row_of_old_block: usize,
    old_block: Option<Block>,
}

impl DeltaCommon for OrderedBlockDelta {
    // NOTE(rescrv):  K is unused, but it is very conceptually easy to think of everything as
    // key-value pairs.  I started to refactor this to remove ArrowWriteableKey, but it was not
    // readable to tell whether I was operating on the key or value type.  Keeping both but
    // suppressing the clippy error is a reasonable alternative.
    #[allow(clippy::extra_unused_type_parameters)]
    fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(id: Uuid) -> Self {
        OrderedBlockDelta {
            builder: V::get_delta_builder(BuilderMutationOrderHint::Ordered),
            id,
            copied_up_to_row_of_old_block: 0,
            old_block: None,
        }
    }

    fn fork_block<K: ArrowWriteableKey, V: ArrowWriteableValue>(id: Uuid, block: &Block) -> Self {
        let mut delta = OrderedBlockDelta::new::<K, V>(id);
        delta.old_block = Some(block.clone());
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
        if let Some(old_block) = self.old_block.clone().as_ref() {
            // todo: is clone expensive here?
            self.copy_past::<K::ReadableKey<'_>, V::ReadableValue<'_>>(
                prefix,
                key.clone().into(),
                old_block,
            );
        }

        // TODO: errors?
        V::add(prefix, key.into(), value, &self.builder);
    }

    pub fn skip<K, V>(&mut self, prefix: &str, key: K)
    where
        K: ArrowWriteableKey,
        V: ArrowWriteableValue,
    {
        if let Some(old_block) = self.old_block.clone().as_ref() {
            // todo: is clone expensive here?
            self.copy_past::<K::ReadableKey<'_>, V::ReadableValue<'_>>(
                prefix,
                key.clone().into(),
                old_block,
            );
        }
    }

    pub fn copy_to_end<K: ArrowWriteableKey, V: ArrowWriteableValue>(&mut self) {
        // Copy remaining rows
        if let Some(old_block) = self.old_block.clone().as_ref() {
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

    fn copy_past<'a, K: ArrowReadableKey<'a>, V: ArrowReadableValue<'a>>(
        &mut self,
        excluded_prefix: &str,
        excluded_key: KeyWrapper,
        old_block: &'a Block,
    ) {
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
            let old_key_wrapped: KeyWrapper = old_key.clone().into(); // todo: remove clone

            if old_prefix > excluded_prefix
                || (old_prefix == excluded_prefix && old_key_wrapped >= excluded_key)
            {
                if old_prefix == excluded_prefix && old_key_wrapped == excluded_key {
                    self.copied_up_to_row_of_old_block += 1;
                }

                break;
            }

            let old_value = V::get(old_block.data.column(2), i);
            K::add_to_delta(old_prefix, old_key, old_value, &mut self.builder);
            self.copied_up_to_row_of_old_block += 1;
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
}
