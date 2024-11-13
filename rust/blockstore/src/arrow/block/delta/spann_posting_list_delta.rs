use std::{collections::BTreeMap, sync::Arc};

use arrow::{array::RecordBatch, util::bit_util};
use chroma_types::SpannPostingList;
use parking_lot::RwLock;

use crate::{
    arrow::types::{ArrowWriteableKey, ArrowWriteableValue},
    key::{CompositeKey, KeyWrapper},
};

use super::{spann_posting_list_size_tracker::SpannPostingListSizeTracker, BlockKeyArrowBuilder};

#[derive(Debug)]
struct Inner {
    storage: BTreeMap<
        CompositeKey,
        <&'static chroma_types::SpannPostingList<'static> as ArrowWriteableValue>::PreparedValue,
    >,
    size_tracker: SpannPostingListSizeTracker,
}

struct SplitInformation {
    split_key: CompositeKey,
    remaining_size: SpannPostingListSizeTracker,
}

#[derive(Debug, Clone)]
pub struct SpannPostingListDelta {
    inner: Arc<RwLock<Inner>>,
}

impl SpannPostingListDelta {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                storage: BTreeMap::new(),
                size_tracker: SpannPostingListSizeTracker::new(),
            })),
        }
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        self.inner.read().size_tracker.get_prefix_size()
    }

    pub(super) fn get_key_size(&self) -> usize {
        self.inner.read().size_tracker.get_key_size()
    }

    pub fn add(&self, prefix: &str, key: KeyWrapper, value: &SpannPostingList<'_>) {
        let mut lock_guard = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };
        // Subtract the old sizes. Remove the old posting list if it exists.
        if let Some(pl) = lock_guard.storage.remove(&composite_key) {
            lock_guard.size_tracker.subtract_value_size(&pl);
            lock_guard
                .size_tracker
                .subtract_prefix_size(composite_key.prefix.len());
            lock_guard
                .size_tracker
                .subtract_key_size(composite_key.key.get_size());
            lock_guard.size_tracker.decrement_item_count();
        }
        // Add the new sizes.
        lock_guard
            .size_tracker
            .add_prefix_size(composite_key.prefix.len());
        lock_guard
            .size_tracker
            .add_key_size(composite_key.key.get_size());
        lock_guard.size_tracker.increment_item_count();

        let prepared = <&chroma_types::SpannPostingList>::prepare(value);
        lock_guard.size_tracker.add_value_size(&prepared);
        // Add the value in the btree.
        lock_guard.storage.insert(composite_key, prepared);
    }

    pub fn delete(&self, prefix: &str, key: KeyWrapper) {
        let mut lock_guard = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };
        if let Some(pl) = lock_guard.storage.remove(&composite_key) {
            lock_guard.size_tracker.subtract_value_size(&pl);
            lock_guard
                .size_tracker
                .subtract_prefix_size(composite_key.prefix.len());
            lock_guard
                .size_tracker
                .subtract_key_size(composite_key.key.get_size());
            lock_guard.size_tracker.decrement_item_count();
        }
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let read_guard = self.inner.read();
        let prefix_size =
            bit_util::round_upto_multiple_of_64(read_guard.size_tracker.get_prefix_size());
        let key_size = bit_util::round_upto_multiple_of_64(read_guard.size_tracker.get_key_size());
        let doc_offset_ids_size =
            bit_util::round_upto_multiple_of_64(read_guard.size_tracker.get_doc_offset_ids_size());
        let doc_versions_size =
            bit_util::round_upto_multiple_of_64(read_guard.size_tracker.get_doc_versions_size());
        let doc_embeddings_size =
            bit_util::round_upto_multiple_of_64(read_guard.size_tracker.get_doc_embeddings_size());

        // Account for offsets.
        let num_elts = read_guard.storage.len();
        let prefix_offset_size = bit_util::round_upto_multiple_of_64((num_elts + 1) * 4);
        let key_offset_size = K::offset_size(num_elts);
        let doc_offset_ids_offset_size = bit_util::round_upto_multiple_of_64((num_elts + 1) * 4);
        let doc_versions_offset_size = bit_util::round_upto_multiple_of_64((num_elts + 1) * 4);
        // validity bitmap for fixed size embeddings list not required since it is not null.
        let doc_embeddings_offset_size = bit_util::round_upto_multiple_of_64((num_elts + 1) * 4);
        prefix_size
            + key_size
            + doc_offset_ids_size
            + doc_versions_size
            + doc_embeddings_size
            + prefix_offset_size
            + key_offset_size
            + doc_offset_ids_offset_size
            + doc_versions_offset_size
            + doc_embeddings_offset_size
    }

    // assumes there is a split point.
    fn split_internal<K: ArrowWriteableKey>(&self, split_size: usize) -> SplitInformation {
        let mut size_up_to_split_key = SpannPostingListSizeTracker::new();
        let mut split_key = None;

        let read_guard = self.inner.read();
        for (key, pl) in &read_guard.storage {
            size_up_to_split_key.add_prefix_size(key.prefix.len());
            size_up_to_split_key.add_key_size(key.key.get_size());
            size_up_to_split_key.add_value_size(pl);
            size_up_to_split_key.increment_item_count();

            let cumulative_count = size_up_to_split_key.get_num_items();

            let prefix_offset_size =
                bit_util::round_upto_multiple_of_64((cumulative_count + 1) * 4);
            let key_offset_size = K::offset_size(cumulative_count);
            let doc_offset_ids_offset_size =
                bit_util::round_upto_multiple_of_64((cumulative_count + 1) * 4);
            let doc_versions_offset_size =
                bit_util::round_upto_multiple_of_64((cumulative_count + 1) * 4);
            let doc_embeddings_offset_size =
                bit_util::round_upto_multiple_of_64((cumulative_count + 1) * 4);
            let total_size =
                bit_util::round_upto_multiple_of_64(size_up_to_split_key.get_prefix_size())
                    + bit_util::round_upto_multiple_of_64(size_up_to_split_key.get_key_size())
                    + bit_util::round_upto_multiple_of_64(
                        size_up_to_split_key.get_doc_offset_ids_size(),
                    )
                    + bit_util::round_upto_multiple_of_64(
                        size_up_to_split_key.get_doc_versions_size(),
                    )
                    + bit_util::round_upto_multiple_of_64(
                        size_up_to_split_key.get_doc_embeddings_size(),
                    )
                    + prefix_offset_size
                    + key_offset_size
                    + doc_offset_ids_offset_size
                    + doc_versions_offset_size
                    + doc_embeddings_offset_size;

            if total_size > split_size {
                split_key = Some(key.clone());
                size_up_to_split_key.subtract_prefix_size(key.prefix.len());
                size_up_to_split_key.subtract_key_size(key.key.get_size());
                size_up_to_split_key.subtract_value_size(pl);
                size_up_to_split_key.decrement_item_count();
                break;
            }
        }
        SplitInformation {
            split_key: split_key.expect("Split key expected to be found"),
            remaining_size: read_guard.size_tracker - size_up_to_split_key,
        }
    }

    pub(super) fn split<K: ArrowWriteableKey>(
        &self,
        split_size: usize,
    ) -> (CompositeKey, SpannPostingListDelta) {
        let split_info = self.split_internal::<K>(split_size);
        let mut write_guard = self.inner.write();
        write_guard.size_tracker = write_guard.size_tracker - split_info.remaining_size;
        let new_storage = write_guard.storage.split_off(&split_info.split_key);
        (
            split_info.split_key,
            SpannPostingListDelta {
                inner: Arc::new(RwLock::new(Inner {
                    storage: new_storage,
                    size_tracker: split_info.remaining_size,
                })),
            },
        )
    }

    pub fn get_min_key(&self) -> Option<CompositeKey> {
        self.inner.read().storage.keys().next().cloned()
    }

    pub(super) fn len(&self) -> usize {
        self.inner.read().storage.len()
    }

    pub(super) fn into_arrow(
        self,
        mut key_builder: BlockKeyArrowBuilder,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        let inner = Arc::try_unwrap(self.inner)
            .expect(
                "Invariant violation: SingleColumnStorage inner should have only one reference.",
            )
            .into_inner();
        let storage = inner.storage;

        let mut value_builder =
            <&SpannPostingList as ArrowWriteableValue>::get_arrow_builder(inner.size_tracker);

        for (key, value) in storage.into_iter() {
            key_builder.add_key(key);
            <&SpannPostingList as ArrowWriteableValue>::append(value, &mut value_builder);
        }

        // Build arrow key with fields.
        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let (struct_field, value_arr) =
            <&SpannPostingList as ArrowWriteableValue>::finish(value_builder, &inner.size_tracker);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            struct_field,
        ]));
        RecordBatch::try_new(schema, vec![prefix_arr, key_arr, value_arr])
    }
}
