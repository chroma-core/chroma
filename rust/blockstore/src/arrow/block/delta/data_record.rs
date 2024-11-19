use super::data_record_size_tracker::DataRecordSizeTracker;
use super::BlockKeyArrowBuilder;
use crate::arrow::block::value::data_record_value::DataRecordStorageEntry;
use crate::arrow::types::ArrowWriteableValue;
use crate::{
    arrow::types::ArrowWriteableKey,
    key::{CompositeKey, KeyWrapper},
};
use arrow::{array::RecordBatch, util::bit_util};
use chroma_types::DataRecord;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug)]
struct Inner {
    storage: BTreeMap<
        CompositeKey,
        <&'static chroma_types::DataRecord<'static> as ArrowWriteableValue>::PreparedValue,
    >,
    size_tracker: DataRecordSizeTracker,
}

#[derive(Clone, Debug)]
pub struct DataRecordStorage {
    inner: Arc<RwLock<Inner>>,
}

struct SplitInformation {
    split_key: CompositeKey,
    remaining_size: DataRecordSizeTracker,
}

impl DataRecordStorage {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                storage: BTreeMap::new(),
                size_tracker: DataRecordSizeTracker::new(),
            })),
        }
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        let inner = self.inner.read();
        inner.size_tracker.get_prefix_size()
    }

    pub(super) fn get_key_size(&self) -> usize {
        let inner = self.inner.read();
        inner.size_tracker.get_key_size()
    }

    pub fn get_owned_value(&self, prefix: &str, key: KeyWrapper) -> Option<DataRecordStorageEntry> {
        let inner = self.inner.read();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };
        inner.storage.get(&composite_key).cloned()
    }

    pub fn add(&self, prefix: &str, key: KeyWrapper, value: &DataRecord<'_>) {
        let mut inner = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        if let Some(previous_entry) = inner.storage.remove(&composite_key) {
            // key already exists, subtract the old size
            inner.size_tracker.subtract_value_size(&previous_entry);
            inner
                .size_tracker
                .subtract_prefix_size(composite_key.prefix.len());
            inner
                .size_tracker
                .subtract_key_size(composite_key.key.get_size());
            inner.size_tracker.decrement_item_count();
        }

        let prefix_size = composite_key.prefix.len();
        let key_size = composite_key.key.get_size();

        let prepared = <&chroma_types::DataRecord>::prepare(value);

        inner.size_tracker.add_value_size(&prepared);
        inner.size_tracker.add_prefix_size(prefix_size);
        inner.size_tracker.add_key_size(key_size);
        inner.size_tracker.increment_item_count();

        inner.storage.insert(composite_key.clone(), prepared);
    }

    pub fn delete(&self, prefix: &str, key: KeyWrapper) {
        let mut inner = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        let maybe_removed_entry = inner.storage.remove(&composite_key);

        if let Some(removed_entry) = maybe_removed_entry {
            inner
                .size_tracker
                .subtract_prefix_size(composite_key.prefix.len());
            inner
                .size_tracker
                .subtract_key_size(composite_key.key.get_size());
            inner.size_tracker.subtract_value_size(&removed_entry);
            inner.size_tracker.decrement_item_count();
        }
    }

    pub fn get_min_key(&self) -> Option<CompositeKey> {
        let inner = self.inner.read();
        inner.storage.keys().next().cloned()
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let inner = self.inner.read();
        let prefix_size = bit_util::round_upto_multiple_of_64(inner.size_tracker.get_prefix_size());
        let key_size = bit_util::round_upto_multiple_of_64(inner.size_tracker.get_key_size());

        let id_size = bit_util::round_upto_multiple_of_64(inner.size_tracker.get_id_size());
        let embedding_size =
            bit_util::round_upto_multiple_of_64(inner.size_tracker.get_embedding_size());
        let metadata_size =
            bit_util::round_upto_multiple_of_64(inner.size_tracker.get_metadata_size());
        let document_size =
            bit_util::round_upto_multiple_of_64(inner.size_tracker.get_document_size());

        // offset sizing
        // https://docs.rs/arrow-buffer/52.2.0/arrow_buffer/buffer/struct.OffsetBuffer.html
        // 4 bytes per offset entry, n+1 entries
        let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let key_offset_bytes: usize = K::offset_size(self.len());
        let id_offset = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let metdata_offset = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let document_offset = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);

        // validity sizing both document and metadata can be null
        // https://docs.rs/arrow-buffer/52.2.0/src/arrow_buffer/buffer/null.rs.html#153-155
        let validity_bytes = bit_util::round_upto_multiple_of_64(bit_util::ceil(self.len(), 8)) * 2;

        prefix_size
            + key_size
            + id_size
            + embedding_size
            + metadata_size
            + document_size
            + prefix_offset_bytes
            + key_offset_bytes
            + id_offset
            + metdata_offset
            + document_offset
            + validity_bytes
    }

    fn split_internal<K: ArrowWriteableKey>(&self, split_size: usize) -> SplitInformation {
        let mut size_up_to_split_key = DataRecordSizeTracker::new();
        let mut split_key = None;

        let inner = self.inner.read();
        let mut iter = inner.storage.iter();

        while let Some((key, entry)) = iter.next() {
            size_up_to_split_key.add_prefix_size(key.prefix.len());
            size_up_to_split_key.add_key_size(key.key.get_size());
            size_up_to_split_key.add_value_size(entry);
            size_up_to_split_key.increment_item_count();

            // offset sizing
            // https://docs.rs/arrow-buffer/52.2.0/arrow_buffer/buffer/struct.OffsetBuffer.html
            // 4 bytes per offset entry, n+1 entries
            let item_count = size_up_to_split_key.get_num_items();
            let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let key_offset_bytes: usize = K::offset_size(item_count);
            let id_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let metdata_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let document_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);

            // validity sizing both document and metadata can be null
            let validity_bytes =
                bit_util::round_upto_multiple_of_64(bit_util::ceil(item_count, 8)) * 2;

            // round all running sizes to 64 and add them together
            let total_size =
                bit_util::round_upto_multiple_of_64(size_up_to_split_key.get_prefix_size())
                    + bit_util::round_upto_multiple_of_64(size_up_to_split_key.get_key_size())
                    + bit_util::round_upto_multiple_of_64(size_up_to_split_key.get_id_size())
                    + bit_util::round_upto_multiple_of_64(
                        size_up_to_split_key.get_embedding_size(),
                    )
                    + bit_util::round_upto_multiple_of_64(size_up_to_split_key.get_metadata_size())
                    + bit_util::round_upto_multiple_of_64(size_up_to_split_key.get_document_size())
                    + prefix_offset_bytes
                    + key_offset_bytes
                    + id_offset
                    + metdata_offset
                    + document_offset
                    + validity_bytes;

            if total_size > split_size {
                split_key = match iter.next() {
                    Some((key, _)) => Some(key.clone()),
                    None => {
                        // Remove the last item since we are splitting at the end
                        size_up_to_split_key.subtract_prefix_size(key.prefix.len());
                        size_up_to_split_key.subtract_key_size(key.key.get_size());
                        size_up_to_split_key.subtract_value_size(entry);
                        size_up_to_split_key.decrement_item_count();
                        Some(key.clone())
                    }
                };
                break;
            }
        }

        SplitInformation {
            split_key: split_key.expect("split key should be set"),
            remaining_size: inner.size_tracker - size_up_to_split_key,
        }
    }

    pub(super) fn split<K: ArrowWriteableKey>(
        &self,
        split_size: usize,
    ) -> (CompositeKey, DataRecordStorage) {
        let split_info = self.split_internal::<K>(split_size);
        let mut inner = self.inner.write();
        let split_storage = inner.storage.split_off(&split_info.split_key);
        inner.size_tracker = inner.size_tracker - split_info.remaining_size;

        let drs = DataRecordStorage {
            inner: Arc::new(RwLock::new(Inner {
                storage: split_storage,
                size_tracker: split_info.remaining_size,
            })),
        };

        (split_info.split_key, drs)
    }

    pub(super) fn len(&self) -> usize {
        let inner = self.inner.read();
        inner.storage.len()
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
            <&DataRecord as ArrowWriteableValue>::get_arrow_builder(inner.size_tracker);

        for (key, value) in storage.into_iter() {
            key_builder.add_key(key);
            <&DataRecord as ArrowWriteableValue>::append(value, &mut value_builder);
        }

        // Build arrow key with fields.
        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let (struct_field, value_arr) =
            <&DataRecord as ArrowWriteableValue>::finish(value_builder, &inner.size_tracker);

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            struct_field,
        ]));
        RecordBatch::try_new(schema, vec![prefix_arr, key_arr, value_arr])
    }
}
