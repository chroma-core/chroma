use super::BlockKeyArrowBuilder;
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
    prefix_size: usize,
    key_size: usize,
    id_size: usize,
    embedding_size: usize,
    metadata_size: usize,
    document_size: usize,
}

#[derive(Clone, Debug)]
pub struct DataRecordStorage {
    inner: Arc<RwLock<Inner>>,
}

struct SplitInformation {
    split_key: CompositeKey,
    remaining_prefix_size: usize,
    remaining_key_size: usize,
    remaining_id_size: usize,
    remaining_embedding_size: usize,
    remaining_metadata_size: usize,
    remaining_document_size: usize,
}

impl DataRecordStorage {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                storage: BTreeMap::new(),
                prefix_size: 0,
                key_size: 0,
                id_size: 0,
                embedding_size: 0,
                metadata_size: 0,
                document_size: 0,
            })),
        }
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        let inner = self.inner.read();
        inner.prefix_size
    }

    pub(super) fn get_key_size(&self) -> usize {
        let inner = self.inner.read();
        inner.key_size
    }

    pub fn get_id_size(&self) -> usize {
        let inner = self.inner.read();
        inner.id_size
    }

    fn get_embedding_size(&self) -> usize {
        let inner = self.inner.read();
        inner.embedding_size
    }

    fn get_metadata_size(&self) -> usize {
        let inner = self.inner.read();
        inner.metadata_size
    }

    fn get_document_size(&self) -> usize {
        let inner = self.inner.read();
        inner.document_size
    }

    pub fn add(&self, prefix: &str, key: KeyWrapper, value: &DataRecord<'_>) {
        let mut inner = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        if inner.storage.contains_key(&composite_key) {
            // key already exists, subtract the old size
            // unwraps are safe because we just checked if the key exists
            let old_id_size = inner.storage.get(&composite_key).unwrap().0.len();
            inner.id_size -= old_id_size;

            let old_embedding_size = inner.storage.get(&composite_key).unwrap().1.len() * 4;
            inner.embedding_size -= old_embedding_size;

            let old_metadata_size = inner
                .storage
                .get(&composite_key)
                .unwrap()
                .2
                .as_ref()
                .map_or(0, |v| v.len());
            inner.metadata_size -= old_metadata_size;

            let old_document_size = inner
                .storage
                .get(&composite_key)
                .unwrap()
                .3
                .as_ref()
                .map_or(0, |v| v.len());
            inner.document_size -= old_document_size;
            inner.prefix_size -= composite_key.prefix.len();
            inner.key_size -= composite_key.key.get_size();
        }

        let prefix_size = composite_key.prefix.len();
        let key_size = composite_key.key.get_size();

        let prepared = <&chroma_types::DataRecord>::prepare(value);

        inner.id_size += prepared.0.len();
        inner.embedding_size += prepared.1.len() * 4;
        inner.metadata_size += prepared.2.as_ref().map_or(0, |v| v.len());
        inner.document_size += prepared.3.as_ref().map_or(0, |v| v.len());
        inner.prefix_size += prefix_size;
        inner.key_size += key_size;

        inner.storage.insert(composite_key.clone(), prepared);
    }

    pub fn delete(&self, prefix: &str, key: KeyWrapper) {
        let mut inner = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        let maybe_removed_entry = inner.storage.remove(&composite_key);

        if let Some((remove_id, remove_embedding, remove_metadata, remove_document)) =
            maybe_removed_entry
        {
            inner.prefix_size -= composite_key.prefix.len();
            inner.key_size -= composite_key.key.get_size();
            inner.id_size -= remove_id.len();
            inner.embedding_size -= remove_embedding.len() * 4;
            inner.metadata_size -= remove_metadata.as_ref().map_or(0, |v| v.len());
            inner.document_size -= remove_document.as_ref().map_or(0, |v| v.len());
        }
    }

    pub fn get_min_key(&self) -> Option<CompositeKey> {
        let inner = self.inner.read();
        inner.storage.keys().next().cloned()
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let prefix_size = bit_util::round_upto_multiple_of_64(self.get_prefix_size());
        let key_size = bit_util::round_upto_multiple_of_64(self.get_key_size());

        let id_size = bit_util::round_upto_multiple_of_64(self.get_id_size());
        let embedding_size = bit_util::round_upto_multiple_of_64(self.get_embedding_size());
        let metadata_size = bit_util::round_upto_multiple_of_64(self.get_metadata_size());
        let document_size = bit_util::round_upto_multiple_of_64(self.get_document_size());

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
        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut id_size = 0;
        let mut embedding_size = 0;
        let mut metadata_size = 0;
        let mut document_size = 0;
        let mut item_count = 0;
        let mut split_key = None;

        let inner = self.inner.read();
        let mut iter = inner.storage.iter();

        while let Some((key, (id, embedding, metadata, document))) = iter.next() {
            prefix_size += key.prefix.len();
            key_size += key.key.get_size();
            id_size += id.len();
            embedding_size += embedding.len() * 4;
            metadata_size += metadata.as_ref().map_or(0, |v| v.len());
            document_size += document.as_ref().map_or(0, |v| v.len());
            item_count += 1;

            // offset sizing
            // https://docs.rs/arrow-buffer/52.2.0/arrow_buffer/buffer/struct.OffsetBuffer.html
            // 4 bytes per offset entry, n+1 entries
            let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let key_offset_bytes: usize = K::offset_size(item_count);
            let id_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let metdata_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let document_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);

            // validity sizing both document and metadata can be null
            let validity_bytes =
                bit_util::round_upto_multiple_of_64(bit_util::ceil(item_count, 8)) * 2;

            // round all running sizes to 64 and add them together
            let total_size = bit_util::round_upto_multiple_of_64(prefix_size)
                + bit_util::round_upto_multiple_of_64(key_size)
                + bit_util::round_upto_multiple_of_64(id_size)
                + bit_util::round_upto_multiple_of_64(embedding_size)
                + bit_util::round_upto_multiple_of_64(metadata_size)
                + bit_util::round_upto_multiple_of_64(document_size)
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
                        prefix_size -= key.prefix.len();
                        key_size -= key.key.get_size();
                        id_size -= id.len();
                        embedding_size -= embedding.len() * 4;
                        metadata_size -= metadata.as_ref().map_or(0, |v| v.len());
                        document_size -= document.as_ref().map_or(0, |v| v.len());
                        Some(key.clone())
                    }
                };
                break;
            }
        }

        SplitInformation {
            split_key: split_key.expect("split key should be set"),
            remaining_prefix_size: inner.prefix_size - prefix_size,
            remaining_key_size: inner.key_size - key_size,
            remaining_id_size: inner.id_size - id_size,
            remaining_embedding_size: inner.embedding_size - embedding_size,
            remaining_metadata_size: inner.metadata_size - metadata_size,
            remaining_document_size: inner.document_size - document_size,
        }
    }

    pub(super) fn split<K: ArrowWriteableKey>(
        &self,
        split_size: usize,
    ) -> (CompositeKey, DataRecordStorage) {
        let split_info = self.split_internal::<K>(split_size);
        let mut inner = self.inner.write();
        let split_storage = inner.storage.split_off(&split_info.split_key);
        inner.prefix_size -= split_info.remaining_prefix_size;
        inner.key_size -= split_info.remaining_key_size;
        inner.id_size -= split_info.remaining_id_size;
        inner.embedding_size -= split_info.remaining_embedding_size;
        inner.metadata_size -= split_info.remaining_metadata_size;
        inner.document_size -= split_info.remaining_document_size;

        let drs = DataRecordStorage {
            inner: Arc::new(RwLock::new(Inner {
                storage: split_storage,
                prefix_size: split_info.remaining_prefix_size,
                key_size: split_info.remaining_key_size,
                id_size: split_info.remaining_id_size,
                embedding_size: split_info.remaining_embedding_size,
                metadata_size: split_info.remaining_metadata_size,
                document_size: split_info.remaining_document_size,
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
        key_builder: BlockKeyArrowBuilder,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        // build arrow key.
        let mut key_builder = key_builder;
        let mut value_builder = <&DataRecord as ArrowWriteableValue>::get_arrow_builder();

        match Arc::try_unwrap(self.inner) {
            Ok(inner) => {
                let inner = inner.into_inner();
                let storage = inner.storage;

                for (key, value) in storage.into_iter() {
                    key_builder.add_key(key);
                    <&DataRecord as ArrowWriteableValue>::append(value, &mut value_builder);
                }
            }
            Err(_) => {
                panic!("Invariant violation: SingleColumnStorage inner should have only one reference.");
            }
        }
        // Build arrow key with fields.
        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let (struct_field, value_arr) = <&DataRecord as ArrowWriteableValue>::finish(value_builder);

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            struct_field,
        ]));
        RecordBatch::try_new(schema, vec![prefix_arr, key_arr, value_arr])
    }
}
