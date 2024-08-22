use super::BlockKeyArrowBuilder;
use crate::{
    arrow::types::ArrowWriteableKey,
    key::{CompositeKey, KeyWrapper},
};
use arrow::{
    array::{
        Array, ArrayRef, BinaryBuilder, FixedSizeListBuilder, Float32Builder, StringBuilder,
        StructArray,
    },
    datatypes::{Field, Fields},
    util::bit_util,
};
use chroma_types::{chroma_proto::UpdateMetadata, DataRecord};
use parking_lot::RwLock;
use prost::Message;
use std::{collections::BTreeMap, sync::Arc};

// Convenience type for the storage entry
// (id, embedding, metadata, document)
type DataRecordStorageEntry = (String, Vec<f32>, Option<Vec<u8>>, Option<String>);

#[derive(Debug)]
struct Inner {
    storage: BTreeMap<CompositeKey, DataRecordStorageEntry>,
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
        let id_size = value.id.len();
        let embedding_size = value.embedding.len() * 4;
        let mut metadata_size = 0;
        let mut document_size = 0;

        let id = value.id.to_string();
        let embedding = value.embedding.to_vec();
        let metadata = match &value.metadata {
            Some(metadata) => {
                let metadata_proto = Into::<UpdateMetadata>::into(metadata.clone());
                let metadata_as_bytes = metadata_proto.encode_to_vec();
                metadata_size = metadata_as_bytes.len();
                Some(metadata_as_bytes)
            }
            None => None,
        };
        let document = match value.document {
            Some(document) => {
                document_size = document.len();
                Some(document.to_string())
            }
            None => None,
        };
        inner
            .storage
            .insert(composite_key.clone(), (id, embedding, metadata, document));
        inner.id_size += id_size;
        inner.embedding_size += embedding_size;
        inner.metadata_size += metadata_size;
        inner.document_size += document_size;
        inner.prefix_size += prefix_size;
        inner.key_size += key_size;
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

        let total_size = prefix_size
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
            + validity_bytes;

        total_size
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

        return SplitInformation {
            split_key: split_key.expect("split key should be set"),
            remaining_prefix_size: inner.prefix_size - prefix_size,
            remaining_key_size: inner.key_size - key_size,
            remaining_id_size: inner.id_size - id_size,
            remaining_embedding_size: inner.embedding_size - embedding_size,
            remaining_metadata_size: inner.metadata_size - metadata_size,
            remaining_document_size: inner.document_size - document_size,
        };
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

    pub(super) fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let inner = self.inner.read();
        let mut builder = builder;
        for (key, _) in inner.storage.iter() {
            builder.add_key(key.clone());
        }
        builder
    }

    pub(super) fn to_arrow(&self) -> (Field, ArrayRef) {
        let inner = self.inner.read();

        let item_capacity = inner.storage.len();
        let mut embedding_builder;
        let mut id_builder;
        let mut metadata_builder;
        let mut document_builder;
        let embedding_dim;
        if item_capacity == 0 {
            // ok to initialize fixed size float list with fixed size as 0.
            embedding_dim = 0;
            embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), 0);
            id_builder = StringBuilder::new();
            metadata_builder = BinaryBuilder::new();
            document_builder = StringBuilder::new();
        } else {
            embedding_dim = inner.storage.iter().next().unwrap().1 .1.len();
            // Assumes all embeddings are of the same length, which is guaranteed by calling code
            // TODO: validate this assumption by throwing an error if it's not true
            let total_embedding_count = embedding_dim * item_capacity;
            id_builder = StringBuilder::with_capacity(item_capacity, inner.id_size);
            embedding_builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(total_embedding_count),
                embedding_dim as i32,
                item_capacity,
            );
            metadata_builder = BinaryBuilder::with_capacity(item_capacity, inner.metadata_size);
            document_builder = StringBuilder::with_capacity(item_capacity, inner.document_size);
        }

        let iter = inner.storage.iter();
        for (_key, (id, embedding, metadata, document)) in iter {
            id_builder.append_value(id);
            let embedding_arr = embedding_builder.values();
            for entry in embedding.iter() {
                embedding_arr.append_value(*entry);
            }
            embedding_builder.append(true);
            metadata_builder.append_option(metadata.as_deref());
            document_builder.append_option(document.as_deref());
        }

        let id_field = Field::new("id", arrow::datatypes::DataType::Utf8, true);
        let embedding_field = Field::new(
            "embedding",
            arrow::datatypes::DataType::FixedSizeList(
                Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Float32,
                    true,
                )),
                embedding_dim as i32,
            ),
            true,
        );
        let metadata_field = Field::new("metadata", arrow::datatypes::DataType::Binary, true);
        let document_field = Field::new("document", arrow::datatypes::DataType::Utf8, true);

        let id_arr = id_builder.finish();
        let embedding_arr = embedding_builder.finish();
        let metadata_arr = metadata_builder.finish();
        let document_arr = document_builder.finish();

        let struct_arr = StructArray::from(vec![
            (Arc::new(id_field.clone()), Arc::new(id_arr) as ArrayRef),
            (
                Arc::new(embedding_field.clone()),
                Arc::new(embedding_arr) as ArrayRef,
            ),
            (
                Arc::new(metadata_field.clone()),
                Arc::new(metadata_arr) as ArrayRef,
            ),
            (
                Arc::new(document_field.clone()),
                Arc::new(document_arr) as ArrayRef,
            ),
        ]);
        let struct_fields = Fields::from(vec![
            id_field,
            embedding_field,
            metadata_field,
            document_field,
        ]);
        let struct_field = Field::new(
            "value",
            arrow::datatypes::DataType::Struct(struct_fields),
            true,
        );
        (
            struct_field,
            (&struct_arr as &dyn Array).slice(0, struct_arr.len()),
        )
    }
}
