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
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Arc},
};

#[derive(Clone, Debug)]
pub struct DataRecordStorage {
    // TODO: move to one locked struct so we don't have to lock multiple times
    pub(crate) id_storage: Arc<RwLock<BTreeMap<CompositeKey, String>>>,
    pub(crate) embedding_storage: Arc<RwLock<BTreeMap<CompositeKey, Vec<f32>>>>,
    pub(crate) metadata_storage: Arc<RwLock<BTreeMap<CompositeKey, Option<Vec<u8>>>>>,
    pub(crate) document_storage: Arc<RwLock<BTreeMap<CompositeKey, Option<String>>>>,

    // size-tracking variables
    pub(in crate::arrow::block) prefix_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) key_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) id_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) embedding_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) metadata_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) document_size: Arc<AtomicUsize>,
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
            id_storage: Arc::new(RwLock::new(BTreeMap::new())),
            embedding_storage: Arc::new(RwLock::new(BTreeMap::new())),
            metadata_storage: Arc::new(RwLock::new(BTreeMap::new())),
            document_storage: Arc::new(RwLock::new(BTreeMap::new())),
            prefix_size: AtomicUsize::new(0).into(),
            key_size: AtomicUsize::new(0).into(),
            id_size: AtomicUsize::new(0).into(),
            embedding_size: AtomicUsize::new(0).into(),
            metadata_size: AtomicUsize::new(0).into(),
            document_size: AtomicUsize::new(0).into(),
        }
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        self.prefix_size.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn get_key_size(&self) -> usize {
        self.key_size.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_id_size(&self) -> usize {
        self.id_size.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn get_embedding_size(&self) -> usize {
        self.embedding_size
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn get_metadata_size(&self) -> usize {
        self.metadata_size.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn get_document_size(&self) -> usize {
        self.document_size.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn get_total_embedding_count(&self) -> usize {
        let embedding_storage = self.embedding_storage.read();
        embedding_storage
            .iter()
            .fold(0, |acc, (_, value)| acc + value.len())
    }

    pub fn add(&self, prefix: &str, key: KeyWrapper, value: &DataRecord<'_>) {
        let mut id_storage = self.id_storage.write();
        let mut embedding_storage = self.embedding_storage.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        id_storage.insert(composite_key.clone(), value.id.to_string());
        self.id_size
            .fetch_add(value.id.len(), std::sync::atomic::Ordering::SeqCst);
        self.prefix_size.fetch_add(
            composite_key.prefix.len(),
            std::sync::atomic::Ordering::SeqCst,
        );
        self.key_size.fetch_add(
            composite_key.key.get_size(),
            std::sync::atomic::Ordering::SeqCst,
        );
        embedding_storage.insert(composite_key.clone(), value.embedding.to_vec());
        self.embedding_size.fetch_add(
            value.embedding.len() * 4,
            std::sync::atomic::Ordering::SeqCst,
        );

        match &value.metadata {
            Some(metadata) => {
                let mut metadata_storage = self.metadata_storage.write();
                let metadata_proto = Into::<UpdateMetadata>::into(metadata.clone());
                let metadata_as_bytes = metadata_proto.encode_to_vec();
                let metadata_size = metadata_as_bytes.len();
                metadata_storage.insert(composite_key.clone(), Some(metadata_as_bytes));
                self.metadata_size
                    .fetch_add(metadata_size, std::sync::atomic::Ordering::SeqCst);
            }
            None => {
                let mut metadata_storage = self.metadata_storage.write();
                metadata_storage.insert(composite_key.clone(), None);
            }
        }

        let mut document_storage = self.document_storage.write();
        let document_len = value.document.unwrap_or_default().len();
        document_storage.insert(
            composite_key,
            value.document.map_or(None, |doc| Some(doc.to_string())),
        );
        self.document_size
            .fetch_add(document_len, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let prefix_size = bit_util::round_upto_multiple_of_64(self.get_prefix_size());
        let key_size = bit_util::round_upto_multiple_of_64(self.get_key_size());

        let id_size = bit_util::round_upto_multiple_of_64(self.get_id_size());
        let embedding_size = bit_util::round_upto_multiple_of_64(self.get_embedding_size());
        let metadata_size = bit_util::round_upto_multiple_of_64(self.get_metadata_size());
        let document_size = bit_util::round_upto_multiple_of_64(self.get_document_size());

        // offset sizing
        let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let key_offset_bytes: usize = K::offset_size(self.len());
        let id_offset = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let metdata_offset = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let document_offset = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);

        // validity sizing both document and metadata can be null
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

    fn split_internal(&self, split_size: usize) -> SplitInformation {
        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut id_size = 0;
        let mut embedding_size = 0;
        let mut metadata_size = 0;
        let mut document_size = 0;

        let id_storage = self.id_storage.read();
        let embedding_storage = self.embedding_storage.read();
        let metadata_storage = self.metadata_storage.read();
        let document_storage = self.document_storage.read();

        let mut index = 0;
        for ((((key, id), (_, embedding)), (_, metadata)), (_, document)) in id_storage
            .iter()
            .zip(embedding_storage.iter())
            .zip(metadata_storage.iter())
            .zip(document_storage.iter())
        {
            prefix_size += key.prefix.len();
            key_size += key.key.get_size();
            id_size += id.len();
            embedding_size += embedding.len() * 4;
            metadata_size += metadata.as_ref().map_or(0, |v| v.len());
            document_size += document.as_ref().map_or(0, |v| v.len());

            // offset sizing
            let id_offset = bit_util::round_upto_multiple_of_64((index + 1 + 1) * 4);
            let metdata_offset = bit_util::round_upto_multiple_of_64((index + 1 + 1) * 4);
            let document_offset = bit_util::round_upto_multiple_of_64((index + 1 + 1) * 4);

            // validity sizing both document and metadata can be null
            let validity_bytes =
                bit_util::round_upto_multiple_of_64(bit_util::ceil(index + 1, 8)) * 2;

            // round all running sizes to 64 and add them together
            let total_size = bit_util::round_upto_multiple_of_64(prefix_size)
                + bit_util::round_upto_multiple_of_64(key_size)
                + bit_util::round_upto_multiple_of_64(id_size)
                + bit_util::round_upto_multiple_of_64(embedding_size)
                + bit_util::round_upto_multiple_of_64(metadata_size)
                + bit_util::round_upto_multiple_of_64(document_size)
                + id_offset
                + metdata_offset
                + document_offset
                + validity_bytes;

            if total_size > split_size {
                break;
            }
            index += 1;
        }

        let curr_split_index = std::cmp::min(index + 1, self.len() - 1);
        let split_key = self.get_key(curr_split_index);

        return SplitInformation {
            split_key,
            remaining_prefix_size: self.get_prefix_size() - prefix_size,
            remaining_key_size: self.get_key_size() - key_size,
            remaining_id_size: self.get_id_size() - id_size,
            remaining_embedding_size: self.get_embedding_size() - embedding_size,
            remaining_metadata_size: self.get_metadata_size() - metadata_size,
            remaining_document_size: self.get_document_size() - document_size,
        };
    }

    pub(super) fn split(&self, split_size: usize) -> (CompositeKey, DataRecordStorage) {
        let split_info = self.split_internal(split_size);
        let split_id = self.id_storage.write().split_off(&split_info.split_key);
        let split_embedding = self
            .embedding_storage
            .write()
            .split_off(&split_info.split_key);
        let split_metadata = self
            .metadata_storage
            .write()
            .split_off(&split_info.split_key);
        let split_document = self
            .document_storage
            .write()
            .split_off(&split_info.split_key);

        // split should reduce MY storage size by the removed amount
        let drs = DataRecordStorage {
            id_storage: Arc::new(RwLock::new(split_id)),
            embedding_storage: Arc::new(RwLock::new(split_embedding)),
            metadata_storage: Arc::new(RwLock::new(split_metadata)),
            document_storage: Arc::new(RwLock::new(split_document)),

            prefix_size: AtomicUsize::new(split_info.remaining_prefix_size).into(),
            key_size: AtomicUsize::new(split_info.remaining_key_size).into(),
            id_size: AtomicUsize::new(split_info.remaining_id_size).into(),
            embedding_size: AtomicUsize::new(split_info.remaining_embedding_size).into(),
            metadata_size: AtomicUsize::new(split_info.remaining_metadata_size).into(),
            document_size: AtomicUsize::new(split_info.remaining_document_size).into(),
        };

        (split_info.split_key, drs)
    }

    pub(super) fn len(&self) -> usize {
        let id_storage = self.id_storage.read();
        id_storage.len()
    }

    fn get_key(&self, index: usize) -> CompositeKey {
        // TODO: this is another inner N^2 loop
        let id_storage = self.id_storage.read();
        let (key, _) = id_storage.iter().nth(index).unwrap();
        key.clone()
    }

    pub(super) fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let id_storage = self.id_storage.read();
        let mut builder = builder;
        for (key, _) in id_storage.iter() {
            builder.add_key(key.clone());
        }
        builder
    }

    pub(super) fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.len();
        let mut embedding_builder;
        let mut id_builder;
        let mut metadata_builder;
        let mut document_builder;
        let embedding_len;
        if item_capacity == 0 {
            // ok to initialize fixed size float list with fixed size as 0.
            embedding_len = 0;
            embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), 0);
            id_builder = StringBuilder::new();
            metadata_builder = BinaryBuilder::new();
            document_builder = StringBuilder::new();
        } else {
            embedding_len = self.embedding_storage.read().iter().next().unwrap().1.len() as i32;
            id_builder = StringBuilder::with_capacity(item_capacity, self.get_id_size());
            embedding_builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(self.get_total_embedding_count()),
                embedding_len,
                item_capacity,
            );
            metadata_builder =
                BinaryBuilder::with_capacity(item_capacity, self.get_metadata_size());
            document_builder =
                StringBuilder::with_capacity(item_capacity, self.get_document_size());
        }

        let id_storage = self.id_storage.read();
        let embedding_storage = self.embedding_storage.read();
        let metadata_storage = self.metadata_storage.read();
        let document_storage = self.document_storage.read();
        let iter = id_storage
            .iter()
            .zip(embedding_storage.iter())
            .zip(metadata_storage.iter())
            .zip(document_storage.iter());
        for ((((_, id), (_, embedding)), (_, metadata)), (_, document)) in iter {
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
                embedding_len,
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
