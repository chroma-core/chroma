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

        if id_storage.contains_key(&composite_key) {
            // key already exists, subtract the old size
            // unwraps are safe because we just checked if the key exists
            let old_id_size = id_storage.get(&composite_key).unwrap().len();
            self.id_size
                .fetch_sub(old_id_size, std::sync::atomic::Ordering::SeqCst);

            let old_embedding_size = embedding_storage.get(&composite_key).unwrap().len() * 4;
            self.embedding_size
                .fetch_sub(old_embedding_size, std::sync::atomic::Ordering::SeqCst);

            let old_metadata_size = self
                .metadata_storage
                .read()
                .get(&composite_key)
                .unwrap()
                .as_ref()
                .map_or(0, |v| v.len());
            self.metadata_size
                .fetch_sub(old_metadata_size, std::sync::atomic::Ordering::SeqCst);

            let old_document_size = self
                .document_storage
                .read()
                .get(&composite_key)
                .unwrap()
                .as_ref()
                .map_or(0, |v| v.len());
            self.document_size
                .fetch_sub(old_document_size, std::sync::atomic::Ordering::SeqCst);

            self.prefix_size.fetch_sub(
                composite_key.prefix.len(),
                std::sync::atomic::Ordering::SeqCst,
            );
            self.key_size.fetch_sub(
                composite_key.key.get_size(),
                std::sync::atomic::Ordering::SeqCst,
            );
        }

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

    pub fn delete(&self, prefix: &str, key: KeyWrapper) {
        let mut id_storage = self.id_storage.write();
        let mut embedding_storage = self.embedding_storage.write();
        let mut metadata_storage = self.metadata_storage.write();
        let mut document_storage = self.document_storage.write();

        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        let maybe_removed_id = id_storage.remove(&composite_key);
        let maybe_removed_embedding = embedding_storage.remove(&composite_key);
        let maybe_removed_metadata = metadata_storage.remove(&composite_key);
        let maybe_removed_document = document_storage.remove(&composite_key);

        if let Some(id) = maybe_removed_id {
            self.id_size
                .fetch_sub(id.len(), std::sync::atomic::Ordering::SeqCst);
            self.prefix_size.fetch_sub(
                composite_key.prefix.len(),
                std::sync::atomic::Ordering::SeqCst,
            );
            self.key_size.fetch_sub(
                composite_key.key.get_size(),
                std::sync::atomic::Ordering::SeqCst,
            );
            self.embedding_size.fetch_sub(
                maybe_removed_embedding.as_ref().map_or(0, |v| v.len() * 4),
                std::sync::atomic::Ordering::SeqCst,
            );
            if let Some(metadata) = maybe_removed_metadata {
                match metadata {
                    Some(metadata) => {
                        self.metadata_size
                            .fetch_sub(metadata.len(), std::sync::atomic::Ordering::SeqCst);
                    }
                    None => {}
                }
            }
            if let Some(document) = maybe_removed_document {
                match document {
                    Some(document) => {
                        self.document_size
                            .fetch_sub(document.len(), std::sync::atomic::Ordering::SeqCst);
                    }
                    None => {}
                }
            }
        }
    }

    pub fn get_min_key(&self) -> Option<CompositeKey> {
        let id_storage = self.id_storage.read();
        id_storage.keys().next().cloned()
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

    fn split_internal<K: ArrowWriteableKey>(&self, split_size: usize) -> SplitInformation {
        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut id_size = 0;
        let mut embedding_size = 0;
        let mut metadata_size = 0;
        let mut document_size = 0;
        let mut item_count = 0;
        let mut split_key = None;

        let id_storage = self.id_storage.read();
        let embedding_storage = self.embedding_storage.read();
        let metadata_storage = self.metadata_storage.read();
        let document_storage = self.document_storage.read();

        let mut iter = id_storage
            .iter()
            .zip(embedding_storage.iter())
            .zip(metadata_storage.iter())
            .zip(document_storage.iter());

        while let Some(((((key, id), (_, embedding)), (_, metadata)), (_, document))) = iter.next()
        {
            prefix_size += key.prefix.len();
            key_size += key.key.get_size();
            id_size += id.len();
            embedding_size += embedding.len() * 4;
            metadata_size += metadata.as_ref().map_or(0, |v| v.len());
            document_size += document.as_ref().map_or(0, |v| v.len());
            item_count += 1;

            // offset sizing
            let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let key_offset_bytes: usize = K::offset_size(item_count);
            let id_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let metdata_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
            let document_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);

            // validity sizing both document and metadata can be null
            let validity_bytes =
                bit_util::round_upto_multiple_of_64(bit_util::ceil(item_count + 1, 8)) * 2;

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
                println!(
                    "[HAMMAD DATA RECORD] the total size is: {} and we are splitting at item: {} in a block of length: {}",
                    total_size, item_count, self.len()
                );
                println!(
                    "The prefix size in the left half is: {} and the overall size is: {}",
                    prefix_size,
                    self.get_prefix_size()
                );
                println!(
                    "The key size in the left half is: {} and the overall size is: {}",
                    key_size,
                    self.get_key_size()
                );
                println!(
                    "The id size in the left half is: {} and the overall size is: {}",
                    id_size,
                    self.get_id_size()
                );
                println!(
                    "The embedding size in the left half is: {} and the overall size is: {}",
                    embedding_size,
                    self.get_embedding_size()
                );
                println!(
                    "The metadata size in the left half is: {} and the overall size is: {}",
                    metadata_size,
                    self.get_metadata_size()
                );
                println!(
                    "The document size in the left half is: {} and the overall size is: {}",
                    document_size,
                    self.get_document_size()
                );
                let iterated_document_size = document_storage
                    .iter()
                    .map(|(_, v)| v.clone().unwrap_or("".to_string()).len())
                    .sum::<usize>();
                println!("The GT document size is: {}", iterated_document_size);
                split_key = match iter.next() {
                    Some((
                        (((next_key, _id), (_, _embedding)), (_, _metadata)),
                        (_, _document),
                    )) => Some(next_key.clone()),
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
            remaining_prefix_size: self.get_prefix_size() - prefix_size,
            remaining_key_size: self.get_key_size() - key_size,
            remaining_id_size: self.get_id_size() - id_size,
            remaining_embedding_size: self.get_embedding_size() - embedding_size,
            remaining_metadata_size: self.get_metadata_size() - metadata_size,
            remaining_document_size: self.get_document_size() - document_size,
        };
    }

    pub(super) fn split<K: ArrowWriteableKey>(
        &self,
        split_size: usize,
    ) -> (CompositeKey, DataRecordStorage) {
        let pre_split_document_size = self.document_size.load(std::sync::atomic::Ordering::SeqCst);
        let split_info = self.split_internal::<K>(split_size);
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

        self.prefix_size.fetch_sub(
            split_info.remaining_prefix_size,
            std::sync::atomic::Ordering::SeqCst,
        );
        self.key_size.fetch_sub(
            split_info.remaining_key_size,
            std::sync::atomic::Ordering::SeqCst,
        );
        self.id_size.fetch_sub(
            split_info.remaining_id_size,
            std::sync::atomic::Ordering::SeqCst,
        );
        self.embedding_size.fetch_sub(
            split_info.remaining_embedding_size,
            std::sync::atomic::Ordering::SeqCst,
        );
        self.metadata_size.fetch_sub(
            split_info.remaining_metadata_size,
            std::sync::atomic::Ordering::SeqCst,
        );
        self.document_size.fetch_sub(
            split_info.remaining_document_size,
            std::sync::atomic::Ordering::SeqCst,
        );

        let left_half_new_document_size =
            self.document_size.load(std::sync::atomic::Ordering::SeqCst);
        let right_half_new_document_size = split_info.remaining_document_size;
        println!(
            "[HAMMAD] The left half document size is: {} and the right half document size is: {}, the pre split document size is: {}",
            left_half_new_document_size, right_half_new_document_size, pre_split_document_size
        );
        if left_half_new_document_size + right_half_new_document_size != pre_split_document_size {
            println!(
                "[HAMMAD] The left half document size is: {} and the right half document size is: {}, the pre split document size is: {}",
                left_half_new_document_size, right_half_new_document_size, pre_split_document_size
            );
            panic!("The document size is not correct");
        }

        let split_documents_size = split_document
            .iter()
            .map(|(_, v)| v.clone().unwrap_or("".to_string()).len())
            .sum::<usize>();
        if split_documents_size != right_half_new_document_size {
            println!(
                "[HAMMAD] The split document size is: {} and the right half document size is: {}",
                split_documents_size, right_half_new_document_size
            );
            panic!("The split document size is not correct");
        }

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
