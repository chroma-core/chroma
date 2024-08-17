use crate::key::{CompositeKey, KeyWrapper};
use arrow::{
    array::{
        Array, ArrayRef, BinaryBuilder, FixedSizeListBuilder, Float32Builder, StringBuilder,
        StructArray,
    },
    datatypes::{Field, Fields},
    util::bit_util,
};
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use super::{calculate_key_size, calculate_prefix_size, BlockKeyArrowBuilder};

#[derive(Clone, Debug)]
pub(in crate::arrow) struct DataRecordStorage {
    pub id_storage: Arc<RwLock<BTreeMap<CompositeKey, String>>>,
    pub embedding_storage: Arc<RwLock<BTreeMap<CompositeKey, Vec<f32>>>>,
    pub metadata_storage: Arc<RwLock<BTreeMap<CompositeKey, Option<Vec<u8>>>>>,
    pub document_storage: Arc<RwLock<BTreeMap<CompositeKey, Option<String>>>>,
}

impl DataRecordStorage {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            id_storage: Arc::new(RwLock::new(BTreeMap::new())),
            embedding_storage: Arc::new(RwLock::new(BTreeMap::new())),
            metadata_storage: Arc::new(RwLock::new(BTreeMap::new())),
            document_storage: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub(super) fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        let id_storage = self.id_storage.read();
        let key_stream = id_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_prefix_size(key_stream)
    }

    pub(super) fn get_key_size(&self, start: usize, end: usize) -> usize {
        let id_storage = self.id_storage.read();
        let key_stream = id_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_key_size(key_stream)
    }

    pub fn get_id_size(&self, start: usize, end: usize) -> usize {
        let id_storage = self.id_storage.read();
        let id_stream = id_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        id_stream.fold(0, |acc, value| acc + value.len())
    }

    pub fn get_embedding_size(&self, start: usize, end: usize) -> usize {
        let embedding_storage = self.embedding_storage.read();
        let embedding_stream = embedding_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        embedding_stream.fold(0, |acc, value| acc + value.len() * 4)
    }

    pub fn get_metadata_size(&self, start: usize, end: usize) -> usize {
        let metadata_storage = self.metadata_storage.read();
        let metadata_stream = metadata_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        metadata_stream.fold(0, |acc, value| acc + value.as_ref().map_or(0, |v| v.len()))
    }

    pub(super) fn get_document_size(&self, start: usize, end: usize) -> usize {
        let document_storage = self.document_storage.read();
        let document_stream = document_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        document_stream.fold(0, |acc, value| acc + value.as_ref().map_or(0, |v| v.len()))
    }

    pub(super) fn get_total_embedding_count(&self) -> usize {
        let embedding_storage = self.embedding_storage.read();
        embedding_storage
            .iter()
            .fold(0, |acc, (_, value)| acc + value.len())
    }

    pub(super) fn get_value_size(&self, start: usize, end: usize) -> usize {
        let id_size = bit_util::round_upto_multiple_of_64(self.get_id_size(start, end));
        let embedding_size =
            bit_util::round_upto_multiple_of_64(self.get_embedding_size(start, end));
        let metadata_size = bit_util::round_upto_multiple_of_64(self.get_metadata_size(start, end));
        let document_size = bit_util::round_upto_multiple_of_64(self.get_document_size(start, end));
        let total_size = id_size + embedding_size + metadata_size + document_size;

        total_size
    }

    pub(super) fn split(&self, prefix: &str, key: KeyWrapper) -> DataRecordStorage {
        let mut id_storage_guard = self.id_storage.write();
        let mut embedding_storage_guard = self.embedding_storage.write();
        let split_id = id_storage_guard.split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key: key.clone(),
        });
        let split_embedding = embedding_storage_guard.split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key: key.clone(),
        });
        let split_metadata = self.metadata_storage.write().split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key: key.clone(),
        });
        let split_document = self.document_storage.write().split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });
        DataRecordStorage {
            id_storage: Arc::new(RwLock::new(split_id)),
            embedding_storage: Arc::new(RwLock::new(split_embedding)),
            metadata_storage: Arc::new(RwLock::new(split_metadata)),
            document_storage: Arc::new(RwLock::new(split_document)),
        }
    }

    pub(super) fn len(&self) -> usize {
        let id_storage = self.id_storage.read();
        id_storage.len()
    }

    pub(super) fn get_key(&self, index: usize) -> CompositeKey {
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
            id_builder =
                StringBuilder::with_capacity(item_capacity, self.get_id_size(0, self.len()));
            embedding_builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(self.get_total_embedding_count()),
                embedding_len,
                item_capacity,
            );
            metadata_builder =
                BinaryBuilder::with_capacity(item_capacity, self.get_metadata_size(0, self.len()));
            document_builder =
                StringBuilder::with_capacity(item_capacity, self.get_document_size(0, self.len()));
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
