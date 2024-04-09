use crate::blockstore::key::{CompositeKey, KeyWrapper};
use arrow::{
    array::{Array, ArrayRef, RecordBatch, StringBuilder},
    datatypes::Field,
};
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use super::delta::BlockDeltaKey;

#[derive(Clone)]
pub enum BlockStorage {
    String(StringValueStorage),
    DataRecord(DataRecordStorage),
}

pub enum BlockKeyArrowBuilder {
    String((StringBuilder, StringBuilder)),
}

impl BlockKeyArrowBuilder {
    fn add_key(&mut self, key: CompositeKey) {
        match key.key {
            KeyWrapper::String(value) => {
                let builder = match self {
                    BlockKeyArrowBuilder::String(builder) => builder,
                };
                builder.0.append_value(key.prefix);
                builder.1.append_value(value);
            }
            KeyWrapper::Float32(value) => {
                todo!()
            }
            KeyWrapper::Bool(value) => {
                todo!()
            }
            KeyWrapper::Uint32(value) => {
                todo!()
            }
        }
    }

    fn to_arrow(&mut self) -> (Field, ArrayRef, Field, ArrayRef) {
        match self {
            BlockKeyArrowBuilder::String((ref mut prefix_builder, ref mut key_builder)) => {
                let prefix_field = Field::new("prefix", arrow::datatypes::DataType::Utf8, false);
                let key_field = Field::new("key", arrow::datatypes::DataType::Utf8, false);
                let prefix_arr = prefix_builder.finish();
                let key_arr = key_builder.finish();
                (
                    prefix_field,
                    (&prefix_arr as &dyn Array).slice(0, prefix_arr.len()),
                    key_field,
                    (&key_arr as &dyn Array).slice(0, key_arr.len()),
                )
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct StringValueStorage {
    pub(super) storage: Arc<RwLock<Option<BTreeMap<CompositeKey, String>>>>,
}

impl StringValueStorage {
    pub(super) fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(Some(BTreeMap::new()))),
        }
    }

    fn get_prefix_size(&self) -> usize {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => {
                let key_stream = storage.iter().map(|(key, _)| key);
                calculate_prefix_size(key_stream)
            }
        }
    }

    fn get_key_size(&self) -> usize {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => {
                let key_stream = storage.iter().map(|(key, _)| key);
                calculate_key_size(key_stream)
            }
        }
    }

    fn get_value_size(&self) -> usize {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => {
                let value_stream = storage.iter().map(|(_, value)| value);
                value_stream.fold(0, |acc, value| acc + value.len())
            }
        }
    }

    fn len(&self) -> usize {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => storage.len(),
        }
    }

    fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => {
                let mut builder = builder;
                for (key, _) in storage.iter() {
                    builder.add_key(key.clone());
                }
                builder
            }
        }
    }

    fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.len();
        let mut value_builder = StringBuilder::with_capacity(item_capacity, self.get_value_size());

        let storage = self.storage.read();
        let storage = match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringDeltaBuilder should have storage."),
            Some(storage) => storage,
        };

        for (_, value) in storage.iter() {
            value_builder.append_value(value);
        }

        let value_field = Field::new("value", arrow::datatypes::DataType::Utf8, false);
        let value_arr = value_builder.finish();
        (
            value_field,
            (&value_arr as &dyn Array).slice(0, value_arr.len()),
        )
    }
}

#[derive(Clone)]
pub(super) struct DataRecordStorage {
    pub(super) id_storage: Arc<RwLock<Option<BTreeMap<CompositeKey, String>>>>,
    pub(super) embedding_storage: Arc<RwLock<Option<BTreeMap<CompositeKey, Vec<f32>>>>>,
}

impl DataRecordStorage {
    pub(super) fn new() -> Self {
        Self {
            id_storage: Arc::new(RwLock::new(Some(BTreeMap::new()))),
            embedding_storage: Arc::new(RwLock::new(Some(BTreeMap::new()))),
        }
    }
}

impl BlockStorage {
    pub(super) fn get_prefix_size(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_prefix_size(),
            BlockStorage::DataRecord(builder) => {
                let storage = builder.id_storage.read();
                match storage.as_ref() {
                    None => unreachable!(
                        "Invariant violation. A DataRecordBuilder should have id_storage."
                    ),
                    Some(storage) => {
                        let key_stream = storage.iter().map(|(key, _)| key);
                        calculate_prefix_size(key_stream)
                    }
                }
            }
        }
    }

    pub(super) fn get_key_size(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_key_size(),
            BlockStorage::DataRecord(builder) => {
                let storage = builder.id_storage.read();
                match storage.as_ref() {
                    None => unreachable!(
                        "Invariant violation. A DataRecordBuilder should have id_storage."
                    ),
                    Some(storage) => {
                        let key_stream = storage.iter().map(|(key, _)| key);
                        calculate_key_size(key_stream)
                    }
                }
            }
        }
    }

    pub(super) fn get_value_size(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_value_size(),
            BlockStorage::DataRecord(builder) => {
                let id_storage = builder.id_storage.read();
                let embedding_storage = builder.embedding_storage.read();
                match (id_storage.as_ref(), embedding_storage.as_ref()) {
                    (None, _) => unreachable!(
                        "Invariant violation. A DataRecordBuilder should have id_storage."
                    ),
                    (_, None) => unreachable!(
                        "Invariant violation. A DataRecordBuilder should have embedding_storage."
                    ),
                    (Some(id_storage), Some(embedding_storage)) => {
                        let id_stream = id_storage.iter().map(|(_, value)| value);
                        let embedding_stream = embedding_storage.iter().map(|(_, value)| value);
                        let id_size = id_stream.fold(0, |acc, value| acc + value.len());
                        let embedding_size =
                            embedding_stream.fold(0, |acc, value| acc + value.len() * 4);
                        // TODO: other fields
                        id_size + embedding_size
                    }
                }
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.len(),
            BlockStorage::DataRecord(builder) => {
                let id_storage = builder.id_storage.read();
                match id_storage.as_ref() {
                    None => unreachable!(
                        "Invariant violation. A DataRecordBuilder should have id_storage."
                    ),
                    Some(id_storage) => id_storage.len(),
                }
            }
        }
    }

    pub(super) fn to_record_batch<K: BlockDeltaKey>(&self) -> RecordBatch {
        let mut key_builder =
            K::get_arrow_builder(self.len(), self.get_prefix_size(), self.get_key_size());
        match self {
            BlockStorage::String(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::DataRecord(builder) => {
                todo!()
            }
        }

        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.to_arrow();
        let (value_field, value_arr) = match self {
            BlockStorage::String(builder) => builder.to_arrow(),
            BlockStorage::DataRecord(builder) => {
                todo!()
            }
        };
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            value_field,
        ]));
        let record_batch = RecordBatch::try_new(schema, vec![prefix_arr, key_arr, value_arr]);
        // TODO: handle error
        record_batch.unwrap()
    }
}

fn calculate_prefix_size<'a>(composite_key_iter: impl Iterator<Item = &'a CompositeKey>) -> usize {
    composite_key_iter.fold(0, |acc, key| acc + key.prefix.len())
}

fn calculate_key_size<'a>(composite_key_iter: impl Iterator<Item = &'a CompositeKey>) -> usize {
    composite_key_iter.fold(0, |acc, key| acc + key.key.get_size())
}
