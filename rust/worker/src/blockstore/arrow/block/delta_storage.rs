use crate::blockstore::{
    arrow::types::ArrowWriteableKey,
    key::{CompositeKey, KeyWrapper},
    Value,
};
use arrow::{
    array::{
        Array, ArrayRef, BinaryBuilder, FixedSizeListBuilder, Float32Builder, Int32Array,
        Int32Builder, ListBuilder, RecordBatch, StringBuilder, StructArray, UInt32Builder,
    },
    datatypes::{Field, Fields},
    util::bit_util,
};
use parking_lot::RwLock;
use std::{
    collections::BTreeMap,
    fmt,
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub enum BlockStorage {
    String(StringValueStorage),
    Int32Array(Int32ArrayStorage),
    UInt32(UInt32Storage),
    RoaringBitmap(RoaringBitmapStorage),
    DataRecord(DataRecordStorage),
}

impl Debug for BlockStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BlockStorage::String(_) => write!(f, "String"),
            BlockStorage::Int32Array(_) => write!(f, "Int32Array"),
            BlockStorage::UInt32(_) => write!(f, "UInt32"),
            BlockStorage::RoaringBitmap(_) => write!(f, "RoaringBitmap"),
            BlockStorage::DataRecord(_) => write!(f, "DataRecord"),
        }
    }
}

pub enum BlockKeyArrowBuilder {
    String((StringBuilder, StringBuilder)),
    Float32((StringBuilder, Float32Builder)),
    UInt32((StringBuilder, UInt32Builder)),
}

impl BlockKeyArrowBuilder {
    fn add_key(&mut self, key: CompositeKey) {
        match key.key {
            KeyWrapper::String(value) => {
                let builder = match self {
                    BlockKeyArrowBuilder::String(builder) => builder,
                    _ => {
                        unreachable!("Invariant violation. BlockKeyArrowBuilder should be String.")
                    }
                };
                builder.0.append_value(key.prefix);
                builder.1.append_value(value);
            }
            KeyWrapper::Float32(value) => {
                let builder = match self {
                    BlockKeyArrowBuilder::Float32(builder) => builder,
                    _ => {
                        unreachable!("Invariant violation. BlockKeyArrowBuilder should be Float32.")
                    }
                };
                builder.0.append_value(key.prefix);
                builder.1.append_value(value);
            }
            KeyWrapper::Bool(value) => {
                todo!()
            }
            KeyWrapper::Uint32(value) => {
                let builder = match self {
                    BlockKeyArrowBuilder::UInt32(builder) => builder,
                    _ => {
                        unreachable!("Invariant violation. BlockKeyArrowBuilder should be UInt32.")
                    }
                };
                builder.0.append_value(key.prefix);
                builder.1.append_value(value);
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
            BlockKeyArrowBuilder::Float32((ref mut prefix_builder, ref mut key_builder)) => {
                let prefix_field = Field::new("prefix", arrow::datatypes::DataType::Utf8, false);
                let key_field = Field::new("key", arrow::datatypes::DataType::Float32, false);
                let prefix_arr = prefix_builder.finish();
                let key_arr = key_builder.finish();
                (
                    prefix_field,
                    (&prefix_arr as &dyn Array).slice(0, prefix_arr.len()),
                    key_field,
                    (&key_arr as &dyn Array).slice(0, key_arr.len()),
                )
            }
            BlockKeyArrowBuilder::UInt32((ref mut prefix_builder, ref mut key_builder)) => {
                let prefix_field = Field::new("prefix", arrow::datatypes::DataType::Utf8, false);
                let key_field = Field::new("key", arrow::datatypes::DataType::UInt32, false);
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

    fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => {
                let key_stream = storage
                    .iter()
                    .skip(start)
                    .take(end - start)
                    .map(|(key, _)| key);
                calculate_prefix_size(key_stream)
            }
        }
    }

    fn get_key_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => {
                let key_stream = storage
                    .iter()
                    .skip(start)
                    .take(end - start)
                    .map(|(key, _)| key);
                calculate_key_size(key_stream)
            }
        }
    }

    fn get_value_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => {
                let value_stream = storage
                    .iter()
                    .skip(start)
                    .take(end - start)
                    .map(|(_, value)| value);
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

    fn split(&self, prefix: &str, key: KeyWrapper) -> StringValueStorage {
        let mut storage = self.storage.write();
        match storage.as_mut() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => {
                let split = storage.split_off(&CompositeKey {
                    prefix: prefix.to_string(),
                    key,
                });
                StringValueStorage {
                    storage: Arc::new(RwLock::new(Some(split))),
                }
            }
        }
    }

    fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.len();
        let mut value_builder =
            StringBuilder::with_capacity(item_capacity, self.get_value_size(0, self.len()));

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
pub(super) struct UInt32Storage {
    pub(super) storage: Arc<RwLock<BTreeMap<CompositeKey, u32>>>,
}

impl UInt32Storage {
    pub(super) fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let key_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_prefix_size(key_stream)
    }

    fn get_key_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let key_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_key_size(key_stream)
    }

    fn get_value_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let value_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        value_stream.fold(0, |acc, value| acc + value.to_string().len())
    }

    fn split(&self, prefix: &str, key: KeyWrapper) -> UInt32Storage {
        let mut storage_guard = self.storage.write();
        let split = storage_guard.split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });
        UInt32Storage {
            storage: Arc::new(RwLock::new(split)),
        }
    }

    fn get_key(&self, index: usize) -> CompositeKey {
        let storage = self.storage.read();
        let (key, _) = storage.iter().nth(index).unwrap();
        key.clone()
    }

    fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let storage = self.storage.read();
        let mut builder = builder;
        for (key, _) in storage.iter() {
            builder.add_key(key.clone());
        }
        builder
    }

    fn len(&self) -> usize {
        let storage = self.storage.read();
        storage.len()
    }

    fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.storage.read().len();
        let mut value_builder = UInt32Builder::with_capacity(item_capacity);
        for (_, value) in self.storage.read().iter() {
            value_builder.append_value(*value);
        }
        let value_field = Field::new("value", arrow::datatypes::DataType::UInt32, false);
        let value_arr = value_builder.finish();
        (
            value_field,
            (&value_arr as &dyn Array).slice(0, value_arr.len()),
        )
    }
}

#[derive(Clone)]
pub(super) struct Int32ArrayStorage {
    pub(super) storage: Arc<RwLock<BTreeMap<CompositeKey, Int32Array>>>,
}

impl Int32ArrayStorage {
    pub(super) fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let key_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_prefix_size(key_stream)
    }

    fn get_key_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let key_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_key_size(key_stream)
    }

    fn get_value_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let value_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        value_stream.fold(0, |acc, value| acc + value.get_size())
    }

    /// The count of the total number of values in the storage across all arrays.
    fn total_value_count(&self) -> usize {
        let storage = self.storage.read();
        storage.iter().fold(0, |acc, (_, value)| acc + value.len())
    }

    fn split(&self, prefix: &str, key: KeyWrapper) -> Int32ArrayStorage {
        let mut storage_guard = self.storage.write();
        let split = storage_guard.split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });
        Int32ArrayStorage {
            storage: Arc::new(RwLock::new(split)),
        }
    }

    fn get_key(&self, index: usize) -> CompositeKey {
        let storage = self.storage.read();
        let (key, _) = storage.iter().nth(index).unwrap();
        key.clone()
    }

    fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let storage = self.storage.read();
        // TODO: mut ref instead of ownership of builder
        let mut builder = builder;
        for (key, _) in storage.iter() {
            builder.add_key(key.clone());
        }
        builder
    }

    fn len(&self) -> usize {
        let storage = self.storage.read();
        storage.len()
    }

    fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.storage.read().len();
        let mut value_builder = ListBuilder::with_capacity(
            Int32Builder::with_capacity(self.total_value_count()),
            item_capacity,
        );

        let storage = self.storage.read();
        for (_, value) in storage.iter() {
            value_builder.append_value(value);
        }

        let value_field = Field::new(
            "value",
            arrow::datatypes::DataType::List(Arc::new(Field::new(
                "item",
                arrow::datatypes::DataType::Int32,
                true,
            ))),
            true,
        );
        let value_arr = value_builder.finish();
        (
            value_field,
            (&value_arr as &dyn Array).slice(0, value_arr.len()),
        )
    }
}

#[derive(Clone)]
pub(super) struct RoaringBitmapStorage {
    pub(super) storage: Arc<RwLock<BTreeMap<CompositeKey, Vec<u8>>>>,
}

impl RoaringBitmapStorage {
    pub(super) fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let key_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_prefix_size(key_stream)
    }

    fn get_key_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let key_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_key_size(key_stream)
    }

    fn get_value_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let value_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        value_stream.fold(0, |acc, value| acc + value.len())
    }

    fn split(&self, prefix: &str, key: KeyWrapper) -> RoaringBitmapStorage {
        let mut storage_guard = self.storage.write();
        let split = storage_guard.split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });
        RoaringBitmapStorage {
            storage: Arc::new(RwLock::new(split)),
        }
    }

    fn total_value_count(&self) -> usize {
        let storage = self.storage.read();
        storage.iter().fold(0, |acc, (_, value)| acc + value.len())
    }

    fn len(&self) -> usize {
        let storage = self.storage.read();
        storage.len()
    }

    fn get_key(&self, index: usize) -> CompositeKey {
        let storage = self.storage.read();
        let (key, _) = storage.iter().nth(index).unwrap();
        key.clone()
    }

    fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let storage = self.storage.read();
        let mut builder = builder;
        for (key, _) in storage.iter() {
            builder.add_key(key.clone());
        }
        builder
    }

    fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.len();
        let mut value_builder =
            BinaryBuilder::with_capacity(item_capacity, self.total_value_count());

        let storage = self.storage.read();
        for (_, value) in storage.iter() {
            value_builder.append_value(value);
        }

        let value_field = Field::new("value", arrow::datatypes::DataType::Binary, true);
        let value_arr = value_builder.finish();
        (
            value_field,
            (&value_arr as &dyn Array).slice(0, value_arr.len()),
        )
    }
}

#[derive(Clone)]
pub(super) struct DataRecordStorage {
    pub(super) id_storage: Arc<RwLock<BTreeMap<CompositeKey, String>>>,
    pub(super) embedding_storage: Arc<RwLock<BTreeMap<CompositeKey, Vec<f32>>>>,
    pub(super) metadata_storage: Arc<RwLock<BTreeMap<CompositeKey, Option<Vec<u8>>>>>,
    pub(super) document_storage: Arc<RwLock<BTreeMap<CompositeKey, Option<String>>>>,
}

impl DataRecordStorage {
    pub(super) fn new() -> Self {
        Self {
            id_storage: Arc::new(RwLock::new(BTreeMap::new())),
            embedding_storage: Arc::new(RwLock::new(BTreeMap::new())),
            metadata_storage: Arc::new(RwLock::new(BTreeMap::new())),
            document_storage: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        let id_storage = self.id_storage.read();
        let key_stream = id_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_prefix_size(key_stream)
    }

    fn get_key_size(&self, start: usize, end: usize) -> usize {
        let id_storage = self.id_storage.read();
        let key_stream = id_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_key_size(key_stream)
    }

    fn get_id_size(&self, start: usize, end: usize) -> usize {
        let id_storage = self.id_storage.read();
        let id_stream = id_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        id_stream.fold(0, |acc, value| acc + value.len())
    }

    fn get_embedding_size(&self, start: usize, end: usize) -> usize {
        let embedding_storage = self.embedding_storage.read();
        let embedding_stream = embedding_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        embedding_stream.fold(0, |acc, value| acc + value.len() * 4)
    }

    fn get_metadata_size(&self, start: usize, end: usize) -> usize {
        let metadata_storage = self.metadata_storage.read();
        let metadata_stream = metadata_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        metadata_stream.fold(0, |acc, value| acc + value.as_ref().map_or(0, |v| v.len()))
    }

    fn get_document_size(&self, start: usize, end: usize) -> usize {
        let document_storage = self.document_storage.read();
        let document_stream = document_storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        document_stream.fold(0, |acc, value| acc + value.as_ref().map_or(0, |v| v.len()))
    }

    fn get_total_embedding_count(&self) -> usize {
        let embedding_storage = self.embedding_storage.read();
        embedding_storage
            .iter()
            .fold(0, |acc, (_, value)| acc + value.len())
    }

    fn get_value_size(&self, start: usize, end: usize) -> usize {
        let id_size = bit_util::round_upto_multiple_of_64(self.get_id_size(start, end));
        let embedding_size =
            bit_util::round_upto_multiple_of_64(self.get_embedding_size(start, end));
        let metadata_size = bit_util::round_upto_multiple_of_64(self.get_metadata_size(start, end));
        let document_size = bit_util::round_upto_multiple_of_64(self.get_document_size(start, end));
        // TODO: I think this will break can_add logic
        let validity_bytes = bit_util::round_upto_multiple_of_64(bit_util::ceil(end - start, 8));
        // Validity bytes are used for metadata and document fields since they are optional
        let total_size =
            id_size + embedding_size + metadata_size + document_size + validity_bytes * 2;

        total_size
    }

    fn split(&self, prefix: &str, key: KeyWrapper) -> DataRecordStorage {
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

    fn len(&self) -> usize {
        let id_storage = self.id_storage.read();
        id_storage.len()
    }

    fn get_key(&self, index: usize) -> CompositeKey {
        let id_storage = self.id_storage.read();
        let (key, _) = id_storage.iter().nth(index).unwrap();
        key.clone()
    }

    fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let id_storage = self.id_storage.read();
        let mut builder = builder;
        for (key, _) in id_storage.iter() {
            builder.add_key(key.clone());
        }
        builder
    }

    fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.len();
        let embedding_len = self.embedding_storage.read().iter().next().unwrap().1.len() as i32;
        let mut id_builder =
            StringBuilder::with_capacity(item_capacity, self.get_id_size(0, self.len()));
        let mut embedding_builder = FixedSizeListBuilder::with_capacity(
            Float32Builder::with_capacity(self.get_total_embedding_count()),
            embedding_len,
            item_capacity,
        );
        let mut metadata_builder =
            BinaryBuilder::with_capacity(item_capacity, self.get_metadata_size(0, self.len()));
        let mut document_builder =
            StringBuilder::with_capacity(item_capacity, self.get_document_size(0, self.len()));

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

impl BlockStorage {
    pub(super) fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_prefix_size(start, end),
            BlockStorage::UInt32(builder) => builder.get_prefix_size(start, end),
            BlockStorage::DataRecord(builder) => builder.get_prefix_size(start, end),
            BlockStorage::Int32Array(builder) => builder.get_prefix_size(start, end),
            BlockStorage::RoaringBitmap(builder) => builder.get_prefix_size(start, end),
        }
    }

    pub(super) fn get_key_size(&self, start: usize, end: usize) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_key_size(start, end),
            BlockStorage::UInt32(builder) => builder.get_key_size(start, end),
            BlockStorage::DataRecord(builder) => builder.get_key_size(start, end),
            BlockStorage::Int32Array(builder) => builder.get_key_size(start, end),
            BlockStorage::RoaringBitmap(builder) => builder.get_key_size(start, end),
        }
    }

    pub(super) fn get_value_size(&self, start: usize, end: usize) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_value_size(start, end),
            BlockStorage::UInt32(builder) => builder.get_value_size(start, end),
            BlockStorage::DataRecord(builder) => builder.get_value_size(start, end),
            BlockStorage::Int32Array(builder) => builder.get_value_size(start, end),
            BlockStorage::RoaringBitmap(builder) => builder.get_value_size(start, end),
        }
    }

    pub(super) fn split(&self, prefix: &str, key: KeyWrapper) -> BlockStorage {
        match self {
            BlockStorage::String(builder) => BlockStorage::String(builder.split(prefix, key)),
            BlockStorage::UInt32(builder) => BlockStorage::UInt32(builder.split(prefix, key)),
            BlockStorage::DataRecord(builder) => {
                BlockStorage::DataRecord(builder.split(prefix, key))
            }
            BlockStorage::Int32Array(builder) => {
                BlockStorage::Int32Array(builder.split(prefix, key))
            }
            BlockStorage::RoaringBitmap(builder) => {
                BlockStorage::RoaringBitmap(builder.split(prefix, key))
            }
        }
    }

    pub(super) fn get_key(&self, index: usize) -> CompositeKey {
        match self {
            BlockStorage::String(builder) => {
                let storage = builder.storage.read();
                match storage.as_ref() {
                    None => unreachable!(
                        "Invariant violation. A StringValueBuilder should have storage."
                    ),
                    Some(storage) => {
                        let (key, _) = storage.iter().nth(index).unwrap();
                        key.clone()
                    }
                }
            }
            BlockStorage::UInt32(builder) => builder.get_key(index),
            BlockStorage::DataRecord(builder) => builder.get_key(index),
            BlockStorage::Int32Array(builder) => builder.get_key(index),
            BlockStorage::RoaringBitmap(builder) => builder.get_key(index),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.len(),
            BlockStorage::UInt32(builder) => builder.len(),
            BlockStorage::DataRecord(builder) => builder.len(),
            BlockStorage::Int32Array(builder) => builder.len(),
            BlockStorage::RoaringBitmap(builder) => builder.len(),
        }
    }

    pub(super) fn to_record_batch<K: ArrowWriteableKey>(&self) -> RecordBatch {
        let mut key_builder = K::get_arrow_builder(
            self.len(),
            self.get_prefix_size(0, self.len()),
            self.get_key_size(0, self.len()),
        );
        match self {
            BlockStorage::String(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::UInt32(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::DataRecord(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::Int32Array(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::RoaringBitmap(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
        }

        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.to_arrow();
        let (value_field, value_arr) = match self {
            BlockStorage::String(builder) => builder.to_arrow(),
            BlockStorage::UInt32(builder) => builder.to_arrow(),
            BlockStorage::DataRecord(builder) => builder.to_arrow(),
            BlockStorage::Int32Array(builder) => builder.to_arrow(),
            BlockStorage::RoaringBitmap(builder) => builder.to_arrow(),
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
