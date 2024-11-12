use super::{single_column_size_tracker::SingleColumnSizeTracker, BlockKeyArrowBuilder};
use crate::{
    arrow::types::{ArrowWriteableKey, ArrowWriteableValue},
    key::{CompositeKey, KeyWrapper},
    Value,
};
use arrow::{
    array::{Array, BinaryBuilder, ListBuilder, StringBuilder, UInt32Array, UInt32Builder},
    datatypes::{Field, Schema},
    util::bit_util,
};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    vec,
};

#[derive(Clone)]
pub struct SingleColumnStorage<T: ArrowWriteableValue> {
    inner: Arc<RwLock<Inner<T>>>,
}

struct Inner<T> {
    storage: BTreeMap<CompositeKey, T>,
    size_tracker: SingleColumnSizeTracker,
}

impl<T: ArrowWriteableValue> SingleColumnStorage<T> {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                storage: BTreeMap::new(),
                size_tracker: SingleColumnSizeTracker::new(),
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

    pub(super) fn get_value_size(&self) -> usize {
        let inner = self.inner.read();
        inner.size_tracker.get_value_size()
    }

    pub(super) fn len(&self) -> usize {
        let inner = self.inner.read();
        inner.storage.len()
    }

    pub fn get_min_key(&self) -> Option<CompositeKey> {
        let inner = self.inner.read();
        inner.storage.keys().next().cloned()
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let inner = self.inner.read();

        let prefix_size = inner.size_tracker.get_arrow_padded_prefix_size();
        let key_size = inner.size_tracker.get_arrow_padded_key_size();
        let value_size = inner.size_tracker.get_arrow_padded_value_size();

        // offset sizing
        // https://docs.rs/arrow-buffer/52.2.0/arrow_buffer/buffer/struct.OffsetBuffer.html
        // 4 bytes per offset entry, n+1 entries
        let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let key_offset_bytes: usize = K::offset_size(self.len());

        let value_offset_bytes = T::offset_size(self.len());
        let value_validity_bytes = T::validity_size(self.len());

        prefix_size
            + key_size
            + value_size
            + prefix_offset_bytes
            + key_offset_bytes
            + value_offset_bytes
            + value_validity_bytes
    }

    pub fn add(&self, prefix: &str, key: KeyWrapper, value: T) {
        let mut inner = self.inner.write();
        let key_len = key.get_size();

        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        if inner.storage.contains_key(&composite_key) {
            // subtract the old value size
            // unwrap is safe here because we just checked if the key exists
            let old_value_size = inner.storage.remove(&composite_key).unwrap().get_size();
            inner.size_tracker.subtract_value_size(old_value_size);
            inner.size_tracker.subtract_key_size(key_len);
            inner.size_tracker.subtract_prefix_size(prefix.len());
        }
        let value_size = value.get_size();

        inner.storage.insert(composite_key, value);
        inner.size_tracker.add_prefix_size(prefix.len());
        inner.size_tracker.add_key_size(key_len);
        inner.size_tracker.add_value_size(value_size);
    }

    pub fn delete(&self, prefix: &str, key: KeyWrapper) {
        let mut inner = self.inner.write();
        let maybe_removed_prefix_len = prefix.len();
        let maybe_removed_key_len = key.get_size();
        let maybe_removed_value = inner.storage.remove(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });

        if let Some(value) = maybe_removed_value {
            inner
                .size_tracker
                .subtract_prefix_size(maybe_removed_prefix_len);
            inner.size_tracker.subtract_key_size(maybe_removed_key_len);
            inner.size_tracker.subtract_value_size(value.get_size());
        }
    }

    pub(super) fn split<K: ArrowWriteableKey>(
        &self,
        split_size: usize,
    ) -> (CompositeKey, SingleColumnStorage<T>) {
        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut value_size = 0;
        let mut split_key = None;

        {
            let inner = self.inner.read();
            let storage = &inner.storage;

            let mut item_count = 0;
            let mut iter = storage.iter();
            while let Some((key, value)) = iter.next() {
                prefix_size += key.prefix.len();
                key_size += key.key.get_size();
                value_size += value.get_size();
                item_count += 1;

                // offset sizing
                let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
                let key_offset_bytes = K::offset_size(item_count);
                let value_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);

                // validitiy sizing
                let value_validity_bytes = T::validity_size(item_count);

                let total_size = bit_util::round_upto_multiple_of_64(prefix_size)
                    + bit_util::round_upto_multiple_of_64(key_size)
                    + bit_util::round_upto_multiple_of_64(value_size)
                    + prefix_offset_bytes
                    + key_offset_bytes
                    + value_offset_bytes
                    + value_validity_bytes;

                if total_size > split_size {
                    split_key = match iter.next() {
                        None => {
                            // Remove the last item since we are splitting at the end
                            prefix_size -= key.prefix.len();
                            key_size -= key.key.get_size();
                            value_size -= value.get_size();
                            Some(key.clone())
                        }
                        Some((next_key, _)) => Some(next_key.clone()),
                    };
                    break;
                }
            }
        }

        let mut inner = self.inner.write();

        let total_prefix_size = inner.size_tracker.get_prefix_size();
        let total_key_size = inner.size_tracker.get_key_size();
        let total_value_size = inner.size_tracker.get_value_size();
        inner
            .size_tracker
            .subtract_prefix_size(total_prefix_size - prefix_size);
        inner
            .size_tracker
            .subtract_key_size(total_key_size - key_size);
        inner
            .size_tracker
            .subtract_value_size(total_value_size - value_size);

        match split_key {
            None => panic!("A storage should have at least one element to be split."),
            Some(split_key) => {
                let new_delta = inner.storage.split_off(&split_key);
                (
                    split_key,
                    SingleColumnStorage {
                        inner: Arc::new(RwLock::new(Inner {
                            storage: new_delta,
                            size_tracker: SingleColumnSizeTracker::with_values(
                                total_prefix_size - prefix_size,
                                total_key_size - key_size,
                                total_value_size - value_size,
                            ),
                        })),
                    },
                )
            }
        }
    }
}

impl SingleColumnStorage<String> {
    pub(super) fn into_arrow(
        self,
        key_builder: BlockKeyArrowBuilder,
        metadata: Option<HashMap<String, String>>,
    ) -> (Arc<Schema>, Vec<Arc<dyn Array>>) {
        // Build key and value.
        let mut key_builder = key_builder;
        let item_capacity = self.len();
        let mut value_builder;
        if item_capacity == 0 {
            value_builder = StringBuilder::new();
        } else {
            value_builder = StringBuilder::with_capacity(item_capacity, self.get_value_size());
        }
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => {
                let storage = inner.into_inner().storage;
                for (key, value) in storage.into_iter() {
                    key_builder.add_key(key);
                    value_builder.append_value(value);
                }
            }
            Err(_) => {
                panic!("Invariant violation: SingleColumnStorage inner should have only one reference.");
            }
        }
        // Build arrow key with fields.
        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        // Build arrow value with fields.
        let value_field = Field::new("value", arrow::datatypes::DataType::Utf8, false);
        let value_arr = value_builder.finish();
        let value_arr = (&value_arr as &dyn Array).slice(0, value_arr.len());
        let schema = arrow::datatypes::Schema::new(vec![prefix_field, key_field, value_field]);

        if let Some(metadata) = metadata {
            let schema = schema.with_metadata(metadata);
            return (schema.into(), vec![prefix_arr, key_arr, value_arr]);
        }

        (schema.into(), vec![prefix_arr, key_arr, value_arr])
    }
}

impl SingleColumnStorage<Vec<u32>> {
    pub(super) fn into_arrow(
        self,
        key_builder: BlockKeyArrowBuilder,
        metadata: Option<HashMap<String, String>>,
    ) -> (Arc<Schema>, Vec<Arc<dyn Array>>) {
        // Build key and value.
        let mut key_builder = key_builder;
        let item_capacity = self.len();
        let mut value_builder;
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => {
                let storage = inner.into_inner().storage;
                let total_value_count = storage.iter().fold(0, |acc, (_, value)| acc + value.len());
                if item_capacity == 0 {
                    value_builder = ListBuilder::new(UInt32Builder::new());
                } else {
                    value_builder = ListBuilder::with_capacity(
                        UInt32Builder::with_capacity(total_value_count),
                        item_capacity,
                    );
                }
                for (key, value) in storage.into_iter() {
                    key_builder.add_key(key);
                    value_builder.append_value(&UInt32Array::from(value));
                }
            }
            Err(_) => {
                panic!("Invariant violation: SingleColumnStorage inner should have only one reference.");
            }
        }
        // Build arrow key and value with fields.
        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();

        let value_field = Field::new(
            "value",
            arrow::datatypes::DataType::List(Arc::new(Field::new(
                "item",
                arrow::datatypes::DataType::UInt32,
                true,
            ))),
            true,
        );
        let value_arr = value_builder.finish();
        let value_arr = (&value_arr as &dyn Array).slice(0, value_arr.len());
        let schema = arrow::datatypes::Schema::new(vec![prefix_field, key_field, value_field]);

        if let Some(metadata) = metadata {
            let schema = schema.with_metadata(metadata);
            return (schema.into(), vec![prefix_arr, key_arr, value_arr]);
        }

        (schema.into(), vec![prefix_arr, key_arr, value_arr])
    }
}

impl SingleColumnStorage<u32> {
    pub(super) fn into_arrow(
        self,
        key_builder: BlockKeyArrowBuilder,
        metadata: Option<HashMap<String, String>>,
    ) -> (Arc<Schema>, Vec<Arc<dyn Array>>) {
        // Build key and value.
        let mut key_builder = key_builder;
        let mut value_builder;
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => {
                let storage = inner.into_inner().storage;
                let item_capacity = storage.len();
                if item_capacity == 0 {
                    value_builder = UInt32Builder::new();
                } else {
                    value_builder = UInt32Builder::with_capacity(item_capacity);
                }
                for (key, value) in storage.into_iter() {
                    key_builder.add_key(key);
                    value_builder.append_value(value);
                }
            }
            Err(_) => {
                panic!("Invariant violation: SingleColumnStorage inner should have only one reference.");
            }
        }
        // Build arrow key with fields.
        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let value_field = Field::new("value", arrow::datatypes::DataType::UInt32, false);
        let value_arr = value_builder.finish();
        let value_arr = (&value_arr as &dyn Array).slice(0, value_arr.len());
        let schema = arrow::datatypes::Schema::new(vec![prefix_field, key_field, value_field]);

        if let Some(metadata) = metadata {
            let schema = schema.with_metadata(metadata);
            return (schema.into(), vec![prefix_arr, key_arr, value_arr]);
        }

        (schema.into(), vec![prefix_arr, key_arr, value_arr])
    }
}

impl SingleColumnStorage<RoaringBitmap> {
    pub(super) fn into_arrow(
        self,
        key_builder: BlockKeyArrowBuilder,
        metadata: Option<HashMap<String, String>>,
    ) -> (Arc<Schema>, Vec<Arc<dyn Array>>) {
        // Build key.
        let mut key_builder = key_builder;
        let item_capacity = self.len();
        let mut value_builder;
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => {
                let storage = inner.into_inner().storage;
                let total_value_count = storage
                    .iter()
                    .fold(0, |acc, (_, value)| acc + value.get_size());
                if item_capacity == 0 {
                    value_builder = BinaryBuilder::new();
                } else {
                    value_builder = BinaryBuilder::with_capacity(item_capacity, total_value_count);
                }
                for (key, value) in storage.into_iter() {
                    key_builder.add_key(key);
                    let mut serialized = Vec::with_capacity(value.serialized_size());
                    let res = value.serialize_into(&mut serialized);
                    // TODO: proper error handling
                    let serialized = match res {
                        Ok(_) => serialized,
                        Err(e) => panic!("Failed to serialize RoaringBitmap: {}", e),
                    };
                    value_builder.append_value(serialized);
                }
            }
            Err(_) => {
                panic!("Invariant violation: SingleColumnStorage inner should have only one reference.");
            }
        }
        // Build arrow key with fields.
        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();

        let value_field = Field::new("value", arrow::datatypes::DataType::Binary, true);
        let value_arr = value_builder.finish();
        let value_arr = (&value_arr as &dyn Array).slice(0, value_arr.len());
        let schema = arrow::datatypes::Schema::new(vec![prefix_field, key_field, value_field]);

        if let Some(metadata) = metadata {
            let schema = schema.with_metadata(metadata);
            return (schema.into(), vec![prefix_arr, key_arr, value_arr]);
        }

        (schema.into(), vec![prefix_arr, key_arr, value_arr])
    }
}
