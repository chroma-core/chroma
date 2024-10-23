use super::{single_column_size_tracker::SingleColumnSizeTracker, BlockKeyArrowBuilder};
use crate::{
    arrow::types::{ArrowWriteableKey, ArrowWriteableValue},
    key::{CompositeKey, KeyWrapper},
};
use arrow::util::bit_util;
use arrow::{array::Array, datatypes::Schema};
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};
use std::{collections::HashMap, vec};

#[derive(Clone)]
pub struct SingleColumnStorage<T: ArrowWriteableValue> {
    inner: Arc<RwLock<Inner<T>>>,
}

struct Inner<T: ArrowWriteableValue> {
    storage: BTreeMap<CompositeKey, (T::PreparedValue, usize)>,
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
            let old_value_size = inner.storage.remove(&composite_key).unwrap().1;
            inner.size_tracker.subtract_value_size(old_value_size);
            inner.size_tracker.subtract_key_size(key_len);
            inner.size_tracker.subtract_prefix_size(prefix.len());
        }
        let value_size = value.get_size();

        inner
            .storage
            .insert(composite_key, (T::prepare(value), value_size));
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

        if let Some((_, value_size)) = maybe_removed_value {
            inner
                .size_tracker
                .subtract_prefix_size(maybe_removed_prefix_len);
            inner.size_tracker.subtract_key_size(maybe_removed_key_len);
            inner.size_tracker.subtract_value_size(value_size);
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
                value_size += value.1;
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
                            value_size -= value.1;
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

    pub(super) fn into_arrow(
        self,
        mut key_builder: BlockKeyArrowBuilder,
        metadata: Option<HashMap<String, String>>,
    ) -> (Arc<Schema>, Vec<Arc<dyn Array>>) {
        let mut value_builder = T::get_arrow_builder();

        match Arc::try_unwrap(self.inner) {
            Ok(inner) => {
                let storage = inner.into_inner().storage;
                for (key, (value, _)) in storage.into_iter() {
                    key_builder.add_key(key);
                    T::append(value, &mut value_builder);
                }
            }
            Err(_) => {
                panic!("Invariant violation: SingleColumnStorage inner should have only one reference.");
            }
        }

        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let (value_field, value_arr) = T::finish(value_builder);
        let schema = arrow::datatypes::Schema::new(vec![prefix_field, key_field, value_field]);

        if let Some(metadata) = metadata {
            let schema = schema.with_metadata(metadata);
            return (schema.into(), vec![prefix_arr, key_arr, value_arr]);
        }

        (schema.into(), vec![prefix_arr, key_arr, value_arr])
    }
}
