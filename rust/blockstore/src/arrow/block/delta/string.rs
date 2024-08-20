use super::BlockKeyArrowBuilder;
use crate::{
    arrow::types::ArrowWriteableKey,
    key::{CompositeKey, KeyWrapper},
};
use arrow::{
    array::{Array, ArrayRef, StringBuilder},
    datatypes::Field,
    util::bit_util,
};
use parking_lot::RwLock;
use prost_types::value;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Arc},
};

#[derive(Clone)]
pub struct StringValueStorage {
    pub(crate) storage: Arc<RwLock<BTreeMap<CompositeKey, String>>>,
    pub(in crate::arrow::block) prefix_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) key_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) value_size: Arc<AtomicUsize>,
}

impl StringValueStorage {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(BTreeMap::new())),

            // size-tracking variables
            prefix_size: Arc::new(AtomicUsize::new(0)),
            key_size: Arc::new(AtomicUsize::new(0)),
            value_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        return self.prefix_size.load(std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn get_key_size(&self) -> usize {
        return self.key_size.load(std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn get_value_size(&self) -> usize {
        return self.value_size.load(std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn len(&self) -> usize {
        let storage = self.storage.read();
        storage.len()
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let prefix_size = bit_util::round_upto_multiple_of_64(self.get_prefix_size());
        let key_size = bit_util::round_upto_multiple_of_64(self.get_key_size());
        let value_size = bit_util::round_upto_multiple_of_64(self.get_value_size());

        let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let key_offset_bytes: usize = K::offset_size(self.len());

        let value_offset_bytes = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);

        prefix_size
            + key_size
            + value_size
            + prefix_offset_bytes
            + key_offset_bytes
            + value_offset_bytes
    }

    pub(super) fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let storage = self.storage.read();
        let mut builder = builder;
        for (key, _) in storage.iter() {
            builder.add_key(key.clone());
        }
        builder
    }

    pub(super) fn split(&self, split_size: usize) -> (CompositeKey, StringValueStorage) {
        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut value_size = 0;
        let mut split_key = None;

        {
            let storage = self.storage.read();

            let mut index = 0;
            let mut iter = storage.iter();
            while let Some((key, value)) = iter.next() {
                prefix_size += key.prefix.len();
                key_size += key.key.get_size();
                value_size += value.len();

                // offset sizing
                let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((index + 1) * 4);
                let key_offset_bytes = bit_util::round_upto_multiple_of_64((index + 1) * 4);
                let value_offset_bytes = bit_util::round_upto_multiple_of_64((index + 1) * 4);

                let total_size = bit_util::round_upto_multiple_of_64(prefix_size)
                    + bit_util::round_upto_multiple_of_64(key_size)
                    + bit_util::round_upto_multiple_of_64(value_size)
                    + prefix_offset_bytes
                    + key_offset_bytes
                    + value_offset_bytes;

                if total_size > split_size {
                    split_key = match iter.next() {
                        None => Some(key.clone()),
                        Some((next_key, _)) => Some(next_key.clone()),
                    };
                }
                index += 1;
            }
        }

        let mut storage = self.storage.write();

        match split_key {
            None => panic!("A StringValueStorage should have at least one element to be split."),
            Some(split_key) => {
                let new_delta = storage.split_off(&split_key);
                // TODO: subtract our sizes from the storage
                (
                    split_key,
                    StringValueStorage {
                        storage: Arc::new(RwLock::new(new_delta)),
                        prefix_size: Arc::new(AtomicUsize::new(prefix_size)),
                        key_size: Arc::new(AtomicUsize::new(key_size)),
                        value_size: Arc::new(AtomicUsize::new(value_size)),
                    },
                )
            }
        }
    }

    pub(super) fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.len();
        let mut value_builder;
        if item_capacity == 0 {
            value_builder = StringBuilder::new();
        } else {
            value_builder = StringBuilder::with_capacity(item_capacity, self.get_value_size());
        }

        let storage = self.storage.read();

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
