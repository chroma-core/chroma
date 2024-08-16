use crate::{
    key::{CompositeKey, KeyWrapper},
    Value,
};
use arrow::{
    array::{Array, ArrayRef, UInt32Builder},
    datatypes::Field,
};
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use super::{calculate_key_size, calculate_prefix_size, BlockKeyArrowBuilder};

#[derive(Clone, Debug)]
pub(in crate::arrow) struct UInt32Storage {
    pub storage: Arc<RwLock<BTreeMap<CompositeKey, u32>>>,
}

impl UInt32Storage {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub(super) fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let key_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_prefix_size(key_stream)
    }

    pub(super) fn get_key_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let key_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(key, _)| key);
        calculate_key_size(key_stream)
    }

    pub(super) fn get_value_size(&self, start: usize, end: usize) -> usize {
        let storage = self.storage.read();
        let value_stream = storage
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(_, value)| value);
        value_stream.fold(0, |acc, value| acc + value.get_size())
    }

    pub(super) fn split(&self, prefix: &str, key: KeyWrapper) -> UInt32Storage {
        let mut storage_guard = self.storage.write();
        let split = storage_guard.split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });
        UInt32Storage {
            storage: Arc::new(RwLock::new(split)),
        }
    }

    pub(super) fn get_key(&self, index: usize) -> CompositeKey {
        let storage = self.storage.read();
        let (key, _) = storage.iter().nth(index).unwrap();
        key.clone()
    }

    pub(super) fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
        let storage = self.storage.read();
        let mut builder = builder;
        for (key, _) in storage.iter() {
            builder.add_key(key.clone());
        }
        builder
    }

    pub(super) fn len(&self) -> usize {
        let storage = self.storage.read();
        storage.len()
    }

    pub(super) fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.storage.read().len();
        let mut value_builder;
        if item_capacity == 0 {
            value_builder = UInt32Builder::new();
        } else {
            value_builder = UInt32Builder::with_capacity(item_capacity);
        }
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
