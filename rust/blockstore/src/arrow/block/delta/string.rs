use super::{calculate_key_size, calculate_prefix_size, BlockKeyArrowBuilder};
use crate::key::{CompositeKey, KeyWrapper};
use arrow::{
    array::{Array, ArrayRef, StringBuilder},
    datatypes::Field,
};
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
pub(in crate::arrow) struct StringValueStorage {
    pub(crate) storage: Arc<RwLock<Option<BTreeMap<CompositeKey, String>>>>,
}

impl StringValueStorage {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(Some(BTreeMap::new()))),
        }
    }

    pub(super) fn get_prefix_size(&self, start: usize, end: usize) -> usize {
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

    pub(super) fn get_key_size(&self, start: usize, end: usize) -> usize {
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

    pub(super) fn get_value_size(&self, start: usize, end: usize) -> usize {
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

    pub(super) fn len(&self) -> usize {
        let storage = self.storage.read();
        match storage.as_ref() {
            None => unreachable!("Invariant violation. A StringValueBuilder should have storage."),
            Some(storage) => storage.len(),
        }
    }

    pub(super) fn build_keys(&self, builder: BlockKeyArrowBuilder) -> BlockKeyArrowBuilder {
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

    pub(super) fn split(&self, prefix: &str, key: KeyWrapper) -> StringValueStorage {
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

    pub(super) fn to_arrow(&self) -> (Field, ArrayRef) {
        let item_capacity = self.len();
        let mut value_builder;
        if item_capacity == 0 {
            value_builder = StringBuilder::new();
        } else {
            value_builder =
                StringBuilder::with_capacity(item_capacity, self.get_value_size(0, self.len()));
        }

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
