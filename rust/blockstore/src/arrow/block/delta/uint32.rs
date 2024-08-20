use super::BlockKeyArrowBuilder;
use crate::{
    key::{CompositeKey, KeyWrapper},
    Value,
};
use arrow::{
    array::{Array, ArrayRef, UInt32Builder},
    datatypes::Field,
};
use parking_lot::RwLock;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Arc},
};

#[derive(Clone, Debug)]
pub struct UInt32Storage {
    pub storage: Arc<RwLock<BTreeMap<CompositeKey, u32>>>,

    // size-tracking variables
    pub(in crate::arrow::block) prefix_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) key_size: Arc<AtomicUsize>,
    pub(in crate::arrow::block) value_size: Arc<AtomicUsize>,
}

impl UInt32Storage {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(BTreeMap::new())),

            // size-tracking variables
            prefix_size: Arc::new(AtomicUsize::new(0)),
            key_size: Arc::new(AtomicUsize::new(0)),
            value_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        return self.prefix_size.load(std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn get_key_size(&self, start: usize, end: usize) -> usize {
        return self.key_size.load(std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn get_value_size(&self, start: usize, end: usize) -> usize {
        return self.value_size.load(std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn split(&self, prefix: &str, key: KeyWrapper) -> UInt32Storage {
        let mut storage_guard = self.storage.write();
        let split = storage_guard.split_off(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });
        UInt32Storage {
            storage: Arc::new(RwLock::new(split)),
            // TODO: make size based off split
            prefix_size: Arc::new(AtomicUsize::new(0)),
            key_size: Arc::new(AtomicUsize::new(0)),
            value_size: Arc::new(AtomicUsize::new(0)),
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
