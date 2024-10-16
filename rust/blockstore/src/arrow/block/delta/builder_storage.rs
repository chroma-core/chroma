use std::collections::BTreeMap;

use crate::{arrow::types::ArrowWriteableValue, key::CompositeKey};

pub struct BTreeBuilderStorage<V: ArrowWriteableValue> {
    storage: BTreeMap<CompositeKey, V>,
}

impl<V: ArrowWriteableValue + 'static> BTreeBuilderStorage<V> {
    fn add(&mut self, key: CompositeKey, value: V) {
        self.storage.insert(key, value);
    }

    fn delete(&mut self, key: &CompositeKey) -> Option<V> {
        self.storage.remove(key)
    }

    fn min_key(&self) -> Option<&CompositeKey> {
        self.storage.keys().next()
    }

    fn split_off(&mut self, key: &CompositeKey) -> Self {
        let split_off = self.storage.split_off(key);
        Self { storage: split_off }
    }

    fn len(&self) -> usize {
        self.storage.len()
    }

    fn iter<'referred_data>(
        &'referred_data self,
    ) -> Box<dyn Iterator<Item = (&'referred_data CompositeKey, &'referred_data V)> + 'referred_data>
    {
        Box::new(self.storage.iter())
    }

    fn into_iter(self) -> Box<dyn Iterator<Item = (CompositeKey, V)>> {
        Box::new(self.storage.into_iter())
    }
}

impl<V: ArrowWriteableValue> Default for BTreeBuilderStorage<V> {
    fn default() -> Self {
        Self {
            storage: BTreeMap::new(),
        }
    }
}

/// This storage assumes that KV pairs are added in order. Deletes are a no-op. Calling `.add()` with the same key more than once is not allowed.
pub struct VecBuilderStorage<V: ArrowWriteableValue> {
    storage: Vec<(CompositeKey, V)>,
}

impl<V: ArrowWriteableValue + 'static> VecBuilderStorage<V> {
    fn add(&mut self, key: CompositeKey, value: V) {
        self.storage.push((key, value));
    }

    fn delete(&mut self, _: &CompositeKey) -> Option<V> {
        None
    }

    fn min_key(&self) -> Option<&CompositeKey> {
        self.storage.first().map(|(key, _)| key)
    }

    fn split_off(&mut self, key: &CompositeKey) -> Self {
        let split_index = self.storage.binary_search_by(|(k, _)| k.cmp(key)).unwrap();
        let split_off = self.storage.split_off(split_index);
        self.storage.shrink_to_fit();
        Self { storage: split_off }
    }

    fn len(&self) -> usize {
        self.storage.len()
    }

    fn iter<'referred_data>(
        &'referred_data self,
    ) -> Box<dyn Iterator<Item = (&'referred_data CompositeKey, &'referred_data V)> + 'referred_data>
    {
        Box::new(self.storage.iter().map(|(k, v)| (k, v)))
    }

    fn into_iter(self) -> Box<dyn Iterator<Item = (CompositeKey, V)>> {
        Box::new(self.storage.into_iter())
    }
}

impl<V: ArrowWriteableValue> Default for VecBuilderStorage<V> {
    fn default() -> Self {
        Self {
            storage: Vec::new(),
        }
    }
}

pub enum BuilderStorageKind<V: ArrowWriteableValue> {
    BTreeBuilderStorage(BTreeBuilderStorage<V>),
    VecBuilderStorage(VecBuilderStorage<V>),
}

impl<V: ArrowWriteableValue + 'static> BuilderStorageKind<V> {
    pub fn add(&mut self, key: CompositeKey, value: V) {
        match self {
            BuilderStorageKind::BTreeBuilderStorage(storage) => storage.add(key, value),
            BuilderStorageKind::VecBuilderStorage(storage) => storage.add(key, value),
        }
    }

    pub fn delete(&mut self, key: &CompositeKey) -> Option<V> {
        match self {
            BuilderStorageKind::BTreeBuilderStorage(storage) => storage.delete(key),
            BuilderStorageKind::VecBuilderStorage(storage) => storage.delete(key),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BuilderStorageKind::BTreeBuilderStorage(storage) => storage.len(),
            BuilderStorageKind::VecBuilderStorage(storage) => storage.len(),
        }
    }

    pub fn min_key(&self) -> Option<&CompositeKey> {
        match self {
            BuilderStorageKind::BTreeBuilderStorage(storage) => storage.min_key(),
            BuilderStorageKind::VecBuilderStorage(storage) => storage.min_key(),
        }
    }

    pub fn split_off(&mut self, key: &CompositeKey) -> Self {
        match self {
            BuilderStorageKind::BTreeBuilderStorage(storage) => {
                BuilderStorageKind::BTreeBuilderStorage(storage.split_off(key))
            }
            BuilderStorageKind::VecBuilderStorage(storage) => {
                BuilderStorageKind::VecBuilderStorage(storage.split_off(key))
            }
        }
    }

    pub fn iter<'referred_data>(
        &'referred_data self,
    ) -> Box<dyn Iterator<Item = (&'referred_data CompositeKey, &'referred_data V)> + 'referred_data>
    {
        match self {
            BuilderStorageKind::BTreeBuilderStorage(storage) => storage.iter(),
            BuilderStorageKind::VecBuilderStorage(storage) => storage.iter(),
        }
    }

    pub fn into_iter(self) -> Box<dyn Iterator<Item = (CompositeKey, V)>>
    where
        <V as ArrowWriteableValue>::PreparedValue: 'static,
    {
        match self {
            BuilderStorageKind::BTreeBuilderStorage(storage) => storage.into_iter(),
            BuilderStorageKind::VecBuilderStorage(storage) => storage.into_iter(),
        }
    }
}
