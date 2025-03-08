use crate::{arrow::types::ArrowWriteableValue, key::CompositeKey};
use std::collections::BTreeMap;

pub struct BTreeBuilderStorage<V: ArrowWriteableValue> {
    storage: BTreeMap<CompositeKey, V>,
}

impl<V: ArrowWriteableValue> BTreeBuilderStorage<V> {
    fn add(&mut self, key: CompositeKey, value: V) {
        self.storage.insert(key, value);
    }

    fn delete(&mut self, key: &CompositeKey) -> Option<V> {
        self.storage.remove(key)
    }

    fn get(&self, key: &CompositeKey) -> Option<V::PreparedValue> {
        if !self.storage.contains_key(key) {
            return None;
        }
        Some(V::prepare(self.storage.get(key).unwrap().clone()))
    }

    fn min_key(&self) -> Option<&CompositeKey> {
        self.storage.keys().next()
    }

    fn split_off(&mut self, key: &CompositeKey) -> Self {
        let split_off = self.storage.split_off(key);
        Self { storage: split_off }
    }

    fn pop_last(&mut self) -> Option<(CompositeKey, V)> {
        self.storage.pop_last()
    }

    fn len(&self) -> usize {
        self.storage.len()
    }

    fn iter<'referred_data>(
        &'referred_data self,
    ) -> Box<
        dyn DoubleEndedIterator<Item = (&'referred_data CompositeKey, &'referred_data V)>
            + 'referred_data,
    > {
        Box::new(self.storage.iter())
    }

    fn into_iter(self) -> impl Iterator<Item = (CompositeKey, V)> {
        self.storage.into_iter()
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

impl<V: ArrowWriteableValue> VecBuilderStorage<V> {
    fn add(&mut self, key: CompositeKey, value: V) {
        self.storage.push((key, value));
    }

    fn delete(&mut self, _: &CompositeKey) -> Option<V> {
        None
    }

    fn get(&self, _: &CompositeKey) -> Option<V::PreparedValue> {
        unimplemented!()
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

    fn pop_last(&mut self) -> Option<(CompositeKey, V)> {
        self.storage.pop()
    }

    fn len(&self) -> usize {
        self.storage.len()
    }

    fn iter<'referred_data>(
        &'referred_data self,
    ) -> Box<
        dyn DoubleEndedIterator<Item = (&'referred_data CompositeKey, &'referred_data V)>
            + 'referred_data,
    > {
        Box::new(self.storage.iter().map(|(k, v)| (k, v))) // .map transforms from &(k, v) to (&k, &v)
    }

    fn into_iter(self) -> impl Iterator<Item = (CompositeKey, V)> {
        self.storage.into_iter()
    }
}

impl<V: ArrowWriteableValue> Default for VecBuilderStorage<V> {
    fn default() -> Self {
        Self {
            storage: Vec::new(),
        }
    }
}

pub enum BuilderStorage<V: ArrowWriteableValue> {
    BTreeBuilderStorage(BTreeBuilderStorage<V>),
    VecBuilderStorage(VecBuilderStorage<V>),
}

enum Either<V, Left: Iterator<Item = (CompositeKey, V)>, Right: Iterator<Item = (CompositeKey, V)>>
{
    Left(Left),
    Right(Right),
}

impl<V, Left, Right> Iterator for Either<V, Left, Right>
where
    Left: Iterator<Item = (CompositeKey, V)>,
    Right: Iterator<Item = (CompositeKey, V)>,
{
    type Item = (CompositeKey, V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Either::Left(left) => left.next(),
            Either::Right(right) => right.next(),
        }
    }
}

impl<V: ArrowWriteableValue> BuilderStorage<V> {
    pub fn add(&mut self, key: CompositeKey, value: V) {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => storage.add(key, value),
            BuilderStorage::VecBuilderStorage(storage) => storage.add(key, value),
        }
    }

    pub fn delete(&mut self, key: &CompositeKey) -> Option<V> {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => storage.delete(key),
            BuilderStorage::VecBuilderStorage(storage) => storage.delete(key),
        }
    }

    pub fn get(&self, key: &CompositeKey) -> Option<V::PreparedValue> {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => storage.get(key),
            BuilderStorage::VecBuilderStorage(storage) => storage.get(key),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => storage.len(),
            BuilderStorage::VecBuilderStorage(storage) => storage.len(),
        }
    }

    pub fn min_key(&self) -> Option<&CompositeKey> {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => storage.min_key(),
            BuilderStorage::VecBuilderStorage(storage) => storage.min_key(),
        }
    }

    pub fn split_off(&mut self, key: &CompositeKey) -> Self {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => {
                BuilderStorage::BTreeBuilderStorage(storage.split_off(key))
            }
            BuilderStorage::VecBuilderStorage(storage) => {
                BuilderStorage::VecBuilderStorage(storage.split_off(key))
            }
        }
    }

    pub fn pop_last(&mut self) -> Option<(CompositeKey, V)> {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => storage.pop_last(),
            BuilderStorage::VecBuilderStorage(storage) => storage.pop_last(),
        }
    }

    pub fn iter<'referred_data>(
        &'referred_data self,
    ) -> Box<
        dyn DoubleEndedIterator<Item = (&'referred_data CompositeKey, &'referred_data V)>
            + 'referred_data,
    > {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => storage.iter(),
            BuilderStorage::VecBuilderStorage(storage) => storage.iter(),
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = (CompositeKey, V)> {
        match self {
            BuilderStorage::BTreeBuilderStorage(storage) => Either::Left(storage.into_iter()),
            BuilderStorage::VecBuilderStorage(storage) => Either::Right(storage.into_iter()),
        }
    }
}
