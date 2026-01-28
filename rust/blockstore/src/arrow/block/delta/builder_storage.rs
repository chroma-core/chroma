use crate::{arrow::types::ArrowWriteableValue, key::CompositeKey};
use ahash::AHashMap;
use rayon::slice::ParallelSliceMut;
use std::collections::BTreeMap;

/// Threshold for using parallel sort. Below this, serial sort is faster due to overhead.
const PARALLEL_SORT_THRESHOLD: usize = 10_000;

/// A storage structure that defers sorting until iteration/commit time.
/// This provides O(1) amortized insert performance instead of O(log n) for BTreeMap.
///
/// The structure works as follows:
/// - Inserts append to a Vec (O(1) amortized)
/// - An AHashMap (faster than std HashMap) tracks indices for deduplication and lookups
/// - Sorting happens lazily when iteration is requested using unstable sort
/// - This is optimal for write-heavy workloads with bulk inserts
pub struct DeferredSortStorage<V: ArrowWriteableValue> {
    /// Unsorted storage of key-value pairs
    storage: Vec<(CompositeKey, V)>,
    /// Maps keys to their index in the storage vec (for deduplication and lookups)
    /// Uses ahash for ~2-5x faster hashing than std HashMap
    index: AHashMap<CompositeKey, usize>,
    /// Whether the storage is currently sorted
    is_sorted: bool,
}

impl<V: ArrowWriteableValue> DeferredSortStorage<V> {
    /// Add a key-value pair to the storage. O(1) amortized.
    #[inline]
    pub fn add(&mut self, key: CompositeKey, value: V) {
        // Use entry API for single lookup instead of get + insert
        match self.index.get(&key) {
            Some(&existing_idx) => {
                // Key exists - update in place (deduplication)
                self.storage[existing_idx].1 = value;
            }
            None => {
                // New key - append to vec
                let idx = self.storage.len();
                self.index.insert(key.clone(), idx);
                self.storage.push((key, value));
                self.is_sorted = false;
            }
        }
    }

    /// Add a key-value pair without checking for duplicates.
    /// Use this when you KNOW the key is unique for maximum performance.
    /// WARNING: Using this with duplicate keys will result in duplicates in the output.
    #[inline]
    pub fn add_unchecked(&mut self, key: CompositeKey, value: V) {
        let idx = self.storage.len();
        self.index.insert(key.clone(), idx);
        self.storage.push((key, value));
        self.is_sorted = false;
    }

    fn delete(&mut self, key: &CompositeKey) -> Option<V> {
        if let Some(idx) = self.index.remove(key) {
            // Use swap_remove for O(1) removal
            let (_, value) = self.storage.swap_remove(idx);

            // If we removed an element that wasn't the last, update the moved element's index
            if idx < self.storage.len() {
                let moved_key = &self.storage[idx].0;
                self.index.insert(moved_key.clone(), idx);
            }

            self.is_sorted = false;
            Some(value)
        } else {
            None
        }
    }

    #[inline]
    fn get(&self, key: &CompositeKey) -> Option<V::PreparedValue> {
        if self.is_sorted {
            // Use binary search for sorted data
            self.storage
                .binary_search_by(|(k, _)| k.cmp(key))
                .ok()
                .map(|idx| V::prepare(self.storage[idx].1.clone()))
        } else {
            // Use HashMap index for unsorted data
            self.index
                .get(key)
                .map(|&idx| V::prepare(self.storage[idx].1.clone()))
        }
    }

    #[inline]
    fn get_ref(&self, key: &CompositeKey) -> Option<&V> {
        if self.is_sorted {
            // Use binary search for sorted data
            self.storage
                .binary_search_by(|(k, _)| k.cmp(key))
                .ok()
                .map(|idx| &self.storage[idx].1)
        } else {
            // Use HashMap index for unsorted data
            self.index.get(key).map(|&idx| &self.storage[idx].1)
        }
    }

    fn min_key(&mut self) -> Option<&CompositeKey> {
        self.ensure_sorted();
        self.storage.first().map(|(key, _)| key)
    }

    #[inline]
    fn ensure_sorted(&mut self) {
        if !self.is_sorted && !self.storage.is_empty() {
            // Use parallel sort for large datasets, serial for small
            if self.storage.len() >= PARALLEL_SORT_THRESHOLD {
                self.storage.par_sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            } else {
                self.storage.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            }
            // After sorting, we can use binary search for lookups
            // No need to rebuild the HashMap index - saves key clones!
            // Just clear it and mark as sorted
            self.index.clear();
            self.is_sorted = true;
        }
    }

    /// Get value using binary search (only valid after sorting)
    #[inline]
    fn get_sorted(&self, key: &CompositeKey) -> Option<&V> {
        debug_assert!(self.is_sorted, "get_sorted called on unsorted storage");
        self.storage
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|idx| &self.storage[idx].1)
    }

    fn split_off(&mut self, key: &CompositeKey) -> Self {
        self.ensure_sorted();
        let split_index = self
            .storage
            .binary_search_by(|(k, _)| k.cmp(key))
            .unwrap_or_else(|i| i);
        let split_off = self.storage.split_off(split_index);

        // Both halves remain sorted, no need to rebuild indices
        // Binary search will be used for lookups
        Self {
            storage: split_off,
            index: AHashMap::new(), // Empty - will use binary search
            is_sorted: true,
        }
    }

    fn len(&self) -> usize {
        self.storage.len()
    }

    /// Iterate over the storage in sorted order. Triggers sorting if needed.
    pub fn iter<'referred_data>(
        &'referred_data mut self,
    ) -> Box<dyn Iterator<Item = (&'referred_data CompositeKey, &'referred_data V)> + 'referred_data>
    {
        self.ensure_sorted();
        Box::new(self.storage.iter().map(|(k, v)| (k, v)))
    }

    fn into_iter(mut self) -> impl Iterator<Item = (CompositeKey, V)> {
        self.ensure_sorted();
        self.storage.into_iter()
    }
}

impl<V: ArrowWriteableValue> Default for DeferredSortStorage<V> {
    fn default() -> Self {
        Self {
            storage: Vec::new(),
            index: AHashMap::new(),
            is_sorted: true,
        }
    }
}

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

    fn get_ref(&self, key: &CompositeKey) -> Option<&V> {
        self.storage.get(key)
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

    fn get_ref(&self, key: &CompositeKey) -> Option<&V> {
        self.storage
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|idx| &self.storage[idx].1)
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
    DeferredSortStorage(DeferredSortStorage<V>),
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
            BuilderStorage::DeferredSortStorage(storage) => storage.add(key, value),
            BuilderStorage::VecBuilderStorage(storage) => storage.add(key, value),
        }
    }

    pub fn delete(&mut self, key: &CompositeKey) -> Option<V> {
        match self {
            BuilderStorage::DeferredSortStorage(storage) => storage.delete(key),
            BuilderStorage::VecBuilderStorage(storage) => storage.delete(key),
        }
    }

    pub fn get(&self, key: &CompositeKey) -> Option<V::PreparedValue> {
        match self {
            BuilderStorage::DeferredSortStorage(storage) => storage.get(key),
            BuilderStorage::VecBuilderStorage(storage) => storage.get(key),
        }
    }

    pub fn get_ref(&self, key: &CompositeKey) -> Option<&V> {
        match self {
            BuilderStorage::DeferredSortStorage(storage) => storage.get_ref(key),
            BuilderStorage::VecBuilderStorage(storage) => storage.get_ref(key),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BuilderStorage::DeferredSortStorage(storage) => storage.len(),
            BuilderStorage::VecBuilderStorage(storage) => storage.len(),
        }
    }

    pub fn min_key(&mut self) -> Option<&CompositeKey> {
        match self {
            BuilderStorage::DeferredSortStorage(storage) => storage.min_key(),
            BuilderStorage::VecBuilderStorage(storage) => storage.min_key(),
        }
    }

    pub fn split_off(&mut self, key: &CompositeKey) -> Self {
        match self {
            BuilderStorage::DeferredSortStorage(storage) => {
                BuilderStorage::DeferredSortStorage(storage.split_off(key))
            }
            BuilderStorage::VecBuilderStorage(storage) => {
                BuilderStorage::VecBuilderStorage(storage.split_off(key))
            }
        }
    }

    pub fn iter<'referred_data>(
        &'referred_data mut self,
    ) -> Box<dyn Iterator<Item = (&'referred_data CompositeKey, &'referred_data V)> + 'referred_data>
    {
        match self {
            BuilderStorage::DeferredSortStorage(storage) => storage.iter(),
            BuilderStorage::VecBuilderStorage(storage) => storage.iter(),
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = (CompositeKey, V)> {
        match self {
            BuilderStorage::DeferredSortStorage(storage) => Either::Left(storage.into_iter()),
            BuilderStorage::VecBuilderStorage(storage) => Either::Right(storage.into_iter()),
        }
    }
}
