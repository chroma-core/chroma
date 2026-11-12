use crate::{arrow::types::ArrowWriteableValue, key::CompositeKey};
use ahash::AHashMap;
use rayon::slice::ParallelSliceMut;
use std::collections::BTreeMap;

/// Threshold for using parallel sort. Below this, serial sort is faster due to overhead.
const PARALLEL_SORT_THRESHOLD: usize = 10_000;

/// Ultra-high-performance storage with deferred sorting.
/// 
/// Optimizations:
/// - Pure append for inserts when no lookups needed (no hashing, no cloning)
/// - Lazy index building only when get() is called
/// - Parallel sort for large datasets  
/// - Binary search for post-sort lookups
pub struct DeferredSortStorage<V: ArrowWriteableValue> {
    /// Storage of key-value pairs
    storage: Vec<(CompositeKey, V)>,
    /// Lazy index - only built when get() is called before commit
    /// None = pure append mode (fastest), Some = indexed mode
    index: Option<AHashMap<CompositeKey, usize>>,
    /// Whether the storage is currently sorted
    is_sorted: bool,
}

impl<V: ArrowWriteableValue> DeferredSortStorage<V> {
    /// Create with pre-allocated capacity
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            storage: Vec::with_capacity(capacity),
            index: None,
            is_sorted: true,
        }
    }

    /// Add with dedup support - builds index on first call, then O(1) per add
    #[inline(always)]
    pub fn add(&mut self, key: CompositeKey, value: V) {
        // Ensure we have an index for O(1) dedup
        self.ensure_index_for_writes();
        
        if let Some(ref mut index) = self.index {
            match index.get(&key) {
                Some(&existing_idx) => {
                    // Key exists - update in place (no allocation)
                    self.storage[existing_idx].1 = value;
                    return;
                }
                None => {
                    // New key - add to index and storage
                    let idx = self.storage.len();
                    index.insert(key.clone(), idx);
                }
            }
        }
        self.storage.push((key, value));
        self.is_sorted = false;
    }

    /// Same as add - both use the same path now
    #[inline(always)]
    pub fn add_unchecked(&mut self, key: CompositeKey, value: V) {
        self.add(key, value);
    }

    /// Build index for write operations (always builds if not present)
    #[inline]
    fn ensure_index_for_writes(&mut self) {
        if self.index.is_none() {
            let mut index = AHashMap::with_capacity(self.storage.len().max(16));
            // For duplicates, later entries win
            for (idx, (key, _)) in self.storage.iter().enumerate() {
                index.insert(key.clone(), idx);
            }
            self.index = Some(index);
        }
    }

    /// Build index lazily when needed for lookups (only if not sorted)
    fn ensure_index(&mut self) {
        if self.index.is_none() && !self.is_sorted {
            self.ensure_index_for_writes();
        }
    }

    fn delete(&mut self, key: &CompositeKey) -> Option<V> {
        self.ensure_index_for_writes();
        
        if let Some(ref mut index) = self.index {
            if let Some(idx) = index.remove(key) {
                let (_, value) = self.storage.swap_remove(idx);
                
                if idx < self.storage.len() {
                    let moved_key = &self.storage[idx].0;
                    index.insert(moved_key.clone(), idx);
                }
                
                self.is_sorted = false;
                return Some(value);
            }
        }
        None
    }

    #[inline]
    fn get(&self, key: &CompositeKey) -> Option<V::PreparedValue> {
        if self.is_sorted {
            self.storage
                .binary_search_by(|(k, _)| k.cmp(key))
                .ok()
                .map(|idx| V::prepare(self.storage[idx].1.clone()))
        } else if let Some(ref index) = self.index {
            index
                .get(key)
                .map(|&idx| V::prepare(self.storage[idx].1.clone()))
        } else {
            // No index yet - linear search from end (last value wins)
            self.storage
                .iter()
                .rev()
                .find(|(k, _)| k == key)
                .map(|(_, v)| V::prepare(v.clone()))
        }
    }

    #[inline]
    fn get_ref(&self, key: &CompositeKey) -> Option<&V> {
        if self.is_sorted {
            self.storage
                .binary_search_by(|(k, _)| k.cmp(key))
                .ok()
                .map(|idx| &self.storage[idx].1)
        } else if let Some(ref index) = self.index {
            index.get(key).map(|&idx| &self.storage[idx].1)
        } else {
            self.storage
                .iter()
                .rev()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v)
        }
    }

    fn min_key(&mut self) -> Option<&CompositeKey> {
        self.ensure_sorted();
        self.storage.first().map(|(key, _)| key)
    }

    #[inline]
    fn ensure_sorted(&mut self) {
        if !self.is_sorted && !self.storage.is_empty() {
            // Sort
            if self.storage.len() >= PARALLEL_SORT_THRESHOLD {
                self.storage.par_sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            } else {
                self.storage.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            }
            
            // Dedup if we might have duplicates (index was None during inserts)
            if self.index.is_none() {
                // Keep last value: reverse, dedup (keeps first of each), reverse back
                self.storage.reverse();
                self.storage.dedup_by(|(a, _), (b, _)| a == b);
                self.storage.reverse();
            }
            
            self.index = None;
            self.is_sorted = true;
        }
    }

    fn split_off(&mut self, key: &CompositeKey) -> Self {
        self.ensure_sorted();
        let split_index = self
            .storage
            .binary_search_by(|(k, _)| k.cmp(key))
            .unwrap_or_else(|i| i);
        let split_off = self.storage.split_off(split_index);

        Self {
            storage: split_off,
            index: None,
            is_sorted: true,
        }
    }

    fn len(&self) -> usize {
        self.storage.len()
    }

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
            index: None,
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
