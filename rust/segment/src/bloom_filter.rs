use chroma_cache::Weighted;
use fastbloom::AtomicBloomFilter;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chroma_storage::{GetOptions, PutOptions, Storage, StorageError};
use thiserror::Error;

const DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.001;
const REBUILD_STALE_RATIO: f64 = 0.3;
const BLOOM_FILTER_SEED: u128 = 0xDEAD_BEEF_CAFE_BABE_0123_4567_89AB_CDEF;

/// Internal bincode-serializable representation of the bloom filter state.
/// A fixed seed is needed so hashing stays consistent after deserialization.
#[derive(Serialize, Deserialize)]
struct BloomFilterRepr {
    /// Raw bit vector backing the bloom filter.
    bits: Vec<u64>,
    /// Number of hash functions used per item.
    num_hashes: u32,
    /// Snapshot of `BloomFilter::live_count` at serialization time.
    live_count: u64,
    /// Snapshot of `BloomFilter::stale_count` at serialization time.
    stale_count: u64,
    /// The capacity the filter was originally sized for.
    capacity: u64,
}

/// A bloom filter that has been serialized to bytes, ready for I/O.
/// Produced by `BloomFilter::serialize()` during commit; written
/// to storage by calling `save()` during flush.
pub struct SerializedBloomFilter {
    bytes: Vec<u8>,
    storage: Arc<Storage>,
}

impl SerializedBloomFilter {
    /// Write the serialized bloom filter bytes to storage.
    pub async fn save(self, path: &str) -> Result<(), BloomFilterError> {
        self.storage
            .put_bytes(path, self.bytes, PutOptions::default())
            .await
            .map_err(BloomFilterError::Storage)?;
        Ok(())
    }
}

struct BloomFilterInner {
    /// The underlying lock-free bloom filter supporting concurrent inserts and lookups.
    filter: AtomicBloomFilter,
    /// Number of items currently live. Incremented on add, decremented on delete.
    live_count: AtomicU64,
    /// Number of items deleted since the last rebuild. These are ghost entries still present
    /// in the bloom filter (bloom filters cannot remove elements), contributing to false positives.
    stale_count: AtomicU64,
    /// The number of expected items the filter was originally sized for. Used together with
    /// live_count and stale_count to decide when the filter's FPR has degraded enough to rebuild.
    capacity: u64,
    /// Storage backend for persistence. None for in-memory-only filters (e.g. tests).
    storage: Option<Arc<Storage>>,
}

/// A thread-safe, cloneable bloom filter for existence checks.
/// Generic over the item type `T` for type safety at the API boundary;
/// the underlying bit vector is type-erased via hashing.
/// Wraps an `Arc<Inner>` so clones share the same underlying filter.
pub struct BloomFilter<T: Hash + ?Sized> {
    inner: Arc<BloomFilterInner>,
    _phantom: PhantomData<fn(&T)>,
}

impl<T: Hash + ?Sized> Clone for BloomFilter<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: Hash + ?Sized> std::fmt::Debug for BloomFilter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field("live_count", &self.inner.live_count.load(Ordering::Relaxed))
            .field(
                "stale_count",
                &self.inner.stale_count.load(Ordering::Relaxed),
            )
            .field("capacity", &self.inner.capacity)
            .field("num_bits", &self.inner.filter.num_bits())
            .finish()
    }
}

#[derive(Error, Debug)]
pub enum BloomFilterError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("No storage backend configured")]
    NoStorage,
}

impl<T: Hash + ?Sized> BloomFilter<T> {
    /// Create a new bloom filter sized for `expected_items` with a 0.1% false positive rate.
    pub fn new(expected_items: u64, storage: Option<Arc<Storage>>) -> Self {
        let capacity = expected_items.max(1);
        let filter = AtomicBloomFilter::with_false_pos(DEFAULT_FALSE_POSITIVE_RATE)
            .seed(&BLOOM_FILTER_SEED)
            .expected_items(capacity as usize);
        Self {
            inner: Arc::new(BloomFilterInner {
                filter,
                live_count: AtomicU64::new(0),
                stale_count: AtomicU64::new(0),
                capacity,
                storage,
            }),
            _phantom: PhantomData,
        }
    }

    /// Thread-safe insert. Call when a new item is added to the segment.
    /// Note: the two atomic operations (filter insert + counter bump) do not need to be
    /// jointly atomic — `live_count` is only used by the `needs_rebuild` heuristic, so a
    /// momentarily stale counter is harmless. `contains` correctness depends only on the
    /// filter, which is updated first.
    pub fn insert(&self, item: &T) {
        self.inner.filter.insert(item);
        self.inner.live_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns `false` if the item is definitely not in the filter.
    /// Returns `true` if the item is possibly in the filter (may be a false positive).
    pub fn contains(&self, item: &T) -> bool {
        self.inner.filter.contains(item)
    }

    /// Thread-safe delete marker. Call when an item is removed from the segment.
    /// The bloom filter cannot actually remove the entry, so this tracks staleness
    /// to trigger a future rebuild.
    /// The two counter updates are individually atomic and only feed the
    /// `needs_rebuild` heuristic, so relaxed ordering is sufficient.
    pub fn mark_deleted(&self) {
        self.inner.stale_count.fetch_add(1, Ordering::Relaxed);
        self.inner.live_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns true when the bloom filter has degraded enough to warrant a rebuild.
    /// Two conditions trigger a rebuild:
    /// 1. Ghost entries from deletes exceed 30% of total entries (FPR degradation).
    /// 2. Total entries exceed the capacity the filter was sized for (FPR degradation).
    pub fn needs_rebuild(&self) -> bool {
        let live = self.inner.live_count.load(Ordering::Relaxed);
        let stale = self.inner.stale_count.load(Ordering::Relaxed);
        let total = live + stale;

        if total > 0 && (stale as f64 / total as f64) > REBUILD_STALE_RATIO {
            return true;
        }

        if total > self.inner.capacity {
            return true;
        }

        false
    }

    pub fn live_count(&self) -> u64 {
        self.inner.live_count.load(Ordering::Relaxed)
    }

    pub fn stale_count(&self) -> u64 {
        self.inner.stale_count.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    /// Approximate memory used by this bloom filter in bytes.
    pub fn memory_size(&self) -> usize {
        let bit_vector_bytes = self.inner.filter.num_bits() / 8;
        let live_count_bytes = std::mem::size_of::<AtomicU64>();
        let stale_count_bytes = std::mem::size_of::<AtomicU64>();
        let capacity_bytes = std::mem::size_of::<u64>();
        let storage_bytes = std::mem::size_of::<Option<Arc<Storage>>>();
        bit_vector_bytes + live_count_bytes + stale_count_bytes + capacity_bytes + storage_bytes
    }

    /// Consume the bloom filter and return a `SerializedBloomFilter`
    /// that can be persisted via `save()`. Must be called after all concurrent writers
    /// are done (e.g. during `commit`).
    /// Fails if other clones of this handle still exist.
    pub fn into_bytes(self) -> Result<Option<SerializedBloomFilter>, BloomFilterError> {
        let inner = Arc::try_unwrap(self.inner).map_err(|_| {
            BloomFilterError::Serialization(
                "Cannot serialize: other references to the bloom filter still exist".to_string(),
            )
        })?;
        let storage = match inner.storage {
            Some(s) => s,
            None => return Ok(None),
        };
        let repr = BloomFilterRepr {
            bits: inner.filter.iter().collect(),
            num_hashes: inner.filter.num_hashes(),
            live_count: inner.live_count.load(Ordering::SeqCst),
            stale_count: inner.stale_count.load(Ordering::SeqCst),
            capacity: inner.capacity,
        };
        let bytes = bincode::serialize(&repr)
            .map_err(|e| BloomFilterError::Serialization(e.to_string()))?;
        Ok(Some(SerializedBloomFilter { bytes, storage }))
    }

    #[cfg(test)]
    fn into_bytes_for_test(self) -> Result<Vec<u8>, BloomFilterError> {
        let inner = Arc::try_unwrap(self.inner).map_err(|_| {
            BloomFilterError::Serialization("other references still exist".to_string())
        })?;
        let repr = BloomFilterRepr {
            bits: inner.filter.iter().collect(),
            num_hashes: inner.filter.num_hashes(),
            live_count: inner.live_count.load(Ordering::SeqCst),
            stale_count: inner.stale_count.load(Ordering::SeqCst),
            capacity: inner.capacity,
        };
        bincode::serialize(&repr).map_err(|e| BloomFilterError::Serialization(e.to_string()))
    }

    pub fn from_bytes(
        bytes: &[u8],
        storage: Option<Arc<Storage>>,
    ) -> Result<Self, BloomFilterError> {
        let repr: BloomFilterRepr = bincode::deserialize(bytes)
            .map_err(|e| BloomFilterError::Deserialization(e.to_string()))?;
        let filter = AtomicBloomFilter::from_vec(repr.bits)
            .seed(&BLOOM_FILTER_SEED)
            .hashes(repr.num_hashes);
        Ok(Self {
            inner: Arc::new(BloomFilterInner {
                filter,
                live_count: AtomicU64::new(repr.live_count),
                stale_count: AtomicU64::new(repr.stale_count),
                capacity: repr.capacity,
                storage,
            }),
            _phantom: PhantomData,
        })
    }

    /// Load a bloom filter from storage, embedding the storage reference for future saves.
    pub async fn load(storage: Arc<Storage>, path: &str) -> Result<Self, BloomFilterError> {
        let bytes = storage
            .get(path, GetOptions::default())
            .await
            .map_err(BloomFilterError::Storage)?;
        Self::from_bytes(&bytes, Some(storage))
    }
}

impl<T: Hash + ?Sized> Weighted for BloomFilter<T> {
    fn weight(&self) -> usize {
        self.memory_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_contains() {
        let bf = BloomFilter::<str>::new(1000, None);
        bf.insert("user_1");
        bf.insert("user_2");

        assert!(bf.contains("user_1"));
        assert!(bf.contains("user_2"));
        assert!(!bf.contains("user_never_inserted"));
        assert_eq!(bf.live_count(), 2);
        assert_eq!(bf.stale_count(), 0);
    }

    #[test]
    fn test_mark_deleted() {
        let bf = BloomFilter::<str>::new(1000, None);
        bf.insert("user_1");
        bf.insert("user_2");
        bf.mark_deleted();

        assert_eq!(bf.live_count(), 1);
        assert_eq!(bf.stale_count(), 1);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let bf = BloomFilter::<str>::new(1000, None);
        for i in 0..100 {
            bf.insert(&format!("user_{}", i));
        }
        bf.mark_deleted();
        bf.mark_deleted();

        let expected_live = bf.live_count();
        let expected_stale = bf.stale_count();
        let expected_capacity = bf.capacity();

        let bytes = bf.into_bytes_for_test().unwrap();
        let restored = BloomFilter::<str>::from_bytes(&bytes, None).unwrap();

        assert_eq!(restored.live_count(), expected_live);
        assert_eq!(restored.stale_count(), expected_stale);
        assert_eq!(restored.capacity(), expected_capacity);

        for i in 0..100 {
            assert!(restored.contains(&format!("user_{}", i)));
        }
        assert!(!restored.contains("user_never_inserted"));
    }

    #[test]
    fn test_needs_rebuild_stale_ratio() {
        let bf = BloomFilter::<str>::new(1000, None);
        for i in 0..10 {
            bf.insert(&format!("user_{}", i));
        }
        assert!(!bf.needs_rebuild());

        // Delete more than 30% => triggers rebuild
        for _ in 0..4 {
            bf.mark_deleted();
        }
        // live=6, stale=4, total=10, stale_ratio=0.4 > 0.3
        assert!(bf.needs_rebuild());
    }

    #[test]
    fn test_needs_rebuild_over_capacity() {
        let bf = BloomFilter::<str>::new(10, None);
        for i in 0..11 {
            bf.insert(&format!("user_{}", i));
        }
        // live=11, stale=0, total=11 > capacity=10
        assert!(bf.needs_rebuild());
    }
}
