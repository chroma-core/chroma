use async_trait::async_trait;
use chroma_cache::{Cache, CacheConfig, Weighted};
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_types::CollectionUuid;
use fastbloom::AtomicBloomFilter;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, PutOptions, Storage, StorageError,
};
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
/// Produced by `BloomFilter::into_bytes()` during commit; written
/// to storage by calling `save()` during flush.
/// Carries the pre-determined storage path from the `BloomFilter`.
pub struct SerializedBloomFilter {
    bytes: Vec<u8>,
    storage: Arc<Storage>,
    path: String,
}

impl SerializedBloomFilter {
    /// Write the serialized bloom filter bytes to its pre-determined storage path.
    pub async fn save(&self) -> Result<(), BloomFilterError> {
        self.storage
            .put_bytes(&self.path, self.bytes.clone(), PutOptions::default())
            .await
            .map_err(BloomFilterError::Storage)?;
        Ok(())
    }

    pub fn path(&self) -> &str {
        &self.path
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
    path: Option<String>,
    manager: Option<BloomFilterManager>,
    _phantom: PhantomData<fn(&T)>,
}

impl<T: Hash + ?Sized> Clone for BloomFilter<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            path: self.path.clone(),
            manager: self.manager.clone(),
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
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
}

impl ChromaError for BloomFilterError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            BloomFilterError::Storage(_) => chroma_error::ErrorCodes::Internal,
            BloomFilterError::Serialization(_) => chroma_error::ErrorCodes::Internal,
            BloomFilterError::Deserialization(_) => chroma_error::ErrorCodes::Internal,
            BloomFilterError::InvalidConfig(_) => chroma_error::ErrorCodes::InvalidArgument,
        }
    }
}

impl<T: Hash + ?Sized> BloomFilter<T> {
    /// Create a new bloom filter sized for `expected_items` with a 0.001% false positive rate.
    /// Generates a unique storage path under `prefix_path` automatically.
    /// Pass `None` for `storage` / `prefix_path` for in-memory-only filters (e.g. tests).
    pub fn new(
        expected_items: u64,
        storage: Option<Arc<Storage>>,
        prefix_path: Option<&str>,
        manager: Option<BloomFilterManager>,
    ) -> Self {
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
            path: prefix_path.map(BloomFilterManager::format_key),
            manager,
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

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// Return a new handle with a freshly generated storage path under the given prefix.
    fn with_fresh_path(mut self, prefix_path: &str) -> Self {
        self.path = Some(BloomFilterManager::format_key(prefix_path));
        self
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

    /// Consume the bloom filter and return a `SerializedBloomFilter` ready for I/O.
    /// Returns `None` if storage or path is not configured.
    pub fn into_bytes(self) -> Result<Option<SerializedBloomFilter>, BloomFilterError> {
        let (storage, path) = match (self.inner.storage.clone(), self.path) {
            (Some(s), Some(p)) => (s, p),
            _ => return Ok(None),
        };
        let num_u64s = self.inner.filter.num_bits() / 64;
        let mut bits = Vec::with_capacity(num_u64s);
        bits.extend(self.inner.filter.iter());
        let repr = BloomFilterRepr {
            bits: self.inner.filter.iter().collect(),
            num_hashes: self.inner.filter.num_hashes(),
            live_count: self.inner.live_count.load(Ordering::SeqCst),
            stale_count: self.inner.stale_count.load(Ordering::SeqCst),
            capacity: self.inner.capacity,
        };
        let bytes = bincode::serialize(&repr)
            .map_err(|e| BloomFilterError::Serialization(e.to_string()))?;
        Ok(Some(SerializedBloomFilter {
            bytes,
            storage,
            path,
        }))
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
        path: Option<String>,
        manager: Option<BloomFilterManager>,
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
            path,
            manager,
            _phantom: PhantomData,
        })
    }
}

impl<T: Hash + ?Sized> Weighted for BloomFilter<T> {
    fn weight(&self) -> usize {
        self.memory_size()
    }
}

/// Configuration for the `BloomFilterManager`, which caches bloom filter
/// instances across queries to avoid redundant loads from storage.
#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct BloomFilterManagerConfig {
    #[serde(default)]
    pub cache_config: CacheConfig,

    /// Controls which collections use bloom filters.
    ///   - `[]` (default) — disabled for all collections.
    ///   - `["all"]` — enabled for every collection.
    ///   - `["<uuid>", ...]` — enabled only for the listed collections.
    #[serde(default)]
    pub enabled_collection_ids: Vec<String>,
    /// Minimum number of unique user IDs in a log batch before we fetch
    /// the bloom filter from storage (if not already cached). Below this
    /// threshold, blockfile lookups are cheap enough to not justify the fetch.
    #[serde(default = "BloomFilterManagerConfig::default_storage_fetch_threshold")]
    pub storage_fetch_threshold: usize,
}

impl BloomFilterManagerConfig {
    fn default_storage_fetch_threshold() -> usize {
        100
    }
}

impl Default for BloomFilterManagerConfig {
    fn default() -> Self {
        Self {
            cache_config: CacheConfig::Nop,
            enabled_collection_ids: Vec::new(),
            storage_fetch_threshold: Self::default_storage_fetch_threshold(),
        }
    }
}

/// Parsed representation of `enabled_collection_ids`.
#[derive(Debug, Clone)]
enum BloomFilterScope {
    /// Disabled for all collections (empty list).
    None,
    /// Enabled for every collection (`["all"]`).
    All,
    /// Enabled for a specific set of collections.
    Some(HashSet<CollectionUuid>),
}

struct BloomFilterManagerInner {
    cache: Box<dyn Cache<String, BloomFilter<str>>>,
    storage: Arc<Storage>,
    scope: BloomFilterScope,
    storage_fetch_threshold: usize,
}

/// Manages a shared cache of bloom filter instances across queries.
/// Keyed by segment storage path so a filter is loaded from storage at most once.
/// Cheaply cloneable via an internal `Arc`.
#[derive(Clone)]
pub struct BloomFilterManager {
    inner: Arc<BloomFilterManagerInner>,
}

impl Debug for BloomFilterManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilterManager").finish()
    }
}

#[async_trait]
impl Configurable<(BloomFilterManagerConfig, Storage)> for BloomFilterManager {
    async fn try_from_config(
        config: &(BloomFilterManagerConfig, Storage),
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (manager_config, storage) = config;
        let cache = chroma_cache::from_config(&manager_config.cache_config).await?;
        let scope = if manager_config.enabled_collection_ids.is_empty() {
            BloomFilterScope::None
        } else if manager_config.enabled_collection_ids.len() == 1
            && manager_config.enabled_collection_ids[0].eq_ignore_ascii_case("all")
        {
            BloomFilterScope::All
        } else {
            let mut ids = HashSet::new();
            for id_str in &manager_config.enabled_collection_ids {
                let uuid = uuid::Uuid::parse_str(id_str).map_err(|_| {
                    Box::new(BloomFilterError::InvalidConfig(format!(
                        "invalid collection UUID: {}",
                        id_str
                    ))) as Box<dyn ChromaError>
                })?;
                ids.insert(CollectionUuid(uuid));
            }
            BloomFilterScope::Some(ids)
        };
        Ok(Self {
            inner: Arc::new(BloomFilterManagerInner {
                cache,
                storage: Arc::new(storage.clone()),
                scope,
                storage_fetch_threshold: manager_config.storage_fetch_threshold,
            }),
        })
    }
}

impl BloomFilterManager {
    pub fn is_enabled_for_collection(&self, collection_id: CollectionUuid) -> bool {
        match &self.inner.scope {
            BloomFilterScope::None => false,
            BloomFilterScope::All => true,
            BloomFilterScope::Some(ids) => ids.contains(&collection_id),
        }
    }

    pub fn format_key(prefix_path: &str) -> String {
        let id = uuid::Uuid::new_v4();
        if prefix_path.is_empty() {
            format!("bloom_filter/{}", id)
        } else {
            format!("{}/bloom_filter/{}", prefix_path, id)
        }
    }

    /// Cache the bloom filter under its path and return the serialized form
    /// ready for flush. Mirrors `BlockManager::commit`.
    pub async fn commit(
        &self,
        bf: BloomFilter<str>,
    ) -> Result<Option<SerializedBloomFilter>, BloomFilterError> {
        if let Some(path) = bf.path() {
            self.inner.cache.insert(path.to_string(), bf.clone()).await;
        }
        bf.into_bytes()
    }

    /// Look up a bloom filter by its storage path. Returns from cache if present,
    /// otherwise loads from storage, caches it, and returns it.
    pub async fn get(&self, path: &str) -> Result<BloomFilter<str>, BloomFilterError> {
        let key = path.to_string();
        if let Ok(Some(cached)) = self.inner.cache.get(&key).await {
            return Ok(cached);
        }
        let storage_for_bf = self.inner.storage.clone();
        let key_for_bf = key.clone();
        let manager_for_bf = self.clone();
        let (bf, _) = self
            .inner
            .storage
            .fetch(
                path,
                GetOptions::new(StorageRequestPriority::P0).with_parallelism(),
                move |bytes_result| async move {
                    let bytes = bytes_result?;
                    BloomFilter::<str>::from_bytes(
                        &bytes,
                        Some(storage_for_bf),
                        Some(key_for_bf),
                        Some(manager_for_bf),
                    )
                    .map_err(|e| StorageError::Message {
                        message: e.to_string(),
                    })
                },
            )
            .await
            .map_err(BloomFilterError::Storage)?;
        // TODO(Sanket-temp): Should deep copy bloom filter here to avoid modifying the original one.
        self.inner.cache.insert(key, bf.clone()).await;
        Ok(bf)
    }

    /// Returns the bloom filter only if it's already in the cache.
    /// Does NOT fetch from storage. Near-zero cost.
    pub async fn get_if_cached(&self, path: &str) -> Option<BloomFilter<str>> {
        self.inner.cache.get(&path.to_string()).await.ok().flatten()
    }

    pub fn storage_fetch_threshold(&self) -> usize {
        self.inner.storage_fetch_threshold
    }

    /// Load an existing bloom filter and fork it for a new compaction cycle.
    /// Generates a fresh storage path under `prefix_path` for the new copy.
    pub async fn fork(
        &self,
        old_path: &str,
        prefix_path: &str,
    ) -> Result<BloomFilter<str>, BloomFilterError> {
        let bf = self.get(old_path).await?;
        Ok(bf.with_fresh_path(prefix_path))
    }

    pub fn new_for_test(storage: Arc<Storage>) -> Self {
        Self {
            inner: Arc::new(BloomFilterManagerInner {
                cache: chroma_cache::new_non_persistent_cache_for_test(),
                storage,
                scope: BloomFilterScope::All,
                storage_fetch_threshold: BloomFilterManagerConfig::default_storage_fetch_threshold(
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_contains() {
        let bf = BloomFilter::<str>::new(1000, None, None, None);
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
        let bf = BloomFilter::<str>::new(1000, None, None, None);
        bf.insert("user_1");
        bf.insert("user_2");
        bf.mark_deleted();

        assert_eq!(bf.live_count(), 1);
        assert_eq!(bf.stale_count(), 1);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let bf = BloomFilter::<str>::new(1000, None, None, None);
        for i in 0..100 {
            bf.insert(&format!("user_{}", i));
        }
        bf.mark_deleted();
        bf.mark_deleted();

        let expected_live = bf.live_count();
        let expected_stale = bf.stale_count();
        let expected_capacity = bf.capacity();

        let bytes = bf.into_bytes_for_test().unwrap();
        let restored = BloomFilter::<str>::from_bytes(&bytes, None, None, None).unwrap();

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
        let bf = BloomFilter::<str>::new(1000, None, None, None);
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
        let bf = BloomFilter::<str>::new(10, None, None, None);
        for i in 0..11 {
            bf.insert(&format!("user_{}", i));
        }
        // live=11, stale=0, total=11 > capacity=10
        assert!(bf.needs_rebuild());
    }

    #[test]
    fn test_is_enabled_for_collection() {
        let storage = Arc::new(Storage::Local(chroma_storage::local::LocalStorage::new(
            "/tmp/bf_test",
        )));

        let c1 = CollectionUuid(uuid::Uuid::new_v4());
        let c2 = CollectionUuid(uuid::Uuid::new_v4());
        let c3 = CollectionUuid(uuid::Uuid::new_v4());

        // Scope::None -> all disabled
        let mgr = BloomFilterManager {
            inner: Arc::new(BloomFilterManagerInner {
                cache: chroma_cache::new_non_persistent_cache_for_test(),
                storage: storage.clone(),
                scope: BloomFilterScope::None,
            }),
        };
        assert!(!mgr.is_enabled_for_collection(c1));
        assert!(!mgr.is_enabled_for_collection(c2));

        // Scope::All -> all enabled
        let mgr = BloomFilterManager {
            inner: Arc::new(BloomFilterManagerInner {
                cache: chroma_cache::new_non_persistent_cache_for_test(),
                storage: storage.clone(),
                scope: BloomFilterScope::All,
            }),
        };
        assert!(mgr.is_enabled_for_collection(c1));
        assert!(mgr.is_enabled_for_collection(c2));

        // Scope::Some -> only listed collections
        let mgr = BloomFilterManager {
            inner: Arc::new(BloomFilterManagerInner {
                cache: chroma_cache::new_non_persistent_cache_for_test(),
                storage: storage.clone(),
                scope: BloomFilterScope::Some(HashSet::from([c1])),
            }),
        };
        assert!(mgr.is_enabled_for_collection(c1));
        assert!(!mgr.is_enabled_for_collection(c2));
        assert!(!mgr.is_enabled_for_collection(c3));
    }
}
