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
    /// Unique identifier for this bloom filter instance.
    id: uuid::Uuid,
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
}

/// A thread-safe, cloneable bloom filter for existence checks.
/// Generic over the item type `T` for type safety at the API boundary;
/// the underlying bit vector is type-erased via hashing.
/// Wraps an `Arc<Inner>` so clones share the same underlying filter.
pub struct BloomFilter<T: Hash + ?Sized> {
    inner: Arc<BloomFilterInner>,
    id: uuid::Uuid,
    _phantom: PhantomData<fn(&T)>,
}

impl<T: Hash + ?Sized> Clone for BloomFilter<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            id: self.id,
            _phantom: PhantomData,
        }
    }
}

impl<T: Hash + ?Sized> std::fmt::Debug for BloomFilter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field("id", &self.id)
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
    pub fn new(expected_items: u64) -> Self {
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
            }),
            id: uuid::Uuid::new_v4(),
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

    pub fn id(&self) -> uuid::Uuid {
        self.id
    }

    /// Approximate memory used by this bloom filter in bytes.
    pub fn memory_size(&self) -> usize {
        let bit_vector_bytes = self.inner.filter.num_bits() / 8;
        let live_count_bytes = std::mem::size_of::<AtomicU64>();
        let stale_count_bytes = std::mem::size_of::<AtomicU64>();
        let capacity_bytes = std::mem::size_of::<u64>();
        bit_vector_bytes + live_count_bytes + stale_count_bytes + capacity_bytes
    }

    /// Consume the bloom filter and return a `SerializedBloomFilter` ready for I/O.
    pub fn into_bytes(
        self,
        storage: Arc<Storage>,
        path: String,
    ) -> Result<SerializedBloomFilter, BloomFilterError> {
        let repr = BloomFilterRepr {
            id: self.id,
            bits: self.inner.filter.iter().collect(),
            num_hashes: self.inner.filter.num_hashes(),
            live_count: self.inner.live_count.load(Ordering::SeqCst),
            stale_count: self.inner.stale_count.load(Ordering::SeqCst),
            capacity: self.inner.capacity,
        };
        let bytes = bincode::serialize(&repr)
            .map_err(|e| BloomFilterError::Serialization(e.to_string()))?;
        Ok(SerializedBloomFilter {
            bytes,
            storage,
            path,
        })
    }

    #[cfg(test)]
    fn into_bytes_for_test(self) -> Result<Vec<u8>, BloomFilterError> {
        let inner = Arc::try_unwrap(self.inner).map_err(|_| {
            BloomFilterError::Serialization("other references still exist".to_string())
        })?;
        let repr = BloomFilterRepr {
            id: self.id,
            bits: inner.filter.iter().collect(),
            num_hashes: inner.filter.num_hashes(),
            live_count: inner.live_count.load(Ordering::SeqCst),
            stale_count: inner.stale_count.load(Ordering::SeqCst),
            capacity: inner.capacity,
        };
        bincode::serialize(&repr).map_err(|e| BloomFilterError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BloomFilterError> {
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
            }),
            id: repr.id,
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

    fn format_key(prefix_path: &str, id: uuid::Uuid) -> String {
        if prefix_path.is_empty() {
            format!("bloom_filter/{}", id)
        } else {
            format!("{}/bloom_filter/{}", prefix_path, id)
        }
    }

    /// Cache the bloom filter and return the serialized form ready for flush.
    /// The full storage path is constructed from `prefix_path` and the filter's id.
    pub async fn commit(
        &self,
        bf: BloomFilter<str>,
        prefix_path: &str,
    ) -> Result<SerializedBloomFilter, BloomFilterError> {
        let path = Self::format_key(prefix_path, bf.id());
        let key = bf.id().to_string();
        self.inner.cache.insert(key, bf.clone()).await;
        bf.into_bytes(self.inner.storage.clone(), path)
    }

    /// Look up a bloom filter by its storage path. Returns from cache if present,
    /// otherwise loads from storage, caches it, and returns it.
    pub async fn get(&self, path: &str) -> Result<BloomFilter<str>, BloomFilterError> {
        // The path ends with the bloom filter's UUID; use it as cache key.
        let cache_key = path.rsplit('/').next().unwrap_or(path).to_string();
        if let Ok(Some(cached)) = self.inner.cache.get(&cache_key).await {
            return Ok(cached);
        }
        let (bf, _) = self
            .inner
            .storage
            .fetch(
                path,
                GetOptions::new(StorageRequestPriority::P0).with_parallelism(),
                move |bytes_result| async move {
                    let bytes = bytes_result?;
                    BloomFilter::<str>::from_bytes(&bytes).map_err(|e| StorageError::Message {
                        message: e.to_string(),
                    })
                },
            )
            .await
            .map_err(BloomFilterError::Storage)?;
        // TODO(Sanket-temp): Should deep copy bloom filter here to avoid modifying the original one.
        self.inner
            .cache
            .insert(bf.id().to_string(), bf.clone())
            .await;
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

    /// Create a brand-new bloom filter sized for `expected_items`.
    pub fn create(&self, expected_items: u64) -> BloomFilter<str> {
        BloomFilter::new(expected_items)
    }

    /// Load an existing bloom filter from cache or storage with a fresh id
    /// for the new compaction cycle.
    pub async fn fork(&self, old_path: &str) -> Result<BloomFilter<str>, BloomFilterError> {
        let mut bf = self.get(old_path).await?;
        bf.id = uuid::Uuid::new_v4();
        Ok(bf)
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

    fn create_test_manager() -> (tempfile::TempDir, Arc<Storage>, BloomFilterManager) {
        let (tmp, storage) = chroma_storage::test_storage();
        let storage = Arc::new(storage);
        let manager = BloomFilterManager::new_for_test(storage.clone());
        (tmp, storage, manager)
    }

    #[tokio::test]
    async fn test_manager_commit_caches_and_serializes() {
        let (_tmp, _storage, manager) = create_test_manager();
        let bf = manager.create(100);
        bf.insert("alice");
        bf.insert("bob");

        let serialized = manager.commit(bf, "test_prefix").await.unwrap();
        let path = serialized.path().to_string();
        assert!(path.starts_with("test_prefix/bloom_filter/"));

        // After commit the BF should be in the cache; get() should return it
        // without needing to read from storage (we haven't called save() yet).
        let cached = manager.get(&path).await.unwrap();
        assert!(cached.contains("alice"));
        assert!(cached.contains("bob"));
        assert!(!cached.contains("charlie"));
        assert_eq!(cached.live_count(), 2);
    }

    #[tokio::test]
    async fn test_manager_get_roundtrip_from_storage() {
        let (_tmp, _storage, manager) = create_test_manager();
        let bf = manager.create(100);
        for i in 0..50 {
            bf.insert(&format!("user_{i}"));
        }

        // Serialize and persist to storage.
        let serialized = manager.commit(bf, "prefix").await.unwrap();
        let path = serialized.path().to_string();
        serialized.save().await.unwrap();

        // Create a *fresh* manager (empty cache) backed by the same storage.
        let manager2 = BloomFilterManager::new_for_test(manager.inner.storage.clone());

        // get() should load from storage, deserialize, cache, and return.
        let loaded = manager2.get(&path).await.unwrap();
        for i in 0..50 {
            assert!(
                loaded.contains(&format!("user_{i}")),
                "should contain user_{i}"
            );
        }
        assert!(!loaded.contains("user_999"));
        assert_eq!(loaded.live_count(), 50);
    }

    #[tokio::test]
    async fn test_manager_get_returns_cached_after_first_load() {
        let (_tmp, _storage, manager) = create_test_manager();
        let bf = manager.create(100);
        bf.insert("cached_item");

        let serialized = manager.commit(bf, "prefix").await.unwrap();
        let path = serialized.path().to_string();
        serialized.save().await.unwrap();

        // First get: loads from storage (fresh manager).
        let manager2 = BloomFilterManager::new_for_test(manager.inner.storage.clone());
        let first = manager2.get(&path).await.unwrap();
        assert!(first.contains("cached_item"));

        // Second get: should return from cache (same result).
        let second = manager2.get(&path).await.unwrap();
        assert!(second.contains("cached_item"));
        assert_eq!(second.live_count(), first.live_count());
    }

    #[tokio::test]
    async fn test_manager_fork_returns_same_contents() {
        let (_tmp, _storage, manager) = create_test_manager();
        let bf = manager.create(100);
        let original_id = bf.id();
        bf.insert("x");
        bf.insert("y");
        bf.mark_deleted();

        // Commit to cache.
        let serialized = manager.commit(bf, "original").await.unwrap();
        let path = serialized.path().to_string();

        // Fork from the committed path.
        let forked = manager.fork(&path).await.unwrap();

        // Forked filter has the same contents but a new id.
        assert!(forked.contains("x"));
        assert!(forked.contains("y"));
        assert_eq!(forked.live_count(), 1);
        assert_eq!(forked.stale_count(), 1);
        assert_ne!(forked.id(), original_id, "fork should assign a new id");
    }

    #[test]
    fn test_format_key() {
        let id = uuid::Uuid::new_v4();
        let key1 = BloomFilterManager::format_key("some/prefix", id);
        assert_eq!(key1, format!("some/prefix/bloom_filter/{}", id));

        let key2 = BloomFilterManager::format_key("", id);
        assert_eq!(key2, format!("bloom_filter/{}", id));

        // Different ids produce different keys.
        let id2 = uuid::Uuid::new_v4();
        assert_ne!(
            BloomFilterManager::format_key("same", id),
            BloomFilterManager::format_key("same", id2),
        );
    }

    #[test]
    fn test_insert_and_contains() {
        let bf = BloomFilter::<str>::new(1000);
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
        let bf = BloomFilter::<str>::new(1000);
        bf.insert("user_1");
        bf.insert("user_2");
        bf.mark_deleted();

        assert_eq!(bf.live_count(), 1);
        assert_eq!(bf.stale_count(), 1);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let bf = BloomFilter::<str>::new(1000);
        for i in 0..100 {
            bf.insert(&format!("user_{}", i));
        }
        bf.mark_deleted();
        bf.mark_deleted();

        let expected_live = bf.live_count();
        let expected_stale = bf.stale_count();
        let expected_capacity = bf.capacity();

        let bytes = bf.into_bytes_for_test().unwrap();
        let restored = BloomFilter::<str>::from_bytes(&bytes).unwrap();

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
        let bf = BloomFilter::<str>::new(1000);
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
        let bf = BloomFilter::<str>::new(10);
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
                storage_fetch_threshold: BloomFilterManagerConfig::default_storage_fetch_threshold(
                ),
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
                storage_fetch_threshold: BloomFilterManagerConfig::default_storage_fetch_threshold(
                ),
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
                storage_fetch_threshold: BloomFilterManagerConfig::default_storage_fetch_threshold(
                ),
            }),
        };
        assert!(mgr.is_enabled_for_collection(c1));
        assert!(!mgr.is_enabled_for_collection(c2));
        assert!(!mgr.is_enabled_for_collection(c3));
    }
}
