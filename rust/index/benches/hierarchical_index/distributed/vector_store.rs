//! Full vectors stored with their owner in the index's own storage, as an
//! Arrow blockfile. Each vector is keyed by its leaf (the blockfile prefix)
//! and its position in that leaf's saved posting list (the u32 key), so the
//! key space is unbounded by corpus size and a leaf's vectors share blocks.
//!
//! A writer streams one owner's leaves into an ordered blockfile and flushes
//! blocks and the root to storage. A reader opens that blockfile and loads
//! blocks lazily through a byte-bounded in-memory block cache, so a reranking
//! query pays for the blocks that hold its candidates and nothing else.
//!
//! The public API is synchronous. Async storage and blockfile calls run on a
//! dedicated tokio runtime owned by this module (worker count read once from
//! `SPANN_STORAGE_RUNTIME_WORKERS`, default 4). Callers must not call this API
//! from inside a tokio runtime thread, because `block_on` panics there.
use super::generation_backend::DurableStoreConfig;
use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider, BlockfileReader,
    BlockfileWriter, BlockfileWriterOptions,
};
use chroma_cache::{foyer::Code, Cache, CacheError, FoyerCacheConfig, PersistentCache, Weighted};
use chroma_config::{registry::Registry, Configurable};
use chroma_storage::{
    config::{S3CredentialsConfig, S3StorageConfig, StorageConfig},
    local::LocalStorage,
    Storage,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

/// Locates one owner's full-vector blockfile in a saved generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorStoreRef {
    /// Layout identifier; readers reject layouts they do not know.
    pub kind: String,
    /// The blockfile's root ID in the blockfile provider.
    pub blockfile_id: String,
    /// Storage path prefix the blockfile's blocks and root live under.
    pub prefix_path: String,
    pub owner: u64,
    pub rows: u64,
    pub dimensions: usize,
    pub max_block_size_bytes: usize,
}

pub const LEAF_ORDINAL_LAYOUT: &str = "arrow-blockfile-leaf-ordinal-v1";

/// The blockfile prefix for a leaf: fixed-width lowercase hex, so string order
/// equals numeric order and an ordered writer can append leaves in ID order.
pub fn leaf_prefix(leaf: u128) -> String {
    format!("{leaf:032x}")
}

/// Default `max_block_size_bytes` (512 KiB). The ordered writer splits a
/// block in half when it outgrows this bound, so appended blocks are stored
/// at about half of it: about 256 KiB, or 85 vectors of 768 f32s.
///
/// A rerank reads about 100 vectors per owner per query, clustered in a few
/// dozen leaves. A cold read costs one storage GET per distinct block touched,
/// and the whole block is transferred and cached. Smaller blocks transfer and
/// cache fewer unrelated vectors per touched leaf; larger blocks mean fewer
/// objects, a smaller root, and fewer GETs when a leaf's candidates span its
/// posting list. At about 256 KiB stored, S3 first-byte latency still
/// dominates transfer time, a cold leaf costs a quarter of the bytes of a
/// 1 MiB block (the reference bench uses 3 MiB, sized for full scans), and a
/// 15 GB owner is about 60,000 objects with a root of a few MiB.
pub const DEFAULT_VECTOR_BLOCK_SIZE_BYTES: usize = 512 * 1024;

/// Bytes of vector payload the writer buffers before it commits and flushes
/// the blockfile and forks it to continue. This bounds writer memory for
/// owners far larger than RAM (see `write_owner_vectors`).
pub const VECTOR_WRITE_CHUNK_BYTES: usize = 256 * 1024 * 1024;

/// Block-cache budget the writer's provider uses. Committed blocks pass
/// through this cache; the writer only reads back the last block of each
/// flushed chunk, so a small cache suffices.
const WRITER_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// Maximum block loads one `get_many` call keeps in flight.
pub const MAX_CONCURRENT_BLOCK_LOADS: usize = 32;
const NUM_CONCURRENT_BLOCK_FLUSHES: usize = 16;
const MIN_BLOCK_SIZE_BYTES: usize = 16 * 1024;
const MAX_BLOCK_SIZE_BYTES: usize = 64 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;
/// Cached roots weigh 1 each; a reader needs one, a writer one per chunk.
const ROOT_CACHE_ENTRIES: usize = 64;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let workers = std::env::var("SPANN_STORAGE_RUNTIME_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| (1..=64).contains(n))
            .unwrap_or(4);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("spann-vector-store")
            .enable_all()
            .build()
            .expect("vector store runtime")
    })
}

/// Where an owner's vector blockfile lives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VectorStoreLocation {
    /// An S3 bucket. Blocks and the root live under
    /// `<config.prefix>/vectors/owner-<owner>/{block,root}/<uuid>`.
    Durable(DurableStoreConfig),
    /// A local directory used as the storage root. Blocks and the root live
    /// under `<dir>/vectors/owner-<owner>/{block,root}/<uuid>`.
    Local(PathBuf),
}

impl VectorStoreLocation {
    /// The blockfile prefix path for one owner's vectors at this location.
    pub fn owner_prefix_path(&self, owner: u64) -> String {
        match self {
            VectorStoreLocation::Durable(config) => {
                format!("{}/vectors/owner-{owner}", config.prefix)
            }
            VectorStoreLocation::Local(_) => format!("vectors/owner-{owner}"),
        }
    }

    async fn storage(&self) -> Result<Storage, String> {
        match self {
            VectorStoreLocation::Durable(config) => {
                config.validate().map_err(|e| e.to_string())?;
                let config = StorageConfig::S3(S3StorageConfig {
                    bucket: config.bucket.clone(),
                    credentials: S3CredentialsConfig::AWS,
                    ..Default::default()
                });
                Storage::try_from_config(&config, &Registry::default())
                    .await
                    .map_err(|e| format!("vector store storage configuration: {e}"))
            }
            VectorStoreLocation::Local(dir) => {
                std::fs::create_dir_all(dir)
                    .map_err(|e| format!("create {}: {e}", dir.display()))?;
                let root = dir
                    .to_str()
                    .ok_or_else(|| format!("non-UTF-8 path {}", dir.display()))?;
                Ok(Storage::Local(LocalStorage::new(root)))
            }
        }
    }
}

/// Cumulative block-cache and storage counters for one reader.
///
/// `blocks_loaded` counts blocks read from storage and inserted into the
/// cache. `bytes_loaded` sums their in-memory Arrow size, which is within a
/// few percent of the stored object size but is not an exact wire count.
/// `cache_hits` and `cache_misses` count block lookups in the shared cache;
/// one `get_many` looks up each distinct block it needs once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VectorStoreCounters {
    pub blocks_loaded: u64,
    pub bytes_loaded: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

#[derive(Default)]
struct CacheStats {
    blocks_loaded: AtomicU64,
    bytes_loaded: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl CacheStats {
    fn snapshot(&self) -> VectorStoreCounters {
        VectorStoreCounters {
            blocks_loaded: self.blocks_loaded.load(Ordering::Relaxed),
            bytes_loaded: self.bytes_loaded.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }
}

/// Wraps the block cache to count hits, misses, and storage loads. It is
/// generic because the blockstore's `Block` type is not nameable outside its
/// crate; `V` is inferred as `Block` where the provider is built.
struct CountingCache<V> {
    inner: Arc<dyn PersistentCache<Uuid, V>>,
    stats: Arc<CacheStats>,
}

impl<V> Clone for CountingCache<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            stats: self.stats.clone(),
        }
    }
}

impl<V> fmt::Debug for CountingCache<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountingCache").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<V> Cache<Uuid, V> for CountingCache<V>
where
    V: Clone + Send + Sync + Weighted + Code + 'static,
{
    async fn insert(&self, key: Uuid, value: V) {
        // A reader's provider never commits, so inserts are storage loads.
        self.stats.blocks_loaded.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_loaded
            .fetch_add(value.estimated_size() as u64, Ordering::Relaxed);
        self.inner.insert(key, value).await
    }
    async fn get(&self, key: &Uuid) -> Result<Option<V>, CacheError> {
        self.inner.get(key).await
    }
    async fn may_contain(&self, key: &Uuid) -> bool {
        self.inner.may_contain(key).await
    }
    async fn remove(&self, key: &Uuid) {
        self.inner.remove(key).await
    }
    async fn clear(&self) -> Result<(), CacheError> {
        self.inner.clear().await
    }
    async fn obtain(&self, key: Uuid) -> Result<Option<V>, CacheError> {
        let value = self.inner.obtain(key).await;
        let counter = match &value {
            Ok(Some(_)) => &self.stats.cache_hits,
            _ => &self.stats.cache_misses,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        value
    }
}

#[async_trait::async_trait]
impl<V> PersistentCache<Uuid, V> for CountingCache<V>
where
    V: Clone + Send + Sync + Weighted + Code + 'static,
{
    async fn insert_to_disk(&self, key: Uuid, value: V) {
        self.insert(key, value).await
    }
    async fn close(&self) -> Result<(), CacheError> {
        self.inner.close().await
    }
}

/// Type-erased handle that clears the block cache without naming `Block`.
trait ClearBlocks: Send + Sync {
    fn clear(&self) -> futures::future::BoxFuture<'_, Result<(), CacheError>>;
}

impl<V> ClearBlocks for CountingCache<V>
where
    V: Clone + Send + Sync + Weighted + Code + 'static,
{
    fn clear(&self) -> futures::future::BoxFuture<'_, Result<(), CacheError>> {
        Box::pin(Cache::clear(self))
    }
}

/// Foyer capacity, in the cache's weight units, for a byte budget.
///
/// A block weighs `ceil(size / MiB)` with a minimum of 1 (see the blockstore's
/// `Weighted for Block`), so every block at or below 1 MiB weighs 1. The
/// capacity therefore counts blocks of at most `max_block_size_bytes`, and the
/// budget is an upper bound. Ordered appends store blocks at about half the
/// maximum, so a full cache typically holds about half its byte budget.
fn block_cache_capacity(cache_bytes: u64, max_block_size_bytes: usize) -> usize {
    let max = max_block_size_bytes as u64;
    let weight_per_block = max.div_ceil(MIB).max(1);
    ((cache_bytes.saturating_mul(weight_per_block)) / max).max(1) as usize
}

/// A storage client and blockfile provider for one vector store location.
struct VectorStoreProvider {
    provider: BlockfileProvider,
    stats: Arc<CacheStats>,
    clear: Arc<dyn ClearBlocks>,
}

type CacheParts<V> = (Box<dyn PersistentCache<Uuid, V>>, Arc<dyn ClearBlocks>);

async fn build_counting_cache<V>(
    capacity: usize,
    stats: Arc<CacheStats>,
) -> Result<CacheParts<V>, String>
where
    V: Clone + Send + Sync + Weighted + Code + 'static,
{
    let inner = FoyerCacheConfig {
        name: "spann_vector_block_cache".to_string(),
        capacity,
        ..FoyerCacheConfig::default()
    }
    .build_memory_persistent::<Uuid, V>()
    .await
    .map_err(|e| format!("vector block cache: {e}"))?;
    let cache = CountingCache {
        inner: inner.into(),
        stats,
    };
    Ok((Box::new(cache.clone()), Arc::new(cache)))
}

impl VectorStoreProvider {
    async fn open(
        location: &VectorStoreLocation,
        cache_bytes: u64,
        max_block_size_bytes: usize,
    ) -> Result<Self, String> {
        validate_block_size(max_block_size_bytes)?;
        let storage = location.storage().await?;
        let stats = Arc::new(CacheStats::default());
        let (block_cache, clear) = build_counting_cache(
            block_cache_capacity(cache_bytes, max_block_size_bytes),
            stats.clone(),
        )
        .await?;
        let root_cache = FoyerCacheConfig {
            name: "spann_vector_root_cache".to_string(),
            capacity: ROOT_CACHE_ENTRIES,
            ..FoyerCacheConfig::default()
        }
        .build_memory_persistent()
        .await
        .map_err(|e| format!("vector root cache: {e}"))?;
        // The provider default is the store's block size: a forked writer
        // takes its block size from the provider, not from writer options.
        let provider = BlockfileProvider::new_arrow(
            storage,
            max_block_size_bytes,
            block_cache,
            root_cache,
            NUM_CONCURRENT_BLOCK_FLUSHES,
            MAX_CONCURRENT_BLOCK_LOADS,
        );
        Ok(Self {
            provider,
            stats,
            clear,
        })
    }
}

fn validate_block_size(max_block_size_bytes: usize) -> Result<(), String> {
    if !(MIN_BLOCK_SIZE_BYTES..=MAX_BLOCK_SIZE_BYTES).contains(&max_block_size_bytes) {
        return Err(format!(
            "vector block size {max_block_size_bytes} outside \
             [{MIN_BLOCK_SIZE_BYTES}, {MAX_BLOCK_SIZE_BYTES}]"
        ));
    }
    Ok(())
}

/// Writes one owner's full vectors as a leaf-ordinal blockfile and returns its
/// reference.
///
/// `leaves` yields `(leaf id, vectors in saved posting order)` in strictly
/// ascending leaf order; an out-of-order or repeated leaf is an error. Vector
/// `i` of a leaf is stored under key `(leaf_prefix(leaf), i)`.
///
/// Memory: the blockstore's ordered writer keeps every completed block in
/// memory until commit. To stay bounded, this function commits, flushes, and
/// forks the blockfile after each `VECTOR_WRITE_CHUNK_BYTES` (256 MiB) of
/// vector payload, at a leaf boundary. Peak memory is about one chunk of Arrow
/// blocks plus the current leaf plus a 64 MiB writer cache, independent of
/// the owner's size. Each fork rewrites the previous chunk's last block, so a
/// flush leaves one superseded block object and one superseded root in
/// storage (about 60 orphaned blocks for a 15 GB owner); only the returned
/// root is referenced.
pub fn write_owner_vectors(
    location: &VectorStoreLocation,
    owner: u64,
    dimensions: usize,
    max_block_size_bytes: usize,
    leaves: impl IntoIterator<Item = (u128, Vec<Arc<[f32]>>)>,
) -> Result<VectorStoreRef, String> {
    write_owner_vectors_chunked(
        location,
        owner,
        dimensions,
        max_block_size_bytes,
        VECTOR_WRITE_CHUNK_BYTES,
        leaves,
    )
}

/// `write_owner_vectors` with an explicit flush interval, so tests can
/// exercise the fork path with small inputs.
pub(super) fn write_owner_vectors_chunked(
    location: &VectorStoreLocation,
    owner: u64,
    dimensions: usize,
    max_block_size_bytes: usize,
    chunk_bytes: usize,
    leaves: impl IntoIterator<Item = (u128, Vec<Arc<[f32]>>)>,
) -> Result<VectorStoreRef, String> {
    if dimensions == 0 {
        return Err("vector store requires nonzero dimensions".into());
    }
    let rt = runtime();
    let store = rt.block_on(VectorStoreProvider::open(
        location,
        WRITER_CACHE_BYTES,
        max_block_size_bytes,
    ))?;
    let prefix_path = location.owner_prefix_path(owner);
    let vector_bytes = dimensions * std::mem::size_of::<f32>();

    let mut last_flushed: Option<Uuid> = None;
    let mut writer: Option<BlockfileWriter> = None;
    let mut chunk = 0usize;
    let mut previous: Option<u128> = None;
    let mut rows = 0u64;

    for (leaf, vectors) in leaves {
        if let Some(previous) = previous.filter(|&p| leaf <= p) {
            return Err(format!(
                "leaf {leaf} after leaf {previous}: leaves must be strictly ascending"
            ));
        }
        previous = Some(leaf);
        if vectors.is_empty() {
            continue;
        }
        if u32::try_from(vectors.len()).is_err() {
            return Err(format!("leaf {leaf} has more than u32::MAX vectors"));
        }
        if let Some((i, v)) = vectors
            .iter()
            .enumerate()
            .find(|(_, v)| v.len() != dimensions)
        {
            return Err(format!(
                "leaf {leaf} vector {i} has {} dimensions, expected {dimensions}",
                v.len()
            ));
        }
        if writer.is_none() {
            writer = Some(rt.block_on(open_writer(&store, &prefix_path, last_flushed))?);
        }
        let w = writer.as_ref().expect("writer opened above");
        let prefix = leaf_prefix(leaf);
        rt.block_on(async {
            for (i, v) in vectors.iter().enumerate() {
                w.set::<u32, Vec<f32>>(&prefix, i as u32, v.to_vec())
                    .await
                    .map_err(|e| format!("write leaf {leaf} vector {i}: {e}"))?;
            }
            Ok::<_, String>(())
        })?;
        rows += vectors.len() as u64;
        chunk += vectors.len() * vector_bytes;
        drop(vectors);
        if chunk >= chunk_bytes {
            let w = writer.take().expect("writer opened above");
            last_flushed = Some(rt.block_on(commit_and_flush(w))?);
            chunk = 0;
        }
    }
    let blockfile_id = match (writer, last_flushed) {
        (Some(w), _) => rt.block_on(commit_and_flush(w))?,
        (None, Some(id)) => id,
        (None, None) => {
            let w = rt.block_on(open_writer(&store, &prefix_path, None))?;
            rt.block_on(commit_and_flush(w))?
        }
    };
    Ok(VectorStoreRef {
        kind: LEAF_ORDINAL_LAYOUT.to_string(),
        blockfile_id: blockfile_id.to_string(),
        prefix_path,
        owner,
        rows,
        dimensions,
        max_block_size_bytes,
    })
}

async fn open_writer(
    store: &VectorStoreProvider,
    prefix_path: &str,
    fork_from: Option<Uuid>,
) -> Result<BlockfileWriter, String> {
    let mut options = BlockfileWriterOptions::new(prefix_path.to_string()).ordered_mutations();
    if let Some(id) = fork_from {
        options = options.fork(id);
    }
    store
        .provider
        .write::<u32, Vec<f32>>(options)
        .await
        .map_err(|e| format!("open vector blockfile writer: {e}"))
}

async fn commit_and_flush(writer: BlockfileWriter) -> Result<Uuid, String> {
    let flusher = writer
        .commit::<u32, Vec<f32>>()
        .await
        .map_err(|e| format!("commit vector blockfile: {e}"))?;
    let id = flusher.id();
    flusher
        .flush::<u32, Vec<f32>>()
        .await
        .map_err(|e| format!("flush vector blockfile: {e}"))?;
    Ok(id)
}

/// Reads one owner's full vectors from a leaf-ordinal blockfile.
///
/// Blocks load lazily into a shared, byte-bounded in-memory cache. Each
/// `get_many` call opens a short-lived blockfile reader, because a blockstore
/// reader pins every block it loads until it is dropped; a per-call reader
/// keeps memory bounded by the cache plus one call's blocks. The root is
/// cached, so opening a per-call reader does no I/O. The reader is Send +
/// Sync and supports concurrent `get_many` calls from many threads.
pub struct VectorStoreReader {
    store: VectorStoreProvider,
    blockfile_id: Uuid,
    prefix_path: String,
    dimensions: usize,
    rows: u64,
}

impl VectorStoreReader {
    /// Opens the blockfile `reference` names at `location`, with a block cache
    /// of at most about `cache_bytes`. Rejects an unknown layout, a prefix
    /// that does not belong to the referenced owner, and a row count that
    /// disagrees with the stored root.
    pub fn open(
        location: &VectorStoreLocation,
        reference: &VectorStoreRef,
        cache_bytes: u64,
    ) -> Result<Self, String> {
        if reference.kind != LEAF_ORDINAL_LAYOUT {
            return Err(format!("unknown vector store layout {:?}", reference.kind));
        }
        if reference.dimensions == 0 {
            return Err("vector store reference has zero dimensions".into());
        }
        let expected_prefix = location.owner_prefix_path(reference.owner);
        if reference.prefix_path != expected_prefix {
            return Err(format!(
                "vector store prefix {:?} does not match owner {} at this location ({:?})",
                reference.prefix_path, reference.owner, expected_prefix
            ));
        }
        let blockfile_id = Uuid::parse_str(&reference.blockfile_id)
            .map_err(|e| format!("vector blockfile id {:?}: {e}", reference.blockfile_id))?;
        let rt = runtime();
        let store = rt.block_on(VectorStoreProvider::open(
            location,
            cache_bytes,
            reference.max_block_size_bytes,
        ))?;
        let reader = Self {
            store,
            blockfile_id,
            prefix_path: reference.prefix_path.clone(),
            dimensions: reference.dimensions,
            rows: reference.rows,
        };
        let count = rt.block_on(reader.count())?;
        if count as u64 != reader.rows {
            return Err(format!(
                "vector blockfile holds {count} rows, reference says {}",
                reader.rows
            ));
        }
        Ok(reader)
    }

    async fn count(&self) -> Result<usize, String> {
        let reader = self.blockfile().await?;
        reader
            .count()
            .await
            .map_err(|e| format!("count vector blockfile: {e}"))
    }

    async fn blockfile(&self) -> Result<BlockfileReader<'_, u32, &'_ [f32]>, String> {
        self.store
            .provider
            .read::<u32, &[f32]>(BlockfileReaderOptions::new(
                self.blockfile_id,
                self.prefix_path.clone(),
            ))
            .await
            .map_err(|e| format!("open vector blockfile {}: {e}", self.blockfile_id))
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Returns the vectors for `(leaf, position)` keys in input order. Loads
    /// the distinct blocks the keys need concurrently, with at most
    /// `MAX_CONCURRENT_BLOCK_LOADS` in flight, then copies each vector out.
    /// A missing key or a vector of the wrong length is an error.
    pub fn get_many(&self, keys: &[(u128, u32)]) -> Result<Vec<Arc<[f32]>>, String> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        runtime().block_on(self.get_many_async(keys))
    }

    async fn get_many_async(&self, keys: &[(u128, u32)]) -> Result<Vec<Arc<[f32]>>, String> {
        use futures::stream::{self, StreamExt, TryStreamExt};
        let reader = self.blockfile().await?;
        // Each get finds its block by binary search in the root's block index
        // and loads it through the shared cache. A batched preload would walk
        // the whole index (about 60,000 blocks per 10M-vector owner) per call.
        let reader = &reader;
        stream::iter(keys.iter().map(|&(leaf, position)| async move {
            let prefix = leaf_prefix(leaf);
            let vector = reader
                .get(&prefix, position)
                .await
                .map_err(|e| format!("read leaf {leaf} position {position}: {e}"))?
                .ok_or_else(|| format!("missing vector for leaf {leaf} position {position}"))?;
            if vector.len() != self.dimensions {
                return Err(format!(
                    "leaf {leaf} position {position} has {} dimensions, expected {}",
                    vector.len(),
                    self.dimensions
                ));
            }
            Ok::<Arc<[f32]>, String>(Arc::from(vector))
        }))
        .buffered(MAX_CONCURRENT_BLOCK_LOADS)
        .try_collect()
        .await
    }

    /// Cumulative counters since this reader opened.
    pub fn counters(&self) -> VectorStoreCounters {
        self.store.stats.snapshot()
    }

    /// Drops every cached block, so the next reads load from storage. The
    /// cached root is kept.
    pub fn clear_cache(&self) -> Result<(), String> {
        runtime()
            .block_on(self.store.clear.clear())
            .map_err(|e| format!("clear vector block cache: {e}"))
    }
}

#[cfg(test)]
#[path = "vector_store_tests.rs"]
mod tests;
