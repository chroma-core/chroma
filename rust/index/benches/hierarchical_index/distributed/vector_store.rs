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
    arrow::provider::BlockfileReaderOptions, key::KeyWrapper, provider::BlockfileProvider, Block,
    BlockfileReader, BlockfileWriter, BlockfileWriterOptions,
};
use chroma_cache::{foyer::Code, Cache, CacheError, FoyerCacheConfig, PersistentCache, Weighted};
use chroma_config::{registry::Registry, Configurable};
use chroma_storage::{
    config::{S3CredentialsConfig, S3StorageConfig, StorageConfig},
    local::LocalStorage,
    Storage,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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
    /// For the segmented layout: one blockfile per contiguous range of
    /// leaves, in ascending leaf order. Empty for the single-blockfile layout.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<VectorStoreSegment>,
}

/// One blockfile of a segmented vector store: the leaves from `first_leaf`
/// (a `leaf_prefix`) up to the next segment's first leaf.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorStoreSegment {
    pub blockfile_id: String,
    pub first_leaf: String,
    pub rows: u64,
}

/// One blockfile holds every vector of the owner.
pub const LEAF_ORDINAL_LAYOUT: &str = "arrow-blockfile-leaf-ordinal-v1";
/// The owner's leaves are cut into contiguous ranges, each written by its own
/// writer to its own blockfile under the same prefix. A reader joins the
/// segments' block indexes into one, so lookups work as in the
/// single-blockfile layout.
pub const SEGMENTED_LEAF_ORDINAL_LAYOUT: &str = "arrow-blockfile-leaf-ordinal-segmented-v1";

/// Default number of segments a save writes in parallel, overridden by
/// `SPANN_VECTOR_STORE_WRITERS` (1 through 256). One writer writes the
/// single-blockfile layout. Writing 6 GB to S3 from an r6i.16xlarge takes
/// 76 MB/s with 1 writer, 305 MB/s with 4 and about 850 MB/s with 16 or 32;
/// at 32 and 64 writers S3 sometimes answers 429 SlowDown, since every
/// segment writes under the owner's one prefix.
pub const DEFAULT_VECTOR_STORE_WRITERS: usize = 16;

/// Attempts per segment. A failed segment (for example an S3 429 SlowDown
/// during a flush) is written again from its first leaf to a new blockfile;
/// the failed attempt's objects stay in storage unreferenced.
const SEGMENT_ATTEMPTS: usize = 4;

pub fn vector_store_writers() -> Result<usize, String> {
    match std::env::var("SPANN_VECTOR_STORE_WRITERS") {
        Err(_) => Ok(DEFAULT_VECTOR_STORE_WRITERS),
        Ok(v) => v
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=256).contains(n))
            .ok_or_else(|| format!("SPANN_VECTOR_STORE_WRITERS must be 1 through 256, got {v:?}")),
    }
}

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

/// Maximum block loads one `get_many` or `with_rows` call keeps in flight.
pub const MAX_CONCURRENT_BLOCK_LOADS: usize = 32;
/// Attempts at loading one block before a read fails.
const BLOCK_LOAD_ATTEMPTS: u32 = 4;
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
/// one `get_many` or `with_rows` call looks up each distinct block it needs
/// once.
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

/// Wraps the block cache to count hits, misses, and storage loads. `V` is
/// the blockstore's `Block`; the wrapper is generic only to state the bounds
/// the cache traits need.
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
    /// The same block cache the provider reads through.
    blocks: CountingCache<Block>,
}

async fn build_counting_cache(
    capacity: usize,
    stats: Arc<CacheStats>,
) -> Result<CountingCache<Block>, String> {
    let inner = FoyerCacheConfig {
        name: "spann_vector_block_cache".to_string(),
        capacity,
        ..FoyerCacheConfig::default()
    }
    .build_memory_persistent::<Uuid, Block>()
    .await
    .map_err(|e| format!("vector block cache: {e}"))?;
    Ok(CountingCache {
        inner: inner.into(),
        stats,
    })
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
        let blocks = build_counting_cache(
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
            Box::new(blocks.clone()),
            root_cache,
            NUM_CONCURRENT_BLOCK_FLUSHES,
            MAX_CONCURRENT_BLOCK_LOADS,
        );
        Ok(Self {
            provider,
            stats,
            blocks,
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
    write_owner_vectors_chunked_results(
        location,
        owner,
        dimensions,
        max_block_size_bytes,
        chunk_bytes,
        leaves.into_iter().map(Ok),
    )
}

/// Writes one owner's full vectors with `writers` writers in parallel and
/// returns the reference.
///
/// `leaves` lists each leaf with its vector IDs in saved posting order, in
/// strictly ascending leaf order. The leaves are cut into `writers`
/// contiguous ranges of about equal vector count; each range is written by
/// its own thread to its own blockfile (`write_owner_vectors` describes one
/// writer's memory and flushes). Vectors are read from `vectors` one leaf at
/// a time as the writer reaches the leaf, so no copy of the owner's vectors
/// is built first. One writer writes the single-blockfile layout; more write
/// the segmented layout.
pub fn write_owner_vectors_parallel(
    location: &VectorStoreLocation,
    owner: u64,
    dimensions: usize,
    max_block_size_bytes: usize,
    writers: usize,
    leaves: &[(u128, &[u64])],
    vectors: &(dyn Fn(u64) -> Result<Arc<[f32]>, String> + Sync),
) -> Result<VectorStoreRef, String> {
    write_owner_vectors_parallel_chunked(
        location,
        owner,
        dimensions,
        max_block_size_bytes,
        VECTOR_WRITE_CHUNK_BYTES,
        writers,
        leaves,
        vectors,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_owner_vectors_parallel_chunked(
    location: &VectorStoreLocation,
    owner: u64,
    dimensions: usize,
    max_block_size_bytes: usize,
    chunk_bytes: usize,
    writers: usize,
    leaves: &[(u128, &[u64])],
    vectors: &(dyn Fn(u64) -> Result<Arc<[f32]>, String> + Sync),
) -> Result<VectorStoreRef, String> {
    if let Some(w) = leaves.windows(2).find(|w| w[1].0 <= w[0].0) {
        return Err(format!(
            "leaf {} after leaf {}: leaves must be strictly ascending",
            w[1].0, w[0].0
        ));
    }
    let load = |&(leaf, ids): &(u128, &[u64])| -> Result<(u128, Vec<Arc<[f32]>>), String> {
        Ok((leaf, ids.iter().map(|id| vectors(*id)).collect::<Result<_, _>>()?))
    };
    let ranges = segment_ranges(leaves, writers.max(1));
    if ranges.len() <= 1 {
        return write_owner_vectors_chunked_results(
            location,
            owner,
            dimensions,
            max_block_size_bytes,
            chunk_bytes,
            leaves.iter().map(load),
        );
    }
    let written: Vec<Result<WrittenSegment, String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = ranges
            .iter()
            .map(|range| {
                let range = &leaves[range.clone()];
                let load = &load;
                scope.spawn(move || {
                    let mut attempt = 1;
                    loop {
                        let written = write_segment(
                            location,
                            owner,
                            dimensions,
                            max_block_size_bytes,
                            chunk_bytes,
                            range.iter().map(load),
                        );
                        match written {
                            Err(e) if attempt < SEGMENT_ATTEMPTS => {
                                eprintln!("vector store segment attempt {attempt} failed, retrying: {e}");
                                std::thread::sleep(std::time::Duration::from_secs(1 << attempt));
                                attempt += 1;
                            }
                            written => break written,
                        }
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("vector store writer panicked".into())))
            .collect()
    });
    let mut segments = Vec::with_capacity(written.len());
    for (range, segment) in ranges.iter().zip(written) {
        let segment = segment?;
        segments.push(VectorStoreSegment {
            blockfile_id: segment.blockfile_id.to_string(),
            first_leaf: leaf_prefix(leaves[range.start].0),
            rows: segment.rows,
        });
    }
    Ok(VectorStoreRef {
        kind: SEGMENTED_LEAF_ORDINAL_LAYOUT.to_string(),
        blockfile_id: segments[0].blockfile_id.clone(),
        prefix_path: location.owner_prefix_path(owner),
        owner,
        rows: segments.iter().map(|s| s.rows).sum(),
        dimensions,
        max_block_size_bytes,
        segments,
    })
}

fn write_owner_vectors_chunked_results(
    location: &VectorStoreLocation,
    owner: u64,
    dimensions: usize,
    max_block_size_bytes: usize,
    chunk_bytes: usize,
    leaves: impl Iterator<Item = Result<(u128, Vec<Arc<[f32]>>), String>>,
) -> Result<VectorStoreRef, String> {
    let segment = write_segment(location, owner, dimensions, max_block_size_bytes, chunk_bytes, leaves)?;
    Ok(VectorStoreRef {
        kind: LEAF_ORDINAL_LAYOUT.to_string(),
        blockfile_id: segment.blockfile_id.to_string(),
        prefix_path: location.owner_prefix_path(owner),
        owner,
        rows: segment.rows,
        dimensions,
        max_block_size_bytes,
        segments: vec![],
    })
}

/// Cuts the non-empty leaves into at most `writers` contiguous index ranges
/// of about equal vector count. Empty leaves stay in whichever range covers
/// them; a writer skips them.
fn segment_ranges(leaves: &[(u128, &[u64])], writers: usize) -> Vec<std::ops::Range<usize>> {
    let total: usize = leaves.iter().map(|(_, ids)| ids.len()).sum();
    if total == 0 {
        return vec![0..leaves.len()];
    }
    let target = total.div_ceil(writers);
    let mut ranges = vec![];
    let mut start = 0;
    let mut rows = 0;
    for (i, (_, ids)) in leaves.iter().enumerate() {
        if rows >= target && !ids.is_empty() {
            ranges.push(start..i);
            start = i;
            rows = 0;
        }
        rows += ids.len();
    }
    ranges.push(start..leaves.len());
    ranges
}

struct WrittenSegment {
    blockfile_id: Uuid,
    rows: u64,
}

/// Writes one range of leaves to its own blockfile, with its own storage
/// client and provider, committing and forking every `chunk_bytes`.
fn write_segment(
    location: &VectorStoreLocation,
    owner: u64,
    dimensions: usize,
    max_block_size_bytes: usize,
    chunk_bytes: usize,
    leaves: impl Iterator<Item = Result<(u128, Vec<Arc<[f32]>>), String>>,
) -> Result<WrittenSegment, String> {
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

    for leaf in leaves {
        let (leaf, vectors) = leaf?;
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
    Ok(WrittenSegment { blockfile_id, rows })
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
/// Blocks load lazily into a shared, byte-bounded in-memory cache. A read
/// fetches the blocks it needs from that cache (or storage) and holds them
/// only for the call, so memory stays bounded by the cache plus one call's
/// blocks. Each call opens a blockfile reader from the cached root, which
/// costs no I/O; the reader is used only to locate blocks, never to pin them.
/// The reader is Send + Sync and supports concurrent reads from many threads.
pub struct VectorStoreReader {
    store: VectorStoreProvider,
    index: BlockIndex,
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
        let segments: Vec<(Uuid, (u128, u32), u64)> = match reference.kind.as_str() {
            LEAF_ORDINAL_LAYOUT if reference.segments.is_empty() => {
                vec![(parse_blockfile_id(&reference.blockfile_id)?, (0, 0), reference.rows)]
            }
            SEGMENTED_LEAF_ORDINAL_LAYOUT if !reference.segments.is_empty() => reference
                .segments
                .iter()
                .map(|s| {
                    let first = u128::from_str_radix(&s.first_leaf, 16)
                        .map_err(|e| format!("vector segment first leaf {:?}: {e}", s.first_leaf))?;
                    Ok((parse_blockfile_id(&s.blockfile_id)?, (first, 0), s.rows))
                })
                .collect::<Result<_, String>>()?,
            kind => return Err(format!("unknown vector store layout {kind:?}")),
        };
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
        let rt = runtime();
        let store = rt.block_on(VectorStoreProvider::open(
            location,
            cache_bytes,
            reference.max_block_size_bytes,
        ))?;
        let mut reader = Self {
            store,
            index: BlockIndex::default(),
            blockfile_id: segments[0].0,
            prefix_path: reference.prefix_path.clone(),
            dimensions: reference.dimensions,
            rows: reference.rows,
        };
        // Segments hold ascending, disjoint leaf ranges, so their block
        // indexes join in order. A segment's first block starts at the
        // smallest key (stored as no start); after the first segment it
        // starts at the segment's first leaf.
        let mut total = 0u64;
        for (k, (id, first, rows)) in segments.iter().enumerate() {
            let (count, mut index) = rt.block_on(async {
                let blockfile = reader.segment(*id).await?;
                let count = blockfile
                    .count()
                    .await
                    .map_err(|e| format!("count vector blockfile {id}: {e}"))?;
                Ok::<_, String>((count, BlockIndex::try_from(blockfile.block_index())?))
            })?;
            if count as u64 != *rows {
                return Err(format!("vector blockfile {id} holds {count} rows, reference says {rows}"));
            }
            if k > 0 && index.starts[0] == (0, 0) {
                index.starts[0] = *first;
            }
            reader.index.append(index)?;
            total += rows;
        }
        if total != reader.rows {
            return Err(format!(
                "vector blockfile holds {total} rows, reference says {}",
                reader.rows
            ));
        }
        Ok(reader)
    }

    async fn segment(&self, id: Uuid) -> Result<BlockfileReader<'_, u32, &'_ [f32]>, String> {
        self.store
            .provider
            .read::<u32, &[f32]>(BlockfileReaderOptions::new(id, self.prefix_path.clone()))
            .await
            .map_err(|e| format!("open vector blockfile {id}: {e}"))
    }

    /// A reader for the first segment. Blocks load by ID from the shared
    /// prefix, so it fetches any segment's blocks.
    async fn blockfile(&self) -> Result<BlockfileReader<'_, u32, &'_ [f32]>, String> {
        self.segment(self.blockfile_id).await
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Keep only the blocks whose key range can hold rows of `leaves`, for a
    /// reader that serves just those leaves. Lookups of other leaves are no
    /// longer valid. Returns (blocks kept, blocks before).
    pub fn restrict_to_leaves(&mut self, leaves: &BTreeSet<u128>) -> (usize, usize) {
        let before = self.index.ids.len();
        let mut starts = vec![];
        let mut ids = vec![];
        for i in 0..before {
            let start = self.index.starts[i];
            let end = self.index.starts.get(i + 1).copied();
            // Rows of leaf L have keys (L, 0..). A block holds some when its
            // start is at most (L, u32::MAX) and the next block starts after (L, 0).
            let first = leaves.range(start.0..).next();
            let holds = leaves
                .range(..=start.0)
                .next_back()
                .into_iter()
                .chain(first)
                .any(|l| (*l, u32::MAX) >= start && end.is_none_or(|e| e > (*l, 0)));
            if holds {
                starts.push(start);
                ids.push(self.index.ids[i]);
            }
        }
        self.index = BlockIndex { starts, ids };
        (self.index.ids.len(), before)
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Returns the vectors for `(leaf, position)` keys in input order, as
    /// owned copies. `with_rows` reads the same rows without copying them.
    pub fn get_many(&self, keys: &[(u128, u32)]) -> Result<Vec<Arc<[f32]>>, String> {
        let mut rows: Vec<Option<Arc<[f32]>>> = vec![None; keys.len()];
        self.with_rows(keys, |i, row| rows[i] = Some(Arc::from(row)))?;
        Ok(rows
            .into_iter()
            .map(|row| row.expect("with_rows visits every key"))
            .collect())
    }

    /// Calls `f(i, row)` once for each key `keys[i]`, with the row borrowed
    /// from its cached block. Calls arrive in storage order (by leaf, then
    /// position), not input order. A missing key or a vector of the wrong
    /// length is an error, reported before `f` is called for any key.
    ///
    /// Each distinct block the keys need is looked up in the block cache
    /// once, and blocks that miss load from storage concurrently, with at most
    /// `MAX_CONCURRENT_BLOCK_LOADS` in flight. When every block is cached the
    /// call does no I/O and does not wait on the storage runtime.
    pub fn with_rows(
        &self,
        keys: &[(u128, u32)],
        mut f: impl FnMut(usize, &[f32]),
    ) -> Result<(), String> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut plan = self.plan(keys);
        self.load_blocks(&mut plan)?;
        let mut rows = Vec::with_capacity(keys.len());
        for &(i, prefix, block, hint) in &plan.keys {
            let (leaf, position) = keys[i];
            let row = plan.blocks[block]
                .as_ref()
                .expect("load_blocks fills every block")
                .get_with_hint::<u32, &[f32]>(&plan.prefixes[prefix], position, hint)
                .ok_or_else(|| format!("missing vector for leaf {leaf} position {position}"))?;
            if row.len() != self.dimensions {
                return Err(format!(
                    "leaf {leaf} position {position} has {} dimensions, expected {}",
                    row.len(),
                    self.dimensions
                ));
            }
            rows.push((i, row));
        }
        for (i, row) in rows {
            f(i, row);
        }
        Ok(())
    }

    /// Sorts the keys and finds the block each belongs to, without I/O.
    fn plan(&self, keys: &[(u128, u32)]) -> RowPlan {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_unstable_by_key(|&i| keys[i]);
        let mut plan = RowPlan {
            keys: Vec::with_capacity(keys.len()),
            prefixes: Vec::new(),
            block_ids: Vec::new(),
            blocks: Vec::new(),
        };
        let mut last_leaf = None;
        // The first key past the previous key's block (`Some(None)` when that
        // is the last block): sorted keys below it stay in the same block.
        let mut current_end: Option<Option<(u128, u32)>> = None;
        let mut current_start = (0u128, 0u32);
        for i in order {
            let key = keys[i];
            if last_leaf != Some(key.0) {
                plan.prefixes.push(leaf_prefix(key.0));
                last_leaf = Some(key.0);
            }
            let same_block = current_end.is_some_and(|end| end.is_none_or(|end| key < end));
            if !same_block {
                let block = self.index.locate(key);
                current_start = self.index.starts[block];
                current_end = Some(self.index.starts.get(block + 1).copied());
                plan.block_ids.push(self.index.ids[block]);
            }
            // A leaf's positions are dense, so within the leaf that starts the
            // block the row index is the position offset. Other keys (and a
            // wrong guess) fall back to a binary search in the block.
            let hint = if key.0 == current_start.0 {
                (key.1 - current_start.1) as usize
            } else {
                usize::MAX
            };
            plan.keys
                .push((i, plan.prefixes.len() - 1, plan.block_ids.len() - 1, hint));
        }
        plan
    }

    /// Fills `plan.blocks`. Each distinct block is looked up in the cache
    /// once, synchronously on the calling thread; only blocks that miss go to
    /// the storage runtime, loaded concurrently. A block is held only by the
    /// plan and the shared cache; no blockfile reader pins it.
    fn load_blocks(&self, plan: &mut RowPlan) -> Result<(), String> {
        use futures::{
            stream::{self, StreamExt, TryStreamExt},
            FutureExt,
        };
        let stats = &self.store.stats;
        plan.blocks = plan
            .block_ids
            .iter()
            .map(|id| {
                // The in-memory cache answers without awaiting. A miss, an
                // error, or a lookup that would wait is left to the async path,
                // whose counted lookup records it.
                let block = self.store.blocks.inner.obtain(*id).now_or_never();
                let block = block.and_then(Result::ok).flatten();
                if block.is_some() {
                    stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                }
                block
            })
            .collect();
        let missing: Vec<usize> = (0..plan.blocks.len())
            .filter(|&b| plan.blocks[b].is_none())
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let ids = &plan.block_ids;
        let loaded: Vec<Block> = runtime().block_on(async {
            let reader = self.blockfile().await?;
            let reader = &reader;
            stream::iter(missing.iter().map(|&b| async move {
                let id = ids[b];
                // Storage reads fail now and then under load (a dispatch
                // failure, a reset connection); a read is safe to repeat.
                let mut attempt = 1;
                loop {
                    match reader.fetch_block(id).await {
                        Ok(block) => break block.ok_or_else(|| format!("vector block {id} not found")),
                        Err(e) if attempt < BLOCK_LOAD_ATTEMPTS => {
                            eprintln!("vector block {id} load attempt {attempt} failed, retrying: {e}");
                            tokio::time::sleep(std::time::Duration::from_millis(100 * attempt as u64)).await;
                            attempt += 1;
                        }
                        Err(e) => break Err(format!("load vector block {id}: {e}")),
                    }
                }
            }))
            .buffered(MAX_CONCURRENT_BLOCK_LOADS)
            .try_collect()
            .await
        })?;
        for (b, block) in missing.into_iter().zip(loaded) {
            plan.blocks[b] = Some(block);
        }
        Ok(())
    }

    /// Cumulative counters since this reader opened.
    pub fn counters(&self) -> VectorStoreCounters {
        self.store.stats.snapshot()
    }

    /// Drops every cached block, so the next reads load from storage. The
    /// cached root is kept.
    pub fn clear_cache(&self) -> Result<(), String> {
        runtime()
            .block_on(Cache::clear(&self.store.blocks))
            .map_err(|e| format!("clear vector block cache: {e}"))
    }
}

fn parse_blockfile_id(id: &str) -> Result<Uuid, String> {
    Uuid::parse_str(id).map_err(|e| format!("vector blockfile id {id:?}: {e}"))
}

/// The blockfile's sparse index in a compact form: block `i` holds the keys
/// in `[starts[i], starts[i + 1])`, and `starts[0]` is the smallest key. One
/// entry per block (about 60,000 for a 10M-vector owner, 2 MiB), built once
/// when the reader opens, so a lookup is a binary search over a flat array
/// instead of a walk of the root's string-keyed tree.
#[derive(Default)]
struct BlockIndex {
    starts: Vec<(u128, u32)>,
    ids: Vec<Uuid>,
}

impl BlockIndex {
    /// Appends a later segment's index; its first start must follow ours.
    fn append(&mut self, other: BlockIndex) -> Result<(), String> {
        if let (Some(last), Some(first)) = (self.starts.last(), other.starts.first()) {
            if first <= last {
                return Err(format!("vector segment starts at {first:?}, before {last:?}"));
            }
        }
        self.starts.extend(other.starts);
        self.ids.extend(other.ids);
        Ok(())
    }

    /// The block whose range holds `key`.
    fn locate(&self, key: (u128, u32)) -> usize {
        self.starts
            .partition_point(|start| *start <= key)
            .saturating_sub(1)
    }
}

impl TryFrom<Vec<(Option<(String, KeyWrapper)>, Uuid)>> for BlockIndex {
    type Error = String;

    fn try_from(entries: Vec<(Option<(String, KeyWrapper)>, Uuid)>) -> Result<Self, String> {
        let mut index = BlockIndex::default();
        for (start, id) in entries {
            let start = match start {
                None => (0, 0),
                Some((prefix, KeyWrapper::Uint32(position))) if prefix.len() == 32 => {
                    let leaf = u128::from_str_radix(&prefix, 16)
                        .map_err(|e| format!("vector block start prefix {prefix:?}: {e}"))?;
                    (leaf, position)
                }
                Some(other) => return Err(format!("vector block start key {other:?}")),
            };
            if index.starts.last().is_some_and(|last| *last >= start) {
                return Err(format!("vector block starts out of order at {start:?}"));
            }
            index.starts.push(start);
            index.ids.push(id);
        }
        if index.ids.is_empty() {
            return Err("vector blockfile has no blocks".into());
        }
        Ok(index)
    }
}

/// One read's work: for each key in storage order its input index, prefix
/// index, block index, and predicted row in that block; then the distinct
/// blocks, in key order.
struct RowPlan {
    keys: Vec<(usize, usize, usize, usize)>,
    prefixes: Vec<String>,
    block_ids: Vec<Uuid>,
    blocks: Vec<Option<Block>>,
}

#[cfg(test)]
#[path = "vector_store_tests.rs"]
mod tests;
