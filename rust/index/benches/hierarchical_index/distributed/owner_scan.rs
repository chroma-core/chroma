//! Replicated-tree reading. Every reader process holds the whole saved routing
//! tree and navigates it locally. Leaves belong to owners: a query machine
//! scans the leaves its process serves and sends one scan request to each
//! other owner whose leaves the walk reached. Each owner scores its leaves'
//! 1-bit codes, exact-reranks its own best `k * rerank` candidates from the
//! full vectors it holds, and returns its best `k`. The query machine merges.
//!
//! Every ordering breaks score ties by vector ID, and scores cross the network
//! as exact float bits, so each owner's local shortlist contains its share of
//! any global shortlist and merging never drops a true neighbor that a single
//! process would keep.
use super::super::common::ReadBeamPolicy;
use super::{
    mapped_vectors::MappedVectorSource,
    vector_store::VectorStoreReader,
    reader_bench::distinct_pages,
    routing::NavigationWork,
    saved_reader::{SavedPostingList, SavedReader},
    transport::{last_call_timing, CallTiming, RequestClass, RpcClient, ServerRequestTiming, TransportSnapshot},
};
use chroma_index::quantization::{Code, QuantizedQuery};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, String>;

/// Attempts at one remote scan before the query fails.
const SCAN_ATTEMPTS: u32 = 3;

/// Ascending score, then ascending ID: one total order for every selection.
fn by_score_then_id(a: &(u64, f32), b: &(u64, f32)) -> std::cmp::Ordering {
    a.1.total_cmp(&b.1).then(a.0.cmp(&b.0))
}

/// A scored vector and where it sits: the request's leaf slot and its
/// position in that leaf's posting list, which locates its full vector.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Candidate {
    pub(crate) id: u64,
    pub(crate) distance: f32,
    pub(crate) slot: u32,
    pub(crate) ordinal: u32,
}
pub(crate) fn candidate_order(a: &Candidate, b: &Candidate) -> std::cmp::Ordering {
    a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id))
}
/// Keep only the best `keep` candidates.
fn trim(candidates: &mut Vec<Candidate>, keep: usize) {
    if candidates.len() > keep {
        candidates.select_nth_unstable_by(keep - 1, candidate_order);
        candidates.truncate(keep);
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct ChunkWork {
    pub(crate) leaves: u64,
    pub(crate) vectors: u64,
    pub(crate) posting_bytes_scanned: u64,
}
impl ChunkWork {
    fn add(&mut self, o: &ChunkWork) {
        self.leaves += o.leaves;
        self.vectors += o.vectors;
        self.posting_bytes_scanned += o.posting_bytes_scanned;
    }
}

/// The best `keep` candidates seen so far, under the (distance, ID) order.
///
/// The list holds up to `2 * keep` entries and trims back to `keep` when
/// full. After the first trim it remembers the worst kept entry, the cutoff:
/// at least `keep` seen candidates are no worse than the cutoff, so any
/// candidate strictly worse than it can never reach the final `keep` and is
/// skipped without being stored. Equal keys are still admitted, so the
/// result matches a full sort exactly.
pub(crate) struct Shortlist {
    keep: usize,
    items: Vec<Candidate>,
    cutoff: Option<(f32, u64)>,
}
impl Shortlist {
    pub(crate) fn new(keep: usize) -> Self {
        Self { keep: keep.max(1), items: vec![], cutoff: None }
    }
    /// The cutoff's distance, or infinity before the first trim.
    #[inline(always)]
    fn cutoff_distance(&self) -> f32 {
        self.cutoff.map_or(f32::INFINITY, |(d, _)| d)
    }
    /// Whether a candidate could still reach the best `keep`.
    #[inline(always)]
    pub(crate) fn admits(&self, distance: f32, id: u64) -> bool {
        match self.cutoff {
            None => true,
            Some((d, i)) => match distance.total_cmp(&d) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => id <= i,
                std::cmp::Ordering::Greater => false,
            },
        }
    }
    #[inline(always)]
    pub(crate) fn push(&mut self, c: Candidate) {
        if self.items.capacity() == 0 {
            self.items.reserve_exact(self.keep * 2);
        }
        self.items.push(c);
        if self.items.len() >= self.keep * 2 {
            self.trim();
        }
    }
    fn trim(&mut self) {
        if self.items.len() > self.keep {
            let (_, nth, _) = self.items.select_nth_unstable_by(self.keep - 1, candidate_order);
            let cut = (nth.distance, nth.id);
            self.items.truncate(self.keep);
            self.cutoff = Some(cut);
        }
    }
    pub(crate) fn merge(&mut self, other: Shortlist) {
        for c in other.items {
            if self.admits(c.distance, c.id) {
                self.push(c);
            }
        }
    }
    pub(crate) fn finish(mut self) -> Vec<Candidate> {
        self.trim();
        self.items
    }
}

/// One leaf ready to score: its slot in the request, its posting list, its
/// scoring centroid and that centroid's norm.
pub(crate) struct LeafInput<'a> {
    pub(crate) slot: u32,
    pub(crate) ids: &'a [u64],
    pub(crate) codes: &'a [u8],
    pub(crate) centroid: &'a [f32],
    pub(crate) centroid_norm: f32,
}

/// The query-only inputs shared by every leaf of one scan.
pub(crate) struct ScanQuery<'a> {
    pub(crate) query: &'a [f32],
    pub(crate) q_norm: f32,
    pub(crate) metric: &'a chroma_distance::DistanceFunction,
    pub(crate) keep: usize,
}

/// Norm of a vector, summed in index order exactly as every scorer does.
pub(crate) fn norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

/// One scan thread's state: its shortlist, work counters and the scratch
/// buffers it reuses for every leaf.
struct Worker {
    shortlist: Shortlist,
    work: ChunkWork,
    residual: Vec<f32>,
    distances: Vec<f32>,
}
impl Worker {
    fn new(keep: usize) -> Self {
        Self { shortlist: Shortlist::new(keep), work: ChunkWork::default(), residual: vec![], distances: vec![] }
    }
    fn scan(&mut self, q: &ScanQuery, leaf: &LeafInput) -> Result<()> {
        let dim = q.query.len();
        self.residual.clear();
        self.residual.extend(q.query.iter().zip(leaf.centroid).map(|(q, c)| q - c));
        let dot = leaf.centroid.iter().zip(q.query).map(|(c, q)| c * q).sum();
        let qq = QuantizedQuery::new(&self.residual, Code::<1>::packed_len(dim), leaf.centroid_norm, dot, q.q_norm);
        let n = leaf.ids.len();
        // The scorer overwrites every slot, so stale values never leak.
        if self.distances.len() < n {
            self.distances.resize(n, 0.0);
        }
        let distances = &mut self.distances[..n];
        Code::<1>::distances_quantized_query(leaf.codes, q.metric, &qq, distances);
        // A non-short-circuiting fold, so the check vectorizes.
        if distances.iter().fold(false, |bad, d| bad | !d.is_finite()) {
            return Err("nonfinite saved posting score".to_string());
        }
        self.work.leaves += 1;
        self.work.vectors += n as u64;
        self.work.posting_bytes_scanned += leaf.codes.len() as u64 + n as u64 * 8;
        // Most vectors score above the cutoff. A plain float `>` against the
        // cutoff distance implies the total order's "greater" for finite
        // values, so a block of eight with no score at or below it is skipped
        // whole; any other block goes through the exact admission test.
        const BLOCK: usize = 8;
        for (b, block) in distances.chunks(BLOCK).enumerate() {
            let cut = self.shortlist.cutoff_distance();
            if block.iter().fold(false, |any, &d| any | !(d > cut)) {
                let first = b * BLOCK;
                for (o, (&id, &distance)) in leaf.ids[first..].iter().zip(block).enumerate() {
                    if self.shortlist.admits(distance, id) {
                        let ordinal = (first + o) as u32;
                        self.shortlist.push(Candidate { id, distance, slot: leaf.slot, ordinal });
                    }
                }
            }
        }
        Ok(())
    }
    fn merge(mut self, other: Worker) -> Worker {
        self.work.add(&other.work);
        self.shortlist.merge(other.shortlist);
        self
    }
}

/// Requests with fewer vectors than this scan on the calling thread: waking
/// the scan pool costs more than it saves.
pub(crate) const PARALLEL_MIN_VECTORS: usize = 16 * 1024;

/// Score every leaf's 1-bit codes and return the best `keep` candidates
/// under the (distance, ID) order, plus the work done. Large requests split
/// their leaves across `pool`; the result does not depend on the split.
pub(crate) fn score_leaves(
    pool: &rayon::ThreadPool,
    q: &ScanQuery,
    leaves: &[LeafInput],
) -> Result<(Vec<Candidate>, ChunkWork)> {
    let vectors: usize = leaves.iter().map(|l| l.ids.len()).sum();
    let threads = pool.current_num_threads();
    let worker = if threads <= 1 || vectors < PARALLEL_MIN_VECTORS || leaves.len() < 2 {
        let mut w = Worker::new(q.keep);
        for leaf in leaves {
            w.scan(q, leaf)?;
        }
        w
    } else {
        // Rayon splits the leaves adaptively and steals work, so a thread
        // that finishes early takes leaves from a busy one. Each split keeps
        // its own worker; the floor on split size bounds how many workers,
        // scratch buffers and merges a request creates.
        let min_len = leaves.len().div_ceil(threads * 4).max(1);
        pool.install(|| {
            leaves
                .par_iter()
                .with_min_len(min_len)
                .try_fold(
                    || Worker::new(q.keep),
                    |mut w, leaf| {
                        w.scan(q, leaf)?;
                        Ok::<_, String>(w)
                    },
                )
                .try_reduce(|| Worker::new(q.keep), |a, b| Ok(a.merge(b)))
        })?
    };
    Ok((worker.shortlist.finish(), worker.work))
}
/// Fixed-width little-endian rows carried as one base64 string. Decimal
/// JSON numbers for ~1,000 query bits and 128-bit leaf IDs cost tens of
/// microseconds to parse; one string field costs a few. The renamed fields
/// make a peer built with the decimal encoding fail to decode instead of
/// misreading.
mod wire {
    use base64::{prelude::BASE64_STANDARD, Engine};
    use serde::{de::Error, Deserializer, Serializer};

    pub trait Row: Sized {
        const WIDTH: usize;
        fn put(&self, out: &mut Vec<u8>);
        fn get(raw: &[u8]) -> Self;
    }
    impl Row for u32 {
        const WIDTH: usize = 4;
        fn put(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.to_le_bytes());
        }
        fn get(raw: &[u8]) -> Self {
            u32::from_le_bytes(raw.try_into().unwrap())
        }
    }
    impl Row for u128 {
        const WIDTH: usize = 16;
        fn put(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.to_le_bytes());
        }
        fn get(raw: &[u8]) -> Self {
            u128::from_le_bytes(raw.try_into().unwrap())
        }
    }
    impl Row for (u64, u32) {
        const WIDTH: usize = 12;
        fn put(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.0.to_le_bytes());
            out.extend_from_slice(&self.1.to_le_bytes());
        }
        fn get(raw: &[u8]) -> Self {
            (u64::from_le_bytes(raw[..8].try_into().unwrap()), u32::from_le_bytes(raw[8..].try_into().unwrap()))
        }
    }
    pub fn serialize<T: Row, S: Serializer>(rows: &[T], serializer: S) -> Result<S::Ok, S::Error> {
        let mut bytes = Vec::with_capacity(rows.len() * T::WIDTH);
        for row in rows {
            row.put(&mut bytes);
        }
        serializer.serialize_str(&BASE64_STANDARD.encode(bytes))
    }
    /// The transport bounds the frame before decoding, which bounds this
    /// allocation too.
    pub fn deserialize<'de, T: Row, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<T>, D::Error> {
        struct Rows<T>(std::marker::PhantomData<T>);
        impl<T: Row> serde::de::Visitor<'_> for Rows<T> {
            type Value = Vec<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "base64 of {}-byte little-endian rows", T::WIDTH)
            }
            fn visit_str<E: Error>(self, encoded: &str) -> Result<Self::Value, E> {
                let bytes = BASE64_STANDARD.decode(encoded).map_err(E::custom)?;
                if bytes.len() % T::WIDTH != 0 {
                    return Err(E::custom("row payload length is not a multiple of the row width"));
                }
                Ok(bytes.chunks_exact(T::WIDTH).map(T::get).collect())
            }
        }
        deserializer.deserialize_str(Rows(std::marker::PhantomData))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanRequest {
    /// Query values as raw float bits, so the owner scores the same numbers.
    #[serde(rename = "query_u32le_v1_base64", with = "wire")]
    pub query: Vec<u32>,
    #[serde(rename = "leaves_u128le_v1_base64", with = "wire")]
    pub leaves: Vec<u128>,
    pub k: usize,
    pub rerank: usize,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScanWork {
    pub leaves: u64,
    pub vectors: u64,
    /// IDs plus 1-bit codes scanned.
    pub posting_bytes_scanned: u64,
    /// Stored size of every visited posting object: what a cold scan fetches.
    pub posting_object_bytes: u64,
    pub posting_bytes_fetched: u64,
    pub posting_fetches: u64,
    pub cache_hits: u64,
    pub reranked: u64,
    pub rerank_row_bytes: u64,
    /// Vector-store blocks and bytes this scan loaded from storage. The
    /// counters are shared by every scan in the process, so they belong to
    /// one scan only when it runs alone.
    pub rerank_blocks_loaded: u64,
    pub rerank_bytes_loaded: u64,
    /// Vector-store blocks the rerank read, cached or not, and the bytes a
    /// cold read of them would load at the store's average block size.
    pub rerank_blocks_touched: u64,
    pub rerank_cold_bytes_estimate: u64,
    /// Distinct 4 KB corpus pages behind the reranked rows.
    pub rerank_page_bytes: u64,
    pub cache_bytes_after: u64,
}
impl ScanWork {
    fn add(&mut self, o: &ScanWork) {
        self.leaves += o.leaves;
        self.vectors += o.vectors;
        self.posting_bytes_scanned += o.posting_bytes_scanned;
        self.posting_object_bytes += o.posting_object_bytes;
        self.posting_bytes_fetched += o.posting_bytes_fetched;
        self.posting_fetches += o.posting_fetches;
        self.cache_hits += o.cache_hits;
        self.reranked += o.reranked;
        self.rerank_row_bytes += o.rerank_row_bytes;
        self.rerank_blocks_loaded += o.rerank_blocks_loaded;
        self.rerank_bytes_loaded += o.rerank_bytes_loaded;
        self.rerank_blocks_touched += o.rerank_blocks_touched;
        self.rerank_cold_bytes_estimate += o.rerank_cold_bytes_estimate;
        self.rerank_page_bytes += o.rerank_page_bytes;
        self.cache_bytes_after = self.cache_bytes_after.max(o.cache_bytes_after);
    }
}
/// Wall time of each scan phase, in nanoseconds.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScanTiming {
    pub fetch_wait: u64,
    pub scan: u64,
    pub select: u64,
    pub rerank: u64,
    pub total: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanReply {
    /// (vector ID, exact or 1-bit distance as raw float bits), best first.
    #[serde(rename = "results_u64le_u32le_v1_base64", with = "wire")]
    pub results: Vec<(u64, u32)>,
    pub work: ScanWork,
    pub timing: ScanTiming,
    /// Process CPU and peak extra heap while the owner scanned this request,
    /// excluding request decoding and reply encoding. Both are process-wide,
    /// so they belong to the request only when it is the owner's sole request
    /// in flight.
    pub cpu_seconds: f64,
    pub heap_peak_bytes: u64,
    /// Owner-side transport phases before this handler started.
    pub server: ServerRequestTiming,
    /// Handler entry to exit on the owner, including query validation and
    /// reply construction; JSON envelope decoding is in `server`.
    pub server_handler_ns: u64,
}

/// Where an owner process reads full vectors for rerank.
pub enum FullVectors {
    /// A local copy of the whole corpus, addressed by vector ID.
    Mapped(Arc<MappedVectorSource>),
    /// Each served owner's vector blockfile in index storage, addressed by
    /// leaf and posting position and cached in memory by block.
    Store(BTreeMap<u64, Arc<VectorStoreReader>>),
}

/// A decoded posting list and the norm of its scoring centroid, which every
/// query against the leaf needs.
struct LeafPosting {
    list: SavedPostingList,
    centroid_norm: f32,
}

struct Slot {
    value: OnceLock<Arc<LeafPosting>>,
    fill: Mutex<()>,
}

/// Scans the leaves of the owners this process serves, caching each decoded
/// posting list on first use.
/// Which reader process scans each leaf. By default a leaf belongs to the
/// part named by its saved owner. A reader can instead split the leaves into
/// `M` parts when it opens an index, independently of how the index was
/// saved: leaves in depth-first order, so whole subtrees stay together, cut
/// into `M` runs of nearly equal posting bytes, numbered 1 to `M`. Every
/// process that serves an index must use the same placement.
pub struct Placement {
    part: HashMap<u128, u64>,
    parts: u64,
    /// "saved" or "subtree".
    pub kind: &'static str,
}
impl Placement {
    pub fn saved(reader: &SavedReader) -> Self {
        let part: HashMap<u128, u64> = reader
            .leaves()
            .filter_map(|leaf| reader.node_owner(leaf).map(|o| (leaf, o)))
            .collect();
        let parts = part.values().copied().collect::<BTreeSet<_>>().len() as u64;
        Self { part, parts, kind: "saved" }
    }
    pub fn subtrees(reader: &SavedReader, parts: u64) -> Result<Self> {
        let order = reader.leaves_depth_first();
        if parts == 0 || parts as usize > order.len() {
            return Err(format!("cannot split {} leaves into {parts} parts", order.len()));
        }
        let bytes: Vec<u64> = order
            .iter()
            .map(|l| reader.posting_object_bytes(*l).unwrap_or(0).max(1))
            .collect();
        let total: u64 = bytes.iter().sum();
        let mut part = HashMap::with_capacity(order.len());
        let (mut current, mut seen) = (0u64, 0u64);
        for (leaf, b) in order.iter().zip(&bytes) {
            // Move to the next part once this leaf's midpoint passes the
            // current part's equal share of all posting bytes.
            while current + 1 < parts && (seen + b / 2) as u128 * parts as u128 > total as u128 * (current + 1) as u128 {
                current += 1;
            }
            part.insert(*leaf, current + 1);
            seen += b;
        }
        Ok(Self { part, parts, kind: "subtree" })
    }
    pub fn part(&self, leaf: u128) -> Option<u64> {
        self.part.get(&leaf).copied()
    }
    pub fn parts(&self) -> u64 {
        self.parts
    }
    pub fn leaves_of(&self, parts: &BTreeSet<u64>) -> BTreeSet<u128> {
        self.part.iter().filter(|(_, p)| parts.contains(p)).map(|(l, _)| *l).collect()
    }
    pub fn leaves_by_part(&self) -> BTreeMap<u64, usize> {
        let mut counts = BTreeMap::new();
        for p in self.part.values() {
            *counts.entry(*p).or_default() += 1;
        }
        counts
    }
}

pub struct ScanEngine {
    reader: Arc<SavedReader>,
    placement: Arc<Placement>,
    vectors: Option<FullVectors>,
    slots: HashMap<u128, Slot>,
    fetch_pool: rayon::ThreadPool,
    scan_pool: rayon::ThreadPool,
    budget_bytes: u64,
    cached_bytes: AtomicU64,
}
impl ScanEngine {
    /// `owners` lists the owners whose leaves this process may scan.
    /// An engine for the leaves of `owners`, by their saved owners.
    pub fn new(
        reader: Arc<SavedReader>,
        owners: &BTreeSet<u64>,
        vectors: Option<FullVectors>,
        fetch_threads: usize,
        scan_threads: usize,
        budget_bytes: u64,
    ) -> Result<Self> {
        let placement = Arc::new(Placement::saved(&reader));
        Self::with_placement(reader, placement, owners, vectors, fetch_threads, scan_threads, budget_bytes)
    }
    /// An engine for the leaves that `placement` assigns to `parts`.
    pub fn with_placement(
        reader: Arc<SavedReader>,
        placement: Arc<Placement>,
        parts: &BTreeSet<u64>,
        vectors: Option<FullVectors>,
        fetch_threads: usize,
        scan_threads: usize,
        budget_bytes: u64,
    ) -> Result<Self> {
        let owners = parts;
        let slots = reader
            .leaves()
            .filter(|leaf| placement.part(*leaf).is_some_and(|o| owners.contains(&o)))
            .map(|leaf| {
                let slot = Slot {
                    value: OnceLock::new(),
                    fill: Mutex::new(()),
                };
                (leaf, slot)
            })
            .collect();
        let fetch_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(fetch_threads.max(1))
            .thread_name(|i| format!("posting-fetch-{i}"))
            .build()
            .map_err(|e| e.to_string())?;
        let scan_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(scan_threads.max(1))
            .thread_name(|i| format!("leaf-scan-{i}"))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            reader,
            placement,
            vectors,
            slots,
            fetch_pool,
            scan_pool,
            budget_bytes,
            cached_bytes: AtomicU64::new(0),
        })
    }
    pub fn placement(&self) -> &Arc<Placement> {
        &self.placement
    }
    pub fn cached_bytes(&self) -> u64 {
        self.cached_bytes.load(Ordering::Relaxed)
    }
    /// Fetch, verify and decode a missing posting list once, even when several
    /// queries want it at the same time. Past the byte budget the list is
    /// still returned but not kept.
    fn fill(&self, leaf: u128, slot: &Slot) -> Result<(Arc<LeafPosting>, bool)> {
        let _filling = slot.fill.lock().map_err(|_| "posting fill lock poisoned")?;
        if let Some(p) = slot.value.get() {
            return Ok((p.clone(), false));
        }
        let list = self.reader.posting_list(leaf)?;
        // v2 postings carry their leaf centroid; v1 falls back to routing.
        let centroid_norm = norm(list.scoring_centroid(&self.reader)?);
        let posting = Arc::new(LeafPosting { list, centroid_norm });
        let bytes = self.reader.posting_object_bytes(leaf).unwrap_or(0);
        if self.cached_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes <= self.budget_bytes {
            let _ = slot.value.set(posting.clone());
        } else {
            self.cached_bytes.fetch_sub(bytes, Ordering::Relaxed);
        }
        Ok((posting, true))
    }
    pub fn scan(
        &self,
        query: &[f32],
        leaves: &[u128],
        k: usize,
        rerank: usize,
    ) -> Result<(Vec<(u64, f32)>, ScanWork, ScanTiming)> {
        let begin = Instant::now();
        let mut work = ScanWork::default();
        let mut timing = ScanTiming::default();
        if k == 0 {
            return Ok((vec![], work, timing));
        }
        if rerank > 0 && self.vectors.is_none() {
            return Err("rerank needs the full-vector corpus".into());
        }
        let mut postings: Vec<Option<Arc<LeafPosting>>> = Vec::with_capacity(leaves.len());
        let mut misses = vec![];
        for (i, leaf) in leaves.iter().enumerate() {
            let slot = self.slots.get(leaf).ok_or("leaf is not served by this process")?;
            work.posting_object_bytes += self.reader.posting_object_bytes(*leaf).unwrap_or(0);
            match slot.value.get() {
                Some(p) => {
                    work.cache_hits += 1;
                    postings.push(Some(p.clone()));
                }
                None => {
                    postings.push(None);
                    misses.push((i, *leaf, slot));
                }
            }
        }
        if !misses.is_empty() {
            let t = Instant::now();
            let fetched: Vec<Result<(Arc<LeafPosting>, bool)>> = self
                .fetch_pool
                .install(|| misses.par_iter().map(|(_, leaf, slot)| self.fill(*leaf, slot)).collect());
            for ((i, leaf, _), result) in misses.iter().zip(fetched) {
                let (posting, fetched) = result?;
                if fetched {
                    work.posting_fetches += 1;
                    work.posting_bytes_fetched += self.reader.posting_object_bytes(*leaf).unwrap_or(0);
                } else {
                    work.cache_hits += 1;
                }
                postings[*i] = Some(posting);
            }
            timing.fetch_wait = t.elapsed().as_nanos() as u64;
        }
        let t = Instant::now();
        let metric = self.reader.metric();
        let keep = k.checked_mul(rerank.max(1)).ok_or("rerank size overflow")?;
        let inputs = postings
            .iter()
            .enumerate()
            .map(|(slot, p)| {
                let p = p.as_ref().expect("every leaf is resolved above");
                Ok(LeafInput {
                    slot: slot as u32,
                    ids: &p.list.ids,
                    codes: &p.list.codes,
                    centroid: p.list.scoring_centroid(&self.reader)?,
                    centroid_norm: p.centroid_norm,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let scan_query = ScanQuery { query, q_norm: norm(query), metric, keep };
        let (mut shortlist, cw) = score_leaves(&self.scan_pool, &scan_query, &inputs)?;
        work.leaves += cw.leaves;
        work.vectors += cw.vectors;
        work.posting_bytes_scanned += cw.posting_bytes_scanned;
        timing.scan = t.elapsed().as_nanos() as u64;
        let t = Instant::now();
        trim(&mut shortlist, keep);
        timing.select = t.elapsed().as_nanos() as u64;
        if rerank > 0 {
            let t = Instant::now();
            match self.vectors.as_ref().unwrap() {
                FullVectors::Mapped(vectors) => {
                    for c in &mut shortlist {
                        c.distance = metric.distance(query, vectors.row(c.id)?);
                    }
                    let d = vectors.descriptor();
                    let row = d.dimensions as u64 * 4;
                    let ids: Vec<u64> = shortlist.iter().map(|c| c.id).collect();
                    work.rerank_row_bytes = ids.len() as u64 * row;
                    work.rerank_page_bytes =
                        distinct_pages(&ids, d.first_id, d.offset_bytes, d.stride_bytes, row)
                            * super::reader_bench::PAGE_BYTES;
                }
                FullVectors::Store(stores) => {
                    let mut by_owner: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
                    for (i, c) in shortlist.iter().enumerate() {
                        let leaf = leaves[c.slot as usize];
                        let owner = self.reader.node_owner(leaf).ok_or("unknown leaf owner")?;
                        by_owner.entry(owner).or_default().push(i);
                    }
                    for (owner, members) in by_owner {
                        let store = stores.get(&owner).ok_or("no vector store for this owner")?;
                        let keys: Vec<(u128, u32)> = members
                            .iter()
                            .map(|&i| (leaves[shortlist[i].slot as usize], shortlist[i].ordinal))
                            .collect();
                        // with_rows checks every row against the store's dimensions.
                        if store.dimensions() != query.len() {
                            return Err("stored vector dimension differs from the query".into());
                        }
                        let before = store.counters();
                        store.with_rows(&keys, |k, row| {
                            shortlist[members[k]].distance = metric.distance(query, row);
                        })?;
                        let after = store.counters();
                        work.rerank_blocks_loaded += after.blocks_loaded - before.blocks_loaded;
                        work.rerank_bytes_loaded += after.bytes_loaded - before.bytes_loaded;
                        let touched = (after.cache_hits + after.cache_misses)
                            - (before.cache_hits + before.cache_misses);
                        work.rerank_blocks_touched += touched;
                        work.rerank_cold_bytes_estimate +=
                            touched * after.bytes_loaded / after.blocks_loaded.max(1);
                        work.rerank_row_bytes += keys.len() as u64 * query.len() as u64 * 4;
                    }
                }
            }
            work.reranked = shortlist.len() as u64;
            timing.rerank = t.elapsed().as_nanos() as u64;
        }
        let mut candidates: Vec<(u64, f32)> = shortlist.iter().map(|c| (c.id, c.distance)).collect();
        candidates.sort_unstable_by(by_score_then_id);
        candidates.truncate(k);
        work.cache_bytes_after = self.cached_bytes();
        timing.total = begin.elapsed().as_nanos() as u64;
        Ok((candidates, work, timing))
    }
    /// Serve one remote scan request.
    pub fn handle(&self, request: ScanRequest) -> Result<ScanReply> {
        let begin = Instant::now();
        let server = super::transport::current_server_request().unwrap_or_default();
        let query: Vec<f32> = request.query.iter().map(|b| f32::from_bits(*b)).collect();
        if query.len() != self.reader.manifest.format.dimensions || query.iter().any(|x| !x.is_finite()) {
            return Err("scan query has the wrong dimension or nonfinite values".into());
        }
        let cpu = super::reader_bench::process_cpu_seconds();
        super::reader_bench::start_heap_peak();
        let scanned = self.scan(&query, &request.leaves, request.k, request.rerank);
        let heap_peak_bytes = super::reader_bench::finish_heap_peak();
        let (results, work, timing) = scanned?;
        let cpu_seconds = super::reader_bench::process_cpu_seconds() - cpu;
        Ok(ScanReply {
            results: results.into_iter().map(|(id, d)| (id, d.to_bits())).collect(),
            work,
            timing,
            cpu_seconds,
            heap_peak_bytes,
            server,
            server_handler_ns: begin.elapsed().as_nanos() as u64,
        })
    }
}

/// A job a remote owner's worker runs: one scan request and its reply.
type RemoteJob = Box<dyn FnOnce() + Send>;

/// A remote owner as seen from the query machine. Its scan requests run on
/// persistent worker threads, so a query hands each remote part to a waiting
/// thread instead of starting one. Starting a thread per part per query cost
/// tens of milliseconds under 32 concurrent queries.
pub struct RemoteOwner {
    pub owner: u64,
    pub client: RpcClient,
    jobs: std::sync::mpsc::Sender<RemoteJob>,
}
impl RemoteOwner {
    /// `workers` bounds the requests in flight to this owner; it should be at
    /// least the number of concurrent queries, or requests queue for a worker.
    pub fn new(owner: u64, client: RpcClient, workers: usize) -> Arc<Self> {
        let (jobs, receiver) = std::sync::mpsc::channel::<RemoteJob>();
        let receiver = Arc::new(Mutex::new(receiver));
        for i in 0..workers.max(1) {
            let receiver = receiver.clone();
            std::thread::Builder::new()
                .name(format!("owner-{owner}-scan-{i}"))
                .spawn(move || loop {
                    let job = match receiver.lock().unwrap().recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    };
                    // A panicking job drops its reply sender, which the query
                    // sees as a failed part; the worker keeps serving.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                })
                .expect("start remote scan worker");
        }
        Arc::new(Self { owner, client, jobs })
    }
    fn submit(&self, job: RemoteJob) -> Result<()> {
        self.jobs
            .send(job)
            .map_err(|_| format!("owner {} scan workers stopped", self.owner))
    }
    /// One scan request with retries, run on a worker thread. `queued` is when
    /// the query handed the request over.
    fn scan(&self, request: ScanRequest, deadline: Duration, queued: Instant) -> Result<(OwnerPart, Vec<(u64, f32)>)> {
        let t = Instant::now();
        let client_spawn_ns = (t - queued).as_nanos() as u64;
        let before = self.client.counters();
        // A scan only reads, so a failed attempt (a reset connection, a
        // storage error on the owner) is retried.
        let mut attempt = 1;
        let reply: ScanReply = loop {
            match self.client.call(&request, RequestClass::Data, Instant::now() + deadline) {
                Ok(reply) => break reply,
                Err(e) if attempt < SCAN_ATTEMPTS => {
                    eprintln!("owner {} scan attempt {attempt} failed, retrying: {e}", self.owner);
                    std::thread::sleep(Duration::from_millis(50 * attempt as u64));
                    attempt += 1;
                }
                Err(e) => return Err(format!("owner {} scan failed: {e}", self.owner)),
            }
        };
        let call = last_call_timing();
        let (sent, received) = delta(&self.client.counters(), &before);
        let decoding = Instant::now();
        let results = reply.results.iter().map(|(id, d)| (*id, f32::from_bits(*d))).collect();
        let client_results_ns = decoding.elapsed().as_nanos() as u64;
        Ok((
            OwnerPart {
                owner: self.owner,
                remote: true,
                leaves: request.leaves.len(),
                work: reply.work,
                timing: reply.timing,
                request_ns: t.elapsed().as_nanos() as u64,
                client_spawn_ns,
                client_build_ns: 0,
                call,
                client_results_ns,
                server: reply.server,
                server_handler_ns: reply.server_handler_ns,
                remote_cpu_seconds: reply.cpu_seconds,
                remote_heap_peak_bytes: reply.heap_peak_bytes,
                transport_bytes_sent: sent,
                transport_bytes_received: received,
            },
            results,
        ))
    }
}
/// Where one owner's share of a query ran and what it cost.
#[derive(Clone, Debug, Default, Serialize)]
pub struct OwnerPart {
    pub owner: u64,
    pub remote: bool,
    pub leaves: usize,
    pub work: ScanWork,
    pub timing: ScanTiming,
    /// Query-machine view of a remote request, from a worker picking it up to
    /// decoded results; building and queueing come before it. The fields
    /// below split it; all are zero for local parts.
    pub request_ns: u64,
    /// Waiting on the query machine for one of this owner's workers to pick
    /// up the request, including the worker's wake-up.
    pub client_spawn_ns: u64,
    /// Building the request (query bits and leaf IDs), before it is queued.
    pub client_build_ns: u64,
    /// The transport call on the query machine: socket checkout, envelope
    /// encode, write, wait for the first reply byte, read and JSON decode.
    pub call: CallTiming,
    /// Turning the decoded reply's raw float bits into results.
    pub client_results_ns: u64,
    /// Owner-side phases from the first request byte to the handler start:
    /// frame read, decode queue, envelope decode, dispatch, worker wake.
    pub server: ServerRequestTiming,
    /// Owner-side handler wall time; `timing.total` is the scan inside it.
    pub server_handler_ns: u64,
    pub remote_cpu_seconds: f64,
    pub remote_heap_peak_bytes: u64,
    pub transport_bytes_sent: u64,
    pub transport_bytes_received: u64,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct QueryRecord {
    pub navigation: NavigationWork,
    pub navigation_ns: u64,
    /// Queries in flight in this process when this one started, itself
    /// included, and whether navigation therefore ran in parallel.
    pub queries_in_flight: usize,
    pub navigation_parallel: bool,
    pub parts: Vec<OwnerPart>,
    pub merge_ns: u64,
    pub total: ScanWork,
}

/// Queries in flight in this process, counted by `OwnerQuery::query`.
static QUERIES_IN_FLIGHT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// Takes one query out of `QUERIES_IN_FLIGHT` when it finishes or fails.
struct InFlight;
impl Drop for InFlight {
    fn drop(&mut self) {
        QUERIES_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
    }
}
/// Cores this process may use.
fn cores() -> usize {
    static CORES: OnceLock<usize> = OnceLock::new();
    *CORES.get_or_init(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
}

/// The query machine: local navigation, per-owner scans and the merge.
/// "Owner" here means a part of the local engine's placement: the saved owner
/// by default, or a read-time subtree part.
pub struct OwnerQuery {
    pub reader: Arc<SavedReader>,
    pub local: Arc<ScanEngine>,
    /// Owners this process scans itself.
    pub local_owners: BTreeSet<u64>,
    pub remotes: Vec<Arc<RemoteOwner>>,
    pub deadline: Duration,
}
fn delta(after: &TransportSnapshot, before: &TransportSnapshot) -> (u64, u64) {
    (
        after.bytes_sent.saturating_sub(before.bytes_sent),
        after.bytes_received.saturating_sub(before.bytes_received),
    )
}
impl OwnerQuery {
    /// Each owner must be scanned by exactly one process, or rerank work and
    /// bytes double and per-owner shortlists stop matching a single reader.
    pub fn check_owners(&self) -> Result<()> {
        let mut seen = self.local_owners.clone();
        for remote in &self.remotes {
            if !seen.insert(remote.owner) {
                return Err(format!("owner {} is served twice", remote.owner));
            }
        }
        Ok(())
    }
    pub fn query(
        &self,
        query: &[f32],
        k: usize,
        policy: &ReadBeamPolicy,
        rerank: usize,
    ) -> Result<(Vec<(u64, f32)>, QueryRecord)> {
        // Parallel navigation pays only on idle cores. With as many queries
        // in flight as cores, its pool dispatch and idle spinning take CPU
        // from the other queries, so each query then navigates on its own
        // thread.
        let queries_in_flight = QUERIES_IN_FLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
        let _in_flight = InFlight;
        let navigation_parallel = queries_in_flight < cores();
        let t = Instant::now();
        let (leaves, navigation) = self.reader.navigate_local_with(query, policy, navigation_parallel)?;
        let mut record = QueryRecord {
            navigation,
            navigation_ns: t.elapsed().as_nanos() as u64,
            queries_in_flight,
            navigation_parallel,
            ..Default::default()
        };
        let mut groups: BTreeMap<u64, Vec<u128>> = BTreeMap::new();
        let placement = self.local.placement();
        for (leaf, _) in &leaves {
            let owner = placement.part(leaf.id).ok_or("navigation reached an unknown leaf")?;
            groups.entry(owner).or_default().push(leaf.id);
        }
        for owner in groups.keys() {
            if !self.local_owners.contains(owner) && !self.remotes.iter().any(|r| r.owner == *owner) {
                return Err(format!("no process serves owner {owner}"));
            }
        }
        // Hand each remote part to its owner's workers, scan the local parts
        // on this thread meanwhile, then collect the replies. Arrival order
        // does not matter: parts and merged results are sorted below.
        let (replies, reply_receiver) = std::sync::mpsc::channel();
        let mut remote_count = 0;
        for remote in &self.remotes {
            let Some(leaves) = groups.get(&remote.owner) else { continue };
            let t = Instant::now();
            let request = ScanRequest { query: query.iter().map(|x| x.to_bits()).collect(), leaves: leaves.clone(), k, rerank };
            let client_build_ns = t.elapsed().as_nanos() as u64;
            let (worker, replies, deadline, queued) = (remote.clone(), replies.clone(), self.deadline, Instant::now());
            remote.submit(Box::new(move || {
                let part = worker.scan(request, deadline, queued).map(|(mut part, results)| {
                    part.client_build_ns = client_build_ns;
                    (part, results)
                });
                let _ = replies.send(part);
            }))?;
            remote_count += 1;
        }
        drop(replies);
        // Each local owner keeps its own shortlist, exactly as a remote owner
        // would, so moving an owner between processes never changes the
        // results.
        let local_parts: Vec<Result<(OwnerPart, Vec<(u64, f32)>)>> = groups
            .iter()
            .filter(|(owner, _)| self.local_owners.contains(owner))
            .map(|(owner, leaves)| {
                let (results, work, timing) = self.local.scan(query, leaves, k, rerank)?;
                Ok((
                    OwnerPart {
                        owner: *owner,
                        leaves: leaves.len(),
                        work,
                        timing,
                        ..Default::default()
                    },
                    results,
                ))
            })
            .collect();
        let remote_parts: Vec<Result<(OwnerPart, Vec<(u64, f32)>)>> = (0..remote_count)
            .map(|_| reply_receiver.recv().unwrap_or_else(|_| Err("remote scan worker panicked".to_string())))
            .collect();
        let t = Instant::now();
        let mut merged = vec![];
        for part in local_parts.into_iter().chain(remote_parts) {
            let (part, results) = part?;
            record.total.add(&part.work);
            record.parts.push(part);
            merged.extend(results);
        }
        merged.sort_unstable_by(by_score_then_id);
        let mut seen = BTreeSet::new();
        merged.retain(|(id, _)| seen.insert(*id));
        merged.truncate(k);
        record.parts.sort_by_key(|p| p.owner);
        record.merge_ns = t.elapsed().as_nanos() as u64;
        Ok((merged, record))
    }
}

/// Leaves each saved owner holds, for reporting placement.
pub fn leaves_by_owner(reader: &SavedReader) -> BTreeMap<u64, usize> {
    let mut counts = BTreeMap::new();
    for leaf in reader.leaves() {
        if let Some(owner) = reader.node_owner(leaf) {
            *counts.entry(owner).or_default() += 1;
        }
    }
    counts
}

#[cfg(test)]
#[path = "owner_scan_tests.rs"]
mod tests;
