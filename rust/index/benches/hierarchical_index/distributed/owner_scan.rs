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
    transport::{RequestClass, RpcClient, TransportSnapshot},
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

/// Ascending score, then ascending ID: one total order for every selection.
fn by_score_then_id(a: &(u64, f32), b: &(u64, f32)) -> std::cmp::Ordering {
    a.1.total_cmp(&b.1).then(a.0.cmp(&b.0))
}

/// A scored vector and where it sits: the request's leaf slot and its
/// position in that leaf's posting list, which locates its full vector.
#[derive(Clone, Copy)]
struct Candidate {
    id: u64,
    distance: f32,
    slot: u32,
    ordinal: u32,
}
fn candidate_order(a: &Candidate, b: &Candidate) -> std::cmp::Ordering {
    a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id))
}
/// Keep only the best `keep` candidates once a list holds twice that many,
/// so scan memory stays near the shortlist size instead of every vector.
fn trim(candidates: &mut Vec<Candidate>, keep: usize) {
    if candidates.len() > keep {
        candidates.select_nth_unstable_by(keep - 1, candidate_order);
        candidates.truncate(keep);
    }
}
#[derive(Default)]
struct ChunkWork {
    leaves: u64,
    vectors: u64,
    posting_bytes_scanned: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanRequest {
    /// Query values as raw float bits, so the owner scores the same numbers.
    pub query: Vec<u32>,
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
    pub results: Vec<(u64, u32)>,
    pub work: ScanWork,
    pub timing: ScanTiming,
    /// Process CPU and peak extra heap while the owner scanned this request,
    /// excluding request decoding and reply encoding. Both are process-wide,
    /// so they belong to the request only when it is the owner's sole request
    /// in flight.
    pub cpu_seconds: f64,
    pub heap_peak_bytes: u64,
}

/// Where an owner process reads full vectors for rerank.
pub enum FullVectors {
    /// A local copy of the whole corpus, addressed by vector ID.
    Mapped(Arc<MappedVectorSource>),
    /// Each served owner's vector blockfile in index storage, addressed by
    /// leaf and posting position and cached in memory by block.
    Store(BTreeMap<u64, Arc<VectorStoreReader>>),
}

struct Slot {
    value: OnceLock<Arc<SavedPostingList>>,
    fill: Mutex<()>,
}

/// Scans the leaves of the owners this process serves, caching each decoded
/// posting list on first use.
pub struct ScanEngine {
    reader: Arc<SavedReader>,
    vectors: Option<FullVectors>,
    slots: HashMap<u128, Slot>,
    fetch_pool: rayon::ThreadPool,
    scan_pool: rayon::ThreadPool,
    budget_bytes: u64,
    cached_bytes: AtomicU64,
}
impl ScanEngine {
    /// `owners` lists the owners whose leaves this process may scan.
    pub fn new(
        reader: Arc<SavedReader>,
        owners: &BTreeSet<u64>,
        vectors: Option<FullVectors>,
        fetch_threads: usize,
        scan_threads: usize,
        budget_bytes: u64,
    ) -> Result<Self> {
        let slots = reader
            .leaves()
            .filter(|leaf| reader.node_owner(*leaf).is_some_and(|o| owners.contains(&o)))
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
            vectors,
            slots,
            fetch_pool,
            scan_pool,
            budget_bytes,
            cached_bytes: AtomicU64::new(0),
        })
    }
    pub fn cached_bytes(&self) -> u64 {
        self.cached_bytes.load(Ordering::Relaxed)
    }
    /// Fetch, verify and decode a missing posting list once, even when several
    /// queries want it at the same time. Past the byte budget the list is
    /// still returned but not kept.
    fn fill(&self, leaf: u128, slot: &Slot) -> Result<(Arc<SavedPostingList>, bool)> {
        let _filling = slot.fill.lock().map_err(|_| "posting fill lock poisoned")?;
        if let Some(p) = slot.value.get() {
            return Ok((p.clone(), false));
        }
        let posting = Arc::new(self.reader.posting_list(leaf)?);
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
        let mut postings: Vec<Option<Arc<SavedPostingList>>> = Vec::with_capacity(leaves.len());
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
            let fetched: Vec<Result<(Arc<SavedPostingList>, bool)>> = self
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
        let dim = query.len();
        let q_norm = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        let metric = self.reader.metric();
        let keep = k.checked_mul(rerank.max(1)).ok_or("rerank size overflow")?;
        // Leaves are split into a few chunks per scan thread; each chunk scores
        // its leaves in order and trims to the shortlist size as it goes.
        let threads = self.scan_pool.current_num_threads();
        let chunk = leaves.len().div_ceil(threads * 2).max(1);
        let slots: Vec<usize> = (0..leaves.len()).collect();
        let scored: Vec<Result<(Vec<Candidate>, ChunkWork)>> = self.scan_pool.install(|| {
            slots
                .par_chunks(chunk)
                .map(|chunk_slots| {
                    let mut out = Vec::with_capacity(keep * 2);
                    let mut cw = ChunkWork::default();
                    let mut distances = vec![];
                    for &slot in chunk_slots {
                        let p = postings[slot].as_ref().expect("every leaf is resolved above");
                        // v2 postings carry their leaf centroid; v1 falls back to routing.
                        let centroid = p.scoring_centroid(&self.reader)?;
                        let residual: Vec<_> = query.iter().zip(centroid).map(|(q, c)| q - c).collect();
                        let norm = centroid.iter().map(|x| x * x).sum::<f32>().sqrt();
                        let dot = centroid.iter().zip(query).map(|(c, q)| c * q).sum();
                        let qq = QuantizedQuery::new(&residual, Code::<1>::packed_len(dim), norm, dot, q_norm);
                        distances.clear();
                        distances.resize(p.ids.len(), 0.0);
                        Code::<1>::distances_quantized_query(&p.codes, metric, &qq, &mut distances);
                        if distances.iter().any(|d| !d.is_finite()) {
                            return Err("nonfinite saved posting score".to_string());
                        }
                        cw.leaves += 1;
                        cw.vectors += p.ids.len() as u64;
                        cw.posting_bytes_scanned += p.codes.len() as u64 + p.ids.len() as u64 * 8;
                        out.extend(p.ids.iter().zip(&distances).enumerate().map(|(ordinal, (id, d))| Candidate {
                            id: *id,
                            distance: *d,
                            slot: slot as u32,
                            ordinal: ordinal as u32,
                        }));
                        if out.len() >= keep * 2 {
                            trim(&mut out, keep);
                        }
                    }
                    trim(&mut out, keep);
                    Ok((out, cw))
                })
                .collect()
        });
        let mut shortlist = Vec::with_capacity(keep * scored.len());
        for result in scored {
            let (part, cw) = result?;
            work.leaves += cw.leaves;
            work.vectors += cw.vectors;
            work.posting_bytes_scanned += cw.posting_bytes_scanned;
            shortlist.extend(part);
        }
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
        if request.query.len() != self.reader.manifest.format.dimensions
            || request.query.iter().any(|b| !f32::from_bits(*b).is_finite())
        {
            return Err("scan query has the wrong dimension or nonfinite values".into());
        }
        let cpu = super::reader_bench::process_cpu_seconds();
        super::reader_bench::start_heap_peak();
        let query: Vec<f32> = request.query.iter().map(|b| f32::from_bits(*b)).collect();
        let scanned = self.scan(&query, &request.leaves, request.k, request.rerank);
        let heap_peak_bytes = super::reader_bench::finish_heap_peak();
        let (results, work, timing) = scanned?;
        Ok(ScanReply {
            results: results.into_iter().map(|(id, d)| (id, d.to_bits())).collect(),
            work,
            timing,
            cpu_seconds: super::reader_bench::process_cpu_seconds() - cpu,
            heap_peak_bytes,
        })
    }
}

/// A remote owner as seen from the query machine.
pub struct RemoteOwner {
    pub owner: u64,
    pub client: RpcClient,
}
/// Where one owner's share of a query ran and what it cost.
#[derive(Clone, Debug, Default, Serialize)]
pub struct OwnerPart {
    pub owner: u64,
    pub remote: bool,
    pub leaves: usize,
    pub work: ScanWork,
    pub timing: ScanTiming,
    /// Query-machine view of a remote request: send to decoded reply.
    pub request_ns: u64,
    pub remote_cpu_seconds: f64,
    pub remote_heap_peak_bytes: u64,
    pub transport_bytes_sent: u64,
    pub transport_bytes_received: u64,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct QueryRecord {
    pub navigation: NavigationWork,
    pub navigation_ns: u64,
    pub parts: Vec<OwnerPart>,
    pub merge_ns: u64,
    pub total: ScanWork,
}

/// The query machine: local navigation, per-owner scans and the merge.
pub struct OwnerQuery {
    pub reader: Arc<SavedReader>,
    pub local: Arc<ScanEngine>,
    /// Owners this process scans itself.
    pub local_owners: BTreeSet<u64>,
    pub remotes: Vec<RemoteOwner>,
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
        let t = Instant::now();
        let (leaves, navigation) = self.reader.navigate_local(query, policy)?;
        let mut record = QueryRecord {
            navigation,
            navigation_ns: t.elapsed().as_nanos() as u64,
            ..Default::default()
        };
        let mut groups: BTreeMap<u64, Vec<u128>> = BTreeMap::new();
        for (leaf, _) in &leaves {
            let owner = self.reader.node_owner(leaf.id).ok_or("navigation reached an unknown leaf")?;
            groups.entry(owner).or_default().push(leaf.id);
        }
        for owner in groups.keys() {
            if !self.local_owners.contains(owner) && !self.remotes.iter().any(|r| r.owner == *owner) {
                return Err(format!("no process serves owner {owner}"));
            }
        }
        let bits: Vec<u32> = query.iter().map(|x| x.to_bits()).collect();
        let (remote_parts, local_parts) = std::thread::scope(|scope| {
            let jobs: Vec<_> = self
                .remotes
                .iter()
                .filter_map(|remote| {
                    let leaves = groups.get(&remote.owner)?.clone();
                    let request = ScanRequest {
                        query: bits.clone(),
                        leaves,
                        k,
                        rerank,
                    };
                    Some(scope.spawn(move || -> Result<(OwnerPart, Vec<(u64, f32)>)> {
                        let before = remote.client.counters();
                        let t = Instant::now();
                        let reply: ScanReply = remote
                            .client
                            .call(&request, RequestClass::Data, Instant::now() + self.deadline)
                            .map_err(|e| format!("owner {} scan failed: {e}", remote.owner))?;
                        let request_ns = t.elapsed().as_nanos() as u64;
                        let (sent, received) = delta(&remote.client.counters(), &before);
                        let results = reply.results.iter().map(|(id, d)| (*id, f32::from_bits(*d))).collect();
                        Ok((
                            OwnerPart {
                                owner: remote.owner,
                                remote: true,
                                leaves: request.leaves.len(),
                                work: reply.work,
                                timing: reply.timing,
                                request_ns,
                                remote_cpu_seconds: reply.cpu_seconds,
                                remote_heap_peak_bytes: reply.heap_peak_bytes,
                                transport_bytes_sent: sent,
                                transport_bytes_received: received,
                            },
                            results,
                        ))
                    }))
                })
                .collect();
            // Each local owner keeps its own shortlist, exactly as a remote
            // owner would, so moving an owner between processes never changes
            // the results.
            let local: Vec<Result<(OwnerPart, Vec<(u64, f32)>)>> = groups
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
            let remote: Vec<_> = jobs
                .into_iter()
                .map(|j| j.join().map_err(|_| "remote scan thread panicked".to_string())?)
                .collect();
            (remote, local)
        });
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
