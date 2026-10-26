//! Reader development benchmark support: frozen queries with exact references,
//! tie-aware recall, summary statistics and per-query byte and heap accounting.
//! The reader binary drives passes; this module holds the reusable pieces.
use super::engine::VectorSource;
use serde::{Deserialize, Serialize};
use std::{
    alloc::{GlobalAlloc, Layout},
    cell::RefCell,
    collections::{BTreeSet, HashMap},
    io::{BufRead, BufReader},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
};

type Result<T> = std::result::Result<T, String>;

/// Ground-truth distances come from a separate float64 scan; duplicate corpus
/// vectors tie exactly, so equality allows only summation-order rounding.
const TIE_RELATIVE_TOLERANCE: f64 = 1e-9;
pub const PAGE_BYTES: u64 = 4096;

#[derive(Deserialize)]
struct QueryRow {
    query_id: String,
    vector: Vec<f64>,
}
#[derive(Deserialize)]
struct TruthRow {
    query_id: String,
    ids: Vec<u64>,
    distances: Vec<f64>,
}

pub struct BenchQuery {
    pub id: String,
    /// Queries are converted to float32 as the reader consumes them.
    pub vector: Vec<f32>,
    pub truth_ids: Vec<u64>,
    pub truth_distances: Vec<f64>,
}

fn read_jsonl<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    BufReader::new(file)
        .lines()
        .filter(|line| !matches!(line, Ok(l) if l.trim().is_empty()))
        .map(|line| {
            serde_json::from_str(&line.map_err(|e| e.to_string())?).map_err(|e| e.to_string())
        })
        .collect()
}

/// Join queries with their exact references by query ID, in query-file order.
pub fn load_queries(
    queries: &Path,
    truth: &Path,
    dimensions: usize,
    limit: Option<usize>,
) -> Result<Vec<BenchQuery>> {
    let mut truth: HashMap<String, TruthRow> = read_jsonl::<TruthRow>(truth)?
        .into_iter()
        .map(|row| (row.query_id.clone(), row))
        .collect();
    let mut out = vec![];
    for row in read_jsonl::<QueryRow>(queries)? {
        if limit.is_some_and(|n| out.len() >= n) {
            break;
        }
        let t = truth
            .remove(&row.query_id)
            .ok_or_else(|| format!("query {} lacks exact references", row.query_id))?;
        let vector: Vec<f32> = row.vector.iter().map(|&x| x as f32).collect();
        if vector.len() != dimensions || vector.iter().any(|x| !x.is_finite()) {
            return Err(format!("query {} has invalid values or dimension", row.query_id));
        }
        if t.ids.is_empty()
            || t.ids.len() != t.distances.len()
            || t.distances.windows(2).any(|w| !(w[0] <= w[1]))
            || t.ids.iter().collect::<BTreeSet<_>>().len() != t.ids.len()
        {
            return Err(format!("query {} has malformed exact references", row.query_id));
        }
        out.push(BenchQuery {
            id: row.query_id,
            vector,
            truth_ids: t.ids,
            truth_distances: t.distances,
        });
    }
    if out.is_empty() {
        return Err("no benchmark queries".into());
    }
    Ok(out)
}

/// One named point of a sweep. Without `tau`, each level keeps `beam_min`
/// children capped at `beam_max`; with `tau`, the two bound a distance filter.
/// `rerank` multiplies `k`: the reader exact-scores its best `k * rerank`
/// 1-bit candidates, and 0 returns 1-bit scores.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BenchConfig {
    pub name: String,
    pub k: usize,
    pub rerank: usize,
    pub beam_min: usize,
    pub beam_max: usize,
    #[serde(default)]
    pub tau: Option<f64>,
    /// Per-level beam caps below the root, replacing `beam_max` at their
    /// level; 0 or a missing entry keeps `beam_max`.
    #[serde(default)]
    pub level_beam_max: Vec<usize>,
    /// Per-level beam floors, replacing `beam_min` at their level.
    #[serde(default)]
    pub level_beam_min: Vec<usize>,
    /// Per-level distance-ratio filters; null keeps `tau` at that level.
    #[serde(default)]
    pub level_tau: Vec<Option<f64>>,
}
#[derive(Deserialize)]
struct ConfigFile {
    configurations: Vec<BenchConfig>,
}
pub fn load_configs(path: &Path) -> Result<Vec<BenchConfig>> {
    let file: ConfigFile = serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let mut names = BTreeSet::new();
    for c in &file.configurations {
        if c.k == 0 || c.beam_min == 0 || c.beam_max == 0 || !names.insert(c.name.clone()) {
            return Err(format!("configuration {} is invalid or repeated", c.name));
        }
    }
    if file.configurations.is_empty() {
        return Err("no configurations".into());
    }
    Ok(file.configurations)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Recall {
    pub k: usize,
    pub hits: usize,
    /// Returned IDs outside the listed references whose exact distance could
    /// not be computed; they count as misses.
    pub unresolved: usize,
}
impl Recall {
    pub fn fraction(&self) -> f64 {
        self.hits as f64 / self.k as f64
    }
}

/// A returned ID is a hit when its exact distance is at most the exact k-th
/// distance. Duplicate corpus vectors make the exact top-k non-unique, so an ID
/// that ties the k-th distance is as correct as the listed one. Listed IDs use
/// the reference distance; an unlisted ID can only tie when the listed ties run
/// to the end of the references, and then `exact` supplies its distance.
pub fn tie_aware_recall(
    returned: &[u64],
    query: &BenchQuery,
    k: usize,
    exact: &mut dyn FnMut(u64) -> Option<f64>,
) -> Result<Recall> {
    if k == 0 || k > query.truth_ids.len() {
        return Err(format!("recall@{k} needs {k} exact references"));
    }
    let kth = query.truth_distances[k - 1];
    let listed: HashMap<u64, f64> = query
        .truth_ids
        .iter()
        .copied()
        .zip(query.truth_distances.iter().copied())
        .collect();
    let ties_may_continue = *query.truth_distances.last().unwrap() <= kth;
    let mut recall = Recall {
        k,
        ..Default::default()
    };
    for id in returned.iter().copied().collect::<BTreeSet<_>>() {
        let hit = match listed.get(&id) {
            Some(&d) => d <= kth,
            None if !ties_may_continue => false,
            None => match exact(id) {
                Some(d) => d <= kth + kth.abs() * TIE_RELATIVE_TOLERANCE,
                None => {
                    recall.unresolved += 1;
                    false
                }
            },
        };
        recall.hits += hit as usize;
    }
    recall.hits = recall.hits.min(k);
    Ok(recall)
}

/// Squared Euclidean distance accumulated in float64, as the references are.
pub fn exact_squared_l2(query: &[f32], vector: &[f32]) -> f64 {
    query
        .iter()
        .zip(vector)
        .map(|(&q, &v)| {
            let d = q as f64 - v as f64;
            d * d
        })
        .sum()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Stats {
    pub count: usize,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}
/// Nearest-rank percentiles; with 1,000 samples p99 rests on about ten values.
/// No samples means the quantity was not measured, which is not zero.
pub fn stats(values: &[f64]) -> Option<Stats> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = |p: f64| sorted[((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1];
    Some(Stats {
        count: sorted.len(),
        mean: sorted.iter().sum::<f64>() / sorted.len() as f64,
        p50: rank(0.50),
        p90: rank(0.90),
        p99: rank(0.99),
        max: *sorted.last().unwrap(),
    })
}

/// Distinct 4 KB pages that hold the given corpus rows. A 3,072-byte row
/// crosses a page boundary about half the time.
pub fn distinct_pages(rows: &[u64], first_id: u64, offset: u64, stride: u64, row_bytes: u64) -> u64 {
    let mut pages = BTreeSet::new();
    for &id in rows {
        let start = offset + (id - first_id) * stride;
        pages.extend(start / PAGE_BYTES..=(start + row_bytes - 1) / PAGE_BYTES);
    }
    pages.len() as u64
}

thread_local! {
    static TOUCHED: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}
/// Records which full vectors a query reads, on the thread that reads them.
/// The saved reader reranks on its calling thread, so one query's reads stay
/// separate from other queries running in parallel.
pub struct RecordingVectors<'a>(pub &'a dyn VectorSource);
impl VectorSource for RecordingVectors<'_> {
    fn get(&self, id: u64) -> Result<Arc<[f32]>> {
        TOUCHED.with(|t| t.borrow_mut().push(id));
        self.0.get(id)
    }
}
/// Copy out and clear the recorded reads. Clearing in place keeps the buffer's
/// capacity, so recording inside a measured query rarely allocates.
pub fn take_touched_vectors() -> Vec<u64> {
    TOUCHED.with(|t| {
        let mut t = t.borrow_mut();
        let out = t.clone();
        t.clear();
        out
    })
}

static HEAP_TRACKING: AtomicBool = AtomicBool::new(false);
static HEAP_LIVE: AtomicI64 = AtomicI64::new(0);
static HEAP_PEAK: AtomicI64 = AtomicI64::new(0);
/// Jemalloc with an optional process-wide high-water mark of heap bytes held
/// since the last reset. Tracking is off except around single in-flight
/// queries, because the shared counters would contend under parallel load.
pub struct CountingJemalloc;
impl CountingJemalloc {
    fn record(delta: i64) {
        if HEAP_TRACKING.load(Ordering::Relaxed) {
            let live = HEAP_LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
            HEAP_PEAK.fetch_max(live, Ordering::Relaxed);
        }
    }
}
unsafe impl GlobalAlloc for CountingJemalloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let out = tikv_jemallocator::Jemalloc.alloc(layout);
        if !out.is_null() {
            Self::record(layout.size() as i64);
        }
        out
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let out = tikv_jemallocator::Jemalloc.alloc_zeroed(layout);
        if !out.is_null() {
            Self::record(layout.size() as i64);
        }
        out
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        Self::record(-(layout.size() as i64));
        tikv_jemallocator::Jemalloc.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let out = tikv_jemallocator::Jemalloc.realloc(ptr, layout, new_size);
        if !out.is_null() {
            Self::record(new_size as i64 - layout.size() as i64);
        }
        out
    }
}
/// Start measuring extra heap held from now on. Frees of older blocks lower
/// the running total, so the peak is the most net new heap held at any point.
pub fn start_heap_peak() {
    HEAP_LIVE.store(0, Ordering::Relaxed);
    HEAP_PEAK.store(0, Ordering::Relaxed);
    HEAP_TRACKING.store(true, Ordering::Relaxed);
}
pub fn finish_heap_peak() -> u64 {
    HEAP_TRACKING.store(false, Ordering::Relaxed);
    HEAP_PEAK.load(Ordering::Relaxed).max(0) as u64
}
/// Net heap allocated since `start_heap_peak` and still held.
pub fn heap_live() -> i64 {
    HEAP_LIVE.load(Ordering::Relaxed)
}

/// Whole-process CPU and context switches since the process started.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessUsage {
    pub user_seconds: f64,
    pub system_seconds: f64,
    /// Switches where a thread waited (I/O, a lock, a channel).
    pub voluntary_switches: i64,
    /// Switches where the kernel preempted a runnable thread: a sign that
    /// runnable threads outnumber the cores.
    pub involuntary_switches: i64,
}
impl ProcessUsage {
    pub fn now() -> Option<Self> {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return None;
        }
        let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        Some(Self {
            user_seconds: secs(usage.ru_utime),
            system_seconds: secs(usage.ru_stime),
            voluntary_switches: usage.ru_nvcsw as i64,
            involuntary_switches: usage.ru_nivcsw as i64,
        })
    }
    /// Usage between `earlier` and `self`, with the cores busy on average
    /// over `seconds` of wall time.
    pub fn since(&self, earlier: &Self, seconds: f64) -> serde_json::Value {
        let user = self.user_seconds - earlier.user_seconds;
        let system = self.system_seconds - earlier.system_seconds;
        serde_json::json!({
            "user_seconds": user,
            "system_seconds": system,
            "cores_busy": (user + system) / seconds,
            "voluntary_switches": self.voluntary_switches - earlier.voluntary_switches,
            "involuntary_switches": self.involuntary_switches - earlier.involuntary_switches,
        })
    }
}

/// User plus system CPU seconds for the whole process.
pub fn process_cpu_seconds() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return f64::NAN;
    }
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    secs(usage.ru_utime) + secs(usage.ru_stime)
}

/// Bytes received on all non-loopback interfaces. The counters include all
/// traffic on the machine, so they are only meaningful as pass totals.
pub fn network_received_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/net/dev").ok()?;
    Some(
        text.lines()
            .skip(2)
            .filter_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.trim() != "lo")
            .filter_map(|(_, fields)| fields.split_whitespace().next()?.parse::<u64>().ok())
            .sum(),
    )
}

/// How the tree walk kept or lost one query's true neighbors.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct WalkDiagnostics {
    /// One entry per tree level below the root.
    pub levels: Vec<LevelDiagnostics>,
    /// Fraction of true neighbors inside the leaves the walk reached.
    pub leaf_reach: f64,
    /// Best possible fraction of true neighbors for the same number of
    /// leaves: the leaves holding the most true neighbors, taken greedily.
    /// Each vector sits in one leaf, so greedy choice is optimal.
    pub optimal_recall: f64,
    pub leaves: usize,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct LevelDiagnostics {
    /// Children scored at this level, and how many the beam kept.
    pub scored: usize,
    pub kept: usize,
    /// Fraction of true neighbors still reachable under the kept nodes and the
    /// leaves already kept at this or shallower levels.
    pub reach: f64,
}

/// Tree shape the diagnostics need, so they stay independent of the reader.
pub trait TreeShape {
    fn parent(&self, id: u128) -> Option<u128>;
    fn child_count(&self, id: u128) -> usize;
}

impl TreeShape for super::saved_reader::SavedReader {
    fn parent(&self, id: u128) -> Option<u128> {
        self.node_parent(id)
    }
    fn child_count(&self, id: u128) -> usize {
        self.node_child_count(id)
    }
}

fn depth(tree: &dyn TreeShape, mut id: u128) -> usize {
    let mut d = 0;
    while let Some(p) = tree.parent(id) {
        id = p;
        d += 1;
    }
    d
}

/// `frontiers[0]` is the root; `frontiers[L]` holds the internal nodes the
/// beam kept at level `L`. `leaves` are the leaves the walk returned and
/// `truth` maps each true neighbor to its leaf.
pub fn walk_diagnostics(
    tree: &dyn TreeShape,
    frontiers: &[Vec<u128>],
    leaves: &[u128],
    truth: &[(u64, u128)],
) -> WalkDiagnostics {
    let n = truth.len().max(1) as f64;
    let visited: BTreeSet<u128> = leaves.iter().copied().collect();
    let leaf_depth: HashMap<u128, usize> = leaves.iter().map(|l| (*l, depth(tree, *l))).collect();
    // Ancestors of each true neighbor's leaf, keyed by depth (the leaf included).
    let paths: Vec<Vec<u128>> = truth
        .iter()
        .map(|(_, leaf)| {
            let mut path = vec![*leaf];
            while let Some(p) = tree.parent(*path.last().unwrap()) {
                path.push(p);
            }
            path.reverse();
            path
        })
        .collect();
    let mut levels = vec![];
    for level in 1..frontiers.len().max(1) + 1 {
        let parents = match frontiers.get(level - 1) {
            Some(p) => p,
            None => break,
        };
        let scored = parents.iter().map(|p| tree.child_count(*p)).sum();
        let internal: BTreeSet<u128> = frontiers.get(level).map(|f| f.iter().copied().collect()).unwrap_or_default();
        let leaves_here = leaf_depth.values().filter(|d| **d == level).count();
        let reached = paths
            .iter()
            .filter(|path| match path.get(level) {
                Some(node) if path.len() - 1 == level => visited.contains(node),
                Some(node) => internal.contains(node),
                None => visited.contains(path.last().unwrap()),
            })
            .count();
        levels.push(LevelDiagnostics {
            scored,
            kept: internal.len() + leaves_here,
            reach: reached as f64 / n,
        });
    }
    let mut per_leaf: HashMap<u128, usize> = HashMap::new();
    for (_, leaf) in truth {
        *per_leaf.entry(*leaf).or_default() += 1;
    }
    let mut counts: Vec<usize> = per_leaf.into_values().collect();
    counts.sort_unstable_by(|a, b| b.cmp(a));
    WalkDiagnostics {
        levels,
        leaf_reach: truth.iter().filter(|(_, l)| visited.contains(l)).count() as f64 / n,
        optimal_recall: counts.iter().take(leaves.len()).sum::<usize>() as f64 / n,
        leaves: leaves.len(),
    }
}

/// Rank every leaf by exact squared distance from the query to its float32
/// centroid, and return the rank (0 = nearest) of each true neighbor's leaf.
/// Ties in distance break by leaf ID.
pub fn exact_leaf_ranks(centroids: &[(u128, &[f32])], query: &[f32], truth: &[(u64, u128)]) -> Vec<usize> {
    let mut scored: Vec<(f64, u128)> = centroids
        .iter()
        .map(|(id, c)| {
            let d: f64 = c.iter().zip(query).map(|(a, b)| ((*a - *b) as f64).powi(2)).sum();
            (d, *id)
        })
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let rank: HashMap<u128, usize> = scored.iter().enumerate().map(|(i, (_, id))| (*id, i)).collect();
    truth.iter().map(|(_, leaf)| rank.get(leaf).copied().unwrap_or(usize::MAX)).collect()
}

#[cfg(test)]
#[path = "reader_bench_tests.rs"]
mod tests;
