#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// Maximum tree depth tracked by per-level navigate counters. Deeper
/// expansions get bucketed into the last slot. 8 covers anything we'd
/// realistically build (current 113M run is depth 4).
pub const MAX_NAV_LEVELS: usize = 8;

// =============================================================================
// Stats (atomic for thread safety)
// =============================================================================

pub struct WriterStats {
    pub adds: AtomicU64,
    pub add_nanos: AtomicU64,
    /// Count of `delete()` calls that flipped a fresh tombstone (idempotent
    /// re-deletes do not increment this).
    pub deletes: AtomicU64,
    /// Count of embeddings actually erased from the vector_data blockfile
    /// at commit time.
    pub embedding_deletes_committed: AtomicU64,
    pub navigates: AtomicU64,
    pub navigate_nanos: AtomicU64,
    pub splits: AtomicU64,
    pub split_nanos: AtomicU64,
    pub merges: AtomicU64,
    pub merge_nanos: AtomicU64,
    pub reassigns: AtomicU64,
    pub reassign_nanos: AtomicU64,
    pub scrubs: AtomicU64,
    pub scrub_nanos: AtomicU64,
    pub scrub_removed: AtomicU64,
    /// Navigate saw a child_id in a parent's children list but the node was
    /// missing from the DashMap (removed by a concurrent split).
    pub navigate_missing_nodes: AtomicU64,
    /// add() could not register in any navigated cluster (all gone) and fell
    /// back to root.
    pub add_missing_nodes: AtomicU64,
    /// register_in_leaf() target was missing or no longer a leaf (e.g. split
    /// by a balance cascade during merge).
    pub register_missing_nodes: AtomicU64,

    pub registers: AtomicU64,
    pub register_nanos: AtomicU64,
    pub register_lock_wait_nanos: AtomicU64,
    pub register_quantize_nanos: AtomicU64,

    /// Number of outer rounds executed by `balance_index` /
    /// `balance_index_parallel`. A "round" is one full pass that found at
    /// least one leaf needing balance and ran balance() on it.
    pub balance_rounds: AtomicU64,

    // Sub-step timing breakdowns (nanos)
    pub add_navigate_nanos: AtomicU64,
    pub add_register_nanos: AtomicU64,
    pub add_balance_nanos: AtomicU64,
    pub split_kmeans_nanos: AtomicU64,
    pub split_quantize_nanos: AtomicU64,
    pub split_npa_cluster_nanos: AtomicU64,
    pub split_npa_neighbor_nanos: AtomicU64,
    /// Number of neighbor leaves visited by apply_npa_to_neighbors
    pub split_npa_neighbors_visited: AtomicU64,
    /// Neighbors where >1% of vectors were reassigned
    pub split_npa_neighbors_active: AtomicU64,
    /// Sum of balance depth values across all splits (for computing average)
    pub split_depth_sum: AtomicU64,
    /// Total vectors reassigned by apply_npa_to_neighbors (across all splits)
    pub split_npa_neighbor_reassigns: AtomicU64,
    /// Total vectors evaluated by apply_npa_to_neighbors (across all splits)
    pub split_npa_neighbor_evaluated: AtomicU64,
    /// Total vectors in groups passed to apply_npa_to_cluster
    pub split_npa_self_total: AtomicU64,
    /// Vectors that passed version+dedup checks in apply_npa_to_cluster
    pub split_npa_self_evaluated: AtomicU64,
    /// Vectors reassigned by apply_npa_to_cluster (new_dist > old_dist)
    pub split_npa_self_reassigns: AtomicU64,
    /// Leaf sizes observed at the moment split_leaf() runs.
    pub split_sizes: Mutex<Vec<u32>>,
    pub reassign_navigate_nanos: AtomicU64,
    pub reassign_register_nanos: AtomicU64,
    pub reassign_balance_nanos: AtomicU64,
    pub navigate_dist_nanos: AtomicU64,
    pub navigate_dist_quantize_nanos: AtomicU64,
    pub navigate_dist_distance_nanos: AtomicU64,
    pub navigate_sort_nanos: AtomicU64,
    pub navigate_rerank_nanos: AtomicU64,
    pub navigate_levels: AtomicU64,
    pub navigate_dist_count: AtomicU64,

    /// Per-level navigate accounting (indexed by level-1, capped at
    /// MAX_NAV_LEVELS - 1).
    ///
    ///  - `nav_in_per_level[L]`: sum of input-beam sizes (number of
    ///    internals being expanded) at level L+1 across all navigate calls.
    ///  - `nav_dist_per_level[L]`: sum of children scored (= distances
    ///    computed) at level L+1, before the effective_beam truncate.
    ///  - `nav_out_per_level[L]`: sum of post-truncate beam sizes at
    ///    level L+1 (i.e. how many candidates survived the tau filter +
    ///    min/max clamp). Includes both leaf and internal survivors.
    ///
    /// Useful to attribute the bulk of `navigate_dist_count` to a
    /// specific level when the per-level beam policy
    /// (--write-level-min-pcts / --write-level-taus) lets intermediate
    /// levels expand far beyond `--write-beam-max`.
    pub nav_in_per_level: [AtomicU64; MAX_NAV_LEVELS],
    pub nav_dist_per_level: [AtomicU64; MAX_NAV_LEVELS],
    pub nav_out_per_level: [AtomicU64; MAX_NAV_LEVELS],
    /// How many navigate() calls reached level L+1 (i.e., executed at
    /// least one expansion at that level). Used to compute the per-call
    /// avg from the sums above.
    pub nav_calls_per_level: [AtomicU64; MAX_NAV_LEVELS],

    // ---- Lazy I/O counters (per-checkpoint when the writer is reopened
    // each checkpoint). All counters are cumulative since the writer was
    // constructed via `new()` or `open()`.
    /// Number of leaf posting lists actually fetched from the blockfile
    /// (cached / no-op `load()` calls do not count).
    pub posting_loads: AtomicU64,
    /// Sum of cluster entry counts across all `posting_loads`. Multiply by
    /// the in-memory per-entry size (`4 + code_size + 1` bytes) to estimate
    /// bytes loaded for posting lists.
    pub posting_load_entries: AtomicU64,
    /// Number of full-precision embeddings actually fetched from the
    /// blockfile (cache hits inside `load_raw()` do not count).
    /// Multiply by `dim * 4` to get bytes.
    pub embedding_loads: AtomicU64,
    /// Number of full-precision embeddings inserted via `add()` (i.e.,
    /// new vectors entering the index this checkpoint). Multiply by
    /// `dim * 4` to get bytes added.
    pub embeddings_added: AtomicU64,
}

impl Default for WriterStats {
    fn default() -> Self {
        Self {
            adds: AtomicU64::new(0),
            add_nanos: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            embedding_deletes_committed: AtomicU64::new(0),
            navigates: AtomicU64::new(0),
            navigate_nanos: AtomicU64::new(0),
            splits: AtomicU64::new(0),
            split_nanos: AtomicU64::new(0),
            merges: AtomicU64::new(0),
            merge_nanos: AtomicU64::new(0),
            reassigns: AtomicU64::new(0),
            reassign_nanos: AtomicU64::new(0),
            scrubs: AtomicU64::new(0),
            scrub_nanos: AtomicU64::new(0),
            scrub_removed: AtomicU64::new(0),
            navigate_missing_nodes: AtomicU64::new(0),
            add_missing_nodes: AtomicU64::new(0),
            register_missing_nodes: AtomicU64::new(0),
            registers: AtomicU64::new(0),
            register_nanos: AtomicU64::new(0),
            register_lock_wait_nanos: AtomicU64::new(0),
            register_quantize_nanos: AtomicU64::new(0),
            balance_rounds: AtomicU64::new(0),
            add_navigate_nanos: AtomicU64::new(0),
            add_register_nanos: AtomicU64::new(0),
            add_balance_nanos: AtomicU64::new(0),
            split_kmeans_nanos: AtomicU64::new(0),
            split_quantize_nanos: AtomicU64::new(0),
            split_npa_cluster_nanos: AtomicU64::new(0),
            split_npa_neighbor_nanos: AtomicU64::new(0),
            split_npa_neighbors_visited: AtomicU64::new(0),
            split_npa_neighbors_active: AtomicU64::new(0),
            split_depth_sum: AtomicU64::new(0),
            split_npa_neighbor_reassigns: AtomicU64::new(0),
            split_npa_neighbor_evaluated: AtomicU64::new(0),
            split_npa_self_total: AtomicU64::new(0),
            split_npa_self_evaluated: AtomicU64::new(0),
            split_npa_self_reassigns: AtomicU64::new(0),
            split_sizes: Mutex::new(Vec::new()),
            reassign_navigate_nanos: AtomicU64::new(0),
            reassign_register_nanos: AtomicU64::new(0),
            reassign_balance_nanos: AtomicU64::new(0),
            navigate_dist_nanos: AtomicU64::new(0),
            navigate_dist_quantize_nanos: AtomicU64::new(0),
            navigate_dist_distance_nanos: AtomicU64::new(0),
            navigate_sort_nanos: AtomicU64::new(0),
            navigate_rerank_nanos: AtomicU64::new(0),
            navigate_levels: AtomicU64::new(0),
            navigate_dist_count: AtomicU64::new(0),
            nav_in_per_level: std::array::from_fn(|_| AtomicU64::new(0)),
            nav_dist_per_level: std::array::from_fn(|_| AtomicU64::new(0)),
            nav_out_per_level: std::array::from_fn(|_| AtomicU64::new(0)),
            nav_calls_per_level: std::array::from_fn(|_| AtomicU64::new(0)),
            posting_loads: AtomicU64::new(0),
            posting_load_entries: AtomicU64::new(0),
            embedding_loads: AtomicU64::new(0),
            embeddings_added: AtomicU64::new(0),
        }
    }
}

pub const TASK_METHODS: &[&str] = &[
    "add", "navigate", "register", "split", "merge", "reassign", "scrub",
];

#[derive(Default, Clone)]
pub struct WriterStatsSnapshot {
    pub calls: [u64; 7],
    pub nanos: [u64; 7],
    pub scrub_removed: u64,
    pub wall_nanos: u64,
    pub navigate_missing_nodes: u64,
    pub add_missing_nodes: u64,
    pub register_missing_nodes: u64,
    // Sub-step breakdowns: [navigate, register, balance]
    pub add_substeps: [u64; 3],
    // Sub-step breakdowns: [kmeans, quantize, npa_cluster, npa_neighbor]
    pub split_substeps: [u64; 4],
    pub split_npa_neighbors_visited: u64,
    pub split_npa_neighbors_active: u64,
    pub split_depth_sum: u64,
    pub split_npa_neighbor_reassigns: u64,
    pub split_npa_neighbor_evaluated: u64,
    pub split_npa_self_total: u64,
    pub split_npa_self_evaluated: u64,
    pub split_npa_self_reassigns: u64,
    pub split_sizes: Vec<u32>,
    // Sub-step breakdowns: [navigate, register, balance]
    pub reassign_substeps: [u64; 3],
    // Sub-step breakdowns: [lock_wait, quantize]
    pub register_substeps: [u64; 2],
    // Sub-step breakdowns: [dist, sort, rerank, dist_quantize, dist_distance]
    pub navigate_substeps: [u64; 5],
    pub navigate_levels: u64,
    pub navigate_dist_count: u64,
    pub nav_in_per_level: [u64; MAX_NAV_LEVELS],
    pub nav_dist_per_level: [u64; MAX_NAV_LEVELS],
    pub nav_out_per_level: [u64; MAX_NAV_LEVELS],
    pub nav_calls_per_level: [u64; MAX_NAV_LEVELS],
    pub posting_loads: u64,
    pub posting_load_entries: u64,
    pub embedding_loads: u64,
    pub embeddings_added: u64,
    pub balance_rounds: u64,
}

impl WriterStats {
    pub fn snapshot(&self) -> WriterStatsSnapshot {
        WriterStatsSnapshot {
            calls: [
                self.adds.load(Ordering::Relaxed),
                self.navigates.load(Ordering::Relaxed),
                self.registers.load(Ordering::Relaxed),
                self.splits.load(Ordering::Relaxed),
                self.merges.load(Ordering::Relaxed),
                self.reassigns.load(Ordering::Relaxed),
                self.scrubs.load(Ordering::Relaxed),
            ],
            nanos: [
                self.add_nanos.load(Ordering::Relaxed),
                self.navigate_nanos.load(Ordering::Relaxed),
                self.register_nanos.load(Ordering::Relaxed),
                self.split_nanos.load(Ordering::Relaxed),
                self.merge_nanos.load(Ordering::Relaxed),
                self.reassign_nanos.load(Ordering::Relaxed),
                self.scrub_nanos.load(Ordering::Relaxed),
            ],
            scrub_removed: self.scrub_removed.load(Ordering::Relaxed),
            wall_nanos: 0,
            navigate_missing_nodes: self.navigate_missing_nodes.load(Ordering::Relaxed),
            add_missing_nodes: self.add_missing_nodes.load(Ordering::Relaxed),
            register_missing_nodes: self.register_missing_nodes.load(Ordering::Relaxed),
            add_substeps: [
                self.add_navigate_nanos.load(Ordering::Relaxed),
                self.add_register_nanos.load(Ordering::Relaxed),
                self.add_balance_nanos.load(Ordering::Relaxed),
            ],
            split_substeps: [
                self.split_kmeans_nanos.load(Ordering::Relaxed),
                self.split_quantize_nanos.load(Ordering::Relaxed),
                self.split_npa_cluster_nanos.load(Ordering::Relaxed),
                self.split_npa_neighbor_nanos.load(Ordering::Relaxed),
            ],
            split_npa_neighbors_visited: self.split_npa_neighbors_visited.load(Ordering::Relaxed),
            split_npa_neighbors_active: self.split_npa_neighbors_active.load(Ordering::Relaxed),
            split_depth_sum: self.split_depth_sum.load(Ordering::Relaxed),
            split_npa_neighbor_reassigns: self.split_npa_neighbor_reassigns.load(Ordering::Relaxed),
            split_npa_neighbor_evaluated: self.split_npa_neighbor_evaluated.load(Ordering::Relaxed),
            split_npa_self_total: self.split_npa_self_total.load(Ordering::Relaxed),
            split_npa_self_evaluated: self.split_npa_self_evaluated.load(Ordering::Relaxed),
            split_npa_self_reassigns: self.split_npa_self_reassigns.load(Ordering::Relaxed),
            split_sizes: self.split_sizes.lock().clone(),
            reassign_substeps: [
                self.reassign_navigate_nanos.load(Ordering::Relaxed),
                self.reassign_register_nanos.load(Ordering::Relaxed),
                self.reassign_balance_nanos.load(Ordering::Relaxed),
            ],
            register_substeps: [
                self.register_lock_wait_nanos.load(Ordering::Relaxed),
                self.register_quantize_nanos.load(Ordering::Relaxed),
            ],
            navigate_substeps: [
                self.navigate_dist_nanos.load(Ordering::Relaxed),
                self.navigate_sort_nanos.load(Ordering::Relaxed),
                self.navigate_rerank_nanos.load(Ordering::Relaxed),
                self.navigate_dist_quantize_nanos.load(Ordering::Relaxed),
                self.navigate_dist_distance_nanos.load(Ordering::Relaxed),
            ],
            navigate_levels: self.navigate_levels.load(Ordering::Relaxed),
            navigate_dist_count: self.navigate_dist_count.load(Ordering::Relaxed),
            nav_in_per_level: std::array::from_fn(|i| {
                self.nav_in_per_level[i].load(Ordering::Relaxed)
            }),
            nav_dist_per_level: std::array::from_fn(|i| {
                self.nav_dist_per_level[i].load(Ordering::Relaxed)
            }),
            nav_out_per_level: std::array::from_fn(|i| {
                self.nav_out_per_level[i].load(Ordering::Relaxed)
            }),
            nav_calls_per_level: std::array::from_fn(|i| {
                self.nav_calls_per_level[i].load(Ordering::Relaxed)
            }),
            posting_loads: self.posting_loads.load(Ordering::Relaxed),
            posting_load_entries: self.posting_load_entries.load(Ordering::Relaxed),
            embedding_loads: self.embedding_loads.load(Ordering::Relaxed),
            embeddings_added: self.embeddings_added.load(Ordering::Relaxed),
            balance_rounds: self.balance_rounds.load(Ordering::Relaxed),
        }
    }

    pub fn snapshot_delta(&self, prev: &WriterStatsSnapshot) -> WriterStatsSnapshot {
        let cur = self.snapshot();
        WriterStatsSnapshot {
            calls: std::array::from_fn(|i| cur.calls[i].saturating_sub(prev.calls[i])),
            nanos: std::array::from_fn(|i| cur.nanos[i].saturating_sub(prev.nanos[i])),
            scrub_removed: cur.scrub_removed.saturating_sub(prev.scrub_removed),
            wall_nanos: 0,
            navigate_missing_nodes: cur
                .navigate_missing_nodes
                .saturating_sub(prev.navigate_missing_nodes),
            add_missing_nodes: cur.add_missing_nodes.saturating_sub(prev.add_missing_nodes),
            register_missing_nodes: cur
                .register_missing_nodes
                .saturating_sub(prev.register_missing_nodes),
            add_substeps: std::array::from_fn::<_, 3, _>(|i| {
                cur.add_substeps[i].saturating_sub(prev.add_substeps[i])
            }),
            split_substeps: std::array::from_fn(|i| {
                cur.split_substeps[i].saturating_sub(prev.split_substeps[i])
            }),
            split_npa_neighbors_visited: cur
                .split_npa_neighbors_visited
                .saturating_sub(prev.split_npa_neighbors_visited),
            split_npa_neighbors_active: cur
                .split_npa_neighbors_active
                .saturating_sub(prev.split_npa_neighbors_active),
            split_depth_sum: cur.split_depth_sum.saturating_sub(prev.split_depth_sum),
            split_npa_neighbor_reassigns: cur
                .split_npa_neighbor_reassigns
                .saturating_sub(prev.split_npa_neighbor_reassigns),
            split_npa_neighbor_evaluated: cur
                .split_npa_neighbor_evaluated
                .saturating_sub(prev.split_npa_neighbor_evaluated),
            split_npa_self_total: cur
                .split_npa_self_total
                .saturating_sub(prev.split_npa_self_total),
            split_npa_self_evaluated: cur
                .split_npa_self_evaluated
                .saturating_sub(prev.split_npa_self_evaluated),
            split_npa_self_reassigns: cur
                .split_npa_self_reassigns
                .saturating_sub(prev.split_npa_self_reassigns),
            split_sizes: if prev.split_sizes.len() <= cur.split_sizes.len() {
                cur.split_sizes[prev.split_sizes.len()..].to_vec()
            } else {
                cur.split_sizes
            },
            reassign_substeps: std::array::from_fn(|i| {
                cur.reassign_substeps[i].saturating_sub(prev.reassign_substeps[i])
            }),
            register_substeps: std::array::from_fn(|i| {
                cur.register_substeps[i].saturating_sub(prev.register_substeps[i])
            }),
            navigate_substeps: std::array::from_fn(|i| {
                cur.navigate_substeps[i].saturating_sub(prev.navigate_substeps[i])
            }),
            navigate_levels: cur.navigate_levels.saturating_sub(prev.navigate_levels),
            navigate_dist_count: cur
                .navigate_dist_count
                .saturating_sub(prev.navigate_dist_count),
            nav_in_per_level: std::array::from_fn(|i| {
                cur.nav_in_per_level[i].saturating_sub(prev.nav_in_per_level[i])
            }),
            nav_dist_per_level: std::array::from_fn(|i| {
                cur.nav_dist_per_level[i].saturating_sub(prev.nav_dist_per_level[i])
            }),
            nav_out_per_level: std::array::from_fn(|i| {
                cur.nav_out_per_level[i].saturating_sub(prev.nav_out_per_level[i])
            }),
            nav_calls_per_level: std::array::from_fn(|i| {
                cur.nav_calls_per_level[i].saturating_sub(prev.nav_calls_per_level[i])
            }),
            posting_loads: cur.posting_loads.saturating_sub(prev.posting_loads),
            posting_load_entries: cur
                .posting_load_entries
                .saturating_sub(prev.posting_load_entries),
            embedding_loads: cur.embedding_loads.saturating_sub(prev.embedding_loads),
            embeddings_added: cur.embeddings_added.saturating_sub(prev.embeddings_added),
            balance_rounds: cur.balance_rounds.saturating_sub(prev.balance_rounds),
        }
    }
}

pub fn format_task_tables(snapshots: &[WriterStatsSnapshot]) -> String {
    use std::fmt::Write;

    let widths: Vec<usize> = TASK_METHODS.iter().map(|m| m.len().max(10)).collect();

    fn fmt_dur(nanos: u64) -> String {
        if nanos == 0 {
            return "-".to_string();
        } else if nanos < 1_000 {
            format!("{}ns", nanos)
        } else if nanos < 1_000_000 {
            format!("{:.1}us", nanos as f64 / 1_000.0)
        } else if nanos < 1_000_000_000 {
            format!("{:.1}ms", nanos as f64 / 1_000_000.0)
        } else if nanos < 60_000_000_000 {
            format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
        } else {
            format!("{:.1}m", nanos as f64 / 60_000_000_000.0)
        }
    }
    fn fmt_count(n: u64) -> String {
        if n < 1_000 {
            n.to_string()
        } else if n < 1_000_000 {
            format!("{:.1}K", n as f64 / 1_000.0)
        } else {
            format!("{:.1}M", n as f64 / 1_000_000.0)
        }
    }

    let mut out = String::new();

    // Task Counts
    writeln!(out, "\n--- Task Counts ---").unwrap();
    write!(out, "| CP |").unwrap();
    for (m, w) in TASK_METHODS.iter().zip(&widths) {
        write!(out, " {:>w$} |", m, w = *w).unwrap();
    }
    write!(out, " scrub_rm |").unwrap();
    writeln!(out).unwrap();
    write!(out, "|----|").unwrap();
    for w in &widths {
        write!(out, "-{:-<w$}-|", "", w = *w).unwrap();
    }
    write!(out, "----------|").unwrap();
    writeln!(out).unwrap();
    for (i, snap) in snapshots.iter().enumerate() {
        write!(out, "| {:>2} |", i + 1).unwrap();
        for (j, w) in widths.iter().enumerate() {
            write!(out, " {:>w$} |", fmt_count(snap.calls[j]), w = *w).unwrap();
        }
        write!(out, " {:>8} |", fmt_count(snap.scrub_removed)).unwrap();
        writeln!(out).unwrap();
    }

    // Task Breakdowns (concurrency diagnostics)
    writeln!(out, "\n--- Task Breakdowns ---").unwrap();
    writeln!(
        out,
        "| CP | navigate.missing_node | add.missing_nodes | register.missing_node |"
    )
    .unwrap();
    writeln!(
        out,
        "|----|----------------------|-------------------|----------------------|"
    )
    .unwrap();
    for (i, snap) in snapshots.iter().enumerate() {
        writeln!(
            out,
            "| {:>2} | {:>20} | {:>17} | {:>20} |",
            i + 1,
            fmt_count(snap.navigate_missing_nodes),
            fmt_count(snap.add_missing_nodes),
            fmt_count(snap.register_missing_nodes),
        )
        .unwrap();
    }

    // Task Total Time
    writeln!(out, "\n--- Task Total Time ---").unwrap();
    write!(out, "| CP |").unwrap();
    for (m, w) in TASK_METHODS.iter().zip(&widths) {
        write!(out, " {:>w$} |", m, w = *w).unwrap();
    }
    write!(out, "     wall |").unwrap();
    writeln!(out).unwrap();
    write!(out, "|----|").unwrap();
    for w in &widths {
        write!(out, "-{:-<w$}-|", "", w = *w).unwrap();
    }
    write!(out, "----------|").unwrap();
    writeln!(out).unwrap();
    for (i, snap) in snapshots.iter().enumerate() {
        write!(out, "| {:>2} |", i + 1).unwrap();
        for (j, w) in widths.iter().enumerate() {
            write!(out, " {:>w$} |", fmt_dur(snap.nanos[j]), w = *w).unwrap();
        }
        write!(out, " {:>8} |", fmt_dur(snap.wall_nanos)).unwrap();
        writeln!(out).unwrap();
    }

    // Task Avg Time
    writeln!(out, "\n--- Task Avg Time ---").unwrap();
    write!(out, "| CP |").unwrap();
    for (m, w) in TASK_METHODS.iter().zip(&widths) {
        write!(out, " {:>w$} |", m, w = *w).unwrap();
    }
    writeln!(out).unwrap();
    write!(out, "|----|").unwrap();
    for w in &widths {
        write!(out, "-{:-<w$}-|", "", w = *w).unwrap();
    }
    writeln!(out).unwrap();
    for (i, snap) in snapshots.iter().enumerate() {
        write!(out, "| {:>2} |", i + 1).unwrap();
        for (j, w) in widths.iter().enumerate() {
            let avg = if snap.calls[j] > 0 {
                fmt_dur(snap.nanos[j] / snap.calls[j])
            } else {
                "-".to_string()
            };
            write!(out, " {:>w$} |", avg, w = *w).unwrap();
        }
        writeln!(out).unwrap();
    }

    let add_sub_names = ["navigate", "register", "balance"];
    let split_sub_names = ["kmeans", "quantize", "npa_clust", "npa_neigh"];
    let reassign_sub_names = ["navigate", "register", "balance"];

    fn fmt_sub_avg(total_nanos: u64, count: u64) -> String {
        if count == 0 {
            "-".into()
        } else {
            fmt_dur(total_nanos / count)
        }
    }
    fn fmt_sub_pct(part_nanos: u64, whole_nanos: u64) -> String {
        if whole_nanos == 0 {
            "-".into()
        } else {
            format!("{:.0}%", part_nanos as f64 / whole_nanos as f64 * 100.0)
        }
    }

    fn write_substep_table(
        out: &mut String,
        title: &str,
        sub_names: &[&str],
        snapshots: &[WriterStatsSnapshot],
        task_idx: usize,
        get_substeps: &dyn Fn(&WriterStatsSnapshot) -> &[u64],
    ) {
        let w = 15usize;
        let aw = 10usize;
        writeln!(out, "\n--- {} Avg Breakdown ---", title).unwrap();
        write!(out, "| CP | {:>aw$} |", "avg", aw = aw).unwrap();
        for name in sub_names {
            write!(out, " {:>w$} |", name, w = w).unwrap();
        }
        writeln!(out).unwrap();
        write!(out, "|----|").unwrap();
        write!(out, "-{:-<aw$}-|", "", aw = aw).unwrap();
        for _ in sub_names {
            write!(out, "-{:-<w$}-|", "", w = w).unwrap();
        }
        writeln!(out).unwrap();
        for (i, snap) in snapshots.iter().enumerate() {
            let n = snap.calls[task_idx];
            let total = snap.nanos[task_idx];
            let subs = get_substeps(snap);
            let avg_total = if n > 0 {
                fmt_dur(total / n)
            } else {
                "-".into()
            };
            write!(out, "| {:>2} | {:>aw$} |", i + 1, avg_total, aw = aw).unwrap();
            for (j, _) in sub_names.iter().enumerate() {
                let cell = if n > 0 {
                    format!(
                        "{} ({})",
                        fmt_sub_avg(subs[j], n),
                        fmt_sub_pct(subs[j], total)
                    )
                } else {
                    "-".into()
                };
                write!(out, " {:>w$} |", cell, w = w).unwrap();
            }
            writeln!(out).unwrap();
        }
    }

    let register_sub_names = ["lock_wait", "quantize"];

    let navigate_sub_names = ["dist", "sort", "rerank", "dist_quantize", "dist_distance"];

    write_substep_table(&mut out, "add()", &add_sub_names, snapshots, 0, &|s| {
        &s.add_substeps
    });
    write_substep_table(
        &mut out,
        "navigate()",
        &navigate_sub_names,
        snapshots,
        1,
        &|s| &s.navigate_substeps,
    );
    {
        writeln!(out, "\n--- navigate() Stats ---").unwrap();
        writeln!(out, "| CP | avg_levels | avg_dists |").unwrap();
        writeln!(out, "|----|------------|-----------|").unwrap();
        for (i, snap) in snapshots.iter().enumerate() {
            let n = snap.calls[1];
            let avg_levels = if n > 0 {
                snap.navigate_levels as f64 / n as f64
            } else {
                0.0
            };
            let avg_dists = if n > 0 {
                snap.navigate_dist_count as f64 / n as f64
            } else {
                0.0
            };
            writeln!(
                out,
                "| {:>2} | {:>10.1} | {:>9.1} |",
                i + 1,
                avg_levels,
                avg_dists
            )
            .unwrap();
        }
    }
    {
        // Per-level beam attribution. Helps answer "where did the
        // distance computations go?" when the per-level write policy
        // (--write-level-min-pcts / --write-level-taus) lets non-leaf
        // levels expand far beyond --write-beam-max.
        //
        //  - in:      avg input-beam size (internals being expanded).
        //  - dists:   avg children scored at this level (= distances).
        //  - out:     avg post-truncate beam size (survivors of tau +
        //             min/max clamp). At the leaf level this is the
        //             number of leaves returned by navigate().
        //  - fan:     avg fan-out of beam parents at this level (= dists/in).
        //  - tau:     avg dists/out -- how aggressively the tau filter
        //             trimmed candidates (1.0 = nothing trimmed; large =
        //             tau is doing real work).
        //
        // Levels where the call never reached are blank.
        writeln!(out, "\n--- navigate() Per-Level ---").unwrap();
        writeln!(
            out,
            "| CP |  L |   calls | calls% |     in |   dists |    out |    fan |    trim |"
        )
        .unwrap();
        writeln!(
            out,
            "|----|----|---------|--------|--------|---------|--------|--------|---------|"
        )
        .unwrap();
        for (i, snap) in snapshots.iter().enumerate() {
            let total_calls = snap.calls[1].max(1);
            for li in 0..MAX_NAV_LEVELS {
                let calls_l = snap.nav_calls_per_level[li];
                if calls_l == 0 {
                    continue;
                }
                let in_l = snap.nav_in_per_level[li] as f64 / calls_l as f64;
                let dist_l = snap.nav_dist_per_level[li] as f64 / calls_l as f64;
                let out_l = snap.nav_out_per_level[li] as f64 / calls_l as f64;
                let fan = if in_l > 0.0 { dist_l / in_l } else { 0.0 };
                let trim = if out_l > 0.0 { dist_l / out_l } else { 0.0 };
                let calls_pct = calls_l as f64 / total_calls as f64 * 100.0;
                writeln!(
                    out,
                    "| {:>2} | {:>2} | {:>7} | {:>5.0}% | {:>6.1} | {:>7.1} | {:>6.1} | {:>6.1} | {:>6.1}x |",
                    i + 1,
                    li + 1,
                    fmt_count(calls_l),
                    calls_pct,
                    in_l,
                    dist_l,
                    out_l,
                    fan,
                    trim,
                )
                .unwrap();
            }
        }
    }
    write_substep_table(
        &mut out,
        "register_in_leaf()",
        &register_sub_names,
        snapshots,
        2,
        &|s| &s.register_substeps,
    );
    write_substep_table(&mut out, "split()", &split_sub_names, snapshots, 3, &|s| {
        &s.split_substeps
    });
    {
        writeln!(out, "\n--- split() Stats ---").unwrap();
        writeln!(out, "| CP | min size | p25 size | p50 size | p75 size | max size |").unwrap();
        writeln!(out, "|----|----------|----------|----------|----------|----------|").unwrap();
        for (i, snap) in snapshots.iter().enumerate() {
            let mut sizes = snap.split_sizes.clone();
            sizes.sort_unstable();
            let as_usize: Vec<usize> = sizes.iter().map(|&v| v as usize).collect();
            let min_size = as_usize.first().copied().unwrap_or(0);
            let p25_size = percentile_usize(&as_usize, 25);
            let p50_size = percentile_usize(&as_usize, 50);
            let p75_size = percentile_usize(&as_usize, 75);
            let max_size = as_usize.last().copied().unwrap_or(0);
            writeln!(
                out,
                "| {:>2} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} |",
                i + 1,
                min_size,
                p25_size,
                p50_size,
                p75_size,
                max_size,
            )
            .unwrap();
        }
    }
    // NPA neighbor stats (appended to split breakdown)
    {
        writeln!(out, "\n--- split() NPA Neighbor Stats ---").unwrap();
        writeln!(out, "| CP | avg_depth |   neighbors |     active |  active% | eval/neigh | reassign/neigh | reassign% | reassigns/split |").unwrap();
        writeln!(out, "|----|----------|-------------|------------|----------|------------|----------------|-----------|-----------------|").unwrap();
        for (i, snap) in snapshots.iter().enumerate() {
            let n_splits = snap.calls[3];
            let visited = snap.split_npa_neighbors_visited;
            let active = snap.split_npa_neighbors_active;
            let evaluated = snap.split_npa_neighbor_evaluated;
            let reassigned = snap.split_npa_neighbor_reassigns;
            let avg_visited = if n_splits > 0 {
                visited as f64 / n_splits as f64
            } else {
                0.0
            };
            let avg_active = if n_splits > 0 {
                active as f64 / n_splits as f64
            } else {
                0.0
            };
            let active_pct = if visited > 0 {
                active as f64 / visited as f64 * 100.0
            } else {
                0.0
            };
            let avg_depth = if n_splits > 0 {
                snap.split_depth_sum as f64 / n_splits as f64
            } else {
                0.0
            };
            let eval_per_neigh = if visited > 0 {
                evaluated as f64 / visited as f64
            } else {
                0.0
            };
            let reassign_per_neigh = if visited > 0 {
                reassigned as f64 / visited as f64
            } else {
                0.0
            };
            let reassign_pct = if evaluated > 0 {
                reassigned as f64 / evaluated as f64 * 100.0
            } else {
                0.0
            };
            let avg_reassigned = if n_splits > 0 {
                reassigned as f64 / n_splits as f64
            } else {
                0.0
            };
            writeln!(
                out,
                "| {:>2} | {:>8.2} | {:>5.1}/split | {:>4.1}/split | {:>6.1}% | {:>10.1} | {:>14.1} | {:>7.1}% | {:>15.1} |",
                i + 1, avg_depth, avg_visited, avg_active, active_pct, eval_per_neigh, reassign_per_neigh, reassign_pct, avg_reassigned,
            ).unwrap();
        }
    }
    {
        writeln!(out, "\n--- split() NPA Self Stats ---").unwrap();
        writeln!(out, "| CP | vectors/split | evaluated/split |  eval% | reassigned/split | reassign% |").unwrap();
        writeln!(out, "|----|---------------|-----------------|--------|------------------|-----------|").unwrap();
        for (i, snap) in snapshots.iter().enumerate() {
            let n_splits = snap.calls[3];
            let total = snap.split_npa_self_total;
            let evaluated = snap.split_npa_self_evaluated;
            let reassigned = snap.split_npa_self_reassigns;
            let avg_total = if n_splits > 0 {
                total as f64 / n_splits as f64
            } else {
                0.0
            };
            let avg_evaluated = if n_splits > 0 {
                evaluated as f64 / n_splits as f64
            } else {
                0.0
            };
            let eval_pct = if total > 0 {
                evaluated as f64 / total as f64 * 100.0
            } else {
                0.0
            };
            let avg_reassigned = if n_splits > 0 {
                reassigned as f64 / n_splits as f64
            } else {
                0.0
            };
            let reassign_pct = if evaluated > 0 {
                reassigned as f64 / evaluated as f64 * 100.0
            } else {
                0.0
            };
            writeln!(
                out,
                "| {:>2} | {:>13.1} | {:>15.1} | {:>5.1}% | {:>16.1} | {:>7.1}% |",
                i + 1, avg_total, avg_evaluated, eval_pct, avg_reassigned, reassign_pct,
            ).unwrap();
        }
    }
    write_substep_table(
        &mut out,
        "reassign()",
        &reassign_sub_names,
        snapshots,
        5,
        &|s| &s.reassign_substeps,
    );

    out
}

/// Per-checkpoint table of bytes loaded from / written to the persisted
/// blockfiles by the lazy I/O paths (`load`, `load_raw`) plus bytes added
/// to the in-memory embedding map by `add()`.
///
/// Each row corresponds to one entry in `snapshots`, which when produced via
/// `snapshot_delta` represent per-checkpoint values. `dim` is the vector
/// dimensionality (used to compute embedding bytes and posting code bytes
/// assuming 1-bit codes).
pub fn format_data_loaded_table(snapshots: &[WriterStatsSnapshot], dim: usize) -> String {
    use std::fmt::Write;

    fn fmt_count(n: u64) -> String {
        if n < 1_000 {
            n.to_string()
        } else if n < 1_000_000 {
            format!("{:.1}K", n as f64 / 1_000.0)
        } else if n < 1_000_000_000 {
            format!("{:.2}M", n as f64 / 1_000_000.0)
        } else {
            format!("{:.2}B", n as f64 / 1_000_000_000.0)
        }
    }
    fn fmt_mb(bytes: u64) -> String {
        let mb = bytes as f64 / (1024.0 * 1024.0);
        if mb < 1.0 {
            format!("{:.0}KB", bytes as f64 / 1024.0)
        } else if mb < 1024.0 {
            format!("{:.1}MB", mb)
        } else {
            format!("{:.2}GB", mb / 1024.0)
        }
    }

    // In-memory bytes per entry for posting lists: id (u32) + code (1 byte
    // per dim/8) + version (u8). For dim=1024 with 1-bit codes:
    // 4 + 128 + 1 = 133 bytes/entry.
    let posting_bytes_per_entry: u64 = (4 + (dim as u64) / 8 + 1) as u64;
    let embedding_bytes_per_vec: u64 = (dim as u64) * 4;

    let mut out = String::new();
    writeln!(out, "\n--- Data Loaded (lazy I/O per checkpoint) ---").unwrap();
    writeln!(
        out,
        "| CP | post nodes | post entries | post bytes | emb loaded |  emb bytes | added vec |   add bytes | total IO |"
    ).unwrap();
    writeln!(
        out,
        "|----|-----------|--------------|------------|-----------|------------|-----------|-------------|----------|"
    ).unwrap();
    for (i, s) in snapshots.iter().enumerate() {
        let post_bytes = s.posting_load_entries.saturating_mul(posting_bytes_per_entry);
        let emb_bytes = s.embedding_loads.saturating_mul(embedding_bytes_per_vec);
        let add_bytes = s.embeddings_added.saturating_mul(embedding_bytes_per_vec);
        let total = post_bytes + emb_bytes + add_bytes;
        writeln!(
            out,
            "| {:>2} | {:>9} | {:>12} | {:>10} | {:>9} | {:>10} | {:>9} | {:>11} | {:>8} |",
            i + 1,
            fmt_count(s.posting_loads),
            fmt_count(s.posting_load_entries),
            fmt_mb(post_bytes),
            fmt_count(s.embedding_loads),
            fmt_mb(emb_bytes),
            fmt_count(s.embeddings_added),
            fmt_mb(add_bytes),
            fmt_mb(total),
        )
        .unwrap();
    }

    // Cumulative totals row.
    let tot_post_loads: u64 = snapshots.iter().map(|s| s.posting_loads).sum();
    let tot_post_entries: u64 = snapshots.iter().map(|s| s.posting_load_entries).sum();
    let tot_emb: u64 = snapshots.iter().map(|s| s.embedding_loads).sum();
    let tot_add: u64 = snapshots.iter().map(|s| s.embeddings_added).sum();
    let tot_post_bytes = tot_post_entries.saturating_mul(posting_bytes_per_entry);
    let tot_emb_bytes = tot_emb.saturating_mul(embedding_bytes_per_vec);
    let tot_add_bytes = tot_add.saturating_mul(embedding_bytes_per_vec);
    let tot_io = tot_post_bytes + tot_emb_bytes + tot_add_bytes;
    writeln!(
        out,
        "| ** | {:>9} | {:>12} | {:>10} | {:>9} | {:>10} | {:>9} | {:>11} | {:>8} |",
        fmt_count(tot_post_loads),
        fmt_count(tot_post_entries),
        fmt_mb(tot_post_bytes),
        fmt_count(tot_emb),
        fmt_mb(tot_emb_bytes),
        fmt_count(tot_add),
        fmt_mb(tot_add_bytes),
        fmt_mb(tot_io),
    )
    .unwrap();

    out
}

// =============================================================================
// Diagnostic structs
// =============================================================================

pub struct LevelRecall {
    pub level: usize,
    pub reachable_100: f64,
    pub beam_size: usize,
    pub total_candidates: usize,
}

pub struct LeafTraits {
    pub rank: usize,
    pub score: f32,
    pub leaf_size: usize,
    pub gt_count: usize,
    pub min_gt_dist: f32,
}

pub struct LeafMissDiagnostic {
    pub beam_size: usize,
    pub total_leaves: usize,
    /// For each GT vector not covered by the beam: (vector_id, rank of the leaf containing it).
    /// rank is 1-indexed in the sorted candidate list. If a GT vector appears in multiple leaves,
    /// we report the best (lowest) rank.
    pub missed_gt_ranks: Vec<(u32, usize)>,
    pub gt_total: usize,
    pub selected_with_gt: Vec<LeafTraits>,
    pub selected_no_gt: Vec<LeafTraits>,
    pub missed_with_gt: Vec<LeafTraits>,
    /// d_best * (1 + tau) at the leaf level: the theoretical beam cutoff.
    pub search_radius: f32,
    /// Score of the farthest centroid actually in the beam (may be < search_radius
    /// when beam_max truncates before the tau threshold).
    pub beam_radius: f32,
    /// dist(query, v) for each GT vector whose embedding is available.
    pub gt_distances: Vec<f32>,
}

pub struct SearchTimings {
    pub navigate_nanos: u64,
    pub quantize_nanos: u64,
    pub distance_nanos: u64,
    pub sort_dedup_nanos: u64,
    pub rerank_nanos: u64,
}

// =============================================================================
// Helpers
// =============================================================================

pub fn percentile_usize(data: &[usize], pct: usize) -> usize {
    if data.is_empty() {
        return 0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable();
    let idx = (pct as f64 / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn percentile_f32(data: &[f32], pct: usize) -> f32 {
    if data.is_empty() {
        return 0.0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = (pct as f64 / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
