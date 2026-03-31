#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery};
use chroma_index::spann::utils::{self, EmbeddingPoint};
use dashmap::{DashMap, DashSet};
use simsimd::SpatialSimilarity;

pub type NodeId = u32;

const MAX_BALANCE_DEPTH: u32 = 4;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NavigationMode {
    Fp,
    OneBit,
    FourBit,
}

impl std::fmt::Display for NavigationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NavigationMode::Fp => write!(f, "fp"),
            NavigationMode::OneBit => write!(f, "1bit"),
            NavigationMode::FourBit => write!(f, "4bit"),
        }
    }
}

impl std::fmt::Debug for NavigationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[derive(Clone)]
pub struct HierarchicalSpannConfig {
    pub branching_factor: usize,
    pub split_threshold: usize,
    pub merge_threshold: usize,
    /// Dynamic beam tau for the write path (add/reassign/merge navigate).
    pub write_beam_tau: f64,
    pub write_beam_min: usize,
    pub write_beam_max: usize,
    /// Dynamic beam tau for the search/query path.
    /// Include children with dist <= d_best * (1 + beam_tau), clamped to [beam_min, beam_max].
    pub beam_tau: f64,
    pub beam_min: usize,
    pub beam_max: usize,
    pub max_replicas: usize,
    pub write_rng_epsilon: f32,
    pub write_rng_factor: f32,
    pub reassign_neighbor_count: usize,
    pub navigation: NavigationMode,
    /// If true, NPA uses full precision f32 distances; if false, NPA uses quantized distances.
    pub fp_npa: bool,
}

impl Default for HierarchicalSpannConfig {
    fn default() -> Self {
        Self {
            branching_factor: 100,
            split_threshold: 2048,
            merge_threshold: 512,
            write_beam_tau: 0.5,
            write_beam_min: 10,
            write_beam_max: 50000,
            beam_tau: 1.0,
            beam_min: 10,
            beam_max: 50000,
            max_replicas: 2,
            write_rng_epsilon: 4.0,
            write_rng_factor: 2.0,
            reassign_neighbor_count: 32,
            navigation: NavigationMode::FourBit,
            fp_npa: true,
        }
    }
}

// =============================================================================
// Node types
// =============================================================================
struct LeafNode {
    centroid: Vec<f32>,
    /// 1-bit RaBitQ code of centroid as residual vs parent centroid.
    centroid_code: Vec<u8>,
    ids: Vec<u32>,
    versions: Vec<u32>,
    /// Per-vector 1-bit RaBitQ codes (residual vs this leaf's centroid).
    codes: Vec<Vec<u8>>,
    parent_id: Option<NodeId>,
}

struct InternalNode {
    centroid: Vec<f32>,
    /// 1-bit RaBitQ code of centroid as residual vs parent centroid.
    centroid_code: Vec<u8>,
    children: Vec<NodeId>,
    parent_id: Option<NodeId>,
}

enum TreeNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl TreeNode {
    fn centroid(&self) -> &[f32] {
        match self {
            TreeNode::Leaf(l) => &l.centroid,
            TreeNode::Internal(i) => &i.centroid,
        }
    }

    fn centroid_code(&self) -> &[u8] {
        match self {
            TreeNode::Leaf(l) => &l.centroid_code,
            TreeNode::Internal(i) => &i.centroid_code,
        }
    }

    fn parent_id(&self) -> Option<NodeId> {
        match self {
            TreeNode::Leaf(l) => l.parent_id,
            TreeNode::Internal(i) => i.parent_id,
        }
    }

    fn set_parent_id(&mut self, parent: Option<NodeId>) {
        match self {
            TreeNode::Leaf(l) => l.parent_id = parent,
            TreeNode::Internal(i) => i.parent_id = parent,
        }
    }
}

// =============================================================================
// Stats (atomic for thread safety)
// =============================================================================

pub struct WriterStats {
    adds: AtomicU64,
    add_nanos: AtomicU64,
    navigates: AtomicU64,
    navigate_nanos: AtomicU64,
    splits: AtomicU64,
    split_nanos: AtomicU64,
    merges: AtomicU64,
    merge_nanos: AtomicU64,
    reassigns: AtomicU64,
    reassign_nanos: AtomicU64,
    scrubs: AtomicU64,
    scrub_nanos: AtomicU64,
    scrub_removed: AtomicU64,
    /// Navigate saw a child_id in a parent's children list but the node was
    /// missing from the DashMap (removed by a concurrent split).
    navigate_missing_nodes: AtomicU64,
    /// add() could not register in any navigated cluster (all gone) and fell
    /// back to root.
    add_missing_nodes: AtomicU64,
    /// register_in_leaf() target was missing or no longer a leaf (e.g. split
    /// by a balance cascade during merge).
    register_missing_nodes: AtomicU64,

    registers: AtomicU64,
    register_nanos: AtomicU64,
    register_lock_wait_nanos: AtomicU64,
    register_quantize_nanos: AtomicU64,

    // Sub-step timing breakdowns (nanos)
    add_navigate_nanos: AtomicU64,
    add_register_nanos: AtomicU64,
    add_balance_nanos: AtomicU64,
    split_kmeans_nanos: AtomicU64,
    split_quantize_nanos: AtomicU64,
    split_npa_cluster_nanos: AtomicU64,
    split_npa_neighbor_nanos: AtomicU64,
    /// Number of neighbor leaves visited by apply_npa_to_neighbors
    split_npa_neighbors_visited: AtomicU64,
    /// Neighbors where >1% of vectors were reassigned
    split_npa_neighbors_active: AtomicU64,
    /// Sum of balance depth values across all splits (for computing average)
    split_depth_sum: AtomicU64,
    /// Total vectors reassigned by apply_npa_to_neighbors (across all splits)
    split_npa_neighbor_reassigns: AtomicU64,
    /// Total vectors evaluated by apply_npa_to_neighbors (across all splits)
    split_npa_neighbor_evaluated: AtomicU64,
    /// Total vectors in groups passed to apply_npa_to_cluster
    split_npa_self_total: AtomicU64,
    /// Vectors that passed version+dedup checks in apply_npa_to_cluster
    split_npa_self_evaluated: AtomicU64,
    /// Vectors reassigned by apply_npa_to_cluster (new_dist > old_dist)
    split_npa_self_reassigns: AtomicU64,
    reassign_navigate_nanos: AtomicU64,
    reassign_register_nanos: AtomicU64,
    reassign_balance_nanos: AtomicU64,
    navigate_dist_nanos: AtomicU64,
    navigate_dist_quantize_nanos: AtomicU64,
    navigate_dist_distance_nanos: AtomicU64,
    navigate_sort_nanos: AtomicU64,
    navigate_rerank_nanos: AtomicU64,
    navigate_levels: AtomicU64,
    navigate_dist_count: AtomicU64,
}

impl Default for WriterStats {
    fn default() -> Self {
        Self {
            adds: AtomicU64::new(0),
            add_nanos: AtomicU64::new(0),
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
    // Sub-step breakdowns: [navigate, register, balance]
    pub reassign_substeps: [u64; 3],
    // Sub-step breakdowns: [lock_wait, quantize]
    pub register_substeps: [u64; 2],
    // Sub-step breakdowns: [dist, sort, rerank, dist_quantize, dist_distance]
    pub navigate_substeps: [u64; 5],
    pub navigate_levels: u64,
    pub navigate_dist_count: u64,
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

pub struct LevelRecall {
    pub level: usize,
    pub reachable_100: f64,
    pub beam_size: usize,
    pub total_candidates: usize,
}

// =============================================================================
// Writer (thread-safe)
// =============================================================================

/// 1-bit quantized hierarchical SPANN index (thread-safe).
///
/// Stores data vectors as 1-bit RaBitQ codes in leaf nodes (posting lists).
/// Node centroids are also stored as 1-bit codes (residuals vs parent centroid).
/// Navigation mode is configurable: fp (f32), 1bit (code-to-code), or 4bit (QuantizedQuery). Search scores data vectors with quantized codes
/// and optionally reranks with f32 embeddings.
///
/// Thread safety:
/// - `nodes` in `DashMap`: per-shard locks serialize concurrent access to the same node
/// - split/merge atomically remove nodes first, so concurrent register_in_leaf fails and add() retries
/// - `balancing`: DashSet guard to prevent duplicate balance work on the same cluster
/// - `embeddings`/`versions` in `DashMap` for concurrent access
/// - `root_id`/`next_node_id` are atomic
/// - Stats use `AtomicU64`
pub struct SearchTimings {
    pub navigate_nanos: u64,
    pub quantize_nanos: u64,
    pub distance_nanos: u64,
    pub sort_dedup_nanos: u64,
    pub rerank_nanos: u64,
}

pub struct HierarchicalSpannWriter {
    dim: usize,
    distance_fn: DistanceFunction,
    config: HierarchicalSpannConfig,

    nodes: DashMap<NodeId, TreeNode>,
    balancing: DashSet<NodeId>,
    root_id: AtomicU32,
    next_node_id: AtomicU32,

    embeddings: DashMap<u32, Arc<[f32]>>,
    versions: DashMap<u32, u32>,

    pub stats: WriterStats,
}

impl HierarchicalSpannWriter {
    pub fn new(dim: usize, distance_fn: DistanceFunction, config: HierarchicalSpannConfig) -> Self {
        let nodes = DashMap::new();
        nodes.insert(
            0,
            TreeNode::Leaf(LeafNode {
                centroid: vec![0.0; dim],
                centroid_code: Vec::new(),
                ids: Vec::new(),
                versions: Vec::new(),
                codes: Vec::new(),
                parent_id: None,
            }),
        );

        Self {
            dim,
            distance_fn,
            config,
            nodes,
            balancing: DashSet::new(),
            root_id: AtomicU32::new(0),
            next_node_id: AtomicU32::new(1),
            embeddings: DashMap::new(),
            versions: DashMap::new(),
            stats: WriterStats::default(),
        }
    }

    fn alloc_node_id(&self) -> NodeId {
        self.next_node_id.fetch_add(1, Ordering::Relaxed)
    }

    fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.distance_fn.distance(a, b)
    }

    fn root_id(&self) -> NodeId {
        self.root_id.load(Ordering::Relaxed)
    }

    // =========================================================================
    // Add
    // =========================================================================

    /// Add a data vector to the index.
    ///
    /// Thread-safe: multiple threads can call add() concurrently.
    /// No global lock -- DashMap per-shard atomicity + retry handles races
    /// with concurrent split/merge operations.
    pub fn add(&self, id: u32, embedding: &[f32]) {
        let add_start = Instant::now();

        let emb: Arc<[f32]> = Arc::from(embedding);
        self.embeddings.insert(id, emb);

        let mut version = {
            let mut v = self.versions.entry(id).or_insert(0);
            *v += 1;
            *v
        };

        loop {
            let nav_start = Instant::now();
            let candidates = self.navigate(
                embedding,
                Some(self.config.write_beam_tau),
                self.config.write_beam_min,
                self.config.write_beam_max,
                1,
            );
            let cluster_ids = self.rng_select(&candidates);
            self.stats
                .add_navigate_nanos
                .fetch_add(nav_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            let reg_start = Instant::now();
            let mut clusters_to_balance = Vec::new();
            for &cluster_id in &cluster_ids {
                if self.register_in_leaf(cluster_id, id, version, embedding) {
                    clusters_to_balance.push(cluster_id);
                }
            }
            self.stats
                .add_register_nanos
                .fetch_add(reg_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if clusters_to_balance.is_empty() {
                self.stats.add_missing_nodes.fetch_add(1, Ordering::Relaxed);
                version = {
                    let mut v = self.versions.entry(id).or_insert(0);
                    *v += 1;
                    *v
                };
                continue;
            }

            let balance_start = Instant::now();
            for cluster_id in clusters_to_balance {
                self.balance(cluster_id, 0);
            }
            self.stats
                .add_balance_nanos
                .fetch_add(balance_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            break;
        }

        self.stats.adds.fetch_add(1, Ordering::Relaxed);
        self.stats
            .add_nanos
            .fetch_add(add_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Register a vector in a leaf. Uses per-leaf DashMap get_mut -- no global lock.
    /// Also computes and stores the 1-bit RaBitQ code of the vector residual.
    fn register_in_leaf(&self, leaf_id: NodeId, id: u32, version: u32, embedding: &[f32]) -> bool {
        let t0 = Instant::now();
        let lock_start = Instant::now();
        if let Some(mut node_ref) = self.nodes.get_mut(&leaf_id) {
            let lock_elapsed = lock_start.elapsed().as_nanos() as u64;
            self.stats
                .register_lock_wait_nanos
                .fetch_add(lock_elapsed, Ordering::Relaxed);
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                let q_start = Instant::now();
                let code = Code::<1>::quantize(embedding, &leaf.centroid);
                self.stats
                    .register_quantize_nanos
                    .fetch_add(q_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                leaf.ids.push(id);
                leaf.versions.push(version);
                leaf.codes.push(code.as_ref().to_vec());
                drop(node_ref);
                self.stats.registers.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .register_nanos
                    .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                return true;
            }
        }
        self.stats.registers.fetch_add(1, Ordering::Relaxed);
        self.stats
            .register_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        false
    }

    // =========================================================================
    // Navigate (f32 -- used by write path and --fp-navigation)
    // =========================================================================

    /// Beam search the tree using f32 centroid distances.
    /// Used by the write path (always) and search path when navigation=Fp.
    fn navigate_f32(
        &self,
        query: &[f32],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
    ) -> Vec<(NodeId, f32)> {
        let nav_t0 = Instant::now();
        let root = self.root_id();
        let Some(root_node) = self.nodes.get(&root) else {
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return Vec::new();
        };

        if matches!(root_node.value(), TreeNode::Leaf(_)) {
            let dist = self.dist(query, root_node.centroid());
            drop(root_node);
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return vec![(root, dist)];
        }
        drop(root_node);

        let mut leaves: Vec<(NodeId, f32)> = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];
        let mut dist_nanos = 0u64;
        let mut sort_nanos = 0u64;
        let mut levels = 0u64;
        let mut dist_count = 0u64;

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            let dist_start = Instant::now();
            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);
                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                let dist = self.dist(query, child.centroid());
                                child_scores.push((child_id, dist));
                            } else {
                                self.stats
                                    .navigate_missing_nodes
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            dist_nanos += dist_start.elapsed().as_nanos() as u64;

            if child_scores.is_empty() {
                break;
            }

            levels += 1;
            dist_count += child_scores.len() as u64;

            let sort_start = Instant::now();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let effective = Self::effective_beam(&child_scores, tau, beam_min, beam_max);
            child_scores.truncate(effective);
            sort_nanos += sort_start.elapsed().as_nanos() as u64;

            let mut next_internals: Vec<NodeId> = Vec::new();
            for &(node_id, dist) in &child_scores {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    match node_ref.value() {
                        TreeNode::Leaf(_) => leaves.push((node_id, dist)),
                        TreeNode::Internal(_) => next_internals.push(node_id),
                    }
                }
            }

            if next_internals.is_empty() {
                break;
            }
            beam = next_internals;
        }

        let sort_start = Instant::now();
        leaves.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        sort_nanos += sort_start.elapsed().as_nanos() as u64;

        self.stats
            .navigate_dist_nanos
            .fetch_add(dist_nanos, Ordering::Relaxed);
        self.stats
            .navigate_sort_nanos
            .fetch_add(sort_nanos, Ordering::Relaxed);
        self.stats
            .navigate_levels
            .fetch_add(levels, Ordering::Relaxed);
        self.stats
            .navigate_dist_count
            .fetch_add(dist_count, Ordering::Relaxed);
        self.stats.navigates.fetch_add(1, Ordering::Relaxed);
        self.stats
            .navigate_nanos
            .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        leaves
    }

    fn effective_beam(
        sorted: &[(NodeId, f32)],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
    ) -> usize {
        if sorted.is_empty() {
            return 0;
        }
        match tau {
            None => beam_min.min(sorted.len()),
            Some(tau) => {
                let d_best = sorted[0].1.max(1e-10_f32);
                let threshold = d_best * (1.0_f32 + tau as f32);
                let count = sorted.iter().take_while(|(_, d)| *d <= threshold).count();
                let floor = beam_min.min(beam_max);
                count.clamp(floor, beam_max).min(sorted.len())
            }
        }
    }

    // =========================================================================
    // Navigate (quantized -- default search path)
    // =========================================================================

    /// Beam search using 1-bit quantized centroid distances.
    /// At each level, scores children using QuantizedQuery against their centroid_code.
    /// Optionally reranks with f32 if rerank_centroids > 1.
    fn navigate_quantized(
        &self,
        query: &[f32],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
        rerank_centroids: usize,
    ) -> Vec<(NodeId, f32)> {
        let nav_t0 = Instant::now();
        let root = self.root_id();
        let Some(root_node) = self.nodes.get(&root) else {
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return Vec::new();
        };

        if matches!(root_node.value(), TreeNode::Leaf(_)) {
            let dist = self.dist(query, root_node.centroid());
            drop(root_node);
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return vec![(root, dist)];
        }
        drop(root_node);

        let q_norm = Self::vec_norm(query);
        let padded_bytes = self.padded_bytes();
        let rerank_factor = rerank_centroids;

        let mut leaves: Vec<(NodeId, f32)> = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];
        let mut dist_nanos = 0u64;
        let mut sort_nanos = 0u64;
        let mut rerank_nanos = 0u64;
        let mut levels = 0u64;
        let mut dist_count = 0u64;
        let mut dist_quantize_nanos = 0u64;
        let mut dist_distance_nanos = 0u64;

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            let dist_start = Instant::now();
            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let parent_centroid = internal.centroid.clone();
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        let c_norm = Self::vec_norm(&parent_centroid);
                        let qt0 = Instant::now();
                        let r_q: Vec<f32> = query
                            .iter()
                            .zip(parent_centroid.iter())
                            .map(|(q, c)| q - c)
                            .collect();
                        let c_dot_q = f32::dot(&parent_centroid, query).unwrap_or(0.0) as f32;
                        let qq = QuantizedQuery::new(&r_q, padded_bytes, c_norm, c_dot_q, q_norm);
                        dist_quantize_nanos += qt0.elapsed().as_nanos() as u64;

                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                let code_bytes = child.centroid_code();
                                let dt0 = Instant::now();
                                let dist = if code_bytes.is_empty() {
                                    self.dist(query, child.centroid())
                                } else {
                                    Code::<1, _>::new(code_bytes)
                                        .distance_quantized_query(&self.distance_fn, &qq)
                                };
                                dist_distance_nanos += dt0.elapsed().as_nanos() as u64;
                                child_scores.push((child_id, dist));
                            } else {
                                self.stats
                                    .navigate_missing_nodes
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            dist_nanos += dist_start.elapsed().as_nanos() as u64;

            if child_scores.is_empty() {
                break;
            }

            levels += 1;
            dist_count += child_scores.len() as u64;

            let sort_start = Instant::now();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            sort_nanos += sort_start.elapsed().as_nanos() as u64;

            if rerank_factor > 1 {
                let effective = Self::effective_beam(&child_scores, tau, beam_min, beam_max);
                let rerank_count = (effective * rerank_factor).min(child_scores.len());
                child_scores.truncate(rerank_count);

                let rerank_start = Instant::now();
                let mut reranked: Vec<(NodeId, f32)> = child_scores
                    .iter()
                    .map(|&(nid, _approx)| {
                        let dist = self
                            .nodes
                            .get(&nid)
                            .map(|n| self.dist(query, n.centroid()))
                            .unwrap_or(f32::MAX);
                        (nid, dist)
                    })
                    .collect();
                reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let final_beam = Self::effective_beam(&reranked, tau, beam_min, beam_max);
                reranked.truncate(final_beam);
                rerank_nanos += rerank_start.elapsed().as_nanos() as u64;
                child_scores = reranked;
            } else {
                let effective = Self::effective_beam(&child_scores, tau, beam_min, beam_max);
                child_scores.truncate(effective);
            }

            let mut next_internals: Vec<NodeId> = Vec::new();
            for &(node_id, dist) in &child_scores {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    match node_ref.value() {
                        TreeNode::Leaf(_) => leaves.push((node_id, dist)),
                        TreeNode::Internal(_) => next_internals.push(node_id),
                    }
                }
            }

            if next_internals.is_empty() {
                break;
            }
            beam = next_internals;
        }

        let sort_start = Instant::now();
        leaves.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        sort_nanos += sort_start.elapsed().as_nanos() as u64;

        self.stats
            .navigate_dist_nanos
            .fetch_add(dist_nanos, Ordering::Relaxed);
        self.stats
            .navigate_dist_quantize_nanos
            .fetch_add(dist_quantize_nanos, Ordering::Relaxed);
        self.stats
            .navigate_dist_distance_nanos
            .fetch_add(dist_distance_nanos, Ordering::Relaxed);
        self.stats
            .navigate_sort_nanos
            .fetch_add(sort_nanos, Ordering::Relaxed);
        self.stats
            .navigate_rerank_nanos
            .fetch_add(rerank_nanos, Ordering::Relaxed);
        self.stats
            .navigate_levels
            .fetch_add(levels, Ordering::Relaxed);
        self.stats
            .navigate_dist_count
            .fetch_add(dist_count, Ordering::Relaxed);
        self.stats.navigates.fetch_add(1, Ordering::Relaxed);
        self.stats
            .navigate_nanos
            .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        leaves
    }

    // =========================================================================
    // Navigate (1-bit code-to-code)
    // =========================================================================

    /// Beam search using 1-bit code-to-code distances.
    /// Quantizes the query against each parent centroid, then uses distance_code()
    /// to compare against children's centroid_code. Faster than QuantizedQuery but
    /// lower precision.
    fn navigate_1bit(
        &self,
        query: &[f32],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
        rerank_centroids: usize,
    ) -> Vec<(NodeId, f32)> {
        let nav_t0 = Instant::now();
        let root = self.root_id();
        let Some(root_node) = self.nodes.get(&root) else {
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return Vec::new();
        };

        if matches!(root_node.value(), TreeNode::Leaf(_)) {
            let dist = self.dist(query, root_node.centroid());
            drop(root_node);
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return vec![(root, dist)];
        }
        drop(root_node);

        let rerank_factor = rerank_centroids;
        let dim = self.dim;

        let mut leaves: Vec<(NodeId, f32)> = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];
        let mut dist_nanos = 0u64;
        let mut sort_nanos = 0u64;
        let mut rerank_nanos = 0u64;
        let mut levels = 0u64;
        let mut dist_count = 0u64;
        let mut dist_quantize_nanos = 0u64;
        let mut dist_distance_nanos = 0u64;

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            let dist_start = Instant::now();
            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let parent_centroid = internal.centroid.clone();
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        let c_norm = Self::vec_norm(&parent_centroid);
                        let qt0 = Instant::now();
                        let query_code = Code::<1>::quantize(query, &parent_centroid);
                        dist_quantize_nanos += qt0.elapsed().as_nanos() as u64;

                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                let code_bytes = child.centroid_code();
                                let dt0 = Instant::now();
                                let dist = if code_bytes.is_empty() {
                                    self.dist(query, child.centroid())
                                } else {
                                    let child_code = Code::<1, _>::new(code_bytes);
                                    query_code.distance_code(
                                        &child_code,
                                        &self.distance_fn,
                                        c_norm,
                                        dim,
                                    )
                                };
                                dist_distance_nanos += dt0.elapsed().as_nanos() as u64;
                                child_scores.push((child_id, dist));
                            } else {
                                self.stats
                                    .navigate_missing_nodes
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            dist_nanos += dist_start.elapsed().as_nanos() as u64;

            if child_scores.is_empty() {
                break;
            }

            levels += 1;
            dist_count += child_scores.len() as u64;

            let sort_start = Instant::now();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            sort_nanos += sort_start.elapsed().as_nanos() as u64;

            if rerank_factor > 1 {
                let effective = Self::effective_beam(&child_scores, tau, beam_min, beam_max);
                let rerank_count = (effective * rerank_factor).min(child_scores.len());
                child_scores.truncate(rerank_count);

                let rerank_start = Instant::now();
                let mut reranked: Vec<(NodeId, f32)> = child_scores
                    .iter()
                    .map(|&(nid, _approx)| {
                        let dist = self
                            .nodes
                            .get(&nid)
                            .map(|n| self.dist(query, n.centroid()))
                            .unwrap_or(f32::MAX);
                        (nid, dist)
                    })
                    .collect();
                reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let final_beam = Self::effective_beam(&reranked, tau, beam_min, beam_max);
                reranked.truncate(final_beam);
                rerank_nanos += rerank_start.elapsed().as_nanos() as u64;
                child_scores = reranked;
            } else {
                let effective = Self::effective_beam(&child_scores, tau, beam_min, beam_max);
                child_scores.truncate(effective);
            }

            let mut next_internals: Vec<NodeId> = Vec::new();
            for &(node_id, dist) in &child_scores {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    match node_ref.value() {
                        TreeNode::Leaf(_) => leaves.push((node_id, dist)),
                        TreeNode::Internal(_) => next_internals.push(node_id),
                    }
                }
            }

            if next_internals.is_empty() {
                break;
            }
            beam = next_internals;
        }

        let sort_start = Instant::now();
        leaves.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        sort_nanos += sort_start.elapsed().as_nanos() as u64;

        self.stats
            .navigate_dist_nanos
            .fetch_add(dist_nanos, Ordering::Relaxed);
        self.stats
            .navigate_dist_quantize_nanos
            .fetch_add(dist_quantize_nanos, Ordering::Relaxed);
        self.stats
            .navigate_dist_distance_nanos
            .fetch_add(dist_distance_nanos, Ordering::Relaxed);
        self.stats
            .navigate_sort_nanos
            .fetch_add(sort_nanos, Ordering::Relaxed);
        self.stats
            .navigate_rerank_nanos
            .fetch_add(rerank_nanos, Ordering::Relaxed);
        self.stats
            .navigate_levels
            .fetch_add(levels, Ordering::Relaxed);
        self.stats
            .navigate_dist_count
            .fetch_add(dist_count, Ordering::Relaxed);
        self.stats.navigates.fetch_add(1, Ordering::Relaxed);
        self.stats
            .navigate_nanos
            .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        leaves
    }

    /// Dispatch to the configured navigate implementation.
    fn navigate(
        &self,
        query: &[f32],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
        rerank_centroids: usize,
    ) -> Vec<(NodeId, f32)> {
        match self.config.navigation {
            NavigationMode::Fp => self.navigate_f32(query, tau, beam_min, beam_max),
            NavigationMode::OneBit => {
                self.navigate_1bit(query, tau, beam_min, beam_max, rerank_centroids)
            }
            NavigationMode::FourBit => {
                self.navigate_quantized(query, tau, beam_min, beam_max, rerank_centroids)
            }
        }
    }

    // =========================================================================
    // RNG select
    // =========================================================================

    /// Select clusters via RNG rule. Uses per-node DashMap gets -- no global lock.
    fn rng_select(&self, candidates: &[(NodeId, f32)]) -> Vec<NodeId> {
        if candidates.is_empty() {
            return Vec::new();
        }

        let first_distance = candidates[0].1;
        let mut result = Vec::new();
        let mut selected_centroids: Vec<Vec<f32>> = Vec::new();

        for &(node_id, distance) in candidates {
            if (distance - first_distance).abs()
                > self.config.write_rng_epsilon * first_distance.abs().max(1e-10)
            {
                break;
            }

            let Some(node_ref) = self.nodes.get(&node_id) else {
                continue;
            };
            let centroid = node_ref.centroid().to_vec();
            drop(node_ref);

            // RNG filter
            // Don't replicate to clusters that are close to each other.
            // If the candidate cluster is farther away from the query than from other already selected clusters, skip it.
            let blocked = selected_centroids
                .iter()
                .any(|sel| distance > self.dist(&centroid, sel) * self.config.write_rng_factor);
            if blocked {
                continue;
            }

            result.push(node_id);
            selected_centroids.push(centroid);

            if result.len() >= self.config.max_replicas {
                break;
            }
        }

        result
    }

    // =========================================================================
    // Balance / Scrub
    // =========================================================================

    /// Balance a cluster: scrub stale entries, then split or merge if needed.
    /// Scrub and size check use per-node DashMap access (no global lock).
    fn balance(&self, cluster_id: NodeId, depth: u32) {
        if depth > MAX_BALANCE_DEPTH {
            return;
        }

        self.scrub(cluster_id);

        let len = match self.nodes.get(&cluster_id) {
            Some(node_ref) => match node_ref.value() {
                TreeNode::Leaf(leaf) => leaf.ids.len(),
                _ => return,
            },
            None => return,
        };

        let needs_split = len > self.config.split_threshold;
        let needs_merge = len > 0 && len < self.config.merge_threshold;

        if needs_split || needs_merge {
            if !self.balancing.insert(cluster_id) {
                return;
            }

            if len > self.config.split_threshold {
                self.split_leaf(cluster_id, depth);
            } else if len > 0 && len < self.config.merge_threshold {
                self.merge_leaf(cluster_id, depth);
            }

            self.balancing.remove(&cluster_id);
        }
    }

    /// Scrub stale entries from a leaf. Uses per-leaf DashMap get_mut (no global lock).
    fn scrub(&self, cluster_id: NodeId) {
        let t0 = Instant::now();
        let Some(mut node_ref) = self.nodes.get_mut(&cluster_id) else {
            return;
        };
        let TreeNode::Leaf(leaf) = node_ref.value_mut() else {
            return;
        };

        let mut removed = 0usize;
        let mut i = 0;
        while i < leaf.ids.len() {
            let id = leaf.ids[i];
            let version = leaf.versions[i];
            let current_version = self.versions.get(&id).map(|r| *r).unwrap_or(0);
            if version < current_version {
                leaf.ids.swap_remove(i);
                leaf.versions.swap_remove(i);
                leaf.codes.swap_remove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }

        drop(node_ref);

        self.stats.scrubs.fetch_add(1, Ordering::Relaxed);
        self.stats
            .scrub_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats
            .scrub_removed
            .fetch_add(removed as u64, Ordering::Relaxed);
    }

    // =========================================================================
    // Split (leaf)
    // =========================================================================

    fn split_leaf(&self, leaf_id: NodeId, depth: u32) {
        let t0 = Instant::now();

        let (old_ids, old_versions, old_codes, parent_id, old_centroid) =
            match self.nodes.remove(&leaf_id) {
                Some((_, TreeNode::Leaf(leaf))) => (
                    leaf.ids,
                    leaf.versions,
                    leaf.codes,
                    leaf.parent_id,
                    leaf.centroid,
                ),
                Some((_, node)) => {
                    self.nodes.insert(leaf_id, node);
                    return;
                }
                None => return,
            };

        let embeddings: Vec<EmbeddingPoint> = old_ids
            .iter()
            .zip(old_versions.iter())
            .filter_map(|(&id, &ver)| {
                let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                if ver >= current_ver {
                    self.embeddings
                        .get(&id)
                        .map(|e| (id, ver, e.value().clone()))
                } else {
                    None
                }
            })
            .collect();

        if embeddings.len() <= self.config.split_threshold {
            // After filtering stale entries the leaf no longer needs splitting.
            // Re-insert it since we already removed it from the DashMap.
            let codes: Vec<Vec<u8>> = embeddings
                .iter()
                .map(|(_, _, emb)| Code::<1>::quantize(emb, &old_centroid).as_ref().to_vec())
                .collect();
            self.nodes.insert(
                leaf_id,
                TreeNode::Leaf(LeafNode {
                    centroid: old_centroid,
                    centroid_code: Vec::new(),
                    ids: embeddings.iter().map(|(id, _, _)| *id).collect(),
                    versions: embeddings.iter().map(|(_, ver, _)| *ver).collect(),
                    codes,
                    parent_id,
                }),
            );
            return;
        }

        let old_code_map: HashMap<u32, Vec<u8>> = old_ids
            .iter()
            .zip(old_codes.iter())
            .map(|(&id, code)| (id, code.clone()))
            .collect();

        let kmeans_start = Instant::now();
        let (left_center, left_group, right_center, right_group) =
            utils::split(embeddings, &self.distance_fn);
        self.stats
            .split_kmeans_nanos
            .fetch_add(kmeans_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let left_id = self.alloc_node_id();
        let right_id = self.alloc_node_id();

        let left_centroid = left_center.to_vec();
        let right_centroid = right_center.to_vec();

        let quantize_start = Instant::now();
        let left_codes: Vec<Vec<u8>> = left_group
            .iter()
            .map(|(_, _, emb)| Code::<1>::quantize(emb, &left_centroid).as_ref().to_vec())
            .collect();
        let right_codes: Vec<Vec<u8>> = right_group
            .iter()
            .map(|(_, _, emb)| Code::<1>::quantize(emb, &right_centroid).as_ref().to_vec())
            .collect();
        self.stats.split_quantize_nanos.fetch_add(
            quantize_start.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );

        self.nodes.insert(
            left_id,
            TreeNode::Leaf(LeafNode {
                centroid: left_centroid,
                centroid_code: Vec::new(),
                ids: left_group.iter().map(|(id, _, _)| *id).collect(),
                versions: left_group.iter().map(|(_, ver, _)| *ver).collect(),
                codes: left_codes,
                parent_id: None,
            }),
        );
        self.nodes.insert(
            right_id,
            TreeNode::Leaf(LeafNode {
                centroid: right_centroid,
                centroid_code: Vec::new(),
                ids: right_group.iter().map(|(id, _, _)| *id).collect(),
                versions: right_group.iter().map(|(_, ver, _)| *ver).collect(),
                codes: right_codes,
                parent_id: None,
            }),
        );

        if let Some(pid) = parent_id {
            self.replace_child(pid, leaf_id, &[left_id, right_id]);
        } else {
            self.create_root_above(&[left_id, right_id]);
        }

        if depth < MAX_BALANCE_DEPTH {
            let mut evaluated = HashSet::new();

            let npa_cluster_start = Instant::now();
            self.apply_npa_to_cluster(
                left_id,
                &left_group,
                &old_centroid,
                &left_center,
                &old_code_map,
                &mut evaluated,
                depth,
            );
            self.apply_npa_to_cluster(
                right_id,
                &right_group,
                &old_centroid,
                &right_center,
                &old_code_map,
                &mut evaluated,
                depth,
            );
            self.stats.split_npa_cluster_nanos.fetch_add(
                npa_cluster_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );

            let npa_neighbor_start = Instant::now();
            self.apply_npa_to_neighbors(
                leaf_id,
                left_id,
                right_id,
                &old_centroid,
                &left_center,
                &right_center,
                &mut evaluated,
                depth,
            );
            self.stats.split_npa_neighbor_nanos.fetch_add(
                npa_neighbor_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
        }

        self.stats.splits.fetch_add(1, Ordering::Relaxed);
        self.stats
            .split_depth_sum
            .fetch_add(depth as u64, Ordering::Relaxed);
        self.stats
            .split_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Nearest neighbor posting assignment (NPA) for split points:
    /// reassign vectors that are farther from the new centroid than the old.
    /// When fp_npa=true, uses full precision f32 distances.
    /// When fp_npa=false, uses quantized distance estimation via codes.
    fn apply_npa_to_cluster(
        &self,
        from_cluster_id: NodeId,
        group: &[EmbeddingPoint],
        old_center: &[f32],
        new_center: &[f32],
        old_code_map: &HashMap<u32, Vec<u8>>,
        evaluated: &mut HashSet<u32>,
        depth: u32,
    ) {
        if self.config.fp_npa {
            self.apply_npa_to_cluster_f32(from_cluster_id, group, old_center, new_center, evaluated, depth);
        } else {
            self.apply_npa_to_cluster_quantized(
                from_cluster_id,
                group,
                old_center,
                new_center,
                old_code_map,
                evaluated,
                depth,
            );
        }
    }

    fn apply_npa_to_cluster_quantized(
        &self,
        from_cluster_id: NodeId,
        group: &[EmbeddingPoint],
        old_center: &[f32],
        new_center: &[f32],
        old_code_map: &HashMap<u32, Vec<u8>>,
        evaluated: &mut HashSet<u32>,
        depth: u32,
    ) {
        let padded_bytes = self.padded_bytes();
        let c_norm = Self::vec_norm(old_center);

        let old_r_q = vec![0.0f32; old_center.len()];
        let old_c_dot_q = c_norm * c_norm;
        let old_q_norm = c_norm;
        let old_qq = QuantizedQuery::new(&old_r_q, padded_bytes, c_norm, old_c_dot_q, old_q_norm);

        let new_r_q: Vec<f32> = new_center
            .iter()
            .zip(old_center.iter())
            .map(|(a, b)| a - b)
            .collect();
        let new_c_dot_q = f32::dot(old_center, new_center).unwrap_or(0.0) as f32;
        let new_q_norm = Self::vec_norm(new_center);
        let new_qq = QuantizedQuery::new(&new_r_q, padded_bytes, c_norm, new_c_dot_q, new_q_norm);

        let mut n_evaluated = 0u64;
        let mut n_reassigned = 0u64;
        for (id, version, _) in group {
            let current_ver = self.versions.get(id).map(|r| *r).unwrap_or(0);
            if *version < current_ver {
                continue;
            }
            if !evaluated.insert(*id) {
                continue;
            }
            let Some(code_bytes) = old_code_map.get(id) else {
                continue;
            };
            n_evaluated += 1;
            let code = Code::<1, _>::new(code_bytes.as_slice());

            let old_dist = code.distance_quantized_query(&self.distance_fn, &old_qq);
            let new_dist = code.distance_quantized_query(&self.distance_fn, &new_qq);
            if new_dist > old_dist {
                n_reassigned += 1;
                self.reassign(from_cluster_id, *id, depth);
            }
        }
        self.stats
            .split_npa_self_total
            .fetch_add(group.len() as u64, Ordering::Relaxed);
        self.stats
            .split_npa_self_evaluated
            .fetch_add(n_evaluated, Ordering::Relaxed);
        self.stats
            .split_npa_self_reassigns
            .fetch_add(n_reassigned, Ordering::Relaxed);
    }

    fn apply_npa_to_cluster_f32(
        &self,
        from_cluster_id: NodeId,
        group: &[EmbeddingPoint],
        old_center: &[f32],
        new_center: &[f32],
        evaluated: &mut HashSet<u32>,
        depth: u32,
    ) {
        let mut n_evaluated = 0u64;
        let mut n_reassigned = 0u64;
        for (id, version, _) in group {
            let current_ver = self.versions.get(id).map(|r| *r).unwrap_or(0);
            if *version < current_ver {
                continue;
            }
            if !evaluated.insert(*id) {
                continue;
            }
            let Some(emb) = self.embeddings.get(id) else {
                continue;
            };
            n_evaluated += 1;
            let old_dist = self.dist(emb.value(), old_center);
            let new_dist = self.dist(emb.value(), new_center);
            if new_dist > old_dist {
                n_reassigned += 1;
                self.reassign(from_cluster_id, *id, depth);
            }
        }
        self.stats
            .split_npa_self_total
            .fetch_add(group.len() as u64, Ordering::Relaxed);
        self.stats
            .split_npa_self_evaluated
            .fetch_add(n_evaluated, Ordering::Relaxed);
        self.stats
            .split_npa_self_reassigns
            .fetch_add(n_reassigned, Ordering::Relaxed);
    }

    /// NPA for neighbor points: check vectors in nearby clusters that might now
    /// be closer to the new left/right centroids than to their current cluster.
    /// Follows the LIRE protocol from SPFresh (Section 3.2).
    fn apply_npa_to_neighbors(
        &self,
        old_leaf_id: NodeId,
        left_id: NodeId,
        right_id: NodeId,
        old_center: &[f32],
        left_center: &[f32],
        right_center: &[f32],
        evaluated: &mut HashSet<u32>,
        depth: u32,
    ) {
        let neighbors = self.navigate(
            old_center,
            Some(self.config.write_beam_tau),
            self.config.write_beam_min,
            self.config.write_beam_max,
            1,
        );

        let mut neighbors_visited = 0u64;
        let mut neighbors_active = 0u64;
        let mut total_evaluated = 0u64;
        let mut total_reassigned = 0u64;

        for &(neighbor_id, _) in neighbors.iter().take(self.config.reassign_neighbor_count) {
            if neighbor_id == old_leaf_id || neighbor_id == left_id || neighbor_id == right_id {
                continue;
            }

            self.scrub(neighbor_id);

            let (n_centroid, n_ids, n_versions, n_codes) = {
                let Some(node_ref) = self.nodes.get(&neighbor_id) else {
                    continue;
                };
                let TreeNode::Leaf(leaf) = node_ref.value() else {
                    continue;
                };
                (
                    leaf.centroid.clone(),
                    leaf.ids.clone(),
                    leaf.versions.clone(),
                    leaf.codes.clone(),
                )
            };

            neighbors_visited += 1;
            let n_total = n_ids.len();
            let mut n_reassigned = 0usize;
            let mut n_evaluated = 0usize;

            if !self.config.fp_npa {
                let padded_bytes = self.padded_bytes();
                let old_q_norm = Self::vec_norm(old_center);
                let left_q_norm = Self::vec_norm(left_center);
                let right_q_norm = Self::vec_norm(right_center);
                let c_norm = Self::vec_norm(&n_centroid);

                let old_r_q: Vec<f32> = old_center
                    .iter()
                    .zip(n_centroid.iter())
                    .map(|(a, b)| a - b)
                    .collect();
                let old_c_dot_q = f32::dot(&n_centroid, old_center).unwrap_or(0.0) as f32;

                let left_r_q: Vec<f32> = left_center
                    .iter()
                    .zip(n_centroid.iter())
                    .map(|(a, b)| a - b)
                    .collect();
                let left_c_dot_q = f32::dot(&n_centroid, left_center).unwrap_or(0.0) as f32;

                let right_r_q: Vec<f32> = right_center
                    .iter()
                    .zip(n_centroid.iter())
                    .map(|(a, b)| a - b)
                    .collect();
                let right_c_dot_q = f32::dot(&n_centroid, right_center).unwrap_or(0.0) as f32;

                let neighbor_r_q = vec![0.0f32; n_centroid.len()];
                let neighbor_c_dot_q = c_norm * c_norm;
                let neighbor_q_norm = c_norm;

                let left_qq =
                    QuantizedQuery::new(&left_r_q, padded_bytes, c_norm, left_c_dot_q, left_q_norm);
                let right_qq = QuantizedQuery::new(
                    &right_r_q,
                    padded_bytes,
                    c_norm,
                    right_c_dot_q,
                    right_q_norm,
                );
                let neighbor_qq = QuantizedQuery::new(
                    &neighbor_r_q,
                    padded_bytes,
                    c_norm,
                    neighbor_c_dot_q,
                    neighbor_q_norm,
                );
                let old_qq =
                    QuantizedQuery::new(&old_r_q, padded_bytes, c_norm, old_c_dot_q, old_q_norm);

                for (i, code_bytes) in n_codes.iter().enumerate() {
                    let id = n_ids[i];
                    let version = n_versions[i];

                    let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                    if version < current_ver {
                        continue;
                    }
                    if !evaluated.insert(id) {
                        continue;
                    }

                    n_evaluated += 1;
                    let code = Code::<1, _>::new(code_bytes.as_slice());

                    let left_dist = code.distance_quantized_query(&self.distance_fn, &left_qq);
                    let right_dist = code.distance_quantized_query(&self.distance_fn, &right_qq);
                    let neighbor_dist =
                        code.distance_quantized_query(&self.distance_fn, &neighbor_qq);

                    if neighbor_dist <= left_dist && neighbor_dist <= right_dist {
                        continue;
                    }

                    let old_dist = code.distance_quantized_query(&self.distance_fn, &old_qq);
                    if old_dist <= left_dist && old_dist <= right_dist {
                        continue;
                    }

                    n_reassigned += 1;
                    self.reassign(neighbor_id, id, depth);
                }
            } else {
                for i in 0..n_ids.len() {
                    let id = n_ids[i];
                    let version = n_versions[i];

                    let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                    if version < current_ver {
                        continue;
                    }
                    if !evaluated.insert(id) {
                        continue;
                    }

                    let Some(emb) = self.embeddings.get(&id) else {
                        continue;
                    };
                    n_evaluated += 1;
                    let emb = emb.value();

                    let left_dist = self.dist(emb, left_center);
                    let right_dist = self.dist(emb, right_center);
                    let neighbor_dist = self.dist(emb, &n_centroid);

                    if neighbor_dist <= left_dist && neighbor_dist <= right_dist {
                        continue;
                    }

                    let old_dist = self.dist(emb, old_center);
                    if old_dist <= left_dist && old_dist <= right_dist {
                        continue;
                    }

                    n_reassigned += 1;
                    self.reassign(neighbor_id, id, depth);
                }
            }

            total_evaluated += n_evaluated as u64;
            total_reassigned += n_reassigned as u64;
            if n_total > 0 && n_reassigned * 100 > n_total {
                neighbors_active += 1;
            }
        }

        self.stats
            .split_npa_neighbors_visited
            .fetch_add(neighbors_visited, Ordering::Relaxed);
        self.stats
            .split_npa_neighbors_active
            .fetch_add(neighbors_active, Ordering::Relaxed);
        self.stats
            .split_npa_neighbor_evaluated
            .fetch_add(total_evaluated, Ordering::Relaxed);
        self.stats
            .split_npa_neighbor_reassigns
            .fetch_add(total_reassigned, Ordering::Relaxed);
    }

    /// Reassign a vector to its best cluster(s).
    fn reassign(&self, from_cluster_id: NodeId, id: u32, depth: u32) {
        let t0 = Instant::now();

        let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
        if !self.is_valid(id, current_ver) {
            return;
        }

        let Some(embedding) = self.embeddings.get(&id).map(|e| e.value().clone()) else {
            return;
        };

        loop {
            let nav_start = Instant::now();
            let candidates = self.navigate(
                &embedding,
                Some(self.config.write_beam_tau),
                self.config.write_beam_min,
                self.config.write_beam_max,
                1,
            );
            let cluster_ids = self.rng_select(&candidates);
            self.stats
                .reassign_navigate_nanos
                .fetch_add(nav_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if cluster_ids.contains(&from_cluster_id) {
                break;
            }
            if !self.is_valid(id, current_ver) {
                return;
            }

            let version = {
                let mut v = self.versions.entry(id).or_insert(0);
                *v += 1;
                *v
            };

            let reg_start = Instant::now();
            let mut clusters_to_balance = Vec::new();
            for &cluster_id in &cluster_ids {
                if self.register_in_leaf(cluster_id, id, version, &embedding) {
                    clusters_to_balance.push(cluster_id);
                }
            }
            self.stats
                .reassign_register_nanos
                .fetch_add(reg_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if clusters_to_balance.is_empty() {
                self.stats.add_missing_nodes.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let balance_start = Instant::now();
            for cluster_id in clusters_to_balance {
                self.balance(cluster_id, depth + 1);
            }
            self.stats
                .reassign_balance_nanos
                .fetch_add(balance_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            break;
        }

        self.stats.reassigns.fetch_add(1, Ordering::Relaxed);
        self.stats
            .reassign_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Check if a point is valid (version matches current version).
    fn is_valid(&self, id: u32, version: u32) -> bool {
        self.versions
            .get(&id)
            .is_some_and(|global_version| *global_version == version)
    }

    // =========================================================================
    // Split (internal)
    // =========================================================================

    fn split_internal(&self, node_id: NodeId) {
        if !self.balancing.insert(node_id) {
            return;
        }

        let (children, parent_id, _old_centroid) = match self.nodes.remove(&node_id) {
            Some((_, TreeNode::Internal(internal))) => {
                if internal.children.len() <= self.config.branching_factor {
                    self.nodes.insert(node_id, TreeNode::Internal(internal));
                    self.balancing.remove(&node_id);
                    return;
                }
                (internal.children, internal.parent_id, internal.centroid)
            }
            Some((_, node)) => {
                self.nodes.insert(node_id, node);
                self.balancing.remove(&node_id);
                return;
            }
            None => {
                self.balancing.remove(&node_id);
                return;
            }
        };

        let child_embeddings: Vec<EmbeddingPoint> = children
            .iter()
            .map(|&child_id| {
                let centroid = self
                    .nodes
                    .get(&child_id)
                    .map(|n| n.centroid().to_vec())
                    .unwrap_or_else(|| vec![0.0; self.dim]);
                (child_id, 0u32, Arc::from(centroid.as_slice()))
            })
            .collect();

        let (left_center, left_group, right_center, right_group) =
            utils::split(child_embeddings, &self.distance_fn);

        let left_children: Vec<NodeId> = left_group.iter().map(|(id, _, _)| *id).collect();
        let right_children: Vec<NodeId> = right_group.iter().map(|(id, _, _)| *id).collect();

        let left_id = self.alloc_node_id();
        let right_id = self.alloc_node_id();

        let left_centroid = left_center.to_vec();
        let right_centroid = right_center.to_vec();

        self.nodes.insert(
            left_id,
            TreeNode::Internal(InternalNode {
                centroid: left_centroid.clone(),
                centroid_code: Vec::new(),
                children: left_children.clone(),
                parent_id: None,
            }),
        );
        self.nodes.insert(
            right_id,
            TreeNode::Internal(InternalNode {
                centroid: right_centroid.clone(),
                centroid_code: Vec::new(),
                children: right_children.clone(),
                parent_id: None,
            }),
        );

        // Recompute centroid_codes for children relative to their new parent
        for &child_id in &left_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(left_id));
                self.recompute_centroid_code(&mut node_ref, &left_centroid);
            }
        }
        for &child_id in &right_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(right_id));
                self.recompute_centroid_code(&mut node_ref, &right_centroid);
            }
        }

        self.balancing.remove(&node_id);

        if let Some(pid) = parent_id {
            self.replace_child(pid, node_id, &[left_id, right_id]);
        } else {
            self.create_root_above(&[left_id, right_id]);
        }
    }

    // =========================================================================
    // Merge
    // =========================================================================

    fn merge_leaf(&self, leaf_id: NodeId, depth: u32) {
        if depth > MAX_BALANCE_DEPTH {
            return;
        }
        let t0 = Instant::now();

        let (source_centroid, source_ids, source_versions, parent_id) =
            match self.nodes.remove(&leaf_id) {
                Some((_, TreeNode::Leaf(leaf))) => {
                    (leaf.centroid, leaf.ids, leaf.versions, leaf.parent_id)
                }
                Some((_, node)) => {
                    self.nodes.insert(leaf_id, node);
                    return;
                }
                None => return,
            };

        let candidates = self.navigate(
            &source_centroid,
            Some(self.config.write_beam_tau),
            self.config.write_beam_min,
            self.config.write_beam_max,
            1,
        );
        let target_id = match candidates.iter().find(|&&(nid, _)| nid != leaf_id) {
            Some(&(nid, _)) => nid,
            None => {
                // No merge target found, re-insert the leaf
                self.nodes.insert(
                    leaf_id,
                    TreeNode::Leaf(LeafNode {
                        centroid: source_centroid,
                        centroid_code: Vec::new(),
                        ids: source_ids,
                        versions: source_versions,
                        codes: Vec::new(),
                        parent_id,
                    }),
                );
                return;
            }
        };

        let target_centroid = match self.nodes.get(&target_id) {
            Some(n) => n.centroid().to_vec(),
            None => {
                // Target gone, re-insert the leaf
                self.nodes.insert(
                    leaf_id,
                    TreeNode::Leaf(LeafNode {
                        centroid: source_centroid,
                        centroid_code: Vec::new(),
                        ids: source_ids,
                        versions: source_versions,
                        codes: Vec::new(),
                        parent_id,
                    }),
                );
                return;
            }
        };
        if let Some(pid) = parent_id {
            self.remove_child_locked(pid, leaf_id);
        }

        self.stats.merges.fetch_add(1, Ordering::Relaxed);
        self.stats
            .merge_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        for (&id, &version) in source_ids.iter().zip(source_versions.iter()) {
            let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
            if version < current_ver {
                continue;
            }
            let Some(embedding) = self.embeddings.get(&id).map(|e| e.value().clone()) else {
                continue;
            };

            let dist_to_target = self.dist(&embedding, &target_centroid);
            let dist_to_source = self.dist(&embedding, &source_centroid);

            if dist_to_target <= dist_to_source {
                if !self.register_in_leaf(target_id, id, version, &embedding) {
                    self.stats
                        .register_missing_nodes
                        .fetch_add(1, Ordering::Relaxed);
                    self.reassign(leaf_id, id, depth);
                }
            } else {
                self.reassign(leaf_id, id, depth);
            }
        }

        self.balance(target_id, depth + 1);
    }

    // =========================================================================
    // Tree structure helpers
    // =========================================================================

    fn replace_child(&self, parent_id: NodeId, old_child: NodeId, new_children: &[NodeId]) {
        let children_clone = {
            let Some(mut node_ref) = self.nodes.get_mut(&parent_id) else {
                // parent is gone, new children are orphaned
                // TODO insert them into the tree where appropriate
                println!("ERROR: parent is gone, new children are orphaned");
                return;
            };
            let TreeNode::Internal(parent) = node_ref.value_mut() else {
                return;
            };
            parent.children.retain(|&c| c != old_child);
            parent.children.extend_from_slice(new_children);
            parent.children.clone()
        };

        let new_centroid = self.compute_centroid_of(&children_clone);
        if let Some(mut node_ref) = self.nodes.get_mut(&parent_id) {
            if let TreeNode::Internal(parent) = node_ref.value_mut() {
                parent.centroid = new_centroid;
            }
        }

        // Get the parent centroid for recomputing children's centroid_codes
        let parent_centroid = self
            .nodes
            .get(&parent_id)
            .map(|n| n.centroid().to_vec())
            .unwrap_or_else(|| vec![0.0; self.dim]);

        for &child_id in new_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(parent_id));
                self.recompute_centroid_code(&mut node_ref, &parent_centroid);
            }
        }

        if children_clone.len() > self.config.branching_factor {
            self.split_internal(parent_id);
        }
    }

    fn remove_child_locked(&self, parent_id: NodeId, child_id: NodeId) {
        let (children_clone, grandparent_id) = {
            let Some(mut node_ref) = self.nodes.get_mut(&parent_id) else {
                return;
            };
            let TreeNode::Internal(parent) = node_ref.value_mut() else {
                return;
            };
            parent.children.retain(|&c| c != child_id);
            (parent.children.clone(), parent.parent_id)
        };

        if children_clone.is_empty() {
            self.nodes.remove(&parent_id);
            if parent_id == self.root_id() {
                let new_root = self.alloc_node_id();
                self.nodes.insert(
                    new_root,
                    TreeNode::Leaf(LeafNode {
                        centroid: vec![0.0; self.dim],
                        centroid_code: Vec::new(),
                        ids: Vec::new(),
                        versions: Vec::new(),
                        codes: Vec::new(),
                        parent_id: None,
                    }),
                );
                self.root_id.store(new_root, Ordering::Relaxed);
            } else if let Some(gp_id) = grandparent_id {
                self.remove_child_locked(gp_id, parent_id);
            }
        } else if children_clone.len() == 1 && parent_id == self.root_id() {
            let only_child = children_clone[0];
            self.nodes.remove(&parent_id);
            if let Some(mut node_ref) = self.nodes.get_mut(&only_child) {
                node_ref.set_parent_id(None);
            }
            self.root_id.store(only_child, Ordering::Relaxed);
        } else {
            let new_centroid = self.compute_centroid_of(&children_clone);
            if let Some(mut node_ref) = self.nodes.get_mut(&parent_id) {
                if let TreeNode::Internal(parent) = node_ref.value_mut() {
                    parent.centroid = new_centroid;
                }
            }
        }
    }

    fn create_root_above(&self, children: &[NodeId]) {
        let root_id = self.alloc_node_id();
        let centroid = self.compute_centroid_of(children);

        self.nodes.insert(
            root_id,
            TreeNode::Internal(InternalNode {
                centroid: centroid.clone(),
                centroid_code: Vec::new(),
                children: children.to_vec(),
                parent_id: None,
            }),
        );

        for &child_id in children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(root_id));
                self.recompute_centroid_code(&mut node_ref, &centroid);
            }
        }

        self.root_id.store(root_id, Ordering::Relaxed);
    }

    fn compute_centroid_of(&self, children: &[NodeId]) -> Vec<f32> {
        let mut mean = vec![0.0f32; self.dim];
        let mut count = 0;
        for &child_id in children {
            if let Some(node_ref) = self.nodes.get(&child_id) {
                for (i, &v) in node_ref.centroid().iter().enumerate() {
                    mean[i] += v;
                }
                count += 1;
            }
        }
        if count > 0 {
            let scale = 1.0 / count as f32;
            for v in &mut mean {
                *v *= scale;
            }
        }
        mean
    }

    /// Recompute a node's centroid_code as a 1-bit quantization of its centroid
    /// relative to the given parent centroid.
    fn recompute_centroid_code(
        &self,
        node_ref: &mut dashmap::mapref::one::RefMut<'_, NodeId, TreeNode>,
        parent_centroid: &[f32],
    ) {
        let centroid = node_ref.centroid().to_vec();
        let code = Code::<1>::quantize(&centroid, parent_centroid);
        let code_bytes = code.as_ref().to_vec();
        match node_ref.value_mut() {
            TreeNode::Leaf(leaf) => leaf.centroid_code = code_bytes,
            TreeNode::Internal(internal) => internal.centroid_code = code_bytes,
        }
    }

    /// Compute the padded byte length for 1-bit codes at this dimension.
    fn padded_bytes(&self) -> usize {
        Code::<1, Vec<u8>>::packed_len(self.dim)
    }

    /// Compute ||v||.
    fn vec_norm(v: &[f32]) -> f32 {
        (f32::dot(v, v).unwrap_or(0.0) as f32).sqrt()
    }

    // =========================================================================
    // Search (no global lock - uses per-node DashMap gets)
    // =========================================================================

    /// Returns (results, vectors_scanned, leaves_scanned).
    /// Returns (top-k results, vectors_scanned, leaves_scanned).
    /// Scores data vectors with 1-bit quantized distances, then optionally
    /// reranks top candidates with f32 embeddings.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        tau: f64,
        beam_min: usize,
        beam_max: usize,
        rerank_centroids: usize,
        rerank_vectors: usize,
    ) -> (Vec<(u32, f32)>, usize, usize, SearchTimings) {
        let nav_t0 = Instant::now();
        let leaves = self.navigate(query, Some(tau), beam_min, beam_max, rerank_centroids);
        let navigate_nanos = nav_t0.elapsed().as_nanos() as u64;

        let leaves_scanned = leaves.len();
        let padded_bytes = self.padded_bytes();
        let q_norm = Self::vec_norm(query);
        let rerank_factor = rerank_vectors;

        let mut results: Vec<(u32, f32)> = Vec::new();
        let mut quantize_nanos = 0u64;
        let mut distance_nanos = 0u64;

        for &(leaf_id, _) in &leaves {
            let Some(node_ref) = self.nodes.get(&leaf_id) else {
                continue;
            };
            let TreeNode::Leaf(leaf) = node_ref.value() else {
                continue;
            };

            let qt0 = Instant::now();
            let r_q: Vec<f32> = query
                .iter()
                .zip(leaf.centroid.iter())
                .map(|(q, c)| q - c)
                .collect();
            let c_norm = Self::vec_norm(&leaf.centroid);
            let c_dot_q = f32::dot(&leaf.centroid, query).unwrap_or(0.0) as f32;
            let qq = QuantizedQuery::new(&r_q, padded_bytes, c_norm, c_dot_q, q_norm);
            quantize_nanos += qt0.elapsed().as_nanos() as u64;

            let dt0 = Instant::now();
            for (i, &id) in leaf.ids.iter().enumerate() {
                let version = leaf.versions[i];
                let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                if version < current_ver {
                    continue;
                }
                let dist = Code::<1, _>::new(leaf.codes[i].as_slice())
                    .distance_quantized_query(&self.distance_fn, &qq);
                results.push((id, dist));
            }
            distance_nanos += dt0.elapsed().as_nanos() as u64;
        }

        let sort_t0 = Instant::now();

        // Deduplicate (same vector in multiple leaves)
        let mut best: std::collections::HashMap<u32, f32> =
            std::collections::HashMap::with_capacity(results.len());
        for (id, dist) in results {
            let entry = best.entry(id).or_insert(f32::MAX);
            if dist < *entry {
                *entry = dist;
            }
        }

        let scanned = best.len();
        let mut deduped: Vec<(u32, f32)> = best.into_iter().collect();
        deduped.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let sort_dedup_nanos = sort_t0.elapsed().as_nanos() as u64;

        if rerank_factor > 1 {
            let rerank_count = (k * rerank_factor).min(deduped.len());
            deduped.truncate(rerank_count);

            let rr_t0 = Instant::now();
            let mut reranked: Vec<(u32, f32)> = deduped
                .into_iter()
                .map(|(id, approx_dist)| {
                    if let Some(emb) = self.embeddings.get(&id) {
                        let dist = self.dist(query, emb.value());
                        (id, dist)
                    } else {
                        (id, approx_dist)
                    }
                })
                .collect();
            reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let rerank_nanos = rr_t0.elapsed().as_nanos() as u64;
            reranked.truncate(k);
            (
                reranked,
                scanned,
                leaves_scanned,
                SearchTimings {
                    navigate_nanos,
                    quantize_nanos,
                    distance_nanos,
                    sort_dedup_nanos,
                    rerank_nanos,
                },
            )
        } else {
            deduped.truncate(k);
            (
                deduped,
                scanned,
                leaves_scanned,
                SearchTimings {
                    navigate_nanos,
                    quantize_nanos,
                    distance_nanos,
                    sort_dedup_nanos,
                    rerank_nanos: 0,
                },
            )
        }
    }

    // =========================================================================
    // Per-level recall diagnostics (no global lock)
    // =========================================================================

    pub fn diagnose_level_recall(
        &self,
        query: &[f32],
        gt_100: &HashSet<u32>,
        tau: f64,
        beam_min: usize,
        beam_max: usize,
        rerank_centroids: usize,
    ) -> Vec<LevelRecall> {
        let root = self.root_id();

        if let Some(root_ref) = self.nodes.get(&root) {
            if matches!(root_ref.value(), TreeNode::Leaf(_)) {
                drop(root_ref);
                let mut reachable = HashSet::new();
                self.collect_all_data_ids(root, &mut reachable);
                let r100 =
                    gt_100.intersection(&reachable).count() as f64 / gt_100.len().max(1) as f64;
                return vec![LevelRecall {
                    level: 1,
                    reachable_100: r100,
                    beam_size: 1,
                    total_candidates: 1,
                }];
            }
        }

        let nav_mode = self.config.navigation;
        let q_norm = Self::vec_norm(query);
        let padded_bytes = self.padded_bytes();
        let rerank_factor = rerank_centroids;
        let dim = self.dim;

        let mut levels = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let parent_centroid = internal.centroid.clone();
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        match nav_mode {
                            NavigationMode::Fp => {
                                for child_id in children {
                                    if let Some(child) = self.nodes.get(&child_id) {
                                        let dist = self.dist(query, child.centroid());
                                        child_scores.push((child_id, dist));
                                    }
                                }
                            }
                            NavigationMode::OneBit => {
                                let c_norm = Self::vec_norm(&parent_centroid);
                                let query_code = Code::<1>::quantize(query, &parent_centroid);

                                for child_id in children {
                                    if let Some(child) = self.nodes.get(&child_id) {
                                        let code_bytes = child.centroid_code();
                                        let dist = if code_bytes.is_empty() {
                                            self.dist(query, child.centroid())
                                        } else {
                                            let child_code = Code::<1, _>::new(code_bytes);
                                            query_code.distance_code(
                                                &child_code,
                                                &self.distance_fn,
                                                c_norm,
                                                dim,
                                            )
                                        };
                                        child_scores.push((child_id, dist));
                                    }
                                }
                            }
                            NavigationMode::FourBit => {
                                let c_norm = Self::vec_norm(&parent_centroid);
                                let r_q: Vec<f32> = query
                                    .iter()
                                    .zip(parent_centroid.iter())
                                    .map(|(q, c)| q - c)
                                    .collect();
                                let c_dot_q =
                                    f32::dot(&parent_centroid, query).unwrap_or(0.0) as f32;
                                let qq = QuantizedQuery::new(
                                    &r_q,
                                    padded_bytes,
                                    c_norm,
                                    c_dot_q,
                                    q_norm,
                                );

                                for child_id in children {
                                    if let Some(child) = self.nodes.get(&child_id) {
                                        let code_bytes = child.centroid_code();
                                        let dist = if code_bytes.is_empty() {
                                            self.dist(query, child.centroid())
                                        } else {
                                            Code::<1, _>::new(code_bytes)
                                                .distance_quantized_query(&self.distance_fn, &qq)
                                        };
                                        child_scores.push((child_id, dist));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if child_scores.is_empty() {
                break;
            }

            let total_candidates = child_scores.len();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            if nav_mode != NavigationMode::Fp && rerank_factor > 1 {
                let effective = Self::effective_beam(&child_scores, Some(tau), beam_min, beam_max);
                let rerank_count = (effective * rerank_factor).min(child_scores.len());
                child_scores.truncate(rerank_count);
                let mut reranked: Vec<(NodeId, f32)> = child_scores
                    .iter()
                    .map(|&(nid, _)| {
                        let dist = self
                            .nodes
                            .get(&nid)
                            .map(|n| self.dist(query, n.centroid()))
                            .unwrap_or(f32::MAX);
                        (nid, dist)
                    })
                    .collect();
                reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let final_beam = Self::effective_beam(&reranked, Some(tau), beam_min, beam_max);
                reranked.truncate(final_beam);
                child_scores = reranked;
            } else {
                let effective = Self::effective_beam(&child_scores, Some(tau), beam_min, beam_max);
                child_scores.truncate(effective);
            }

            let mut reachable: HashSet<u32> = HashSet::new();
            for &(node_id, _) in &child_scores {
                self.collect_all_data_ids(node_id, &mut reachable);
            }

            let r100 = gt_100.intersection(&reachable).count() as f64 / gt_100.len().max(1) as f64;

            levels.push(LevelRecall {
                level: levels.len() + 1,
                reachable_100: r100,
                beam_size: child_scores.len(),
                total_candidates,
            });

            let mut next_internals: Vec<NodeId> = Vec::new();
            for &(node_id, _) in &child_scores {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if matches!(node_ref.value(), TreeNode::Internal(_)) {
                        next_internals.push(node_id);
                    }
                }
            }

            if next_internals.is_empty() {
                break;
            }
            beam = next_internals;
        }

        levels
    }

    fn collect_all_data_ids(&self, node_id: NodeId, ids: &mut HashSet<u32>) {
        let Some(node_ref) = self.nodes.get(&node_id) else {
            return;
        };
        match node_ref.value() {
            TreeNode::Leaf(leaf) => {
                for (i, &id) in leaf.ids.iter().enumerate() {
                    let version = leaf.versions[i];
                    let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                    if version >= current_ver {
                        ids.insert(id);
                    }
                }
            }
            TreeNode::Internal(internal) => {
                let children: Vec<NodeId> = internal.children.clone();
                drop(node_ref);
                for child_id in children {
                    self.collect_all_data_ids(child_id, ids);
                }
            }
        }
    }

    // =========================================================================
    // Info / diagnostics (no global lock - uses per-node DashMap gets)
    // =========================================================================

    pub fn depth(&self) -> usize {
        self.depth_of(self.root_id())
    }

    /// Returns the total number of nodes at each level (0-indexed).
    pub fn level_node_counts(&self) -> Vec<usize> {
        let root = self.root_id();
        let depth = self.depth_of(root);
        let mut counts = vec![0usize; depth];
        let mut queue: Vec<(NodeId, usize)> = vec![(root, 0)];
        while let Some((node_id, level)) = queue.pop() {
            if level >= depth {
                continue;
            }
            counts[level] += 1;
            if let Some(node_ref) = self.nodes.get(&node_id) {
                if let TreeNode::Internal(internal) = node_ref.value() {
                    let children: Vec<NodeId> = internal.children.clone();
                    drop(node_ref);
                    for child_id in children {
                        queue.push((child_id, level + 1));
                    }
                }
            }
        }
        counts
    }

    fn depth_of(&self, node_id: NodeId) -> usize {
        let Some(node_ref) = self.nodes.get(&node_id) else {
            return 0;
        };
        match node_ref.value() {
            TreeNode::Leaf(_) => 1,
            TreeNode::Internal(internal) => {
                let children: Vec<NodeId> = internal.children.clone();
                drop(node_ref);
                1 + children
                    .iter()
                    .map(|&c| self.depth_of(c))
                    .max()
                    .unwrap_or(0)
            }
        }
    }

    pub fn leaf_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|entry| matches!(entry.value(), TreeNode::Leaf(_)))
            .count()
    }

    pub fn internal_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|entry| matches!(entry.value(), TreeNode::Internal(_)))
            .count()
    }

    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    pub fn leaf_sizes(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .filter_map(|entry| match entry.value() {
                TreeNode::Leaf(l) => Some(l.ids.len()),
                _ => None,
            })
            .collect()
    }

    pub fn total_vectors(&self) -> usize {
        self.embeddings.len()
    }

    pub fn total_leaf_entries(&self) -> usize {
        self.nodes
            .iter()
            .filter_map(|entry| match entry.value() {
                TreeNode::Leaf(l) => Some(l.ids.len()),
                _ => None,
            })
            .sum()
    }

    /// Count vectors in `embeddings` that have no valid (non-stale) entry in any leaf.
    pub fn count_orphaned_vectors(&self) -> usize {
        let mut valid_ids: HashSet<u32> = HashSet::new();
        for entry in self.nodes.iter() {
            if let TreeNode::Leaf(leaf) = entry.value() {
                for (i, &id) in leaf.ids.iter().enumerate() {
                    let version = leaf.versions[i];
                    let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                    if version >= current_ver {
                        valid_ids.insert(id);
                    }
                }
            }
        }
        self.embeddings.len().saturating_sub(valid_ids.len())
    }

    pub fn print_tree_stats(&self, format_count_fn: fn(usize) -> String) {
        let root = self.root_id();
        let depth = self.depth_of(root);

        struct LevelStats {
            internal_count: usize,
            child_counts: Vec<usize>,
            leaf_count: usize,
            leaf_sizes: Vec<usize>,
            child_to_parent_dists: Vec<f32>,
        }

        let mut levels: Vec<LevelStats> = (0..depth)
            .map(|_| LevelStats {
                internal_count: 0,
                child_counts: Vec::new(),
                leaf_count: 0,
                leaf_sizes: Vec::new(),
                child_to_parent_dists: Vec::new(),
            })
            .collect();

        let mut queue: Vec<(NodeId, usize)> = vec![(root, 0)];
        let mut total_leaf_entries = 0usize;

        while let Some((node_id, level)) = queue.pop() {
            if level >= depth {
                continue;
            }
            if let Some(node_ref) = self.nodes.get(&node_id) {
                match node_ref.value() {
                    TreeNode::Internal(internal) => {
                        levels[level].internal_count += 1;
                        levels[level].child_counts.push(internal.children.len());
                        let parent_centroid = internal.centroid.clone();
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);
                        for child_id in children {
                            if let Some(child_ref) = self.nodes.get(&child_id) {
                                let d = self.dist(child_ref.centroid(), &parent_centroid);
                                let child_level = level + 1;
                                if child_level < depth {
                                    levels[child_level].child_to_parent_dists.push(d);
                                }
                            }
                            queue.push((child_id, level + 1));
                        }
                    }
                    TreeNode::Leaf(leaf) => {
                        levels[level].leaf_count += 1;
                        levels[level].leaf_sizes.push(leaf.ids.len());
                        total_leaf_entries += leaf.ids.len();
                    }
                }
            }
        }

        println!("\n--- Tree Structure ---");

        for (i, ls) in levels.iter().enumerate() {
            let is_last = i == depth - 1;
            let prefix = if i == 0 { "  *  " } else { "  |  " };

            if ls.internal_count > 0 {
                let counts = &ls.child_counts;
                let min_c = counts.iter().copied().min().unwrap_or(0);
                let max_c = counts.iter().copied().max().unwrap_or(0);
                let avg_c = counts.iter().sum::<usize>() as f64 / counts.len().max(1) as f64;
                let total_children: usize = counts.iter().sum();
                println!(
                    "{}Level {} : {} internal node{}, {} total children (fan-out: min={}, avg={:.0}, max={})",
                    prefix, i,
                    format_count_fn(ls.internal_count),
                    if ls.internal_count == 1 { "" } else { "s" },
                    format_count_fn(total_children),
                    min_c, avg_c, max_c,
                );
            }

            if ls.leaf_count > 0 {
                let sizes = &ls.leaf_sizes;
                let min_s = sizes.iter().copied().min().unwrap_or(0);
                let max_s = sizes.iter().copied().max().unwrap_or(0);
                let total_vecs: usize = sizes.iter().sum();
                let p25 = percentile_usize(sizes, 25);
                let p50 = percentile_usize(sizes, 50);
                let p75 = percentile_usize(sizes, 75);
                println!(
                    "{}Level {} : {} lea{}, {} total vectors (size: min={}, p25={}, p50={}, p75={}, max={})",
                    prefix, i,
                    format_count_fn(ls.leaf_count),
                    if ls.leaf_count == 1 { "f" } else { "ves" },
                    format_count_fn(total_vecs),
                    min_s, p25, p50, p75, max_s,
                );
            }

            if !ls.child_to_parent_dists.is_empty() {
                let dists = &ls.child_to_parent_dists;
                let min_d = percentile_f32(dists, 0);
                let p25_d = percentile_f32(dists, 25);
                let p50_d = percentile_f32(dists, 50);
                let p75_d = percentile_f32(dists, 75);
                let max_d = percentile_f32(dists, 100);
                println!(
                    "  |            dist to parent: min={:.1}, p25={:.1}, p50={:.1}, p75={:.1}, max={:.1}",
                    min_d, p25_d, p50_d, p75_d, max_d,
                );
            }

            if !is_last {
                println!("  |");
            }
        }

        let total_vectors = self.total_vectors();
        let _replication = if total_vectors > 0 {
            total_leaf_entries as f64 / total_vectors as f64
        } else {
            0.0
        };
        let orphaned = self.count_orphaned_vectors();
        let valid_entries: usize = self
            .nodes
            .iter()
            .filter_map(|entry| match entry.value() {
                TreeNode::Leaf(leaf) => Some(
                    leaf.ids
                        .iter()
                        .enumerate()
                        .filter(|&(i, &id)| {
                            let ver = leaf.versions[i];
                            let cur = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                            ver >= cur
                        })
                        .count(),
                ),
                _ => None,
            })
            .sum();
        let valid_replication = if total_vectors > 0 && orphaned < total_vectors {
            valid_entries as f64 / (total_vectors - orphaned) as f64
        } else {
            0.0
        };
        println!(
            "\nTotal entries: {} ({} valid) | Unique vectors: {} ({} orphaned) | Avg replication: {:.2}x",
            format_count_fn(total_leaf_entries),
            format_count_fn(valid_entries),
            format_count_fn(total_vectors),
            format_count_fn(orphaned),
            valid_replication,
        );
    }
}

fn percentile_usize(data: &[usize], pct: usize) -> usize {
    if data.is_empty() {
        return 0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable();
    let idx = (pct as f64 / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn percentile_f32(data: &[f32], pct: usize) -> f32 {
    if data.is_empty() {
        return 0.0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = (pct as f64 / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
