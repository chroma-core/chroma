#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chroma_distance::DistanceFunction;
use chroma_index::spann::utils::{self, EmbeddingPoint};
use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;

pub type NodeId = u32;

const MAX_BALANCE_DEPTH: u32 = 4;

#[derive(Clone)]
pub struct HierarchicalSpannConfig {
    pub branching_factor: usize,
    pub split_threshold: usize,
    pub merge_threshold: usize,
    /// Fixed beam width for the write path (add/reassign/merge navigate).
    pub write_nprobe: usize,
    /// Dynamic beam tau for the search/query path.
    /// Include children with dist <= d_best * (1 + beam_tau), clamped to [beam_min, beam_max].
    pub beam_tau: f64,
    pub beam_min: usize,
    pub beam_max: usize,
    pub nreplica_count: usize,
    pub write_rng_epsilon: f32,
    pub write_rng_factor: f32,
    pub reassign_neighbor_count: usize,
}

impl Default for HierarchicalSpannConfig {
    fn default() -> Self {
        Self {
            branching_factor: 100,
            split_threshold: 2048,
            merge_threshold: 512,
            write_nprobe: 64,
            beam_tau: 1.0,
            beam_min: 10,
            beam_max: 50000,
            nreplica_count: 2,
            write_rng_epsilon: 8.0,
            write_rng_factor: 4.0,
            reassign_neighbor_count: 32,
        }
    }
}

// =============================================================================
// Node types
// =============================================================================
struct LeafNode {
    centroid: Vec<f32>,
    ids: Vec<u32>,
    versions: Vec<u32>,
    parent_id: Option<NodeId>,
}

struct InternalNode {
    centroid: Vec<f32>,
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
    add_register_fallbacks: AtomicU64,
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
            add_register_fallbacks: AtomicU64::new(0),
        }
    }
}

pub const TASK_METHODS: &[&str] = &[
    "add", "navigate", "split", "merge", "reassign", "scrub",
];

#[derive(Default, Clone)]
pub struct WriterStatsSnapshot {
    pub calls: [u64; 6],
    pub nanos: [u64; 6],
    pub scrub_removed: u64,
    pub wall_nanos: u64,
    pub navigate_missing_nodes: u64,
    pub add_missing_nodes: u64,
}

impl WriterStats {
    pub fn snapshot(&self) -> WriterStatsSnapshot {
        WriterStatsSnapshot {
            calls: [
                self.adds.load(Ordering::Relaxed),
                self.navigates.load(Ordering::Relaxed),
                self.splits.load(Ordering::Relaxed),
                self.merges.load(Ordering::Relaxed),
                self.reassigns.load(Ordering::Relaxed),
                self.scrubs.load(Ordering::Relaxed),
            ],
            nanos: [
                self.add_nanos.load(Ordering::Relaxed),
                self.navigate_nanos.load(Ordering::Relaxed),
                self.split_nanos.load(Ordering::Relaxed),
                self.merge_nanos.load(Ordering::Relaxed),
                self.reassign_nanos.load(Ordering::Relaxed),
                self.scrub_nanos.load(Ordering::Relaxed),
            ],
            scrub_removed: self.scrub_removed.load(Ordering::Relaxed),
            wall_nanos: 0,
            navigate_missing_nodes: self.navigate_missing_nodes.load(Ordering::Relaxed),
            add_missing_nodes: self.add_register_fallbacks.load(Ordering::Relaxed),
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
            add_missing_nodes: cur
                .add_missing_nodes
                .saturating_sub(prev.add_missing_nodes),
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
    writeln!(out, "| CP | navigate.missing_node | add.missing_nodes |").unwrap();
    writeln!(out, "|----|-----------------|------------------|").unwrap();
    for (i, snap) in snapshots.iter().enumerate() {
        writeln!(
            out,
            "| {:>2} | {:>15} | {:>16} |",
            i + 1,
            fmt_count(snap.navigate_missing_nodes),
            fmt_count(snap.add_missing_nodes),
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

/// Full-precision hierarchical SPANN index (thread-safe).
///
/// Stores data vectors in leaf nodes (posting lists). Internal nodes route
/// queries via beam search using f32 centroid distances. The tree grows
/// bottom-up: vectors are always added to leaf nodes, and splits propagate
/// upward when a parent exceeds the branching factor.
///
/// Thread safety:
/// - `nodes` in `DashMap`: navigate and register use per-node shard locks (no global lock)
/// - `structure_lock`: only acquired for structural modifications (split/merge)
/// - `balancing`: DashSet guard to prevent duplicate balance work on the same cluster
/// - `embeddings`/`versions` in `DashMap` for concurrent access
/// - `root_id`/`next_node_id` are atomic
/// - Stats use `AtomicU64`
pub struct HierarchicalSpannWriter {
    dim: usize,
    distance_fn: DistanceFunction,
    config: HierarchicalSpannConfig,

    nodes: DashMap<NodeId, TreeNode>,
    structure_lock: RwLock<()>,
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
                ids: Vec::new(),
                versions: Vec::new(),
                parent_id: None,
            }),
        );

        Self {
            dim,
            distance_fn,
            config,
            nodes,
            structure_lock: RwLock::new(()),
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
    /// Navigate and register use per-node DashMap access (no global lock).
    /// Only balance/split/merge acquires the structure_lock.
    pub fn add(&self, id: u32, embedding: &[f32]) {
        let add_start = Instant::now();

        let emb: Arc<[f32]> = Arc::from(embedding);
        self.embeddings.insert(id, emb);

        let version = {
            let mut v = self.versions.entry(id).or_insert(0);
            *v += 1;
            *v
        };

        // Phase 1: navigate (per-node DashMap gets, no global lock)
        let cluster_ids = {
            let nav_start = Instant::now();
            let nprobe = self.config.write_nprobe;
            let candidates = self.navigate(embedding, None, nprobe, nprobe);
            let cluster_ids = self.rng_select(&candidates);
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats.navigate_nanos.fetch_add(
                nav_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
            cluster_ids
        };

        // Phase 2: register (per-leaf DashMap get_mut, no global lock)
        let mut clusters_to_balance = Vec::new();
        for &cluster_id in &cluster_ids {
            if self.register_in_leaf(cluster_id, id, version) {
                clusters_to_balance.push(cluster_id);
            }
        }

        if clusters_to_balance.is_empty() {
            self.stats.add_register_fallbacks.fetch_add(1, Ordering::Relaxed);
            self.add(id, embedding);
            return;
        }

        // Phase 3: balance (structure_lock acquired only if split/merge needed)
        for cluster_id in clusters_to_balance {
            self.balance(cluster_id, 0);
        }

        self.stats.adds.fetch_add(1, Ordering::Relaxed);
        self.stats.add_nanos.fetch_add(
            add_start.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );
    }

    /// Register a vector in a leaf. Uses per-leaf DashMap get_mut -- no global lock.
    fn register_in_leaf(
        &self,
        leaf_id: NodeId,
        id: u32,
        version: u32,
    ) -> bool {
        if let Some(mut node_ref) = self.nodes.get_mut(&leaf_id) {
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                leaf.ids.push(id);
                leaf.versions.push(version);
                return true;
            }
        }
        false
    }

    // =========================================================================
    // Navigate
    // =========================================================================

    /// Beam search the tree to find the nearest leaf nodes.
    /// Uses per-node DashMap gets -- no global lock required.
    fn navigate(
        &self,
        query: &[f32],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
    ) -> Vec<(NodeId, f32)> {
        let root = self.root_id();
        let Some(root_node) = self.nodes.get(&root) else {
            return Vec::new();
        };

        if matches!(root_node.value(), TreeNode::Leaf(_)) {
            let dist = self.dist(query, root_node.centroid());
            drop(root_node);
            return vec![(root, dist)];
        }
        drop(root_node);

        let mut leaves: Vec<(NodeId, f32)> = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

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
                                self.stats.navigate_missing_nodes.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }

            if child_scores.is_empty() {
                break;
            }

            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let effective = Self::effective_beam(&child_scores, tau, beam_min, beam_max);
            child_scores.truncate(effective);

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

        leaves.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
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
                let count = sorted
                    .iter()
                    .take_while(|(_, d)| *d <= threshold)
                    .count();
                let floor = beam_min.min(beam_max);
                count.clamp(floor, beam_max).min(sorted.len())
            }
        }
    }

    // =========================================================================
    // RNG select
    // =========================================================================

    /// Select clusters via RNG rule. Uses per-node DashMap gets -- no global lock.
    fn rng_select(
        &self,
        candidates: &[(NodeId, f32)],
    ) -> Vec<NodeId> {
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
            let blocked = selected_centroids
                .iter()
                .any(|sel| self.config.write_rng_factor * self.dist(&centroid, sel) <= distance);
            if blocked {
                continue;
            }

            result.push(node_id);
            selected_centroids.push(centroid);

            if result.len() >= self.config.nreplica_count {
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
    /// Only acquires structure_lock if split/merge is needed.
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

            let _lock = self.structure_lock.write();

            // Re-check size under lock (may have changed)
            let len = match self.nodes.get(&cluster_id) {
                Some(node_ref) => match node_ref.value() {
                    TreeNode::Leaf(leaf) => leaf.ids.len(),
                    _ => {
                        self.balancing.remove(&cluster_id);
                        return;
                    }
                },
                None => {
                    self.balancing.remove(&cluster_id);
                    return;
                }
            };

            if len > self.config.split_threshold {
                self.split_leaf_locked(cluster_id, depth);
            } else if len > 0 && len < self.config.merge_threshold {
                self.merge_leaf_locked(cluster_id, depth);
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
                removed += 1;
            } else {
                i += 1;
            }
        }

        drop(node_ref);

        self.stats.scrubs.fetch_add(1, Ordering::Relaxed);
        self.stats.scrub_nanos.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats.scrub_removed.fetch_add(removed as u64, Ordering::Relaxed);
    }

    // =========================================================================
    // Split (leaf) - caller holds structure_lock.write()
    // =========================================================================

    fn split_leaf_locked(&self, leaf_id: NodeId, depth: u32) {
        let t0 = Instant::now();

        let (old_ids, old_versions, parent_id, old_centroid) = {
            let Some(node_ref) = self.nodes.get(&leaf_id) else { return };
            let TreeNode::Leaf(leaf) = node_ref.value() else { return };
            (
                leaf.ids.clone(),
                leaf.versions.clone(),
                leaf.parent_id,
                leaf.centroid.clone(),
            )
        };

        let embeddings: Vec<EmbeddingPoint> = old_ids
            .iter()
            .zip(old_versions.iter())
            .filter_map(|(&id, &ver)| {
                let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                if ver >= current_ver {
                    self.embeddings.get(&id).map(|e| (id, ver, e.value().clone()))
                } else {
                    None
                }
            })
            .collect();

        if embeddings.len() <= self.config.split_threshold {
            return;
        }

        let (left_center, left_group, right_center, right_group) =
            utils::split(embeddings, &self.distance_fn);

        let left_id = self.alloc_node_id();
        let right_id = self.alloc_node_id();

        self.nodes.insert(
            left_id,
            TreeNode::Leaf(LeafNode {
                centroid: left_center.to_vec(),
                ids: left_group.iter().map(|(id, _, _)| *id).collect(),
                versions: left_group.iter().map(|(_, ver, _)| *ver).collect(),
                parent_id: None,
            }),
        );
        self.nodes.insert(
            right_id,
            TreeNode::Leaf(LeafNode {
                centroid: right_center.to_vec(),
                ids: right_group.iter().map(|(id, _, _)| *id).collect(),
                versions: right_group.iter().map(|(_, ver, _)| *ver).collect(),
                parent_id: None,
            }),
        );

        self.nodes.remove(&leaf_id);

        if let Some(pid) = parent_id {
            self.replace_child_locked(pid, leaf_id, &[left_id, right_id]);
        } else {
            self.create_root_above_locked(&[left_id, right_id]);
        }

        self.stats.splits.fetch_add(1, Ordering::Relaxed);
        self.stats.split_nanos.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        if depth < MAX_BALANCE_DEPTH {
            self.npa_split_points_locked(&left_group, &old_centroid, &left_center, depth);
            self.npa_split_points_locked(&right_group, &old_centroid, &right_center, depth);
        }
    }

    /// NPA: reassign vectors that are farther from the new centroid than the old.
    /// Caller holds structure_lock.write().
    fn npa_split_points_locked(
        &self,
        group: &[EmbeddingPoint],
        old_center: &[f32],
        new_center: &[f32],
        depth: u32,
    ) {
        for (id, version, embedding) in group {
            let current_ver = self.versions.get(id).map(|r| *r).unwrap_or(0);
            if *version < current_ver {
                continue;
            }
            let old_dist = self.dist(embedding, old_center);
            let new_dist = self.dist(embedding, new_center);
            if new_dist > old_dist {
                self.reassign_locked(*id, depth);
            }
        }
    }

    /// Reassign a vector to its best cluster. Caller holds structure_lock.write().
    fn reassign_locked(&self, id: u32, depth: u32) {
        let t0 = Instant::now();

        let new_version = {
            let mut v = self.versions.entry(id).or_insert(0);
            *v += 1;
            *v
        };

        let Some(embedding) = self.embeddings.get(&id).map(|e| e.value().clone()) else {
            return;
        };

        let nprobe = self.config.write_nprobe;
        let candidates = self.navigate(&embedding, None, nprobe, nprobe);
        let cluster_ids = self.rng_select(&candidates);

        let mut clusters_to_balance = Vec::new();
        for &cluster_id in &cluster_ids {
            if self.register_in_leaf(cluster_id, id, new_version) {
                clusters_to_balance.push(cluster_id);
            }
        }

        self.stats.reassigns.fetch_add(1, Ordering::Relaxed);
        self.stats.reassign_nanos.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        for cluster_id in clusters_to_balance {
            self.balance_under_lock(cluster_id, depth + 1);
        }
    }

    /// Balance called when we already hold structure_lock.write() (from split/merge NPA).
    fn balance_under_lock(&self, cluster_id: NodeId, depth: u32) {
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

        if len > self.config.split_threshold {
            self.split_leaf_locked(cluster_id, depth);
        } else if len > 0 && len < self.config.merge_threshold {
            self.merge_leaf_locked(cluster_id, depth);
        }
    }

    // =========================================================================
    // Split (internal) - caller holds structure_lock.write()
    // =========================================================================

    fn split_internal_locked(&self, node_id: NodeId) {
        let (children, parent_id) = {
            let Some(node_ref) = self.nodes.get(&node_id) else { return };
            let TreeNode::Internal(internal) = node_ref.value() else { return };
            if internal.children.len() <= self.config.branching_factor {
                return;
            }
            (internal.children.clone(), internal.parent_id)
        };

        let child_embeddings: Vec<EmbeddingPoint> = children
            .iter()
            .map(|&child_id| {
                let centroid = self.nodes
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

        self.nodes.insert(
            left_id,
            TreeNode::Internal(InternalNode {
                centroid: left_center.to_vec(),
                children: left_children.clone(),
                parent_id: None,
            }),
        );
        self.nodes.insert(
            right_id,
            TreeNode::Internal(InternalNode {
                centroid: right_center.to_vec(),
                children: right_children.clone(),
                parent_id: None,
            }),
        );

        for &child_id in &left_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(left_id));
            }
        }
        for &child_id in &right_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(right_id));
            }
        }

        self.nodes.remove(&node_id);

        if let Some(pid) = parent_id {
            self.replace_child_locked(pid, node_id, &[left_id, right_id]);
        } else {
            self.create_root_above_locked(&[left_id, right_id]);
        }
    }

    // =========================================================================
    // Merge - caller holds structure_lock.write()
    // =========================================================================

    fn merge_leaf_locked(&self, leaf_id: NodeId, depth: u32) {
        if depth > MAX_BALANCE_DEPTH {
            return;
        }
        let t0 = Instant::now();

        let (source_centroid, source_ids, source_versions, parent_id) = {
            let Some(node_ref) = self.nodes.get(&leaf_id) else { return };
            let TreeNode::Leaf(leaf) = node_ref.value() else { return };
            (
                leaf.centroid.clone(),
                leaf.ids.clone(),
                leaf.versions.clone(),
                leaf.parent_id,
            )
        };

        let nprobe = self.config.write_nprobe;
        let candidates = self.navigate(&source_centroid, None, nprobe, nprobe);
        let target_id = match candidates.iter().find(|&&(nid, _)| nid != leaf_id) {
            Some(&(nid, _)) => nid,
            None => return,
        };

        let target_centroid = match self.nodes.get(&target_id) {
            Some(n) => n.centroid().to_vec(),
            None => return,
        };

        self.nodes.remove(&leaf_id);
        if let Some(pid) = parent_id {
            self.remove_child_locked(pid, leaf_id);
        }

        self.stats.merges.fetch_add(1, Ordering::Relaxed);
        self.stats.merge_nanos.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

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
                self.register_in_leaf(target_id, id, version);
            } else {
                self.reassign_locked(id, depth);
            }
        }

        self.balance_under_lock(target_id, depth + 1);
    }

    // =========================================================================
    // Tree structure helpers - caller holds structure_lock.write()
    // =========================================================================

    fn replace_child_locked(
        &self,
        parent_id: NodeId,
        old_child: NodeId,
        new_children: &[NodeId],
    ) {
        let children_clone = {
            let Some(mut node_ref) = self.nodes.get_mut(&parent_id) else { return };
            let TreeNode::Internal(parent) = node_ref.value_mut() else { return };
            parent.children.retain(|&c| c != old_child);
            parent.children.extend_from_slice(new_children);
            parent.children.clone()
        };

        for &child_id in new_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(parent_id));
            }
        }

        let new_centroid = self.compute_centroid_of(&children_clone);
        if let Some(mut node_ref) = self.nodes.get_mut(&parent_id) {
            if let TreeNode::Internal(parent) = node_ref.value_mut() {
                parent.centroid = new_centroid;
            }
        }

        if children_clone.len() > self.config.branching_factor {
            self.split_internal_locked(parent_id);
        }
    }

    fn remove_child_locked(&self, parent_id: NodeId, child_id: NodeId) {
        let (children_clone, grandparent_id) = {
            let Some(mut node_ref) = self.nodes.get_mut(&parent_id) else { return };
            let TreeNode::Internal(parent) = node_ref.value_mut() else { return };
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
                        ids: Vec::new(),
                        versions: Vec::new(),
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

    fn create_root_above_locked(&self, children: &[NodeId]) {
        let root_id = self.alloc_node_id();
        let centroid = self.compute_centroid_of(children);

        self.nodes.insert(
            root_id,
            TreeNode::Internal(InternalNode {
                centroid,
                children: children.to_vec(),
                parent_id: None,
            }),
        );

        for &child_id in children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(root_id));
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

    // =========================================================================
    // Search (no global lock - uses per-node DashMap gets)
    // =========================================================================

    /// Returns (results, vectors_scanned, leaves_scanned).
    pub fn search(&self, query: &[f32], k: usize) -> (Vec<(u32, f32)>, usize, usize) {
        self.search_with_tau(
            query,
            k,
            self.config.beam_tau,
            self.config.beam_min,
            self.config.beam_max,
        )
    }

    /// Returns (top-k results, vectors_scanned, leaves_scanned).
    pub fn search_with_tau(
        &self,
        query: &[f32],
        k: usize,
        tau: f64,
        beam_min: usize,
        beam_max: usize,
    ) -> (Vec<(u32, f32)>, usize, usize) {
        let leaves = self.navigate(query, Some(tau), beam_min, beam_max);
        let leaves_scanned = leaves.len();

        let mut results: Vec<(u32, f32)> = Vec::new();

        for &(leaf_id, _) in &leaves {
            if let Some(node_ref) = self.nodes.get(&leaf_id) {
                if let TreeNode::Leaf(leaf) = node_ref.value() {
                    let ids = leaf.ids.clone();
                    let versions = leaf.versions.clone();
                    drop(node_ref);
                    for (i, &id) in ids.iter().enumerate() {
                        let version = versions[i];
                        let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                        if version < current_ver {
                            continue;
                        }
                        if let Some(emb) = self.embeddings.get(&id) {
                            let dist = self.dist(query, emb.value());
                            results.push((id, dist));
                        }
                    }
                }
            }
        }

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
        deduped.truncate(k);
        (deduped, scanned, leaves_scanned)
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

        let mut levels = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);
                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                let dist = self.dist(query, child.centroid());
                                child_scores.push((child_id, dist));
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
            let effective =
                Self::effective_beam(&child_scores, Some(tau), beam_min, beam_max);
            child_scores.truncate(effective);

            let mut reachable: HashSet<u32> = HashSet::new();
            for &(node_id, _) in &child_scores {
                self.collect_all_data_ids(node_id, &mut reachable);
            }

            let r100 =
                gt_100.intersection(&reachable).count() as f64 / gt_100.len().max(1) as f64;

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
        let Some(node_ref) = self.nodes.get(&node_id) else { return };
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

    fn depth_of(&self, node_id: NodeId) -> usize {
        let Some(node_ref) = self.nodes.get(&node_id) else { return 0 };
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

    pub fn print_tree_stats(&self, format_count_fn: fn(usize) -> String) {
        let root = self.root_id();
        let depth = self.depth_of(root);

        struct LevelStats {
            internal_count: usize,
            child_counts: Vec<usize>,
            leaf_count: usize,
            leaf_sizes: Vec<usize>,
        }

        let mut levels: Vec<LevelStats> = (0..depth)
            .map(|_| LevelStats {
                internal_count: 0,
                child_counts: Vec::new(),
                leaf_count: 0,
                leaf_sizes: Vec::new(),
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
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);
                        for child_id in children {
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

            if !is_last {
                println!("  |");
            }
        }

        let total_vectors = self.total_vectors();
        let replication = if total_vectors > 0 {
            total_leaf_entries as f64 / total_vectors as f64
        } else {
            0.0
        };
        println!(
            "\nTotal entries: {} | Unique vectors: {} | Avg replication: {:.2}x",
            format_count_fn(total_leaf_entries),
            format_count_fn(total_vectors),
            replication,
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
