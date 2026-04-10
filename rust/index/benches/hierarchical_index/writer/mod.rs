#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery};
use chroma_index::spann::utils::{self, EmbeddingPoint};
use dashmap::{DashMap, DashSet};
use parking_lot::ReentrantMutex;
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
    pub write_level_taus: Vec<Option<f64>>,
    pub write_level_min_pcts: Vec<f64>,
    /// Dynamic beam tau for the search/query path.
    /// Include children with dist <= d_best * (1 + beam_tau), clamped to [beam_min, beam_max].
    pub beam_tau: f64,
    pub beam_min: usize,
    pub beam_max: usize,
    pub max_replicas: usize,
    pub write_rng_epsilon: f32,
    pub write_rng_factor: f32,
    pub reassign_neighbor_count: usize,
    pub write_navigation: NavigationMode,
    pub read_navigation: NavigationMode,
    /// If true, NPA uses full precision f32 distances; if false, NPA uses quantized distances.
    pub fp_npa: bool,
    /// If true, add() skips inline balance; caller must invoke balance_index() explicitly.
    pub deferred_balance: bool,
}

impl Default for HierarchicalSpannConfig {
    fn default() -> Self {
        Self {
            branching_factor: 100,
            split_threshold: 2048,
            merge_threshold: 512,
            write_beam_tau: 1.5,
            write_beam_min: 10,
            write_beam_max: 50000,
            write_level_taus: Vec::new(),
            write_level_min_pcts: Vec::new(),
            beam_tau: 2.0,
            beam_min: 10,
            beam_max: 50000,
            max_replicas: 2,
            write_rng_epsilon: 4.0,
            write_rng_factor: 2.0,
            reassign_neighbor_count: 32,
            write_navigation: NavigationMode::Fp,
            read_navigation: NavigationMode::OneBit,
            fp_npa: true,
            deferred_balance: false,
        }
    }
}

// =============================================================================
// Node types
// =============================================================================
pub(super) struct LeafNode {
    pub(super) centroid: Vec<f32>,
    /// 1-bit RaBitQ code of centroid as residual vs parent centroid.
    pub(super) centroid_code: Vec<u8>,
    pub(super) ids: Vec<u32>,
    pub(super) versions: Vec<u32>,
    /// Per-vector 1-bit RaBitQ codes packed into one contiguous buffer.
    pub(super) codes: Vec<u8>,
    pub(super) parent_id: Option<NodeId>,
    /// Total posting count for lazy-load detection. When `ids.len() < length`,
    /// the posting data has not yet been loaded from the blockfile.
    pub(super) length: usize,
}

pub(super) struct InternalNode {
    pub(super) centroid: Vec<f32>,
    /// 1-bit RaBitQ code of centroid as residual vs parent centroid.
    pub(super) centroid_code: Vec<u8>,
    pub(super) children: Vec<NodeId>,
    pub(super) parent_id: Option<NodeId>,
}

pub(super) enum TreeNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

pub(super) fn code_slice(codes: &[u8], index: usize, code_size: usize) -> &[u8] {
    let start = index * code_size;
    &codes[start..start + code_size]
}

fn push_code(codes: &mut Vec<u8>, code: &[u8]) {
    codes.extend_from_slice(code);
}

fn swap_remove_code(codes: &mut Vec<u8>, index: usize, code_size: usize) {
    let last_index = codes.len() / code_size - 1;
    if index != last_index {
        let dst = index * code_size;
        let src = last_index * code_size;
        codes.copy_within(src..src + code_size, dst);
    }
    codes.truncate(codes.len() - code_size);
}

impl TreeNode {
    pub(super) fn centroid(&self) -> &[f32] {
        match self {
            TreeNode::Leaf(l) => &l.centroid,
            TreeNode::Internal(i) => &i.centroid,
        }
    }

    pub(super) fn centroid_code(&self) -> &[u8] {
        match self {
            TreeNode::Leaf(l) => &l.centroid_code,
            TreeNode::Internal(i) => &i.centroid_code,
        }
    }

    pub(super) fn parent_id(&self) -> Option<NodeId> {
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

mod diagnostics;
pub mod persistence;

pub use super::instrumentation::*;
pub use persistence::HierarchicalSpannIds;

#[derive(Clone, Debug)]
pub struct ReadBeamPolicy {
    pub(super) default_tau: Option<f64>,
    pub(super) default_beam_min: usize,
    pub(super) default_beam_max: usize,
    pub(super) level_taus: Vec<Option<f64>>,
    pub(super) level_min_pcts: Vec<f64>,
    pub(super) level_widths: Vec<usize>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct LevelBeamParams {
    pub(super) tau: Option<f64>,
    pub(super) beam_min: usize,
    pub(super) beam_max: usize,
}

impl ReadBeamPolicy {
    pub fn uniform(tau: Option<f64>, beam_min: usize, beam_max: usize) -> Self {
        Self {
            default_tau: tau,
            default_beam_min: beam_min,
            default_beam_max: beam_max,
            level_taus: Vec::new(),
            level_min_pcts: Vec::new(),
            level_widths: Vec::new(),
        }
    }

    pub fn with_level_overrides(
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
        level_taus: Vec<Option<f64>>,
        level_min_pcts: Vec<f64>,
        level_widths: Vec<usize>,
    ) -> Self {
        Self {
            default_tau: tau,
            default_beam_min: beam_min,
            default_beam_max: beam_max,
            level_taus,
            level_min_pcts,
            level_widths,
        }
    }

    pub(super) fn level_params(&self, level: usize) -> LevelBeamParams {
        let idx = level.saturating_sub(1);
        let level_width = self.level_widths.get(idx).copied();
        let pct_min = self.level_min_pcts.get(idx).copied();
        let mut beam_min = match (level_width, pct_min) {
            (Some(width), Some(pct)) => ((width as f64) * (pct / 100.0)).ceil() as usize,
            _ => self.default_beam_min,
        };
        let mut beam_max = match (level_width, pct_min) {
            (Some(width), Some(_)) => width,
            _ => self.default_beam_max,
        };
        if idx + 1 == self.level_widths.len() {
            beam_min = beam_min.max(self.default_beam_min);
            beam_max = beam_max.min(self.default_beam_max);
        }
        beam_min = beam_min.min(beam_max);
        LevelBeamParams {
            tau: self
                .level_taus
                .get(idx)
                .copied()
                .flatten()
                .or(self.default_tau),
            beam_min,
            beam_max,
        }
    }
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
pub struct HierarchicalSpannWriter {
    pub(super) dim: usize,
    pub(super) distance_fn: DistanceFunction,
    pub(super) config: HierarchicalSpannConfig,

    pub(super) nodes: DashMap<NodeId, TreeNode>,
    balancing: DashSet<NodeId>,
    /// Serializes tree structure modifications (replace_child, remove_child_locked,
    /// create_root_above, split_internal) to prevent races when concurrent splits
    /// modify the same parent. Reentrant because these functions are mutually recursive.
    tree_lock: ReentrantMutex<()>,
    pub(super) root_id: AtomicU32,
    pub(super) next_node_id: AtomicU32,

    pub(super) embeddings: DashMap<u32, Arc<[f32]>>,
    pub(super) versions: DashMap<u32, u32>,

    pub stats: WriterStats,

    // Blockfile readers for lazy loading from persisted state.
    pub(super) posting_list_reader:
        Option<chroma_blockstore::BlockfileReader<'static, u32, chroma_types::QuantizedCluster<'static>>>,
    pub(super) vector_data_reader:
        Option<chroma_blockstore::BlockfileReader<'static, u32, &'static [f32]>>,
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
                length: 0,
            }),
        );

        Self {
            dim,
            distance_fn,
            config,
            nodes,
            balancing: DashSet::new(),
            tree_lock: ReentrantMutex::new(()),
            root_id: AtomicU32::new(0),
            next_node_id: AtomicU32::new(1),
            embeddings: DashMap::new(),
            versions: DashMap::new(),
            stats: WriterStats::default(),
            posting_list_reader: None,
            vector_data_reader: None,
        }
    }

    fn write_beam_policy(&self) -> ReadBeamPolicy {
        if self.config.write_level_taus.is_empty() && self.config.write_level_min_pcts.is_empty() {
            ReadBeamPolicy::uniform(
                Some(self.config.write_beam_tau),
                self.config.write_beam_min,
                self.config.write_beam_max,
            )
        } else {
            let level_widths: Vec<usize> = self
                .level_node_counts()
                .into_iter()
                .skip(1)
                .collect();
            ReadBeamPolicy::with_level_overrides(
                Some(self.config.write_beam_tau),
                self.config.write_beam_min,
                self.config.write_beam_max,
                self.config.write_level_taus.clone(),
                self.config.write_level_min_pcts.clone(),
                level_widths,
            )
        }
    }

    fn alloc_node_id(&self) -> NodeId {
        self.next_node_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.distance_fn.distance(a, b)
    }

    pub(super) fn root_id(&self) -> NodeId {
        self.root_id.load(Ordering::Relaxed)
    }

    /// Insert a raw embedding into the in-memory map (for populating after resume).
    pub fn insert_embedding(&self, id: u32, embedding: Arc<[f32]>) {
        self.embeddings.insert(id, embedding);
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
            let policy = self.write_beam_policy();
            let candidates = self.navigate_with_policy(
                embedding,
                1,
                self.config.write_navigation,
                &policy,
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

            if !self.config.deferred_balance {
                let balance_start = Instant::now();
                for cluster_id in clusters_to_balance {
                    self.balance(cluster_id, 0);
                }
                self.stats
                    .add_balance_nanos
                    .fetch_add(balance_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

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
                push_code(&mut leaf.codes, code.as_ref());
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
    pub(super) fn navigate_f32(
        &self,
        query: &[f32],
        policy: &ReadBeamPolicy,
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
            let params = policy.level_params(levels as usize);

            let sort_start = Instant::now();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let effective =
                Self::effective_beam(&child_scores, params.tau, params.beam_min, params.beam_max);
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

    pub(super) fn effective_beam(
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
                let threshold = d_best * (tau as f32);
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
    pub(super) fn navigate_quantized(
        &self,
        query: &[f32],
        rerank_centroids: usize,
        policy: &ReadBeamPolicy,
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
            let params = policy.level_params(levels as usize);

            let sort_start = Instant::now();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            sort_nanos += sort_start.elapsed().as_nanos() as u64;

            if rerank_factor > 1 {
                let effective = Self::effective_beam(
                    &child_scores,
                    params.tau,
                    params.beam_min,
                    params.beam_max,
                );
                let rerank_count = (effective * rerank_factor).min(child_scores.len());
                child_scores.truncate(rerank_count);

                let rerank_start = Instant::now();
                let mut reranked: Vec<(NodeId, f32)> = child_scores
                    .iter()
                    .map(|&(nid, _approx)| {
                        let dist = self.nodes.get(&nid)
                            .map(|n| self.dist(query, n.centroid()))
                            .unwrap_or(f32::MAX);
                        (nid, dist)
                    })
                    .collect();
                reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let final_beam = Self::effective_beam(
                    &reranked,
                    params.tau,
                    params.beam_min,
                    params.beam_max,
                );
                reranked.truncate(final_beam);
                rerank_nanos += rerank_start.elapsed().as_nanos() as u64;
                child_scores = reranked;
            } else {
                let effective = Self::effective_beam(
                    &child_scores,
                    params.tau,
                    params.beam_min,
                    params.beam_max,
                );
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
    pub(super) fn navigate_1bit(
        &self,
        query: &[f32],
        rerank_centroids: usize,
        policy: &ReadBeamPolicy,
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
            let params = policy.level_params(levels as usize);

            let sort_start = Instant::now();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            sort_nanos += sort_start.elapsed().as_nanos() as u64;

            if rerank_factor > 1 {
                let effective = Self::effective_beam(
                    &child_scores,
                    params.tau,
                    params.beam_min,
                    params.beam_max,
                );
                let rerank_count = (effective * rerank_factor).min(child_scores.len());
                child_scores.truncate(rerank_count);

                let rerank_start = Instant::now();
                let mut reranked: Vec<(NodeId, f32)> = child_scores
                    .iter()
                    .map(|&(nid, _approx)| {
                        let dist = self.nodes.get(&nid)
                            .map(|n| self.dist(query, n.centroid()))
                            .unwrap_or(f32::MAX);
                        (nid, dist)
                    })
                    .collect();
                reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let final_beam = Self::effective_beam(
                    &reranked,
                    params.tau,
                    params.beam_min,
                    params.beam_max,
                );
                reranked.truncate(final_beam);
                rerank_nanos += rerank_start.elapsed().as_nanos() as u64;
                child_scores = reranked;
            } else {
                let effective = Self::effective_beam(
                    &child_scores,
                    params.tau,
                    params.beam_min,
                    params.beam_max,
                );
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
    pub(super) fn navigate_with_policy(
        &self,
        query: &[f32],
        rerank_centroids: usize,
        mode: NavigationMode,
        policy: &ReadBeamPolicy,
    ) -> Vec<(NodeId, f32)> {
        match mode {
            NavigationMode::Fp => self.navigate_f32(query, policy),
            NavigationMode::OneBit => self.navigate_1bit(query, rerank_centroids, policy),
            NavigationMode::FourBit => self.navigate_quantized(query, rerank_centroids, policy),
        }
    }

    pub(super) fn navigate(
        &self,
        query: &[f32],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
        rerank_centroids: usize,
        mode: NavigationMode,
    ) -> Vec<(NodeId, f32)> {
        let policy = ReadBeamPolicy::uniform(tau, beam_min, beam_max);
        self.navigate_with_policy(query, rerank_centroids, mode, &policy)
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
            // If the candidate cluster is farther away from the query than 
            // from other already selected clusters, skip it.
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

    /// Balance all leaves that exceed split_threshold or fall below merge_threshold.
    /// Repeats until no more work is needed (convergence).
    pub fn balance_index(&self) {
        loop {
            let leaf_ids: Vec<NodeId> = self
                .nodes
                .iter()
                .filter_map(|entry| match entry.value() {
                    TreeNode::Leaf(leaf) => {
                        let len = leaf.ids.len();
                        if len > self.config.split_threshold
                            || (len > 0 && len < self.config.merge_threshold)
                        {
                            Some(*entry.key())
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .collect();

            if leaf_ids.is_empty() {
                break;
            }

            for leaf_id in leaf_ids {
                self.balance(leaf_id, 0);
            }
        }
    }

    /// Collect all descendant leaf NodeIds under a given subtree root.
    fn collect_leaves_under(&self, node_id: NodeId) -> Vec<NodeId> {
        let mut leaves = Vec::new();
        let mut stack = vec![node_id];
        while let Some(nid) = stack.pop() {
            match self.nodes.get(&nid) {
                Some(node_ref) => match node_ref.value() {
                    TreeNode::Leaf(_) => leaves.push(nid),
                    TreeNode::Internal(internal) => {
                        stack.extend(internal.children.iter().copied());
                    }
                },
                None => {}
            }
        }
        leaves
    }

    /// Count leaves needing balance under a subtree root.
    fn count_work_under(&self, node_id: NodeId) -> usize {
        let mut count = 0;
        let mut stack = vec![node_id];
        while let Some(nid) = stack.pop() {
            match self.nodes.get(&nid) {
                Some(node_ref) => match node_ref.value() {
                    TreeNode::Leaf(leaf) => {
                        let len = leaf.ids.len();
                        if len > self.config.split_threshold
                            || (len > 0 && len < self.config.merge_threshold)
                        {
                            count += 1;
                        }
                    }
                    TreeNode::Internal(internal) => {
                        stack.extend(internal.children.iter().copied());
                    }
                },
                None => {}
            }
        }
        count
    }

    /// Find subtree roots at the tree level that gives us >= num_threads partitions.
    /// Returns (subtree_root_id, estimated_work) pairs.
    fn find_partition_roots(&self, num_threads: usize) -> Vec<(NodeId, usize)> {
        let root = self.root_id();
        let mut frontier = vec![root];

        loop {
            if frontier.len() >= num_threads {
                break;
            }
            let mut next_frontier = Vec::new();
            let mut all_leaves = true;
            for &nid in &frontier {
                match self.nodes.get(&nid) {
                    Some(node_ref) => match node_ref.value() {
                        TreeNode::Internal(internal) => {
                            all_leaves = false;
                            next_frontier.extend(internal.children.iter().copied());
                        }
                        TreeNode::Leaf(_) => {
                            next_frontier.push(nid);
                        }
                    },
                    None => {}
                }
            }
            if all_leaves || next_frontier.len() <= frontier.len() {
                break;
            }
            frontier = next_frontier;
        }

        frontier
            .into_iter()
            .map(|nid| {
                let work = self.count_work_under(nid);
                (nid, work)
            })
            .collect()
    }

    /// Parallel version of balance_index. Partitions the tree into subtrees and
    /// distributes them across threads, weighted by estimated work.
    pub fn balance_index_parallel(&self, num_threads: usize) {
        if num_threads <= 1 {
            return self.balance_index();
        }

        loop {
            let has_work = self.nodes.iter().any(|entry| match entry.value() {
                TreeNode::Leaf(leaf) => {
                    let len = leaf.ids.len();
                    len > self.config.split_threshold
                        || (len > 0 && len < self.config.merge_threshold)
                }
                _ => false,
            });
            if !has_work {
                break;
            }

            let mut partitions = self.find_partition_roots(num_threads);
            partitions.sort_by(|a, b| b.1.cmp(&a.1));

            // Greedy assignment: assign each subtree to the thread with least work.
            let mut thread_work: Vec<usize> = vec![0; num_threads];
            let mut thread_subtrees: Vec<Vec<NodeId>> = vec![Vec::new(); num_threads];

            for (nid, work) in &partitions {
                let min_thread = thread_work
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, w)| **w)
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                thread_subtrees[min_thread].push(*nid);
                thread_work[min_thread] += work.max(&1);
            }

            std::thread::scope(|s| {
                for subtrees in &thread_subtrees {
                    if subtrees.is_empty() {
                        continue;
                    }
                    s.spawn(move || {
                        for &subtree_root in subtrees {
                            let leaves = self.collect_leaves_under(subtree_root);
                            for leaf_id in leaves {
                                self.balance(leaf_id, 0);
                            }
                        }
                    });
                }
            });
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

        let code_size = self.code_size();
        let mut removed = 0usize;
        let mut i = 0;
        while i < leaf.ids.len() {
            let id = leaf.ids[i];
            let version = leaf.versions[i];
            let current_version = self.versions.get(&id).map(|r| *r).unwrap_or(0);
            if version < current_version {
                leaf.ids.swap_remove(i);
                leaf.versions.swap_remove(i);
                swap_remove_code(&mut leaf.codes, i, code_size);
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
        let code_size = self.code_size();

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
            let mut codes = Vec::with_capacity(embeddings.len() * code_size);
            for (_, _, emb) in &embeddings {
                let code = Code::<1>::quantize(emb, &old_centroid);
                push_code(&mut codes, code.as_ref());
            }
            let len = embeddings.len();
            self.nodes.insert(
                leaf_id,
                TreeNode::Leaf(LeafNode {
                    centroid: old_centroid,
                    centroid_code: Vec::new(),
                    ids: embeddings.iter().map(|(id, _, _)| *id).collect(),
                    versions: embeddings.iter().map(|(_, ver, _)| *ver).collect(),
                    codes,
                    parent_id,
                    length: len,
                }),
            );
            return;
        }

        let old_code_slots: HashMap<u32, usize> = old_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
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
        let mut left_codes = Vec::with_capacity(left_group.len() * code_size);
        for (_, _, emb) in &left_group {
            let code = Code::<1>::quantize(emb, &left_centroid);
            push_code(&mut left_codes, code.as_ref());
        }
        let mut right_codes = Vec::with_capacity(right_group.len() * code_size);
        for (_, _, emb) in &right_group {
            let code = Code::<1>::quantize(emb, &right_centroid);
            push_code(&mut right_codes, code.as_ref());
        }

        self.stats.split_quantize_nanos.fetch_add(
            quantize_start.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );

        let left_len = left_group.len();
        let right_len = right_group.len();
        self.nodes.insert(
            left_id,
            TreeNode::Leaf(LeafNode {
                centroid: left_centroid,
                centroid_code: Vec::new(),
                ids: left_group.iter().map(|(id, _, _)| *id).collect(),
                versions: left_group.iter().map(|(_, ver, _)| *ver).collect(),
                codes: left_codes,
                parent_id: None,
                length: left_len,
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
                length: right_len,
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
                &old_codes,
                &old_code_slots,
                &mut evaluated,
                depth,
            );
            self.apply_npa_to_cluster(
                right_id,
                &right_group,
                &old_centroid,
                &right_center,
                &old_codes,
                &old_code_slots,
                &mut evaluated,
                depth,
            );
            self.stats.split_npa_cluster_nanos.fetch_add(
                npa_cluster_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );

            let npa_neighbor_start = Instant::now();
            let write_policy = self.write_beam_policy();
            self.apply_npa_to_neighbors(
                leaf_id,
                left_id,
                right_id,
                &old_centroid,
                &left_center,
                &right_center,
                &mut evaluated,
                depth,
                &write_policy,
            );
            self.stats.split_npa_neighbor_nanos.fetch_add(
                npa_neighbor_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
        }

        self.stats
            .split_sizes
            .lock()
            .push(old_ids.len() as u32);
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
        old_codes: &[u8],
        old_code_slots: &HashMap<u32, usize>,
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
                old_codes,
                old_code_slots,
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
        old_codes: &[u8],
        old_code_slots: &HashMap<u32, usize>,
        evaluated: &mut HashSet<u32>,
        depth: u32,
    ) {
        let padded_bytes = self.padded_bytes();
        let c_norm = Self::vec_norm(old_center);
        let code_size = self.code_size();

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
            let Some(&code_slot) = old_code_slots.get(id) else {
                continue;
            };
            let code_bytes = code_slice(old_codes, code_slot, code_size);
            n_evaluated += 1;
            let code = Code::<1, _>::new(code_bytes);

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
        for (id, version, emb) in group {
            let current_ver = self.versions.get(id).map(|r| *r).unwrap_or(0);
            if *version < current_ver {
                continue;
            }
            if !evaluated.insert(*id) {
                continue;
            }
            n_evaluated += 1;
            let old_dist = self.dist(emb, old_center);
            let new_dist = self.dist(emb, new_center);
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
    fn apply_npa_to_quantized_neighbor(
        &self,
        neighbor_id: NodeId,
        old_center: &[f32],
        left_center: &[f32],
        right_center: &[f32],
        evaluated: &mut HashSet<u32>,
        depth: u32,
    ) -> Option<(usize, usize, usize)> {
        let (n_centroid, n_ids, n_versions, n_codes) = {
            let Some(node_ref) = self.nodes.get(&neighbor_id) else {
                return None;
            };
            let TreeNode::Leaf(leaf) = node_ref.value() else {
                return None;
            };
            (
                leaf.centroid.clone(),
                leaf.ids.clone(),
                leaf.versions.clone(),
                leaf.codes.clone(),
            )
        };

        let n_total = n_ids.len();
        let mut n_reassigned = 0usize;
        let mut n_evaluated = 0usize;

        let code_size = self.code_size();
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
        let old_qq = QuantizedQuery::new(&old_r_q, padded_bytes, c_norm, old_c_dot_q, old_q_norm);

        for i in 0..n_ids.len() {
            let code_bytes = code_slice(&n_codes, i, code_size);
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
            let code = Code::<1, _>::new(code_bytes);

            let left_dist = code.distance_quantized_query(&self.distance_fn, &left_qq);
            let right_dist = code.distance_quantized_query(&self.distance_fn, &right_qq);
            let neighbor_dist = code.distance_quantized_query(&self.distance_fn, &neighbor_qq);

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

        Some((n_total, n_evaluated, n_reassigned))
    }

    fn apply_npa_to_fp_neighbor(
        &self,
        neighbor_id: NodeId,
        old_center: &[f32],
        left_center: &[f32],
        right_center: &[f32],
        evaluated: &mut HashSet<u32>,
        depth: u32,
    ) -> Option<(usize, usize, usize)> {
        let (n_centroid, n_ids, n_versions, n_embeddings) = {
            let Some(node_ref) = self.nodes.get(&neighbor_id) else {
                return None;
            };
            let TreeNode::Leaf(leaf) = node_ref.value() else {
                return None;
            };
            (
                leaf.centroid.clone(),
                leaf.ids.clone(),
                leaf.versions.clone(),
                leaf.ids
                    .iter()
                    .map(|id| self.embeddings.get(id).map(|e| e.value().clone()))
                    .collect::<Vec<_>>(),
            )
        };

        let n_total = n_ids.len();
        let mut n_reassigned = 0usize;
        let mut n_evaluated = 0usize;

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

            let Some(emb) = n_embeddings[i].as_deref() else {
                continue;
            };
            n_evaluated += 1;

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

        Some((n_total, n_evaluated, n_reassigned))
    }

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
        write_policy: &ReadBeamPolicy,
    ) {
        let neighbors = self.navigate_with_policy(
            old_center,
            1,
            self.config.write_navigation,
            write_policy,
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
            let Some((n_total, n_evaluated, n_reassigned)) = (if self.config.fp_npa {
                self.apply_npa_to_fp_neighbor(
                    neighbor_id,
                    old_center,
                    left_center,
                    right_center,
                    evaluated,
                    depth,
                )
            } else {
                self.apply_npa_to_quantized_neighbor(
                    neighbor_id,
                    old_center,
                    left_center,
                    right_center,
                    evaluated,
                    depth,
                )
            }) else {
                continue;
            };

            neighbors_visited += 1;
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
            let policy = self.write_beam_policy();
            let candidates = self.navigate_with_policy(
                &embedding,
                1,
                self.config.write_navigation,
                &policy,
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

        let policy = self.write_beam_policy();
        let candidates = self.navigate_with_policy(
            &source_centroid,
            1,
            self.config.write_navigation,
            &policy,
        );
        let target_id = match candidates.iter().find(|&&(nid, _)| nid != leaf_id) {
            Some(&(nid, _)) => nid,
            None => {
                // No merge target found, re-insert the leaf
                let len = source_ids.len();
                self.nodes.insert(
                    leaf_id,
                    TreeNode::Leaf(LeafNode {
                        centroid: source_centroid,
                        centroid_code: Vec::new(),
                        ids: source_ids,
                        versions: source_versions,
                        codes: Vec::new(),
                        parent_id,
                        length: len,
                    }),
                );
                return;
            }
        };

        let target_centroid = match self.nodes.get(&target_id) {
            Some(n) => n.centroid().to_vec(),
            None => {
                // Target gone, re-insert the leaf
                let len = source_ids.len();
                self.nodes.insert(
                    leaf_id,
                    TreeNode::Leaf(LeafNode {
                        centroid: source_centroid,
                        centroid_code: Vec::new(),
                        ids: source_ids,
                        versions: source_versions,
                        codes: Vec::new(),
                        parent_id,
                        length: len,
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

    /// When a parent node has been removed by a concurrent split_internal,
    /// navigate from root to find the correct internal node and insert orphans.
    fn adopt_orphans(&self, orphan_ids: &[NodeId]) {
        for &orphan_id in orphan_ids {
            let centroid = match self.nodes.get(&orphan_id) {
                Some(node_ref) => node_ref.centroid().to_vec(),
                None => continue,
            };
            let is_leaf = matches!(
                self.nodes.get(&orphan_id).map(|n| matches!(n.value(), TreeNode::Leaf(_))),
                Some(true)
            );

            // Navigate from root to find the internal node at the right depth.
            let mut current = self.root_id();
            loop {
                match self.nodes.get(&current) {
                    Some(node_ref) => match node_ref.value() {
                        TreeNode::Internal(internal) => {
                            let children = internal.children.clone();
                            drop(node_ref);

                            // Check if this level's children match the orphan type.
                            // If orphan is a leaf, we want an internal node whose children are leaves.
                            // If orphan is internal, we want one level higher.
                            let child_is_leaf = children.iter().any(|&c| {
                                self.nodes
                                    .get(&c)
                                    .map_or(false, |n| matches!(n.value(), TreeNode::Leaf(_)))
                            });

                            if (is_leaf && child_is_leaf) || (is_leaf && children.is_empty()) {
                                // Insert orphan here
                                if let Some(mut node_ref) = self.nodes.get_mut(&current) {
                                    if let TreeNode::Internal(parent) = node_ref.value_mut() {
                                        if !parent.children.contains(&orphan_id) {
                                            parent.children.push(orphan_id);
                                        }
                                    }
                                }
                                if let Some(mut node_ref) = self.nodes.get_mut(&orphan_id) {
                                    node_ref.set_parent_id(Some(current));
                                }
                                break;
                            }
                            if !is_leaf && !child_is_leaf {
                                if let Some(mut node_ref) = self.nodes.get_mut(&current) {
                                    if let TreeNode::Internal(parent) = node_ref.value_mut() {
                                        if !parent.children.contains(&orphan_id) {
                                            parent.children.push(orphan_id);
                                        }
                                    }
                                }
                                if let Some(mut node_ref) = self.nodes.get_mut(&orphan_id) {
                                    node_ref.set_parent_id(Some(current));
                                }
                                break;
                            }

                            // Go deeper: pick closest child
                            let closest = children
                                .iter()
                                .filter_map(|&c| {
                                    self.nodes
                                        .get(&c)
                                        .map(|n| (c, self.dist(&centroid, n.centroid())))
                                })
                                .min_by(|a, b| {
                                    a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                                })
                                .map(|(c, _)| c);

                            match closest {
                                Some(c) => current = c,
                                None => break,
                            }
                        }
                        TreeNode::Leaf(_) => {
                            // Root is a leaf; create a new root above both.
                            self.create_root_above(&[current, orphan_id]);
                            break;
                        }
                    },
                    None => break,
                }
            }
        }
    }

    fn replace_child(&self, parent_id: NodeId, old_child: NodeId, new_children: &[NodeId]) {
        let _guard = self.tree_lock.lock();
        let children_clone = {
            let Some(mut node_ref) = self.nodes.get_mut(&parent_id) else {
                // eprintln!("WARN: replace_child: parent {} gone, adopting orphans", parent_id);
                self.adopt_orphans(new_children);
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
        let _guard = self.tree_lock.lock();
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
                        length: 0,
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
        let _guard = self.tree_lock.lock();
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
    pub(super) fn padded_bytes(&self) -> usize {
        Code::<1, Vec<u8>>::packed_len(self.dim)
    }

    pub(super) fn code_size(&self) -> usize {
        Code::<1, Vec<u8>>::size(self.dim)
    }

    /// Compute ||v||.
    pub(super) fn vec_norm(v: &[f32]) -> f32 {
        (f32::dot(v, v).unwrap_or(0.0) as f32).sqrt()
    }

}

// Search, diagnostics, and tree info methods are in diagnostics.rs.

