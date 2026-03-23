#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use chroma_distance::DistanceFunction;
use chroma_index::spann::utils::{self, EmbeddingPoint};

use super::compute_distance;

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
// Stats
// =============================================================================

#[derive(Default, Clone)]
pub struct WriterStats {
    pub adds: u64,
    pub add_nanos: u64,
    pub navigates: u64,
    pub navigate_nanos: u64,
    pub splits: u64,
    pub split_nanos: u64,
    pub merges: u64,
    pub merge_nanos: u64,
    pub reassigns: u64,
    pub reassign_nanos: u64,
    pub scrubs: u64,
    pub scrub_removed: u64,
}

#[allow(dead_code)]
pub struct LevelRecall {
    pub level: usize,
    pub reachable_100: f64,
    pub beam_size: usize,
    pub total_candidates: usize,
}

// =============================================================================
// Writer
// =============================================================================

/// Full-precision hierarchical SPANN index.
///
/// Stores data vectors in leaf nodes (posting lists). Internal nodes route
/// queries via beam search using f32 centroid distances. The tree grows
/// bottom-up: vectors are always added to leaf nodes, and splits propagate
/// upward when a parent exceeds the branching factor.
///
/// Closely mirrors `QuantizedSpannIndexWriter`'s add/balance/split/merge
/// pipeline but uses f32 throughout (no quantization).
pub struct HierarchicalSpannWriter {
    dim: usize,
    distance_fn: DistanceFunction,
    config: HierarchicalSpannConfig,

    nodes: HashMap<NodeId, TreeNode>,
    root_id: NodeId,
    next_node_id: u32,

    embeddings: HashMap<u32, Arc<[f32]>>,
    versions: HashMap<u32, u32>,

    pub stats: WriterStats,
}

impl HierarchicalSpannWriter {
    pub fn new(dim: usize, distance_fn: DistanceFunction, config: HierarchicalSpannConfig) -> Self {
        let mut nodes = HashMap::new();
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
            root_id: 0,
            next_node_id: 1,
            embeddings: HashMap::new(),
            versions: HashMap::new(),
            stats: WriterStats::default(),
        }
    }

    fn alloc_node_id(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        compute_distance(a, b, &self.distance_fn)
    }

    // =========================================================================
    // Add
    // =========================================================================

    /// Add a data vector to the index.
    ///
    /// Flow (mirrors `QuantizedSpannIndexWriter::add`):
    /// 1. Store embedding, bump version
    /// 2. Navigate tree to find nearest leaf nodes
    /// 3. RNG-select clusters for replication
    /// 4. Register vector in each selected leaf
    /// 5. Balance each modified leaf (scrub -> split or merge)
    pub fn add(&mut self, id: u32, embedding: &[f32]) {
        let add_start = Instant::now();

        let emb: Arc<[f32]> = Arc::from(embedding);
        self.embeddings.insert(id, emb);

        let version = {
            let v = self.versions.entry(id).or_insert(0);
            *v += 1;
            *v
        };

        let nav_start = Instant::now();
        let candidates = self.navigate_core(embedding, None, self.config.write_nprobe, self.config.write_nprobe);
        self.stats.navigates += 1;
        self.stats.navigate_nanos += nav_start.elapsed().as_nanos() as u64;

        let cluster_ids = self.rng_select(&candidates);

        let mut clusters_to_balance = Vec::new();
        for &cluster_id in &cluster_ids {
            if self.register_in_leaf(cluster_id, id, version) {
                clusters_to_balance.push(cluster_id);
            }
        }

        if clusters_to_balance.is_empty() {
            if self.register_in_leaf(self.root_id, id, version) {
                clusters_to_balance.push(self.root_id);
            }
        }

        for cluster_id in clusters_to_balance {
            self.balance(cluster_id, 0);
        }

        self.stats.adds += 1;
        self.stats.add_nanos += add_start.elapsed().as_nanos() as u64;
    }

    fn register_in_leaf(&mut self, leaf_id: NodeId, id: u32, version: u32) -> bool {
        if let Some(TreeNode::Leaf(leaf)) = self.nodes.get_mut(&leaf_id) {
            leaf.ids.push(id);
            leaf.versions.push(version);
            true
        } else {
            false
        }
    }

    // =========================================================================
    // Navigate
    // =========================================================================

    /// Beam search the tree to find the nearest leaf nodes.
    ///
    /// Two modes (mirroring `HierarchicalCentroidIndex::effective_beam`):
    /// - `tau = None`: fixed beam, keep top `beam_min` candidates per level.
    /// - `tau = Some(t)`: dynamic beam, keep children with
    ///   `dist <= d_best * (1 + t)`, clamped to `[beam_min, beam_max]`.
    fn navigate_core(
        &self,
        query: &[f32],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
    ) -> Vec<(NodeId, f32)> {
        let Some(root) = self.nodes.get(&self.root_id) else {
            return Vec::new();
        };

        if matches!(root, TreeNode::Leaf(_)) {
            let dist = self.dist(query, root.centroid());
            return vec![(self.root_id, dist)];
        }

        let mut leaves: Vec<(NodeId, f32)> = Vec::new();
        let mut beam: Vec<NodeId> = vec![self.root_id];

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            for &node_id in &beam {
                if let Some(TreeNode::Internal(internal)) = self.nodes.get(&node_id) {
                    for &child_id in &internal.children {
                        if let Some(child) = self.nodes.get(&child_id) {
                            let dist = self.dist(query, child.centroid());
                            child_scores.push((child_id, dist));
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
                match self.nodes.get(&node_id) {
                    Some(TreeNode::Leaf(_)) => leaves.push((node_id, dist)),
                    Some(TreeNode::Internal(_)) => next_internals.push(node_id),
                    None => {}
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

    /// Compute effective beam width from sorted distance scores.
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
    // RNG select (epsilon + RNG filtering, same as SPANN)
    // =========================================================================

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

            let Some(node) = self.nodes.get(&node_id) else {
                continue;
            };
            let centroid = node.centroid();

            let blocked = selected_centroids
                .iter()
                .any(|sel| self.config.write_rng_factor * self.dist(centroid, sel) <= distance);
            if blocked {
                continue;
            }

            result.push(node_id);
            selected_centroids.push(centroid.to_vec());

            if result.len() >= self.config.nreplica_count {
                break;
            }
        }

        result
    }

    // =========================================================================
    // Balance / Scrub
    // =========================================================================

    fn balance(&mut self, cluster_id: NodeId, depth: u32) {
        if depth > MAX_BALANCE_DEPTH {
            return;
        }

        self.scrub(cluster_id);

        let len = match self.nodes.get(&cluster_id) {
            Some(TreeNode::Leaf(leaf)) => leaf.ids.len(),
            _ => return,
        };

        if len > self.config.split_threshold {
            self.split_leaf(cluster_id, depth);
        } else if len > 0 && len < self.config.merge_threshold {
            self.merge_leaf(cluster_id, depth);
        }
    }

    /// Remove stale version entries from a leaf.
    fn scrub(&mut self, cluster_id: NodeId) {
        let Some(TreeNode::Leaf(leaf)) = self.nodes.get_mut(&cluster_id) else {
            return;
        };

        let mut removed = 0usize;
        let mut i = 0;
        while i < leaf.ids.len() {
            let id = leaf.ids[i];
            let version = leaf.versions[i];
            let current_version = self.versions.get(&id).copied().unwrap_or(0);
            if version < current_version {
                leaf.ids.swap_remove(i);
                leaf.versions.swap_remove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }

        self.stats.scrubs += 1;
        self.stats.scrub_removed += removed as u64;
    }

    // =========================================================================
    // Split (leaf)
    // =========================================================================

    /// Split a leaf into two using SPANN's 2-means implementation.
    ///
    /// After creating two new leaves, the old leaf is removed and the parent
    /// is updated. If the parent exceeds branching_factor, it splits too
    /// (propagating upward). NPA checks reassign boundary vectors that are
    /// farther from their new centroid than from the old one.
    fn split_leaf(&mut self, leaf_id: NodeId, depth: u32) {
        let t0 = Instant::now();

        let (old_ids, old_versions, parent_id, old_centroid) = {
            let Some(TreeNode::Leaf(leaf)) = self.nodes.get(&leaf_id) else {
                return;
            };
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
                let current_ver = self.versions.get(&id).copied().unwrap_or(0);
                if ver >= current_ver {
                    self.embeddings.get(&id).map(|e| (id, ver, e.clone()))
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
            self.replace_child(pid, leaf_id, &[left_id, right_id]);
        } else {
            self.create_root_above(&[left_id, right_id]);
        }

        self.stats.splits += 1;
        self.stats.split_nanos += t0.elapsed().as_nanos() as u64;

        // NPA: reassign vectors that are farther from their new centroid
        // than from the old one (indicates they were assigned to the wrong side).
        if depth < MAX_BALANCE_DEPTH {
            self.npa_split_points(&left_group, &old_centroid, &left_center, depth);
            self.npa_split_points(&right_group, &old_centroid, &right_center, depth);
        }
    }

    fn npa_split_points(
        &mut self,
        group: &[EmbeddingPoint],
        old_center: &[f32],
        new_center: &[f32],
        depth: u32,
    ) {
        for (id, version, embedding) in group {
            let current_ver = self.versions.get(id).copied().unwrap_or(0);
            if *version < current_ver {
                continue;
            }
            let old_dist = self.dist(embedding, old_center);
            let new_dist = self.dist(embedding, new_center);
            if new_dist > old_dist {
                self.reassign(*id, depth);
            }
        }
    }

    /// Reassign a vector: bump version (invalidating old entries), navigate
    /// to find new clusters, register in those clusters, and balance.
    fn reassign(&mut self, id: u32, depth: u32) {
        let t0 = Instant::now();

        let new_version = {
            let v = self.versions.entry(id).or_insert(0);
            *v += 1;
            *v
        };

        let Some(embedding) = self.embeddings.get(&id).cloned() else {
            return;
        };

        let nprobe = self.config.write_nprobe;
        let candidates = self.navigate_core(&embedding, None, nprobe, nprobe);
        let cluster_ids = self.rng_select(&candidates);

        let mut clusters_to_balance = Vec::new();
        for &cluster_id in &cluster_ids {
            if self.register_in_leaf(cluster_id, id, new_version) {
                clusters_to_balance.push(cluster_id);
            }
        }

        self.stats.reassigns += 1;
        self.stats.reassign_nanos += t0.elapsed().as_nanos() as u64;

        for cluster_id in clusters_to_balance {
            self.balance(cluster_id, depth + 1);
        }
    }

    // =========================================================================
    // Split (internal)
    // =========================================================================

    /// Split an internal node that has exceeded the branching factor.
    /// Uses 2-means on children's centroids to partition into two groups.
    fn split_internal(&mut self, node_id: NodeId) {
        let (children, parent_id) = {
            let Some(TreeNode::Internal(internal)) = self.nodes.get(&node_id) else {
                return;
            };
            if internal.children.len() <= self.config.branching_factor {
                return;
            }
            (internal.children.clone(), internal.parent_id)
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
            if let Some(node) = self.nodes.get_mut(&child_id) {
                node.set_parent_id(Some(left_id));
            }
        }
        for &child_id in &right_children {
            if let Some(node) = self.nodes.get_mut(&child_id) {
                node.set_parent_id(Some(right_id));
            }
        }

        self.nodes.remove(&node_id);

        if let Some(pid) = parent_id {
            self.replace_child(pid, node_id, &[left_id, right_id]);
        } else {
            self.create_root_above(&[left_id, right_id]);
        }
    }

    // =========================================================================
    // Merge
    // =========================================================================

    /// Merge a small leaf into its nearest neighbor.
    ///
    /// Mirrors `QuantizedSpannIndexWriter::merge`: find nearest leaf, drop
    /// source, move vectors to target if closer, otherwise full reassign.
    fn merge_leaf(&mut self, leaf_id: NodeId, depth: u32) {
        if depth > MAX_BALANCE_DEPTH {
            return;
        }
        let t0 = Instant::now();

        let (source_centroid, source_ids, source_versions, parent_id) = {
            let Some(TreeNode::Leaf(leaf)) = self.nodes.get(&leaf_id) else {
                return;
            };
            (
                leaf.centroid.clone(),
                leaf.ids.clone(),
                leaf.versions.clone(),
                leaf.parent_id,
            )
        };

        let nprobe = self.config.write_nprobe;
        let candidates = self.navigate_core(&source_centroid, None, nprobe, nprobe);
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
            self.remove_child(pid, leaf_id);
        }

        self.stats.merges += 1;
        self.stats.merge_nanos += t0.elapsed().as_nanos() as u64;

        for (&id, &version) in source_ids.iter().zip(source_versions.iter()) {
            let current_ver = self.versions.get(&id).copied().unwrap_or(0);
            if version < current_ver {
                continue;
            }
            let Some(embedding) = self.embeddings.get(&id).cloned() else {
                continue;
            };

            let dist_to_target = self.dist(&embedding, &target_centroid);
            let dist_to_source = self.dist(&embedding, &source_centroid);

            if dist_to_target <= dist_to_source {
                self.register_in_leaf(target_id, id, version);
            } else {
                self.reassign(id, depth);
            }
        }

        self.balance(target_id, depth + 1);
    }

    // =========================================================================
    // Tree structure helpers
    // =========================================================================

    /// Replace `old_child` in parent's children with `new_children`.
    /// Recomputes parent centroid and triggers internal split if needed.
    fn replace_child(&mut self, parent_id: NodeId, old_child: NodeId, new_children: &[NodeId]) {
        let children_clone = {
            let Some(TreeNode::Internal(parent)) = self.nodes.get_mut(&parent_id) else {
                return;
            };
            parent.children.retain(|&c| c != old_child);
            parent.children.extend_from_slice(new_children);
            parent.children.clone()
        };

        for &child_id in new_children {
            if let Some(node) = self.nodes.get_mut(&child_id) {
                node.set_parent_id(Some(parent_id));
            }
        }

        let new_centroid = self.compute_centroid_of(&children_clone);
        if let Some(TreeNode::Internal(parent)) = self.nodes.get_mut(&parent_id) {
            parent.centroid = new_centroid;
        }

        if children_clone.len() > self.config.branching_factor {
            self.split_internal(parent_id);
        }
    }

    /// Remove a child from a parent. Handles empty/single-child cleanup.
    fn remove_child(&mut self, parent_id: NodeId, child_id: NodeId) {
        let (children_clone, grandparent_id) = {
            let Some(TreeNode::Internal(parent)) = self.nodes.get_mut(&parent_id) else {
                return;
            };
            parent.children.retain(|&c| c != child_id);
            (parent.children.clone(), parent.parent_id)
        };

        if children_clone.is_empty() {
            self.nodes.remove(&parent_id);
            if parent_id == self.root_id {
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
                self.root_id = new_root;
            } else if let Some(gp_id) = grandparent_id {
                self.remove_child(gp_id, parent_id);
            }
        } else if children_clone.len() == 1 && parent_id == self.root_id {
            let only_child = children_clone[0];
            self.nodes.remove(&parent_id);
            if let Some(node) = self.nodes.get_mut(&only_child) {
                node.set_parent_id(None);
            }
            self.root_id = only_child;
        } else {
            let new_centroid = self.compute_centroid_of(&children_clone);
            if let Some(TreeNode::Internal(parent)) = self.nodes.get_mut(&parent_id) {
                parent.centroid = new_centroid;
            }
        }
    }

    fn create_root_above(&mut self, children: &[NodeId]) {
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
            if let Some(node) = self.nodes.get_mut(&child_id) {
                node.set_parent_id(Some(root_id));
            }
        }

        self.root_id = root_id;
    }

    fn compute_centroid_of(&self, children: &[NodeId]) -> Vec<f32> {
        let mut mean = vec![0.0f32; self.dim];
        let mut count = 0;
        for &child_id in children {
            if let Some(node) = self.nodes.get(&child_id) {
                for (i, &v) in node.centroid().iter().enumerate() {
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
    // Search
    // =========================================================================

    /// Search using the config's default tau/beam parameters.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        self.search_with_tau(query, k, self.config.beam_tau, self.config.beam_min, self.config.beam_max)
    }

    /// Search with explicit tau/beam parameters (for sweeping in benchmarks).
    pub fn search_with_tau(
        &self,
        query: &[f32],
        k: usize,
        tau: f64,
        beam_min: usize,
        beam_max: usize,
    ) -> Vec<(u32, f32)> {
        let leaves = self.navigate_core(query, Some(tau), beam_min, beam_max);

        let mut results: Vec<(u32, f32)> = Vec::new();

        for &(leaf_id, _) in &leaves {
            if let Some(TreeNode::Leaf(leaf)) = self.nodes.get(&leaf_id) {
                for (i, &id) in leaf.ids.iter().enumerate() {
                    let version = leaf.versions[i];
                    let current_ver = self.versions.get(&id).copied().unwrap_or(0);
                    if version < current_ver {
                        continue;
                    }
                    if let Some(emb) = self.embeddings.get(&id) {
                        let dist = self.dist(query, emb);
                        results.push((id, dist));
                    }
                }
            }
        }

        let mut best: HashMap<u32, f32> = HashMap::with_capacity(results.len());
        for (id, dist) in results {
            let entry = best.entry(id).or_insert(f32::MAX);
            if dist < *entry {
                *entry = dist;
            }
        }

        let mut deduped: Vec<(u32, f32)> = best.into_iter().collect();
        deduped.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        deduped.truncate(k);
        deduped
    }

    // =========================================================================
    // Per-level recall diagnostics
    // =========================================================================

    /// For each level of the beam search, compute what fraction of the ground
    /// truth keys (R@100) are still reachable from the selected beam nodes.
    /// Reveals whether recall is lost at upper routing levels or at the leaves.
    pub fn diagnose_level_recall(
        &self,
        query: &[f32],
        gt_100: &HashSet<u32>,
        tau: f64,
        beam_min: usize,
        beam_max: usize,
    ) -> Vec<LevelRecall> {
        if matches!(self.nodes.get(&self.root_id), Some(TreeNode::Leaf(_))) {
            let mut reachable = HashSet::new();
            self.collect_all_data_ids(self.root_id, &mut reachable);
            let r100 =
                gt_100.intersection(&reachable).count() as f64 / gt_100.len().max(1) as f64;
            return vec![LevelRecall {
                level: 1,
                reachable_100: r100,
                beam_size: 1,
                total_candidates: 1,
            }];
        }

        let mut levels = Vec::new();
        let mut beam: Vec<NodeId> = vec![self.root_id];

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            for &node_id in &beam {
                if let Some(TreeNode::Internal(internal)) = self.nodes.get(&node_id) {
                    for &child_id in &internal.children {
                        if let Some(child) = self.nodes.get(&child_id) {
                            let dist = self.dist(query, child.centroid());
                            child_scores.push((child_id, dist));
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
                if matches!(self.nodes.get(&node_id), Some(TreeNode::Internal(_))) {
                    next_internals.push(node_id);
                }
            }

            if next_internals.is_empty() {
                break;
            }
            beam = next_internals;
        }

        levels
    }

    /// Recursively collect all valid data vector IDs reachable from a node.
    fn collect_all_data_ids(&self, node_id: NodeId, ids: &mut HashSet<u32>) {
        match self.nodes.get(&node_id) {
            Some(TreeNode::Leaf(leaf)) => {
                for (i, &id) in leaf.ids.iter().enumerate() {
                    let version = leaf.versions[i];
                    let current_ver = self.versions.get(&id).copied().unwrap_or(0);
                    if version >= current_ver {
                        ids.insert(id);
                    }
                }
            }
            Some(TreeNode::Internal(internal)) => {
                for &child_id in &internal.children {
                    self.collect_all_data_ids(child_id, ids);
                }
            }
            None => {}
        }
    }

    // =========================================================================
    // Info / diagnostics
    // =========================================================================

    pub fn depth(&self) -> usize {
        self.depth_of(self.root_id)
    }

    fn depth_of(&self, node_id: NodeId) -> usize {
        match self.nodes.get(&node_id) {
            Some(TreeNode::Leaf(_)) => 1,
            Some(TreeNode::Internal(internal)) => {
                1 + internal
                    .children
                    .iter()
                    .map(|&c| self.depth_of(c))
                    .max()
                    .unwrap_or(0)
            }
            None => 0,
        }
    }

    pub fn leaf_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| matches!(n, TreeNode::Leaf(_)))
            .count()
    }

    pub fn internal_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| matches!(n, TreeNode::Internal(_)))
            .count()
    }

    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    pub fn leaf_sizes(&self) -> Vec<usize> {
        self.nodes
            .values()
            .filter_map(|n| match n {
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
            .values()
            .filter_map(|n| match n {
                TreeNode::Leaf(l) => Some(l.ids.len()),
                _ => None,
            })
            .sum()
    }

    pub fn print_tree_stats(&self, format_count_fn: fn(usize) -> String) {
        let depth = self.depth();

        // BFS to collect per-level stats
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

        let mut queue: Vec<(NodeId, usize)> = vec![(self.root_id, 0)];
        let mut total_leaf_entries = 0usize;

        while let Some((node_id, level)) = queue.pop() {
            if level >= depth {
                continue;
            }
            match self.nodes.get(&node_id) {
                Some(TreeNode::Internal(internal)) => {
                    levels[level].internal_count += 1;
                    levels[level].child_counts.push(internal.children.len());
                    for &child_id in &internal.children {
                        queue.push((child_id, level + 1));
                    }
                }
                Some(TreeNode::Leaf(leaf)) => {
                    levels[level].leaf_count += 1;
                    levels[level].leaf_sizes.push(leaf.ids.len());
                    total_leaf_entries += leaf.ids.len();
                }
                None => {}
            }
        }

        println!("\n  --- Tree Structure ---");

        for (i, ls) in levels.iter().enumerate() {
            let is_last = i == depth - 1;
            let prefix = if i == 0 { "    *  " } else { "    |  " };

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
                println!("    |");
            }
        }

        let replication = if self.total_vectors() > 0 {
            total_leaf_entries as f64 / self.total_vectors() as f64
        } else {
            0.0
        };
        println!(
            "\n  Total entries: {} | Unique vectors: {} | Avg replication: {:.2}x",
            format_count_fn(total_leaf_entries),
            format_count_fn(self.total_vectors()),
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
