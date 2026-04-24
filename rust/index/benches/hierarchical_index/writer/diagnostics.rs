#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use chroma_index::quantization::{Code, QuantizedQuery};

use super::super::common::{NodeId, TreeNode};
use super::{percentile_f32, percentile_usize};
use super::{HierarchicalSpannWriter, LeafMissDiagnostic, LeafTraits, LevelRecall};
use super::super::common::ReadBeamPolicy;

// =============================================================================
// Search + Diagnostics + Tree Info
// =============================================================================

impl HierarchicalSpannWriter {
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
        let policy = ReadBeamPolicy::uniform(Some(tau), beam_min, beam_max);
        self.diagnose_level_recall_with_policy(query, gt_100, &policy)
    }

    pub fn diagnose_level_recall_with_policy(
        &self,
        query: &[f32],
        gt_100: &HashSet<u32>,
        policy: &ReadBeamPolicy,
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

        let q_norm = Self::vec_norm(query);
        let padded_bytes = self.padded_bytes();
        let mut levels = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];

        let qq_abs = QuantizedQuery::new(query, padded_bytes, 0.0, 0.0, q_norm);

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                let code_bytes = child.centroid_code();
                                let dist = Code::<1, _>::new(code_bytes)
                                    .distance_quantized_query(&self.distance_fn, &qq_abs);
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
            let level = levels.len() + 1;
            let params = policy.level_params(level);
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            let effective =
                Self::effective_beam(&child_scores, params.tau, params.beam_min, params.beam_max);
            child_scores.truncate(effective);

            let mut next_internals: Vec<NodeId> = Vec::new();
            for &(node_id, _) in &child_scores {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if matches!(node_ref.value(), TreeNode::Internal(_)) {
                        next_internals.push(node_id);
                    }
                }
            }

            let mut reachable: HashSet<u32> = HashSet::new();
            for &(node_id, _) in &child_scores {
                self.collect_all_data_ids(node_id, &mut reachable);
            }

            let r100 = gt_100.intersection(&reachable).count() as f64 / gt_100.len().max(1) as f64;

            levels.push(LevelRecall {
                level,
                reachable_100: r100,
                beam_size: child_scores.len(),
                total_candidates,
            });

            if next_internals.is_empty() {
                break;
            }
            beam = next_internals;
        }

        levels
    }

    /// For a set of GT vectors, count how many distinct leaves contain at least one.
    /// Returns (p100_clusters, p95_clusters, p90_clusters) via greedy max-coverage ordering.
    pub fn gt_cluster_counts(&self, gt_100: &HashSet<u32>) -> (usize, usize, usize) {
        if gt_100.is_empty() {
            return (0, 0, 0);
        }

        let mut leaf_covers: Vec<HashSet<u32>> = Vec::new();
        for entry in self.nodes.iter() {
            if let TreeNode::Leaf(leaf) = entry.value() {
                let mut covered: HashSet<u32> = HashSet::new();
                for (i, &id) in leaf.ids.iter().enumerate() {
                    if gt_100.contains(&id) {
                        let version = leaf.versions[i];
                        let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                        if version == current_ver {
                            covered.insert(id);
                        }
                    }
                }
                if !covered.is_empty() {
                    leaf_covers.push(covered);
                }
            }
        }

        let total = gt_100.len();
        let p90_target = (total as f64 * 0.90).ceil() as usize;
        let p95_target = (total as f64 * 0.95).ceil() as usize;

        // Greedy max-coverage ordering to find minimum clusters for each threshold.
        let mut uncovered: HashSet<u32> = gt_100.clone();
        let mut covered_count = 0usize;
        let mut picked = 0usize;
        let mut p90 = 0usize;
        let mut p95 = 0usize;
        let mut used = vec![false; leaf_covers.len()];

        while covered_count < total && picked < leaf_covers.len() {
            let best_idx = leaf_covers
                .iter()
                .enumerate()
                .filter(|(i, _)| !used[*i])
                .max_by_key(|(_, covers)| covers.intersection(&uncovered).count())
                .map(|(i, _)| i);
            let Some(idx) = best_idx else { break };
            used[idx] = true;
            let newly: Vec<u32> = leaf_covers[idx].intersection(&uncovered).copied().collect();
            if newly.is_empty() {
                break;
            }
            covered_count += newly.len();
            for id in newly {
                uncovered.remove(&id);
            }
            picked += 1;
            if p90 == 0 && covered_count >= p90_target {
                p90 = picked;
            }
            if p95 == 0 && covered_count >= p95_target {
                p95 = picked;
            }
        }

        let p100 = leaf_covers.iter().filter(|c| !c.is_empty()).count();

        (p100, p95, p90)
    }

    /// Greedy max-coverage: find the best `m` leaves that maximize recall@100.
    pub fn optimal_leaf_recall(&self, gt_100: &HashSet<u32>, m: usize) -> f64 {
        if m == 0 || gt_100.is_empty() {
            return 0.0;
        }

        // For each leaf, find which GT vectors it contains.
        let mut leaf_covers: Vec<(NodeId, HashSet<u32>)> = Vec::new();
        for entry in self.nodes.iter() {
            if let TreeNode::Leaf(leaf) = entry.value() {
                let mut covered: HashSet<u32> = HashSet::new();
                for (i, &id) in leaf.ids.iter().enumerate() {
                    if gt_100.contains(&id) {
                        let version = leaf.versions[i];
                        let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                        if version == current_ver {
                            covered.insert(id);
                        }
                    }
                }
                if !covered.is_empty() {
                    leaf_covers.push((*entry.key(), covered));
                }
            }
        }

        // Greedy max-coverage: repeatedly pick the leaf adding the most uncovered GT vectors.
        let mut uncovered: HashSet<u32> = gt_100.clone();
        let mut total_covered = 0usize;
        for _ in 0..m {
            if uncovered.is_empty() {
                break;
            }
            let best_idx = leaf_covers
                .iter()
                .enumerate()
                .max_by_key(|(_, (_, covers))| covers.intersection(&uncovered).count())
                .map(|(i, _)| i);
            let Some(idx) = best_idx else { break };
            let (_, covers) = &leaf_covers[idx];
            let newly_covered: Vec<u32> = covers.intersection(&uncovered).copied().collect();
            if newly_covered.is_empty() {
                break;
            }
            total_covered += newly_covered.len();
            for id in newly_covered {
                uncovered.remove(&id);
            }
        }

        total_covered as f64 / gt_100.len() as f64
    }

    /// Replay navigation to the leaf level and report, for each missed GT vector,
    /// the best centroid-distance rank of any leaf that contains it.
    pub fn diagnose_leaf_miss_ranks(
        &self,
        query: &[f32],
        gt_100: &HashSet<u32>,
        policy: &ReadBeamPolicy,
    ) -> LeafMissDiagnostic {
        let root = self.root_id();
        let q_norm = Self::vec_norm(query);
        let padded_bytes = self.padded_bytes();

        let qq_abs = QuantizedQuery::new(query, padded_bytes, 0.0, 0.0, q_norm);

        let mut beam: Vec<NodeId> = vec![root];
        let mut level_depth: usize = 0;

        loop {
            level_depth += 1;
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                let code_bytes = child.centroid_code();
                                let dist = Code::<1, _>::new(code_bytes)
                                    .distance_quantized_query(&self.distance_fn, &qq_abs);
                                child_scores.push((child_id, dist));
                            }
                        }
                    }
                }
            }

            if child_scores.is_empty() {
                break;
            }

            let level = 0;
            let _ = level;
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            // Check if this is the leaf level (no internal children).
            let has_internals = child_scores.iter().any(|(nid, _)| {
                self.nodes
                    .get(nid)
                    .map_or(false, |n| matches!(n.value(), TreeNode::Internal(_)))
            });

            if !has_internals {
                // This is the leaf level. Compute the diagnostic.
                // child_scores is sorted by score. Apply rerank if applicable, but
                // we want the FULL sorted list before truncation, plus the truncated beam.

                let total_leaves = child_scores.len();

                // Build rank map: node_id -> 1-indexed rank in sorted order.
                let rank_map: HashMap<NodeId, usize> = child_scores
                    .iter()
                    .enumerate()
                    .map(|(i, (nid, _))| (*nid, i + 1))
                    .collect();

                // Determine the beam (which leaves are selected).
                let params = policy.level_params(level_depth);
                let mut beam_scores = child_scores.clone();

                let effective = Self::effective_beam(
                    &beam_scores,
                    params.tau,
                    params.beam_min,
                    params.beam_max,
                );
                beam_scores.truncate(effective);

                let beam_set: HashSet<NodeId> = beam_scores.iter().map(|(nid, _)| *nid).collect();
                let beam_size = beam_set.len();

                let tau_f = params.tau.unwrap_or(0.0) as f32;
                let search_radius = if !beam_scores.is_empty() {
                    beam_scores[0].1 * (1.0 + tau_f)
                } else {
                    0.0
                };
                let beam_radius = beam_scores.last().map(|(_, s)| *s).unwrap_or(0.0);

                // Build score map for looking up scores by node id.
                let score_map: HashMap<NodeId, f32> = child_scores
                    .iter()
                    .map(|&(nid, score)| (nid, score))
                    .collect();

                // For each leaf, find which GT vectors it contains and compute traits.
                let mut covered_by_beam: HashSet<u32> = HashSet::new();
                let mut gt_in_leaf: HashMap<NodeId, Vec<u32>> = HashMap::new();

                struct LeafInfo {
                    nid: NodeId,
                    leaf_size: usize,
                    gt_ids: Vec<u32>,
                    min_gt_dist: f32,
                }

                let mut leaf_infos: Vec<LeafInfo> = Vec::with_capacity(child_scores.len());

                for &(nid, _) in &child_scores {
                    if let Some(node_ref) = self.nodes.get(&nid) {
                        if let TreeNode::Leaf(leaf) = node_ref.value() {
                            let leaf_size = leaf.ids.len();
                            let mut gt_ids_for_leaf = Vec::new();
                            let mut min_gt_dist = f32::MAX;

                            for (i, &id) in leaf.ids.iter().enumerate() {
                                if gt_100.contains(&id) {
                                    let version = leaf.versions[i];
                                    let current_ver =
                                        self.versions.get(&id).map(|r| *r).unwrap_or(0);
                                    if version == current_ver {
                                        gt_ids_for_leaf.push(id);
                                        if let Some(emb) = self.embeddings.get(&id) {
                                            let d = self.dist(query, &emb);
                                            if d < min_gt_dist {
                                                min_gt_dist = d;
                                            }
                                        }
                                        if beam_set.contains(&nid) {
                                            covered_by_beam.insert(id);
                                        }
                                    }
                                }
                            }

                            gt_in_leaf.entry(nid).or_default().extend(&gt_ids_for_leaf);

                            leaf_infos.push(LeafInfo {
                                nid,
                                leaf_size,
                                gt_ids: gt_ids_for_leaf,
                                min_gt_dist,
                            });
                        }
                    }
                }

                // Build per-category leaf traits.
                let mut selected_with_gt: Vec<LeafTraits> = Vec::new();
                let mut selected_no_gt: Vec<LeafTraits> = Vec::new();
                let mut missed_with_gt: Vec<LeafTraits> = Vec::new();

                for info in &leaf_infos {
                    let rank = rank_map.get(&info.nid).copied().unwrap_or(total_leaves);
                    let score = score_map.get(&info.nid).copied().unwrap_or(f32::MAX);
                    let in_beam = beam_set.contains(&info.nid);
                    let has_gt = !info.gt_ids.is_empty();

                    let traits = LeafTraits {
                        rank,
                        score,
                        leaf_size: info.leaf_size,
                        gt_count: info.gt_ids.len(),
                        min_gt_dist: if has_gt { info.min_gt_dist } else { f32::MAX },
                    };

                    match (in_beam, has_gt) {
                        (true, true) => selected_with_gt.push(traits),
                        (true, false) => selected_no_gt.push(traits),
                        (false, true) => missed_with_gt.push(traits),
                        (false, false) => {} // true negatives -- not interesting
                    }
                }

                // For each missed GT vector, find the best rank of any leaf containing it.
                let mut best_rank_for_gt: HashMap<u32, usize> = HashMap::new();
                for (nid, gt_ids) in &gt_in_leaf {
                    if beam_set.contains(nid) {
                        continue;
                    }
                    let rank = rank_map.get(nid).copied().unwrap_or(total_leaves);
                    for &gid in gt_ids {
                        if !covered_by_beam.contains(&gid) {
                            let entry = best_rank_for_gt.entry(gid).or_insert(rank);
                            if rank < *entry {
                                *entry = rank;
                            }
                        }
                    }
                }

                let mut missed_gt_ranks: Vec<(u32, usize)> = best_rank_for_gt.into_iter().collect();
                missed_gt_ranks.sort_by_key(|&(_, rank)| rank);

                let gt_distances: Vec<f32> = gt_100
                    .iter()
                    .filter_map(|&id| self.embeddings.get(&id).map(|emb| self.dist(query, &emb)))
                    .collect();

                return LeafMissDiagnostic {
                    beam_size,
                    total_leaves,
                    missed_gt_ranks,
                    gt_total: gt_100.len(),
                    selected_with_gt,
                    selected_no_gt,
                    missed_with_gt,
                    search_radius,
                    beam_radius,
                    gt_distances,
                };
            }

            // Not the leaf level yet -- truncate beam and continue down.
            let params = policy.level_params(level_depth);

            let effective =
                Self::effective_beam(&child_scores, params.tau, params.beam_min, params.beam_max);
            child_scores.truncate(effective);

            beam = child_scores
                .iter()
                .filter_map(|(nid, _)| {
                    self.nodes
                        .get(nid)
                        .filter(|n| matches!(n.value(), TreeNode::Internal(_)))
                        .map(|_| *nid)
                })
                .collect();

            if beam.is_empty() {
                break;
            }
        }

        LeafMissDiagnostic {
            beam_size: 0,
            total_leaves: 0,
            missed_gt_ranks: Vec::new(),
            gt_total: gt_100.len(),
            selected_with_gt: Vec::new(),
            selected_no_gt: Vec::new(),
            missed_with_gt: Vec::new(),
            search_radius: 0.0,
            beam_radius: 0.0,
            gt_distances: Vec::new(),
        }
    }

    pub(super) fn collect_all_data_ids(&self, node_id: NodeId, ids: &mut HashSet<u32>) {
        let Some(node_ref) = self.nodes.get(&node_id) else {
            return;
        };
        match node_ref.value() {
            TreeNode::Leaf(leaf) => {
                for (i, &id) in leaf.ids.iter().enumerate() {
                    let version = leaf.versions[i];
                    let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                    if version == current_ver {
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

    /// Estimated in-memory footprint of the writer's owned data structures.
    ///
    /// Counts the bytes the writer explicitly retains in heap-owned
    /// `DashMap`/`Vec` payloads. **Excludes** blockfile reader/cache
    /// pages (which are owned by the `BlockfileProvider`'s caches),
    /// allocator slack, jemalloc retained pages, and the `tokio` runtime.
    /// Per-allocation overheads (DashMap shard locks, capacity slack,
    /// `Arc` headers) are also excluded.
    ///
    /// Use this between checkpoints to see which writer-owned pool is
    /// growing — particularly useful for diagnosing post-reopen
    /// retention (which should ideally drop to near-zero on reopen).
    pub fn memory_usage(&self) -> WriterMemoryUsage {
        let dim = self.dim;
        let code_byte_len = Code::<1, Vec<u8>>::size(dim) as u64;
        let f32_centroid_bytes = (dim as u64) * 4;

        let mut leaf_count: u64 = 0;
        let mut internal_count: u64 = 0;
        let mut tree_bytes: u64 = 0;
        let mut centroid_bytes: u64 = 0;
        let mut posting_entries: u64 = 0;
        let mut posting_bytes: u64 = 0;

        for entry in self.nodes.iter() {
            match entry.value() {
                TreeNode::Leaf(leaf) => {
                    leaf_count += 1;
                    tree_bytes += leaf.centroid_code.len() as u64;
                    if !leaf.centroid.is_empty() {
                        centroid_bytes += f32_centroid_bytes;
                    }
                    let n = leaf.ids.len() as u64;
                    posting_entries += n;
                    // ids (u32) + per-vector codes + versions (u8). The
                    // writer also stores `length` (the persisted size)
                    // separately for lazy-load detection; that's a fixed
                    // 8 bytes per leaf and folded into the per-node
                    // overhead below rather than counted here.
                    posting_bytes += n.saturating_mul(4 + code_byte_len + 1);
                }
                TreeNode::Internal(internal) => {
                    internal_count += 1;
                    tree_bytes += internal.centroid_code.len() as u64;
                    if !internal.centroid.is_empty() {
                        centroid_bytes += f32_centroid_bytes;
                    }
                    tree_bytes += (internal.children.len() as u64).saturating_mul(4);
                }
            }
        }

        let embedding_count = self.embeddings.len() as u64;
        let embedding_bytes = embedding_count.saturating_mul(f32_centroid_bytes);

        let versions_count = self.versions.len() as u64;
        // DashMap<u32, u8> entry: ~5 bytes payload + per-entry hash
        // bookkeeping. Use 5 to count payload only; documented as
        // "payload" in the struct.
        let versions_bytes = versions_count.saturating_mul(5);

        let tombstones_count = self.tombstones.len() as u64;
        let balancing_count = self.balancing.len() as u64;
        let dirty_nodes_count = self.dirty_nodes.len() as u64;
        let dirty_versions_count = self.dirty_versions.len() as u64;
        let dirty_embeddings_count = self.dirty_embeddings.len() as u64;
        // DashSet<u32> entry: 4 bytes payload.
        let small_sets_bytes = tombstones_count
            .saturating_add(balancing_count)
            .saturating_add(dirty_nodes_count)
            .saturating_add(dirty_versions_count)
            .saturating_add(dirty_embeddings_count)
            .saturating_mul(4);

        WriterMemoryUsage {
            dim,
            leaf_count,
            internal_count,
            tree_bytes,
            centroid_bytes,
            posting_entries,
            posting_bytes,
            embedding_count,
            embedding_bytes,
            versions_count,
            versions_bytes,
            tombstones_count,
            balancing_count,
            dirty_nodes_count,
            dirty_versions_count,
            dirty_embeddings_count,
            small_sets_bytes,
        }
    }

    pub fn total_leaf_entries(&self) -> usize {
        self.nodes
            .iter()
            .filter_map(|entry| match entry.value() {
                // Materialized leaves: live ids count. Lazy shells (ids empty
                // but length>0): the persisted length, since the actual entries
                // live on disk and have not been loaded yet.
                TreeNode::Leaf(l) => Some(if l.ids.is_empty() {
                    l.length
                } else {
                    l.ids.len()
                }),
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
                    if version == current_ver {
                        valid_ids.insert(id);
                    }
                }
            }
        }
        self.embeddings.len().saturating_sub(valid_ids.len())
    }

    /// `canonical_indexed_total`: vectors indexed in this benchmark run (pass when the writer was
    /// reopened and `embeddings` is empty, or to align with checkpoint totals).
    pub fn print_tree_stats(
        &self,
        format_count_fn: fn(usize) -> String,
        canonical_indexed_total: Option<usize>,
    ) {
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
                        let size = if leaf.ids.is_empty() {
                            leaf.length
                        } else {
                            leaf.ids.len()
                        };
                        levels[level].leaf_sizes.push(size);
                        total_leaf_entries += size;
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

        // Prefer the canonical run-level total (e.g. checkpoint-aggregate count from the bench)
        // when supplied: after a reopen the writer's `embeddings` map starts empty, so
        // `self.total_vectors()` would underreport.
        let total_vectors = canonical_indexed_total.unwrap_or_else(|| self.total_vectors());
        let orphaned = self.count_orphaned_vectors();
        let mut live_entry_counts: HashMap<u32, usize> = HashMap::new();
        // Track which leaves each vector appears in (for replica distance analysis).
        let mut vector_leaves: HashMap<u32, Vec<NodeId>> = HashMap::new();
        let valid_entries: usize = self
            .nodes
            .iter()
            .filter_map(|entry| {
                let nid = *entry.key();
                match entry.value() {
                    TreeNode::Leaf(leaf) => Some(
                        leaf.ids
                            .iter()
                            .enumerate()
                            .filter(|&(i, &id)| {
                                let ver = leaf.versions[i];
                                let cur = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                                ver == cur
                            })
                            .inspect(|&(_, &id)| {
                                *live_entry_counts.entry(id).or_default() += 1;
                                vector_leaves.entry(id).or_default().push(nid);
                            })
                            .count(),
                    ),
                    _ => None,
                }
            })
            .sum();
        let live_vectors = total_vectors.saturating_sub(orphaned);
        let valid_replication = if total_vectors > 0 && orphaned < total_vectors {
            valid_entries as f64 / live_vectors as f64
        } else {
            0.0
        };
        let vectors_with_replicas = live_entry_counts
            .values()
            .filter(|&&count| count > 1)
            .count();
        let replica_pct = if live_vectors > 0 {
            vectors_with_replicas as f64 * 100.0 / live_vectors as f64
        } else {
            0.0
        };
        println!(
            "\nTotal entries: {} ({} valid) | Unique vectors: {} ({} orphaned) | Avg replication: {:.2}x | % w/ replica: {:.1}%",
            format_count_fn(total_leaf_entries),
            format_count_fn(valid_entries),
            format_count_fn(total_vectors),
            format_count_fn(orphaned),
            valid_replication,
            replica_pct,
        );

        if vectors_with_replicas > 0 {
            // Replica count distribution.
            let mut replica_counts: Vec<usize> = live_entry_counts
                .values()
                .filter(|&&c| c > 1)
                .copied()
                .collect();
            replica_counts.sort_unstable();
            let avg_rep = replica_counts.iter().sum::<usize>() as f64 / replica_counts.len() as f64;

            let mut count_histogram: HashMap<usize, usize> = HashMap::new();
            for &c in &replica_counts {
                *count_histogram.entry(c).or_default() += 1;
            }
            let mut hist_keys: Vec<usize> = count_histogram.keys().copied().collect();
            hist_keys.sort_unstable();
            let hist_str: String = hist_keys
                .iter()
                .map(|k| format!("{}x={}", k, count_histogram[k]))
                .collect::<Vec<_>>()
                .join(" ");

            println!(
                "  Replicated vectors: {} | Avg copies: {:.2} | Distribution: {}",
                format_count_fn(vectors_with_replicas),
                avg_rep,
                hist_str,
            );

            // For replicated vectors, compute distance stats.
            // Sample to keep this tractable on large datasets.
            let sample_cap = 100_000usize;
            let mut replicated_vids: Vec<u32> = vector_leaves
                .keys()
                .filter(|vid| vector_leaves[vid].len() >= 2)
                .copied()
                .collect();
            replicated_vids.sort_unstable();
            if replicated_vids.len() > sample_cap {
                let step = replicated_vids.len() as f64 / sample_cap as f64;
                replicated_vids = (0..sample_cap)
                    .map(|i| replicated_vids[(i as f64 * step) as usize])
                    .collect();
            }

            let mut d_ratio_values: Vec<f32> = Vec::new();
            let mut inter_centroid_dists: Vec<f32> = Vec::new();
            let mut d_nearest_values: Vec<f32> = Vec::new();
            let mut d_farthest_values: Vec<f32> = Vec::new();

            for &vid in &replicated_vids {
                let leaves = &vector_leaves[&vid];
                let emb = match self.embeddings.get(&vid) {
                    Some(e) => e.value().clone(),
                    None => continue,
                };

                let mut dists: Vec<f32> = leaves
                    .iter()
                    .filter_map(|&nid| self.nodes.get(&nid).map(|n| self.dist(&emb, n.centroid())))
                    .collect();
                dists.sort_by(|a, b| a.partial_cmp(b).unwrap());

                if dists.len() >= 2 {
                    let d1 = dists[0].max(1e-10);
                    d_ratio_values.push(dists[1] / d1);
                    d_nearest_values.push(dists[0]);
                    d_farthest_values.push(*dists.last().unwrap());
                }

                let centroids: Vec<Vec<f32>> = leaves
                    .iter()
                    .filter_map(|&nid| self.nodes.get(&nid).map(|n| n.centroid().to_vec()))
                    .collect();
                for i in 0..centroids.len() {
                    for j in (i + 1)..centroids.len() {
                        inter_centroid_dists.push(self.dist(&centroids[i], &centroids[j]));
                    }
                }
            }

            if !d_ratio_values.is_empty() {
                d_ratio_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                d_nearest_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                d_farthest_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                inter_centroid_dists.sort_by(|a, b| a.partial_cmp(b).unwrap());

                let pf = |v: &[f32], p: f64| v[(p * (v.len() - 1) as f64) as usize];
                let favg = |v: &[f32]| v.iter().map(|x| *x as f64).sum::<f64>() / v.len() as f64;

                println!(
                    "  {:30}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}",
                    "metric", "min", "p25", "p50", "avg", "p75", "p90", "max"
                );
                println!(
                    "  {:30}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}",
                    "------------------------------",
                    "-------",
                    "-------",
                    "-------",
                    "-------",
                    "-------",
                    "-------",
                    "-------"
                );
                println!(
                    "  {:30}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}",
                    "d2/d1 (boundary proximity)",
                    d_ratio_values[0],
                    pf(&d_ratio_values, 0.25),
                    pf(&d_ratio_values, 0.5),
                    favg(&d_ratio_values),
                    pf(&d_ratio_values, 0.75),
                    pf(&d_ratio_values, 0.9),
                    d_ratio_values[d_ratio_values.len() - 1]
                );
                println!(
                    "  {:30}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7}  {:>7.4}",
                    "d_nearest (to closest cent.)",
                    d_nearest_values[0],
                    pf(&d_nearest_values, 0.25),
                    pf(&d_nearest_values, 0.5),
                    favg(&d_nearest_values),
                    pf(&d_nearest_values, 0.75),
                    "",
                    d_nearest_values[d_nearest_values.len() - 1]
                );
                println!(
                    "  {:30}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7}  {:>7.4}",
                    "d_farthest (to farthest cent.)",
                    d_farthest_values[0],
                    pf(&d_farthest_values, 0.25),
                    pf(&d_farthest_values, 0.5),
                    favg(&d_farthest_values),
                    pf(&d_farthest_values, 0.75),
                    "",
                    d_farthest_values[d_farthest_values.len() - 1]
                );
                println!(
                    "  {:30}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7}  {:>7.4}",
                    "inter-centroid dist",
                    inter_centroid_dists[0],
                    pf(&inter_centroid_dists, 0.25),
                    pf(&inter_centroid_dists, 0.5),
                    favg(&inter_centroid_dists),
                    pf(&inter_centroid_dists, 0.75),
                    "",
                    inter_centroid_dists[inter_centroid_dists.len() - 1]
                );
            }
        }
    }
}

/// Estimated in-memory footprint breakdown for a `HierarchicalSpannWriter`.
/// Mirrors `ReaderMemoryUsage` in `reader.rs`, plus the writer-specific
/// `versions` / `tombstones` / `balancing` pools.
///
/// All byte counts are estimates of the *payload* size of the owned
/// containers and exclude per-allocation overhead, allocator slack, and
/// jemalloc-retained dirty pages.
#[derive(Debug, Clone, Copy)]
pub struct WriterMemoryUsage {
    pub dim: usize,
    pub leaf_count: u64,
    pub internal_count: u64,
    /// `centroid_code` (1-bit RaBitQ) on every node + `children` Vec
    /// payloads on internal nodes.
    pub tree_bytes: u64,
    /// Full-precision (f32) centroids on leaves/internals. Present on
    /// the writer for nodes touched on the write path; absent on lazy
    /// shells.
    pub centroid_bytes: u64,
    /// Sum of materialized leaf `ids.len()` across the tree.
    pub posting_entries: u64,
    /// `posting_entries * (4 [id] + code_size + 1 [version])`.
    pub posting_bytes: u64,
    /// Per-vector full-precision embeddings retained in the writer's
    /// `embeddings` DashMap (used for full-precision distance recompute
    /// during NPA / balance).
    pub embedding_count: u64,
    pub embedding_bytes: u64,
    /// Per-vector u8 versions in the writer's `versions` DashMap.
    pub versions_count: u64,
    pub versions_bytes: u64,
    /// Tombstoned NodeIds awaiting commit-time deletion.
    pub tombstones_count: u64,
    /// Currently-balancing NodeIds (transient, normally 0 at boundary).
    pub balancing_count: u64,
    /// NodeIds inserted or in-place mutated since the last commit. Drives the
    /// per-node iteration in `commit()` (`dirty_nodes`).
    pub dirty_nodes_count: u64,
    /// Vector ids whose `versions` entry was bumped since the last commit
    /// (`dirty_versions`).
    pub dirty_versions_count: u64,
    /// Vector ids whose `embeddings` entry was inserted since the last
    /// commit (`dirty_embeddings`).
    pub dirty_embeddings_count: u64,
    /// Combined payload bytes for `tombstones` + `balancing` + dirty sets.
    pub small_sets_bytes: u64,
}

impl WriterMemoryUsage {
    pub fn total_bytes(&self) -> u64 {
        self.tree_bytes
            .saturating_add(self.centroid_bytes)
            .saturating_add(self.posting_bytes)
            .saturating_add(self.embedding_bytes)
            .saturating_add(self.versions_bytes)
            .saturating_add(self.small_sets_bytes)
    }
}
