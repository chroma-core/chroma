#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use chroma_index::quantization::{Code, QuantizedQuery};
use simsimd::SpatialSimilarity;

use super::{
    code_slice, HierarchicalSpannWriter, LeafMissDiagnostic, LeafTraits, LevelRecall, NavigationMode,
    NodeId, ReadBeamPolicy, SearchTimings, TreeNode,
};
use super::{percentile_f32, percentile_usize};

// =============================================================================
// Search + Diagnostics + Tree Info
// =============================================================================

impl HierarchicalSpannWriter {
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
        nav_mode: NavigationMode,
    ) -> (Vec<(u32, f32)>, usize, usize, SearchTimings) {
        let policy = ReadBeamPolicy::uniform(Some(tau), beam_min, beam_max);
        self.search_with_policy(query, k, rerank_centroids, rerank_vectors, nav_mode, &policy)
    }

    pub fn search_with_policy(
        &self,
        query: &[f32],
        k: usize,
        rerank_centroids: usize,
        rerank_vectors: usize,
        nav_mode: NavigationMode,
        policy: &ReadBeamPolicy,
    ) -> (Vec<(u32, f32)>, usize, usize, SearchTimings) {
        let nav_t0 = Instant::now();
        let leaves = self.navigate_with_policy(query, rerank_centroids, nav_mode, policy);
        let navigate_nanos = nav_t0.elapsed().as_nanos() as u64;

        let leaves_scanned = leaves.len();
        let padded_bytes = self.padded_bytes();
        let code_size = self.code_size();
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

            // Search runs after writes complete, so we skip the global version-map lookup here
            // and just score the leaf-local codes directly. That keeps this loop streaming over
            // contiguous leaf storage instead of doing a random DashMap probe per vector.
            results.reserve(leaf.ids.len());
            let dt0 = Instant::now();
            for (i, &id) in leaf.ids.iter().enumerate() {
                let dist = Code::<1, _>::new(code_slice(&leaf.codes, i, code_size))
                    .distance_quantized_query(&self.distance_fn, &qq);
                results.push((id, dist));
            }
            distance_nanos += dt0.elapsed().as_nanos() as u64;
        }

        let sort_t0 = Instant::now();
        let m = (k * rerank_factor).max(k);
        let scanned;
        let mut deduped: Vec<(u32, f32)> = if self.config.max_replicas == 1 {
            // With replica count fixed at 1, a vector should only appear once in live search
            // results, so we can skip the HashMap-based dedup pass entirely.
            scanned = results.len();
            results
        } else {
            // Deduplicate (same vector in multiple leaves)
            let mut best: std::collections::HashMap<u32, f32> =
                std::collections::HashMap::with_capacity(results.len());
            for (id, dist) in results {
                let entry = best.entry(id).or_insert(f32::MAX);
                if dist < *entry {
                    *entry = dist;
                }
            }
            scanned = best.len();
            best.into_iter().collect()
        };

        if deduped.len() > m {
            let nth = m - 1;
            deduped.select_nth_unstable_by(nth, |a, b| a.1.partial_cmp(&b.1).unwrap());
            deduped.truncate(m);
        }
        deduped.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let sort_dedup_nanos = sort_t0.elapsed().as_nanos() as u64;

        if rerank_factor > 1 {
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
        nav_mode: NavigationMode,
    ) -> Vec<LevelRecall> {
        let policy = ReadBeamPolicy::uniform(Some(tau), beam_min, beam_max);
        self.diagnose_level_recall_with_policy(query, gt_100, rerank_centroids, nav_mode, &policy)
    }

    pub fn diagnose_level_recall_with_policy(
        &self,
        query: &[f32],
        gt_100: &HashSet<u32>,
        rerank_centroids: usize,
        nav_mode: NavigationMode,
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

        let nav_mode = nav_mode;
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

                        let c_norm = Self::vec_norm(&parent_centroid);
                        let r_q: Vec<f32> = query.iter().zip(parent_centroid.iter()).map(|(q, c)| q - c).collect();
                        let c_dot_q = f32::dot(&parent_centroid, query).unwrap_or(0.0) as f32;

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
            let level = levels.len() + 1;
            let params = policy.level_params(level);
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            if nav_mode != NavigationMode::Fp && rerank_factor > 1 {
                let effective = Self::effective_beam(
                    &child_scores,
                    params.tau,
                    params.beam_min,
                    params.beam_max,
                );
                let rerank_count = (effective * rerank_factor).min(child_scores.len());
                child_scores.truncate(rerank_count);
                let mut reranked: Vec<(NodeId, f32)> = child_scores
                    .iter()
                    .map(|&(nid, _)| {
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
                        if version >= current_ver {
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
                        if version >= current_ver {
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
        rerank_centroids: usize,
        nav_mode: NavigationMode,
        policy: &ReadBeamPolicy,
    ) -> LeafMissDiagnostic {
        let root = self.root_id();
        let q_norm = Self::vec_norm(query);
        let padded_bytes = self.padded_bytes();
        let rerank_factor = rerank_centroids;
        let dim = self.dim;

        let mut beam: Vec<NodeId> = vec![root];
        let mut level_depth: usize = 0;

        loop {
            level_depth += 1;
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let parent_centroid = internal.centroid.clone();
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        let c_norm = Self::vec_norm(&parent_centroid);
                        let r_q: Vec<f32> = query.iter().zip(parent_centroid.iter()).map(|(q, c)| q - c).collect();
                        let c_dot_q = f32::dot(&parent_centroid, query).unwrap_or(0.0) as f32;

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
                                let query_code = Code::<1>::quantize(query, &parent_centroid);
                                for child_id in children {
                                    if let Some(child) = self.nodes.get(&child_id) {
                                        let code_bytes = child.centroid_code();
                                        let dist = if code_bytes.is_empty() {
                                            self.dist(query, child.centroid())
                                        } else {
                                            let child_code = Code::<1, _>::new(code_bytes);
                                            query_code.distance_code(&child_code, &self.distance_fn, c_norm, dim)
                                        };
                                        child_scores.push((child_id, dist));
                                    }
                                }
                            }
                            NavigationMode::FourBit => {
                                let qq = QuantizedQuery::new(&r_q, padded_bytes, c_norm, c_dot_q, q_norm);
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

            let level = 0; // only need level for params lookup; we count from beam depth
            let _ = level;
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            // Check if this is the leaf level (no internal children).
            let has_internals = child_scores.iter().any(|(nid, _)| {
                self.nodes.get(nid).map_or(false, |n| matches!(n.value(), TreeNode::Internal(_)))
            });

            if !has_internals {
                // This is the leaf level. Compute the diagnostic.
                // child_scores is sorted by score. Apply rerank if applicable, but
                // we want the FULL sorted list before truncation, plus the truncated beam.

                // If quantized nav + rerank, re-score with fp distances.
                if nav_mode != NavigationMode::Fp && rerank_factor > 1 {
                    // Rerank path: the actual beam is computed from reranked top candidates.
                    // But we want ranks in the ORIGINAL scoring for the diagnostic,
                    // because that's what determines which leaves get into the rerank set.
                    // So we report ranks from the pre-rerank sorted order.
                    // (The rerank can only shuffle within the rerank window, not rescue
                    // leaves that were already cut.)
                }

                let total_leaves = child_scores.len();

                // Build rank map: node_id -> 1-indexed rank in sorted order.
                let rank_map: HashMap<NodeId, usize> = child_scores.iter().enumerate()
                    .map(|(i, (nid, _))| (*nid, i + 1))
                    .collect();

                // Determine the beam (which leaves are selected).
                let params = policy.level_params(level_depth);
                let mut beam_scores = child_scores.clone();

                if nav_mode != NavigationMode::Fp && rerank_factor > 1 {
                    let effective = Self::effective_beam(&beam_scores, params.tau, params.beam_min, params.beam_max);
                    let rerank_count = (effective * rerank_factor).min(beam_scores.len());
                    beam_scores.truncate(rerank_count);
                    let mut reranked: Vec<(NodeId, f32)> = beam_scores.iter()
                        .map(|&(nid, _)| {
                            let dist = self.nodes.get(&nid)
                                .map(|n| self.dist(query, n.centroid()))
                                .unwrap_or(f32::MAX);
                            (nid, dist)
                        })
                        .collect();
                    reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                    let final_beam = Self::effective_beam(&reranked, params.tau, params.beam_min, params.beam_max);
                    reranked.truncate(final_beam);
                    beam_scores = reranked;
                } else {
                    let effective = Self::effective_beam(&beam_scores, params.tau, params.beam_min, params.beam_max);
                    beam_scores.truncate(effective);
                }

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
                let score_map: HashMap<NodeId, f32> = child_scores.iter()
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
                                    let current_ver = self.versions.get(&id).map(|r| *r).unwrap_or(0);
                                    if version >= current_ver {
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

                let gt_distances: Vec<f32> = gt_100.iter()
                    .filter_map(|&id| {
                        self.embeddings.get(&id).map(|emb| self.dist(query, &emb))
                    })
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

            if nav_mode != NavigationMode::Fp && rerank_factor > 1 {
                let effective = Self::effective_beam(&child_scores, params.tau, params.beam_min, params.beam_max);
                let rerank_count = (effective * rerank_factor).min(child_scores.len());
                child_scores.truncate(rerank_count);
                let mut reranked: Vec<(NodeId, f32)> = child_scores.iter()
                    .map(|&(nid, _)| {
                        let dist = self.nodes.get(&nid)
                            .map(|n| self.dist(query, n.centroid()))
                            .unwrap_or(f32::MAX);
                        (nid, dist)
                    })
                    .collect();
                reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let final_beam = Self::effective_beam(&reranked, params.tau, params.beam_min, params.beam_max);
                reranked.truncate(final_beam);
                child_scores = reranked;
            } else {
                let effective = Self::effective_beam(&child_scores, params.tau, params.beam_min, params.beam_max);
                child_scores.truncate(effective);
            }

            beam = child_scores.iter()
                .filter_map(|(nid, _)| {
                    self.nodes.get(nid)
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
                                ver >= cur
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
            let mut replica_counts: Vec<usize> = live_entry_counts.values()
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
            let hist_str: String = hist_keys.iter()
                .map(|k| format!("{}x={}", k, count_histogram[k]))
                .collect::<Vec<_>>()
                .join(" ");

            println!("  Replicated vectors: {} | Avg copies: {:.2} | Distribution: {}",
                format_count_fn(vectors_with_replicas),
                avg_rep,
                hist_str,
            );

            // For replicated vectors, compute distance stats.
            // Sample to keep this tractable on large datasets.
            let sample_cap = 100_000usize;
            let mut replicated_vids: Vec<u32> = vector_leaves.keys()
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

                let mut dists: Vec<f32> = leaves.iter()
                    .filter_map(|&nid| {
                        self.nodes.get(&nid).map(|n| self.dist(&emb, n.centroid()))
                    })
                    .collect();
                dists.sort_by(|a, b| a.partial_cmp(b).unwrap());

                if dists.len() >= 2 {
                    let d1 = dists[0].max(1e-10);
                    d_ratio_values.push(dists[1] / d1);
                    d_nearest_values.push(dists[0]);
                    d_farthest_values.push(*dists.last().unwrap());
                }

                let centroids: Vec<Vec<f32>> = leaves.iter()
                    .filter_map(|&nid| self.nodes.get(&nid).map(|n| n.centroid().to_vec()))
                    .collect();
                for i in 0..centroids.len() {
                    for j in (i+1)..centroids.len() {
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

                println!("  {:30}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}",
                    "metric", "min", "p25", "p50", "avg", "p75", "p90", "max");
                println!("  {:30}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}",
                    "------------------------------", "-------", "-------", "-------", "-------", "-------", "-------", "-------");
                println!("  {:30}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}",
                    "d2/d1 (boundary proximity)",
                    d_ratio_values[0], pf(&d_ratio_values, 0.25), pf(&d_ratio_values, 0.5),
                    favg(&d_ratio_values), pf(&d_ratio_values, 0.75), pf(&d_ratio_values, 0.9),
                    d_ratio_values[d_ratio_values.len()-1]);
                println!("  {:30}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7}  {:>7.4}",
                    "d_nearest (to closest cent.)",
                    d_nearest_values[0], pf(&d_nearest_values, 0.25), pf(&d_nearest_values, 0.5),
                    favg(&d_nearest_values), pf(&d_nearest_values, 0.75), "", 
                    d_nearest_values[d_nearest_values.len()-1]);
                println!("  {:30}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7}  {:>7.4}",
                    "d_farthest (to farthest cent.)",
                    d_farthest_values[0], pf(&d_farthest_values, 0.25), pf(&d_farthest_values, 0.5),
                    favg(&d_farthest_values), pf(&d_farthest_values, 0.75), "",
                    d_farthest_values[d_farthest_values.len()-1]);
                println!("  {:30}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7}  {:>7.4}",
                    "inter-centroid dist",
                    inter_centroid_dists[0], pf(&inter_centroid_dists, 0.25), pf(&inter_centroid_dists, 0.5),
                    favg(&inter_centroid_dists), pf(&inter_centroid_dists, 0.75), "",
                    inter_centroid_dists[inter_centroid_dists.len()-1]);
            }
        }
    }
}
