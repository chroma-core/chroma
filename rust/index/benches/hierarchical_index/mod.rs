//! Hierarchical centroid tree index for SPANN-style centroid lookup.
//!
//! Builds a k-means tree top-down, with optional SPANN-style balanced clustering
//! (lambda-penalized k-means) and posting list expansion (boundary vector replication).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery};
use chroma_index::spann::utils::{KMeansAlgorithmInput, KMeansAlgorithmOutput};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use simsimd::SpatialSimilarity;

// =============================================================================
// Distance helpers
// =============================================================================

pub fn compute_distance(a: &[f32], b: &[f32], df: &DistanceFunction) -> f32 {
    match df {
        DistanceFunction::Euclidean => f32::sqeuclidean(a, b).unwrap_or(f64::MAX) as f32,
        DistanceFunction::InnerProduct => {
            let ip = f32::inner(a, b).unwrap_or(0.0) as f32;
            1.0 - ip
        }
        DistanceFunction::Cosine => f32::cosine(a, b).unwrap_or(f64::MAX) as f32,
    }
}

fn sqeuclidean_fast(a: &[f32], b: &[f32]) -> f32 {
    f32::sqeuclidean(a, b).unwrap_or(f64::MAX) as f32
}

fn dedup_and_topk(results: &mut Vec<(u32, f32)>, k: usize) -> Vec<(u32, f32)> {
    let mut best: HashMap<u32, f32> = HashMap::with_capacity(results.len());
    for &(key, dist) in results.iter() {
        let entry = best.entry(key).or_insert(f32::MAX);
        if dist < *entry {
            *entry = dist;
        }
    }
    let mut deduped: Vec<(u32, f32)> = best.into_iter().collect();
    deduped.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    deduped.truncate(k);
    deduped
}

// =============================================================================
// K-means helpers (unbalanced, used as fallback)
// =============================================================================

pub fn compute_mean(data: &[f32], n: usize, dim: usize) -> Vec<f32> {
    let mut mean = vec![0.0f32; dim];
    for i in 0..n {
        let point = &data[i * dim..(i + 1) * dim];
        for d in 0..dim {
            mean[d] += point[d];
        }
    }
    let scale = 1.0 / n as f32;
    for v in &mut mean {
        *v *= scale;
    }
    mean
}

fn reorder_flat(data: &[f32], order: &[usize], dim: usize) -> Vec<f32> {
    let n = order.len();
    let mut result = vec![0.0f32; n * dim];
    for (new_pos, &old_pos) in order.iter().enumerate() {
        result[new_pos * dim..(new_pos + 1) * dim]
            .copy_from_slice(&data[old_pos * dim..(old_pos + 1) * dim]);
    }
    result
}

fn compute_group_ranges(sorted_assignments: &[usize], k: usize) -> Vec<usize> {
    let mut counts = vec![0usize; k];
    for &a in sorted_assignments {
        counts[a] += 1;
    }
    let mut ranges = vec![0usize; k + 1];
    for i in 0..k {
        ranges[i + 1] = ranges[i] + counts[i];
    }
    ranges
}

fn kmeans_pp_init(data: &[f32], n: usize, k: usize, dim: usize, rng: &mut StdRng) -> Vec<f32> {
    let mut centers = Vec::with_capacity(k * dim);

    let idx = rng.gen_range(0..n);
    centers.extend_from_slice(&data[idx * dim..(idx + 1) * dim]);

    let mut min_dists = vec![f32::MAX; n];

    for c in 1..k {
        let last_center = centers[(c - 1) * dim..c * dim].to_vec();

        min_dists
            .par_iter_mut()
            .enumerate()
            .for_each(|(i, md)| {
                let point = &data[i * dim..(i + 1) * dim];
                let d = sqeuclidean_fast(point, &last_center);
                *md = md.min(d);
            });

        let total: f64 = min_dists.iter().map(|&d| d as f64).sum();
        if total <= 0.0 {
            let idx = rng.gen_range(0..n);
            centers.extend_from_slice(&data[idx * dim..(idx + 1) * dim]);
            continue;
        }

        let threshold = rng.gen::<f64>() * total;
        let mut cumsum = 0.0;
        let mut chosen = n - 1;
        for (i, &d) in min_dists.iter().enumerate() {
            cumsum += d as f64;
            if cumsum >= threshold {
                chosen = i;
                break;
            }
        }
        centers.extend_from_slice(&data[chosen * dim..(chosen + 1) * dim]);
    }

    centers
}

fn kmeans_unbalanced(
    data: &[f32],
    n: usize,
    k: usize,
    dim: usize,
    max_iters: usize,
) -> (Vec<usize>, Vec<f32>) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut centers = kmeans_pp_init(data, n, k, dim, &mut rng);
    let mut assignments = vec![0usize; n];

    for _ in 0..max_iters {
        assignments = (0..n)
            .into_par_iter()
            .map(|i| {
                let point = &data[i * dim..(i + 1) * dim];
                let mut best = 0;
                let mut best_dist = f32::MAX;
                for j in 0..k {
                    let center = &centers[j * dim..(j + 1) * dim];
                    let d = sqeuclidean_fast(point, center);
                    if d < best_dist {
                        best_dist = d;
                        best = j;
                    }
                }
                best
            })
            .collect();

        let mut new_centers = vec![0.0f32; k * dim];
        let mut counts = vec![0usize; k];

        for i in 0..n {
            let cluster = assignments[i];
            counts[cluster] += 1;
            let point = &data[i * dim..(i + 1) * dim];
            let center = &mut new_centers[cluster * dim..(cluster + 1) * dim];
            for d in 0..dim {
                center[d] += point[d];
            }
        }

        for j in 0..k {
            if counts[j] > 0 {
                let center = &mut new_centers[j * dim..(j + 1) * dim];
                let scale = 1.0 / counts[j] as f32;
                for v in center.iter_mut() {
                    *v *= scale;
                }
            } else {
                let idx = rng.gen_range(0..n);
                new_centers[j * dim..(j + 1) * dim]
                    .copy_from_slice(&data[idx * dim..(idx + 1) * dim]);
            }
        }

        centers = new_centers;
    }

    (assignments, centers)
}

// =============================================================================
// Balanced k-means using SPANN's lambda-penalized cluster()
// =============================================================================

fn kmeans_balanced(
    data: &[f32],
    n: usize,
    k: usize,
    dim: usize,
    distance_fn: &DistanceFunction,
    initial_lambda: f32,
) -> (Vec<usize>, Vec<f32>) {
    let embeddings: Vec<Arc<[f32]>> = (0..n)
        .map(|i| Arc::from(&data[i * dim..(i + 1) * dim]))
        .collect();
    let indices: Vec<usize> = (0..n).collect();

    let num_samples = n.min(10_000);

    let mut input = KMeansAlgorithmInput::new(
        indices,
        &embeddings,
        dim,
        k,
        0,
        n,
        num_samples,
        distance_fn.clone(),
        initial_lambda,
    );

    let output: KMeansAlgorithmOutput = chroma_index::spann::utils::cluster(&mut input)
        .expect("Balanced k-means clustering failed");

    let mut assignments = vec![0usize; n];
    for (&idx, &label) in output.cluster_labels.iter() {
        assignments[idx] = label as usize;
    }

    let mut centers_flat = vec![0.0f32; k * dim];
    for (j, center) in output.cluster_centers.iter().enumerate() {
        if j < k {
            centers_flat[j * dim..(j + 1) * dim].copy_from_slice(center);
        }
    }

    (assignments, centers_flat)
}

// =============================================================================
// Hierarchical centroid tree
// =============================================================================

#[derive(Serialize, Deserialize)]
pub enum CentroidTreeNode {
    Leaf {
        keys: Vec<u32>,
        centroids: Vec<f32>,
        codes: Option<Vec<u8>>,
    },
    Internal {
        centers: Vec<f32>,
        codes: Option<Vec<u8>>,
        children: Vec<CentroidTreeNode>,
    },
}

pub struct HierarchicalCentroidIndex {
    pub root: CentroidTreeNode,
    pub dim: usize,
    pub beam_width: usize,
    /// Dynamic beam: include child if dist <= d_best * (1 + beam_tau), bounded by [beam_min, beam_max].
    /// None = fixed beam (beam_width). Some(tau) = query-aware pruning (SPFresh-style).
    pub beam_tau: Option<f64>,
    pub beam_min: usize,
    pub beam_max: usize,
    pub distance_fn: DistanceFunction,
    pub quantization_center: Option<Vec<f32>>,
    pub code_size: usize,
    pub overflow: Mutex<Vec<(u32, Vec<f32>)>>,
    pub tombstones: Mutex<HashSet<u32>>,
    pub tree_size: usize,
}

pub struct TreeBuildConfig {
    pub branching_factor: usize,
    pub beam_width: usize,
    pub expansion_factor: f64,
    pub max_replicas: usize,
    pub kmeans_iters: usize,
    pub balanced: bool,
    pub initial_lambda: f32,
}

fn do_kmeans(
    data: &[f32],
    n: usize,
    k: usize,
    dim: usize,
    cfg: &TreeBuildConfig,
    distance_fn: &DistanceFunction,
) -> (Vec<usize>, Vec<f32>) {
    if cfg.balanced {
        kmeans_balanced(data, n, k, dim, distance_fn, cfg.initial_lambda)
    } else {
        kmeans_unbalanced(data, n, k, dim, cfg.kmeans_iters)
    }
}

/// Recursively builds the centroid tree top-down.
///
/// The algorithm partitions `n` vectors into `branching_factor` groups via k-means,
/// then recurses on each group until a group fits in a single leaf (<= bf vectors).
///
/// Two k-means strategies are available (selected by `cfg.balanced`):
///   - Unbalanced: standard Lloyd's with k-means++ init
///   - Balanced: SPANN's lambda-penalized k-means that discourages large clusters
///
/// At the level just above leaves, SPANN-style "posting list expansion" can replicate
/// boundary vectors into multiple clusters to improve recall at the cost of memory.
fn build_tree_node(
    data: &[f32],   // flat array of n vectors, each `dim` floats
    keys: &[u32],   // parallel array of vector IDs
    n: usize,
    dim: usize,
    cfg: &TreeBuildConfig,
    distance_fn: &DistanceFunction,
    quant_center: Option<&[f32]>, // if Some, quantize all stored vectors for 1-bit search
) -> CentroidTreeNode {
    let bf = cfg.branching_factor;

    // Base case: group is small enough to be a leaf node.
    if n <= bf {
        let codes = quant_center.map(|center| {
            let mut all_codes = Vec::with_capacity(n * Code::<1>::size(dim));
            for i in 0..n {
                let vec = &data[i * dim..(i + 1) * dim];
                let code = Code::<1>::quantize(vec, center);
                all_codes.extend_from_slice(code.as_ref());
            }
            all_codes
        });
        return CentroidTreeNode::Leaf {
            keys: keys.to_vec(),
            centroids: data.to_vec(),
            codes,
        };
    }

    // Partition into k clusters (k = min(branching_factor, n)).
    let k = bf.min(n);
    let (assignments, centers) = do_kmeans(data, n, k, dim, cfg, distance_fn);

    // Decide whether to apply boundary replication at this level.
    // We only expand at the penultimate level (where children will be leaves),
    // since that's where routing errors cause the most recall loss.
    let do_expansion = cfg.expansion_factor > 0.0 && cfg.max_replicas > 1;
    let avg_group_size = n / k;
    let children_are_leaves = avg_group_size <= bf;
    let expand_this_level = do_expansion && children_are_leaves;

    if expand_this_level {
        // -----------------------------------------------------------------
        // Expansion path: assign each vector to multiple nearby clusters.
        //
        // A vector is replicated to cluster j if:
        //   dist(x, c_j) <= (1 + eps)^2 * dist(x, c_nearest)
        // subject to at most `max_replicas` assignments per vector.
        // The squared threshold works because we compare squared L2 distances.
        // -----------------------------------------------------------------
        let threshold_factor =
            ((1.0 + cfg.expansion_factor) * (1.0 + cfg.expansion_factor)) as f32;

        // For each vector, compute distances to all k centers and keep those
        // within the expansion threshold, up to max_replicas.
        let multi_assignments: Vec<Vec<usize>> = (0..n)
            .into_par_iter()
            .map(|i| {
                let point = &data[i * dim..(i + 1) * dim];
                let mut dists: Vec<(usize, f32)> = (0..k)
                    .map(|j| {
                        let center = &centers[j * dim..(j + 1) * dim];
                        (j, sqeuclidean_fast(point, center))
                    })
                    .collect();
                dists.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

                let d_min_sq = dists[0].1;
                let thresh = d_min_sq * threshold_factor;

                dists
                    .into_iter()
                    .take(cfg.max_replicas)
                    .take_while(|&(_, d)| d <= thresh)
                    .map(|(j, _)| j)
                    .collect()
            })
            .collect();

        // Invert the per-vector assignments into per-cluster membership lists.
        // A vector can appear in multiple groups (replication).
        let mut groups: Vec<Vec<usize>> = vec![Vec::new(); k];
        for (i, assigns) in multi_assignments.iter().enumerate() {
            for &g in assigns {
                groups[g].push(i);
            }
        }

        // Build a leaf node for each non-empty cluster, gathering the
        // (possibly replicated) vectors and their keys.
        let mut children = Vec::with_capacity(k);
        let mut live_centers = Vec::with_capacity(k * dim);

        for g in 0..k {
            let group = &groups[g];
            if group.is_empty() {
                continue;
            }
            let group_n = group.len();

            let mut group_data = vec![0.0f32; group_n * dim];
            let mut group_keys = vec![0u32; group_n];
            for (j, &orig_idx) in group.iter().enumerate() {
                group_data[j * dim..(j + 1) * dim]
                    .copy_from_slice(&data[orig_idx * dim..(orig_idx + 1) * dim]);
                group_keys[j] = keys[orig_idx];
            }

            let codes = quant_center.map(|center| {
                let mut all_codes = Vec::with_capacity(group_n * Code::<1>::size(dim));
                for j in 0..group_n {
                    let vec = &group_data[j * dim..(j + 1) * dim];
                    let code = Code::<1>::quantize(vec, center);
                    all_codes.extend_from_slice(code.as_ref());
                }
                all_codes
            });

            children.push(CentroidTreeNode::Leaf {
                keys: group_keys,
                centroids: group_data,
                codes,
            });
            live_centers.extend_from_slice(&centers[g * dim..(g + 1) * dim]);
        }

        // Quantize the k-means centers themselves for internal node routing.
        let center_codes = quant_center.map(|center| {
            let num_children = children.len();
            let mut all_codes = Vec::with_capacity(num_children * Code::<1>::size(dim));
            for i in 0..num_children {
                let vec = &live_centers[i * dim..(i + 1) * dim];
                let code = Code::<1>::quantize(vec, center);
                all_codes.extend_from_slice(code.as_ref());
            }
            all_codes
        });

        return CentroidTreeNode::Internal {
            centers: live_centers,
            codes: center_codes,
            children,
        };
    }

    // -----------------------------------------------------------------
    // Standard path (no expansion): each vector assigned to exactly one
    // cluster. Sort by cluster ID so each group is contiguous in memory,
    // then recurse on each group's slice.
    // -----------------------------------------------------------------
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| assignments[i]);

    let sorted_data = reorder_flat(data, &order, dim);
    let sorted_keys: Vec<u32> = order.iter().map(|&i| keys[i]).collect();
    let sorted_assignments: Vec<usize> = order.iter().map(|&i| assignments[i]).collect();
    let group_ranges = compute_group_ranges(&sorted_assignments, k);

    let mut children = Vec::with_capacity(k);
    for g in 0..k {
        let gs = group_ranges[g];
        let ge = group_ranges[g + 1];
        if ge > gs {
            let child = build_tree_node(
                &sorted_data[gs * dim..ge * dim],
                &sorted_keys[gs..ge],
                ge - gs,
                dim,
                cfg,
                distance_fn,
                quant_center,
            );
            children.push(child);
        }
    }

    // Quantize centers for internal-node routing (only when using 1-bit mode).
    let center_codes = quant_center.map(|center| {
        let num_children = children.len();
        let mut all_codes = Vec::with_capacity(num_children * Code::<1>::size(dim));
        for i in 0..num_children {
            let c_start = i * dim;
            let vec = &centers[c_start..c_start + dim];
            let code = Code::<1>::quantize(vec, center);
            all_codes.extend_from_slice(code.as_ref());
        }
        all_codes
    });

    // Empty clusters from k-means produce no children, so the centers array
    // may need filtering to stay aligned with the children vec.
    let actual_centers: Vec<f32> = {
        let num_children = children.len();
        if num_children == k {
            centers
        } else {
            let mut filtered = Vec::with_capacity(num_children * dim);
            for g in 0..k {
                if group_ranges[g + 1] > group_ranges[g] {
                    filtered.extend_from_slice(&centers[g * dim..(g + 1) * dim]);
                }
            }
            filtered
        }
    };

    CentroidTreeNode::Internal {
        centers: actual_centers,
        codes: center_codes,
        children,
    }
}

pub fn tree_node_size(node: &CentroidTreeNode) -> usize {
    match node {
        CentroidTreeNode::Leaf { keys, .. } => keys.len(),
        CentroidTreeNode::Internal { children, .. } => {
            children.iter().map(tree_node_size).sum()
        }
    }
}

pub fn tree_depth(node: &CentroidTreeNode) -> usize {
    match node {
        CentroidTreeNode::Leaf { .. } => 1,
        CentroidTreeNode::Internal { children, .. } => {
            1 + children.iter().map(tree_depth).max().unwrap_or(0)
        }
    }
}

// =============================================================================
// Tree statistics and diagram
// =============================================================================

pub struct LevelStats {
    pub internal_nodes: usize,
    pub leaf_nodes: usize,
    pub child_counts: Vec<usize>,
    pub leaf_sizes: Vec<usize>,
}

pub fn collect_level_stats(node: &CentroidTreeNode) -> Vec<LevelStats> {
    let depth = tree_depth(node);
    let mut levels: Vec<LevelStats> = (0..depth)
        .map(|_| LevelStats {
            internal_nodes: 0,
            leaf_nodes: 0,
            child_counts: Vec::new(),
            leaf_sizes: Vec::new(),
        })
        .collect();
    collect_level_stats_recurse(node, 0, &mut levels);
    levels
}

fn collect_level_stats_recurse(
    node: &CentroidTreeNode,
    level: usize,
    levels: &mut Vec<LevelStats>,
) {
    match node {
        CentroidTreeNode::Leaf { keys, .. } => {
            levels[level].leaf_nodes += 1;
            levels[level].leaf_sizes.push(keys.len());
        }
        CentroidTreeNode::Internal { children, .. } => {
            levels[level].internal_nodes += 1;
            levels[level].child_counts.push(children.len());
            for child in children {
                collect_level_stats_recurse(child, level + 1, levels);
            }
        }
    }
}

pub fn print_tree_diagram(node: &CentroidTreeNode, unique_centroids: usize, format_count_fn: fn(usize) -> String) {
    let levels = collect_level_stats(node);
    let depth = levels.len();
    let total_entries = tree_node_size(node);

    println!("\n--- Tree Structure ---");

    for (i, ls) in levels.iter().enumerate() {
        let is_last = i == depth - 1;
        let prefix = if i == 0 { "  *  " } else { "  |  " };

        if ls.internal_nodes > 0 {
            let counts = &ls.child_counts;
            let min_c = counts.iter().copied().min().unwrap_or(0);
            let max_c = counts.iter().copied().max().unwrap_or(0);
            let avg_c = counts.iter().sum::<usize>() as f64 / counts.len() as f64;
            let total_children: usize = counts.iter().sum();
            println!(
                "{}Level {} : {} internal node{}, {} total children (fan-out: min={}, avg={:.0}, max={})",
                prefix,
                i,
                format_count_fn(ls.internal_nodes),
                if ls.internal_nodes == 1 { "" } else { "s" },
                format_count_fn(total_children),
                min_c, avg_c, max_c
            );
        }

        if ls.leaf_nodes > 0 {
            let sizes = &ls.leaf_sizes;
            let min_s = sizes.iter().copied().min().unwrap_or(0);
            let max_s = sizes.iter().copied().max().unwrap_or(0);
            let total_vecs: usize = sizes.iter().sum();

            let p25 = percentile(sizes, 25);
            let p50 = percentile(sizes, 50);
            let p75 = percentile(sizes, 75);

            println!(
                "{}Level {} : {} lea{}, {} total vectors (size: min={}, p25={}, p50={}, p75={}, max={})",
                prefix,
                i,
                format_count_fn(ls.leaf_nodes),
                if ls.leaf_nodes == 1 { "f" } else { "ves" },
                format_count_fn(total_vecs),
                min_s, p25, p50, p75, max_s
            );
        }

        if !is_last {
            println!("  |");
        }
    }

    let avg_replication = total_entries as f64 / unique_centroids as f64;
    println!(
        "\nTotal entries: {} | Unique centroids: {} | Avg replication: {:.2}x",
        format_count_fn(total_entries),
        format_count_fn(unique_centroids),
        avg_replication,
    );
}

fn percentile(data: &[usize], pct: usize) -> usize {
    if data.is_empty() {
        return 0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable();
    let idx = (pct as f64 / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// =============================================================================
// HierarchicalCentroidIndex impl
// =============================================================================

impl HierarchicalCentroidIndex {
    pub fn build(
        vectors: &[f32],
        keys: &[u32],
        n: usize,
        dim: usize,
        distance_fn: DistanceFunction,
        centroid_bits: Option<u8>,
        cfg: &TreeBuildConfig,
    ) -> Self {
        let quantization_center = centroid_bits.map(|_| compute_mean(vectors, n, dim));
        let quant_ref = quantization_center.as_deref();

        let root = build_tree_node(vectors, keys, n, dim, cfg, &distance_fn, quant_ref);
        let tree_size = tree_node_size(&root);
        let code_size = if centroid_bits.is_some() {
            Code::<1>::size(dim)
        } else {
            0
        };

        Self {
            root,
            dim,
            beam_width: cfg.beam_width,
            beam_tau: None,
            beam_min: cfg.beam_width,
            beam_max: cfg.beam_width,
            distance_fn,
            quantization_center,
            code_size,
            overflow: Mutex::new(Vec::new()),
            tombstones: Mutex::new(HashSet::new()),
            tree_size,
        }
    }

    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        self.search_with_beam(query, k, self.beam_width, None)
    }

    pub fn search_with_beam(
        &self,
        query: &[f32],
        k: usize,
        beam: usize,
        tau_override: Option<f64>,
    ) -> Vec<(u32, f32)> {
        if self.quantization_center.is_some() {
            self.search_quantized(query, k, beam, tau_override)
        } else {
            self.search_f32(query, k, beam, tau_override)
        }
    }

    /// Compute effective beam from sorted (node, dist) pairs.
    /// Fixed mode (tau=None): use `beam` directly.
    /// Dynamic mode: include children with dist <= d_best * (1 + tau), clamped to [beam_min, beam_max].
    fn effective_beam(
        &self,
        sorted_scores: &[(&CentroidTreeNode, f32)],
        beam: usize,
        tau_override: Option<f64>,
    ) -> usize {
        if sorted_scores.is_empty() {
            return 0;
        }
        let tau = tau_override.or(self.beam_tau);
        match tau {
            None => beam.min(sorted_scores.len()),
            Some(tau) => {
                let d_best = sorted_scores[0].1.max(1e-10_f32);
                let threshold = d_best * (1.0_f32 + tau as f32);
                let count = sorted_scores
                    .iter()
                    .take_while(|(_, d)| *d <= threshold)
                    .count();
                let effective = count.clamp(self.beam_min, self.beam_max);
                effective.min(sorted_scores.len())
            }
        }
    }

    fn search_f32(
        &self,
        query: &[f32],
        k: usize,
        beam: usize,
        tau_override: Option<f64>,
    ) -> Vec<(u32, f32)> {
        let dim = self.dim;
        let df = &self.distance_fn;
        let mut all_results: Vec<(u32, f32)> = Vec::new();
        let mut current: Vec<&CentroidTreeNode> = vec![&self.root];

        loop {
            let mut child_scores: Vec<(&CentroidTreeNode, f32)> = Vec::new();

            for node in &current {
                if let CentroidTreeNode::Internal {
                    centers, children, ..
                } = node
                {
                    for (i, child) in children.iter().enumerate() {
                        let c = &centers[i * dim..(i + 1) * dim];
                        let dist = compute_distance(query, c, df);
                        child_scores.push((child, dist));
                    }
                }
            }

            if child_scores.is_empty() {
                break;
            }

            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let effective = self.effective_beam(&child_scores, beam, tau_override);
            child_scores.truncate(effective);

            let mut next_internals: Vec<&CentroidTreeNode> = Vec::new();
            for (node, _) in &child_scores {
                match node {
                    CentroidTreeNode::Leaf {
                        keys, centroids, ..
                    } => {
                        for (i, &key) in keys.iter().enumerate() {
                            let c = &centroids[i * dim..(i + 1) * dim];
                            let dist = compute_distance(query, c, df);
                            all_results.push((key, dist));
                        }
                    }
                    CentroidTreeNode::Internal { .. } => {
                        next_internals.push(node);
                    }
                }
            }

            if next_internals.is_empty() {
                break;
            }
            current = next_internals;
        }

        dedup_and_topk(&mut all_results, k)
    }

    fn search_quantized(
        &self,
        query: &[f32],
        k: usize,
        beam: usize,
        tau_override: Option<f64>,
    ) -> Vec<(u32, f32)> {
        let dim = self.dim;
        let df = &self.distance_fn;
        let center = self.quantization_center.as_ref().unwrap();
        let cs = self.code_size;
        let mut all_results: Vec<(u32, f32)> = Vec::new();

        let r_q: Vec<f32> = query.iter().zip(center.iter()).map(|(q, c)| q - c).collect();
        let c_norm = center.iter().map(|c| c * c).sum::<f32>().sqrt();
        let c_dot_q: f32 = center.iter().zip(query.iter()).map(|(c, q)| c * q).sum();
        let q_norm = query.iter().map(|q| q * q).sum::<f32>().sqrt();
        let padded_bytes = Code::<1>::packed_len(dim);
        let qq = QuantizedQuery::new(&r_q, padded_bytes, c_norm, c_dot_q, q_norm);

        let mut current: Vec<&CentroidTreeNode> = vec![&self.root];

        loop {
            let mut child_scores: Vec<(&CentroidTreeNode, f32)> = Vec::new();

            for node in &current {
                if let CentroidTreeNode::Internal {
                    codes,
                    children,
                    ..
                } = node
                {
                    let codes_buf = codes.as_ref().unwrap();
                    for (i, child) in children.iter().enumerate() {
                        let code_slice = &codes_buf[i * cs..(i + 1) * cs];
                        let code = Code::<1, &[u8]>::new(code_slice);
                        let dist = code.distance_quantized_query(df, &qq);
                        child_scores.push((child, dist));
                    }
                }
            }

            if child_scores.is_empty() {
                break;
            }

            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let effective = self.effective_beam(&child_scores, beam, tau_override);
            child_scores.truncate(effective);

            let mut next_internals: Vec<&CentroidTreeNode> = Vec::new();
            for (node, _) in &child_scores {
                match node {
                    CentroidTreeNode::Leaf {
                        keys,
                        codes,
                        ..
                    } => {
                        let codes_buf = codes.as_ref().unwrap();
                        for (i, &key) in keys.iter().enumerate() {
                            let code_slice = &codes_buf[i * cs..(i + 1) * cs];
                            let code = Code::<1, &[u8]>::new(code_slice);
                            let dist = code.distance_quantized_query(df, &qq);
                            all_results.push((key, dist));
                        }
                    }
                    CentroidTreeNode::Internal { .. } => {
                        next_internals.push(node);
                    }
                }
            }

            if next_internals.is_empty() {
                break;
            }
            current = next_internals;
        }

        dedup_and_topk(&mut all_results, k)
    }

    pub fn add(&self, key: u32, vector: Vec<f32>) {
        self.overflow.lock().push((key, vector));
    }

    pub fn remove(&self, key: u32) {
        self.tombstones.lock().insert(key);
    }

    pub fn len(&self) -> usize {
        self.tree_size + self.overflow.lock().len() - self.tombstones.lock().len()
    }

    /// For each level of the beam search, compute what fraction of the ground truth
    /// keys are still reachable from the selected beam nodes. This reveals whether
    /// recall is lost at upper routing levels or at the leaf level.
    pub fn diagnose_level_recall(
        &self,
        query: &[f32],
        beam: usize,
        gt_10: &HashSet<u32>,
        gt_100: &HashSet<u32>,
        tau_override: Option<f64>,
    ) -> Vec<LevelRecall> {
        let dim = self.dim;
        let df = &self.distance_fn;
        let cs = self.code_size;

        let quant_state: Option<(Vec<f32>, f32, f32, f32)> =
            self.quantization_center.as_ref().map(|center| {
                let r_q: Vec<f32> =
                    query.iter().zip(center.iter()).map(|(q, c)| q - c).collect();
                let c_norm = center.iter().map(|c| c * c).sum::<f32>().sqrt();
                let c_dot_q: f32 = center.iter().zip(query.iter()).map(|(c, q)| c * q).sum();
                let q_norm = query.iter().map(|q| q * q).sum::<f32>().sqrt();
                (r_q, c_norm, c_dot_q, q_norm)
            });
        let qq = quant_state.as_ref().map(|(r_q, c_norm, c_dot_q, q_norm)| {
            let padded_bytes = Code::<1>::packed_len(dim);
            QuantizedQuery::new(r_q, padded_bytes, *c_norm, *c_dot_q, *q_norm)
        });

        let mut levels = Vec::new();
        let mut current: Vec<&CentroidTreeNode> = vec![&self.root];

        loop {
            let mut child_scores: Vec<(&CentroidTreeNode, f32)> = Vec::new();

            for node in &current {
                if let CentroidTreeNode::Internal {
                    centers,
                    codes,
                    children,
                } = node
                {
                    match &qq {
                        Some(qq) => {
                            let codes_buf = codes.as_ref().unwrap();
                            for (i, child) in children.iter().enumerate() {
                                let code_slice = &codes_buf[i * cs..(i + 1) * cs];
                                let code = Code::<1, &[u8]>::new(code_slice);
                                let dist = code.distance_quantized_query(df, qq);
                                child_scores.push((child, dist));
                            }
                        }
                        None => {
                            for (i, child) in children.iter().enumerate() {
                                let c = &centers[i * dim..(i + 1) * dim];
                                let dist = compute_distance(query, c, df);
                                child_scores.push((child, dist));
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
            let effective = self.effective_beam(&child_scores, beam, tau_override);
            child_scores.truncate(effective);

            let mut reachable: HashSet<u32> = HashSet::new();
            let mut leaves = 0usize;
            let mut leaf_vectors = 0usize;
            for (node, _) in &child_scores {
                collect_all_keys_into(node, &mut reachable);
                if let CentroidTreeNode::Leaf { keys, .. } = node {
                    leaves += 1;
                    leaf_vectors += keys.len();
                }
            }

            let r10 =
                gt_10.intersection(&reachable).count() as f64 / gt_10.len().max(1) as f64;
            let r100 =
                gt_100.intersection(&reachable).count() as f64 / gt_100.len().max(1) as f64;

            levels.push(LevelRecall {
                level: levels.len() + 1,
                reachable_10: r10,
                reachable_100: r100,
                beam_size: child_scores.len(),
                total_candidates,
                leaves_scanned: leaves,
                vectors_scanned: leaf_vectors,
            });

            let mut next_internals: Vec<&CentroidTreeNode> = Vec::new();
            for (node, _) in &child_scores {
                if matches!(node, CentroidTreeNode::Internal { .. }) {
                    next_internals.push(node);
                }
            }

            if next_internals.is_empty() {
                break;
            }
            current = next_internals;
        }

        levels
    }
}

#[allow(dead_code)]
pub struct LevelRecall {
    pub level: usize,
    pub reachable_10: f64,
    pub reachable_100: f64,
    pub beam_size: usize,
    pub total_candidates: usize,
    pub leaves_scanned: usize,
    pub vectors_scanned: usize,
}

fn collect_all_keys_into(node: &CentroidTreeNode, keys: &mut HashSet<u32>) {
    match node {
        CentroidTreeNode::Leaf { keys: ks, .. } => {
            keys.extend(ks.iter().copied());
        }
        CentroidTreeNode::Internal { children, .. } => {
            for child in children {
                collect_all_keys_into(child, keys);
            }
        }
    }
}
