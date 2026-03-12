//! Benchmark for hierarchical centroid tree under a realistic SPANN write workload.
//!
//! Replaces USearch HNSW with a hierarchical k-means tree for centroid lookup.
//! The tree is built top-down: recursively split centroids into groups of
//! `branching_factor`, with beam search at query time.
//!
//! Phase 1: Build tree from N centroid vectors using recursive k-means.
//! Phase 2: Simulate adding 1M data vectors. The centroid index sees:
//!   - navigate (search) ~3.05x per data vector
//!   - spawn (add)        ~1.14% of data vectors
//!   - drop (remove)      ~0.57% of data vectors
//! Phase 3: Recall – brute-force recall@10/100 against a held-out query set.

#[allow(dead_code)]
mod datasets;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use simsimd::SpatialSimilarity;

use datasets::{format_count, Dataset, DatasetType, MetricType};

// =============================================================================
// CLI Arguments
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "hierarchical_centroid_profile")]
#[command(about = "Benchmark hierarchical centroid tree under SPANN workload")]
#[command(trailing_var_arg = true)]
struct Args {
    /// Dataset to use
    #[arg(long, default_value = "db-pedia")]
    dataset: DatasetType,

    /// Distance metric
    #[arg(long, default_value = "l2")]
    metric: MetricType,

    /// Quantization bit-width for centroid codes (1 only). Omit for full precision f32.
    #[arg(long)]
    centroid_bits: Option<u8>,

    /// Number of initial centroid vectors to bootstrap
    #[arg(long, default_value = "5700")]
    initial_centroids: usize,

    /// Number of simulated data vector adds (drives navigate/spawn/drop)
    #[arg(long, default_value = "1000000")]
    data_vectors: usize,

    /// Number of threads for the SPANN simulation
    #[arg(long, default_value = "32")]
    threads: usize,

    /// Tree branching factor (children per internal node)
    #[arg(long, default_value = "100")]
    branching_factor: usize,

    /// Beam width for tree search (candidates kept per level)
    #[arg(long, default_value = "10")]
    beam_width: usize,

    /// Expansion factor (epsilon) for boundary vector replication (SPANN posting list expansion).
    /// A vector is assigned to cluster j if dist(x, c_j) <= (1+eps) * dist(x, c_nearest).
    /// 0 = disabled.
    #[arg(long, default_value = "0.0")]
    expansion_factor: f64,

    /// Maximum number of clusters a vector can be assigned to (with expansion)
    #[arg(long, default_value = "1")]
    max_replicas: usize,

    /// Number of k-means iterations per level
    #[arg(long, default_value = "10")]
    kmeans_iters: usize,

    /// Number of queries for recall evaluation
    #[arg(long, default_value = "200")]
    num_queries: usize,

    /// Extra arguments (ignored, for compatibility with cargo bench)
    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}

// =============================================================================
// Load profile ratios (from SPANN CP1 @ 1M data vectors)
// =============================================================================

const NAVIGATES_PER_ADD: f64 = 3.05;
const SPAWN_RATE: f64 = 0.0114;
const DROP_RATE: f64 = 0.0057;
const NPROBE: usize = 64;

// =============================================================================
// Stats tracking
// =============================================================================

#[derive(Default, Clone)]
struct MethodStats {
    calls: u64,
    total: Duration,
}

impl MethodStats {
    fn record(&mut self, elapsed: Duration) {
        self.calls += 1;
        self.total += elapsed;
    }

    fn merge(&mut self, other: &MethodStats) {
        self.calls += other.calls;
        self.total += other.total;
    }

    fn avg_nanos(&self) -> u64 {
        if self.calls == 0 {
            0
        } else {
            self.total.as_nanos() as u64 / self.calls
        }
    }
}

#[derive(Default, Clone)]
struct PhaseStats {
    navigate: MethodStats,
    spawn: MethodStats,
    drop_op: MethodStats,
    wall: Duration,
}

impl PhaseStats {
    fn merge(&mut self, other: &PhaseStats) {
        self.navigate.merge(&other.navigate);
        self.spawn.merge(&other.spawn);
        self.drop_op.merge(&other.drop_op);
    }
}

// =============================================================================
// Formatting helpers
// =============================================================================

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 0.000_001 {
        format!("{:.0}ns", secs * 1_000_000_000.0)
    } else if secs < 0.001 {
        format!("{:.1}\u{00b5}s", secs * 1_000_000.0)
    } else if secs < 1.0 {
        format!("{:.2}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.2}s", secs)
    } else {
        format!("{:.1}m", secs / 60.0)
    }
}

fn format_nanos(nanos: u64) -> String {
    format_duration(Duration::from_nanos(nanos))
}

// =============================================================================
// Distance helpers
// =============================================================================

fn compute_distance(a: &[f32], b: &[f32], df: &DistanceFunction) -> f32 {
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

/// Deduplicate results by key (keep min distance), then sort and truncate to top-k.
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
// K-means helpers
// =============================================================================

fn compute_mean(data: &[f32], n: usize, dim: usize) -> Vec<f32> {
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

fn kmeans(data: &[f32], n: usize, k: usize, dim: usize, max_iters: usize) -> (Vec<usize>, Vec<f32>) {
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
// Hierarchical centroid tree
// =============================================================================

#[derive(Serialize, Deserialize)]
enum CentroidTreeNode {
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

struct HierarchicalCentroidIndex {
    root: CentroidTreeNode,
    dim: usize,
    beam_width: usize,
    distance_fn: DistanceFunction,
    quantization_center: Option<Vec<f32>>,
    code_size: usize,
    overflow: Mutex<Vec<(u32, Vec<f32>)>>,
    tombstones: Mutex<HashSet<u32>>,
    tree_size: usize,
}

fn build_tree_node(
    data: &[f32],
    keys: &[u32],
    n: usize,
    dim: usize,
    bf: usize,
    quant_center: Option<&[f32]>,
    expansion_factor: f64,
    max_replicas: usize,
    kmeans_iters: usize,
) -> CentroidTreeNode {
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

    let k = bf.min(n);
    let (assignments, centers) = kmeans(data, n, k, dim, kmeans_iters);

    let do_expansion = expansion_factor > 0.0 && max_replicas > 1;

    // Check if children will be leaves (avg group size <= bf).
    // Expansion is applied only at the leaf level to avoid compound growth.
    let avg_group_size = n / k;
    let children_are_leaves = avg_group_size <= bf;
    let expand_this_level = do_expansion && children_are_leaves;

    if expand_this_level {
        // SPANN-style posting list expansion: assign boundary vectors to multiple clusters.
        // Formula: x in X_j iff dist(x, c_j) <= (1+eps)^2 * dist(x, c_nearest)
        // We use squared distances, so threshold = (1+eps)^2 * d_min_sq.
        let threshold_factor = ((1.0 + expansion_factor) * (1.0 + expansion_factor)) as f32;

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
                    .take(max_replicas)
                    .take_while(|&(_, d)| d <= thresh)
                    .map(|(j, _)| j)
                    .collect()
            })
            .collect();

        // Build expanded groups: each group collects all vectors assigned to it (including replicas)
        let mut groups: Vec<Vec<usize>> = vec![Vec::new(); k];
        for (i, assigns) in multi_assignments.iter().enumerate() {
            for &g in assigns {
                groups[g].push(i);
            }
        }

        let mut children = Vec::with_capacity(k);
        let mut live_centers = Vec::with_capacity(k * dim);

        for g in 0..k {
            let group = &groups[g];
            if group.is_empty() {
                continue;
            }
            let group_n = group.len();

            // Expanded groups become leaves directly (they're at the leaf level)
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

    // Standard path: single assignment, recurse
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
                bf,
                quant_center,
                expansion_factor,
                max_replicas,
                kmeans_iters,
            );
            children.push(child);
        }
    }

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

fn tree_node_size(node: &CentroidTreeNode) -> usize {
    match node {
        CentroidTreeNode::Leaf { keys, .. } => keys.len(),
        CentroidTreeNode::Internal { children, .. } => {
            children.iter().map(tree_node_size).sum()
        }
    }
}

fn tree_depth(node: &CentroidTreeNode) -> usize {
    match node {
        CentroidTreeNode::Leaf { .. } => 1,
        CentroidTreeNode::Internal { children, .. } => {
            1 + children.iter().map(tree_depth).max().unwrap_or(0)
        }
    }
}

impl HierarchicalCentroidIndex {
    fn build(
        vectors: &[f32],
        keys: &[u32],
        n: usize,
        dim: usize,
        bf: usize,
        beam_width: usize,
        distance_fn: DistanceFunction,
        centroid_bits: Option<u8>,
        expansion_factor: f64,
        max_replicas: usize,
        kmeans_iters: usize,
    ) -> Self {
        let quantization_center = centroid_bits.map(|_| compute_mean(vectors, n, dim));
        let quant_ref = quantization_center.as_deref();

        let root = build_tree_node(
            vectors, keys, n, dim, bf, quant_ref,
            expansion_factor, max_replicas, kmeans_iters,
        );
        let tree_size = tree_node_size(&root);
        let code_size = if centroid_bits.is_some() {
            Code::<1>::size(dim)
        } else {
            0
        };

        Self {
            root,
            dim,
            beam_width,
            distance_fn,
            quantization_center,
            code_size,
            overflow: Mutex::new(Vec::new()),
            tombstones: Mutex::new(HashSet::new()),
            tree_size,
        }
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        self.search_with_beam(query, k, self.beam_width)
    }

    fn search_with_beam(&self, query: &[f32], k: usize, beam: usize) -> Vec<(u32, f32)> {
        if self.quantization_center.is_some() {
            self.search_quantized(query, k, beam)
        } else {
            self.search_f32(query, k, beam)
        }
    }

    fn search_f32(&self, query: &[f32], k: usize, beam: usize) -> Vec<(u32, f32)> {
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
            child_scores.truncate(beam);

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

    fn search_quantized(&self, query: &[f32], k: usize, beam: usize) -> Vec<(u32, f32)> {
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
            child_scores.truncate(beam);

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

    fn add(&self, key: u32, vector: Vec<f32>) {
        self.overflow.lock().push((key, vector));
    }

    fn remove(&self, key: u32) {
        self.tombstones.lock().insert(key);
    }

    fn len(&self) -> usize {
        self.tree_size + self.overflow.lock().len() - self.tombstones.lock().len()
    }
}

// =============================================================================
// Dataset loading
// =============================================================================

fn load_vectors(args: &Args) -> (Vec<Vec<f32>>, usize, DistanceFunction) {
    let distance_fn = args.metric.to_distance_function();

    let total_needed = args.initial_centroids
        + (args.data_vectors as f64 * SPAWN_RATE) as usize
        + args.num_queries
        + 1024;

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    let dataset: Box<dyn Dataset> = rt.block_on(async {
        match args.dataset {
            DatasetType::DbPedia => Box::new(
                datasets::dbpedia::DbPedia::load()
                    .await
                    .expect("Failed to load DBPedia dataset"),
            ) as Box<dyn Dataset>,
            DatasetType::Arxiv => Box::new(
                datasets::arxiv::Arxiv::load()
                    .await
                    .expect("Failed to load Arxiv dataset"),
            ),
            DatasetType::Sec => Box::new(
                datasets::sec::Sec::load()
                    .await
                    .expect("Failed to load SEC dataset"),
            ),
            DatasetType::MsMarco => Box::new(
                datasets::msmarco::MsMarco::load()
                    .await
                    .expect("Failed to load MS MARCO dataset"),
            ),
            DatasetType::WikipediaEn => Box::new(
                datasets::wikipedia::Wikipedia::load()
                    .await
                    .expect("Failed to load Wikipedia dataset"),
            ),
            DatasetType::Synthetic => todo!("Synthetic dataset not supported"),
        }
    });

    let dim = dataset.dimension();
    let load_count = total_needed.min(dataset.data_len());
    println!(
        "Loading {} vectors from {} (dim={})...",
        format_count(load_count),
        dataset.name(),
        dim
    );
    let pairs = dataset
        .load_range(0, load_count)
        .expect("Failed to load dataset");
    let vectors: Vec<Vec<f32>> = pairs.into_iter().map(|(_, v)| v.to_vec()).collect();
    (vectors, dim, distance_fn)
}

// =============================================================================
// Brute-force ground truth
// =============================================================================

fn brute_force_knn(
    query: &[f32],
    corpus: &[Vec<f32>],
    corpus_keys: &[u32],
    k: usize,
    distance_fn: &DistanceFunction,
) -> Vec<u32> {
    let mut dists: Vec<(u32, f32)> = corpus_keys
        .iter()
        .zip(corpus.iter())
        .map(|(&key, vec)| {
            let d = compute_distance(query, vec, distance_fn);
            (key, d)
        })
        .collect();
    dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    dists.into_iter().take(k).map(|(k, _)| k).collect()
}

// =============================================================================
// Main benchmark
// =============================================================================

fn main() {
    let args = Args::parse();
    let centroid_bits = args.centroid_bits;
    let initial_centroids = args.initial_centroids;
    let data_vectors = args.data_vectors;
    let num_threads = args.threads;
    let branching_factor = args.branching_factor;
    let beam_width = args.beam_width;
    let expansion_factor = args.expansion_factor;
    let max_replicas = args.max_replicas;
    let kmeans_iters = args.kmeans_iters;
    let num_queries = args.num_queries;

    if let Some(bits) = centroid_bits {
        assert_eq!(bits, 1, "Only 1-bit quantization is supported for hierarchical tree");
    }

    let (all_vectors, dim, distance_fn) = load_vectors(&args);

    let bits_label = match centroid_bits {
        Some(b) => format!("{}", b),
        None => "f32".to_string(),
    };

    println!("\n=== Hierarchical Centroid Tree Profile ===");
    println!(
        "Dim: {} | Metric: {:?} | Centroid bits: {} | Threads: {}",
        dim, args.metric, bits_label, num_threads
    );
    println!(
        "Initial centroids: {} | Data vectors: {} | Queries: {}",
        format_count(initial_centroids),
        format_count(data_vectors),
        num_queries
    );
    println!(
        "Branching factor: {} | Beam width: {} | K-means iters: {}",
        branching_factor, beam_width, kmeans_iters
    );
    if expansion_factor > 0.0 && max_replicas > 1 {
        println!(
            "Expansion: eps={:.1} | Max replicas: {}",
            expansion_factor, max_replicas
        );
    }
    println!(
        "Load profile per data vector: {:.2} navigates, {:.4} spawns, {:.4} drops",
        NAVIGATES_PER_ADD, SPAWN_RATE, DROP_RATE
    );

    // =========================================================================
    // Phase 1: Build tree from initial centroids (with disk cache)
    // =========================================================================
    let n = initial_centroids.min(all_vectors.len());

    let cache_dir = PathBuf::from("target/hierarchical_cache");
    let exp_label = if expansion_factor > 0.0 && max_replicas > 1 {
        format!("_eps{:.1}_r{}", expansion_factor, max_replicas)
    } else {
        String::new()
    };
    let cache_file = cache_dir.join(format!(
        "tree_{:?}_{}_bf{}_ki{}{}_{:?}_{}.bin",
        args.dataset, initial_centroids, branching_factor, kmeans_iters,
        exp_label, args.metric, bits_label,
    ));

    let index = if cache_file.exists() {
        println!(
            "\n--- Phase 1: Loading cached tree from {} ---",
            cache_file.display()
        );
        let load_start = Instant::now();
        let data = std::fs::read(&cache_file).expect("Failed to read cache file");
        let (root, quantization_center): (CentroidTreeNode, Option<Vec<f32>>) =
            bincode::deserialize(&data).expect("Failed to deserialize tree");
        let ts = tree_node_size(&root);
        let depth = tree_depth(&root);
        let idx = HierarchicalCentroidIndex {
            tree_size: ts,
            root,
            dim,
            beam_width,
            distance_fn: distance_fn.clone(),
            quantization_center,
            code_size: if centroid_bits.is_some() { Code::<1>::size(dim) } else { 0 },
            overflow: Mutex::new(Vec::new()),
            tombstones: Mutex::new(HashSet::new()),
        };
        println!(
            "Loaded {} centroids in {} (depth={})",
            format_count(ts),
            format_duration(load_start.elapsed()),
            depth,
        );
        idx
    } else {
        println!(
            "\n--- Phase 1: Build tree ({} centroids) ---",
            format_count(initial_centroids)
        );

        let mut flat_vectors = Vec::with_capacity(n * dim);
        for v in &all_vectors[..n] {
            flat_vectors.extend_from_slice(v);
        }
        let keys: Vec<u32> = (0..n as u32).collect();

        let build_start = Instant::now();
        let idx = HierarchicalCentroidIndex::build(
            &flat_vectors,
            &keys,
            n,
            dim,
            branching_factor,
            beam_width,
            distance_fn.clone(),
            centroid_bits,
            expansion_factor,
            max_replicas,
            kmeans_iters,
        );
        let build_time = build_start.elapsed();

        let depth = tree_depth(&idx.root);
        let tree_entries = tree_node_size(&idx.root);
        let expansion_ratio = tree_entries as f64 / n as f64;
        println!(
            "Built tree with {} centroids in {} (depth={}, {:.0} vec/s)",
            format_count(n),
            format_duration(build_time),
            depth,
            n as f64 / build_time.as_secs_f64()
        );
        if tree_entries != n {
            println!(
                "Tree entries: {} ({:.2}x expansion from boundary replication)",
                format_count(tree_entries), expansion_ratio
            );
        }

        std::fs::create_dir_all(&cache_dir).expect("Failed to create cache directory");
        let encoded = bincode::serialize(&(&idx.root, &idx.quantization_center))
            .expect("Failed to serialize tree");
        std::fs::write(&cache_file, &encoded).expect("Failed to write cache file");
        let cache_size_mb = encoded.len() as f64 / (1024.0 * 1024.0);
        println!("Cached tree to {} ({:.0} MB)", cache_file.display(), cache_size_mb);

        idx
    };
    println!("Tree size: {}", index.len());

    // =========================================================================
    // Phase 2: Simulated SPANN workload (multi-threaded)
    // =========================================================================
    println!(
        "\n--- Phase 2: SPANN workload ({} data vectors, {} threads) ---",
        format_count(data_vectors),
        num_threads
    );

    let next_key = AtomicU32::new(initial_centroids as u32);
    let live_entries: Mutex<Vec<(u32, usize)>> = Mutex::new(
        (0..initial_centroids).map(|i| (i as u32, i)).collect(),
    );

    let total_navigates = (data_vectors as f64 * NAVIGATES_PER_ADD) as u64;
    let total_spawns = (data_vectors as f64 * SPAWN_RATE) as u64;
    let total_drops = (data_vectors as f64 * DROP_RATE) as u64;
    let total_ops = total_navigates + total_spawns + total_drops;

    let progress = ProgressBar::new(total_ops);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[SPANN sim] {wide_bar} {pos}/{len} [{elapsed_precise}<{eta_precise}]")
            .unwrap(),
    );

    let nav_per_add = NAVIGATES_PER_ADD.floor() as usize;
    let nav_frac = NAVIGATES_PER_ADD - nav_per_add as f64;
    let vec_pool_start = initial_centroids;
    let vec_pool_size = all_vectors.len() - vec_pool_start;

    let phase2_start = Instant::now();

    let chunk_size = (data_vectors + num_threads - 1) / num_threads;
    let thread_stats: Vec<PhaseStats> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let index = &index;
                let all_vectors = &all_vectors;
                let next_key = &next_key;
                let live_entries = &live_entries;
                let progress = &progress;
                s.spawn(move || {
                    let mut local_stats = PhaseStats::default();
                    let mut rng = StdRng::seed_from_u64(123 + thread_id as u64);
                    let start = thread_id * chunk_size;
                    let end = (start + chunk_size).min(data_vectors);

                    for i in start..end {
                        let pool_idx = i % vec_pool_size;
                        let query_vec = &all_vectors[vec_pool_start + pool_idx];

                        let mut n_nav = nav_per_add;
                        if rng.gen::<f64>() < nav_frac {
                            n_nav += 1;
                        }
                        for _ in 0..n_nav {
                            let t = Instant::now();
                            let _ = index.search(query_vec, NPROBE);
                            local_stats.navigate.record(t.elapsed());
                            progress.inc(1);
                        }

                        if rng.gen::<f64>() < SPAWN_RATE {
                            let spawn_idx = (i + 1) % vec_pool_size;
                            let vec_index = vec_pool_start + spawn_idx;
                            let spawn_vec = &all_vectors[vec_index];
                            let key = next_key.fetch_add(1, Ordering::Relaxed);

                            let t = Instant::now();
                            index.add(key, spawn_vec.clone());
                            local_stats.spawn.record(t.elapsed());
                            live_entries.lock().push((key, vec_index));
                            progress.inc(1);
                        }

                        if rng.gen::<f64>() < DROP_RATE {
                            let entry = {
                                let mut entries = live_entries.lock();
                                if entries.len() > 100 {
                                    let idx = rng.gen_range(0..entries.len());
                                    Some(entries.swap_remove(idx))
                                } else {
                                    None
                                }
                            };
                            if let Some((key, _)) = entry {
                                let t = Instant::now();
                                index.remove(key);
                                local_stats.drop_op.record(t.elapsed());
                                progress.inc(1);
                            }
                        }
                    }

                    local_stats
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    progress.finish_and_clear();

    let mut stats = PhaseStats::default();
    for ts in &thread_stats {
        stats.merge(ts);
    }
    stats.wall = phase2_start.elapsed();

    let final_live_entries = live_entries.into_inner();

    println!(
        "Completed in {} | Index size: {}",
        format_duration(stats.wall),
        index.len()
    );

    println!("\n=== Phase 2: Task Counts ===");
    println!(
        "| {:>10} | {:>10} | {:>10} |",
        "navigate", "spawn", "drop"
    );
    println!("|------------|------------|------------|");
    println!(
        "| {:>10} | {:>10} | {:>10} |",
        format_count(stats.navigate.calls as usize),
        format_count(stats.spawn.calls as usize),
        format_count(stats.drop_op.calls as usize),
    );

    println!("\n=== Phase 2: Task Total Time ===");
    println!(
        "| {:>10} | {:>10} | {:>10} | {:>10} |",
        "navigate", "spawn", "drop", "wall"
    );
    println!("|------------|------------|------------|------------|");
    println!(
        "| {:>10} | {:>10} | {:>10} | {:>10} |",
        format_duration(stats.navigate.total),
        format_duration(stats.spawn.total),
        format_duration(stats.drop_op.total),
        format_duration(stats.wall),
    );

    println!("\n=== Phase 2: Task Avg Time ===");
    println!(
        "| {:>10} | {:>10} | {:>10} |",
        "navigate", "spawn", "drop"
    );
    println!("|------------|------------|------------|");
    println!(
        "| {:>10} | {:>10} | {:>10} |",
        format_nanos(stats.navigate.avg_nanos()),
        format_nanos(stats.spawn.avg_nanos()),
        format_nanos(stats.drop_op.avg_nanos()),
    );

    // =========================================================================
    // Phase 3: Recall evaluation
    // =========================================================================
    println!("\n--- Phase 3: Recall ({} queries, k=100) ---", num_queries);

    let corpus_vecs: Vec<Vec<f32>> = final_live_entries
        .iter()
        .filter_map(|&(_, vec_idx)| {
            if vec_idx < all_vectors.len() {
                Some(all_vectors[vec_idx].clone())
            } else {
                None
            }
        })
        .collect();
    let corpus_keys: Vec<u32> = final_live_entries
        .iter()
        .filter(|&&(_, vec_idx)| vec_idx < all_vectors.len())
        .map(|&(key, _)| key)
        .collect();

    let query_start = vec_pool_start;
    let query_vecs: Vec<&Vec<f32>> = all_vectors[query_start..]
        .iter()
        .take(num_queries)
        .collect();

    let k = 100;
    let mut recall_10_sum = 0.0f64;
    let mut recall_100_sum = 0.0f64;
    let mut total_latency = Duration::ZERO;

    let progress = ProgressBar::new(query_vecs.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[Recall] {wide_bar} {pos}/{len} [{elapsed_precise}]")
            .unwrap(),
    );

    for query in &query_vecs {
        let gt = brute_force_knn(query, &corpus_vecs, &corpus_keys, k, &distance_fn);

        let t = Instant::now();
        let result = index.search(query, k);
        total_latency += t.elapsed();

        let predicted: std::collections::HashSet<u32> =
            result.iter().map(|&(key, _)| key).collect();
        let gt_10: std::collections::HashSet<u32> = gt.iter().take(10).copied().collect();
        let gt_100: std::collections::HashSet<u32> = gt.iter().take(k).copied().collect();
        recall_10_sum +=
            predicted.intersection(&gt_10).count() as f64 / gt_10.len().max(1) as f64;
        recall_100_sum +=
            predicted.intersection(&gt_100).count() as f64 / gt_100.len().max(1) as f64;

        progress.inc(1);
    }
    progress.finish_and_clear();

    let n_q = query_vecs.len() as f64;
    let avg_recall_10 = recall_10_sum / n_q * 100.0;
    let avg_recall_100 = recall_100_sum / n_q * 100.0;
    let avg_latency = total_latency / query_vecs.len() as u32;

    println!("\n=== Recall Summary ===");
    println!(
        "Corpus size: {} | Queries: {} | k: {}",
        format_count(corpus_keys.len()),
        query_vecs.len(),
        k
    );
    println!(
        "Recall@10: {:.2}% | Recall@100: {:.2}% | Avg latency: {}",
        avg_recall_10, avg_recall_100, format_duration(avg_latency)
    );

    // =========================================================================
    // Phase 3b: Beam width sweep
    // =========================================================================
    println!(
        "\n--- Phase 3b: Beam width sweep ({} queries) ---",
        num_queries
    );

    let ground_truths: Vec<Vec<u32>> = query_vecs
        .iter()
        .map(|q| brute_force_knn(q, &corpus_vecs, &corpus_keys, k, &distance_fn))
        .collect();

    let sweep_widths: &[usize] = &[5, 10, 20, 50, 100];

    println!(
        "| {:>5} | {:>11} | {:>11} | {:>10} |",
        "Beam", "Recall@10", "Recall@100", "Avg lat"
    );
    println!("|-------|-------------|-------------|------------|");

    for &bw in sweep_widths {
        let mut r10_sum = 0.0f64;
        let mut r100_sum = 0.0f64;
        let mut lat_total = Duration::ZERO;

        for (qi, query) in query_vecs.iter().enumerate() {
            let t = Instant::now();
            let result = index.search_with_beam(query, k, bw);
            lat_total += t.elapsed();

            let predicted: std::collections::HashSet<u32> =
                result.iter().map(|&(key, _)| key).collect();
            let gt = &ground_truths[qi];
            let gt_10: std::collections::HashSet<u32> = gt.iter().take(10).copied().collect();
            let gt_100: std::collections::HashSet<u32> = gt.iter().take(k).copied().collect();

            r10_sum +=
                predicted.intersection(&gt_10).count() as f64 / gt_10.len().max(1) as f64;
            r100_sum +=
                predicted.intersection(&gt_100).count() as f64 / gt_100.len().max(1) as f64;
        }

        let n_q = query_vecs.len() as f64;
        println!(
            "| {:>5} | {:>10.2}% | {:>10.2}% | {:>10} |",
            bw,
            r10_sum / n_q * 100.0,
            r100_sum / n_q * 100.0,
            format_duration(lat_total / query_vecs.len() as u32),
        );
    }

    println!("\n=== Legend ===");
    println!(
        "navigate - beam search the centroid tree (beam={}, nprobe={})",
        beam_width, NPROBE
    );
    println!("spawn    - append to overflow buffer (from cluster split)");
    println!("drop     - add to tombstone set (from cluster split/merge)");
    println!("wall     - wall-clock time for the full SPANN simulation phase");
}
