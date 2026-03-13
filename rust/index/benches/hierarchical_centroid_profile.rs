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
mod hierarchical_index;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use datasets::{format_count, Dataset, DatasetType, MetricType};
use hierarchical_index::{
    compute_distance, print_tree_diagram, tree_depth, tree_node_size, CentroidTreeNode,
    HierarchicalCentroidIndex, TreeBuildConfig,
};
use std::collections::HashSet;

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

    /// Number of k-means iterations per level (unbalanced mode only)
    #[arg(long, default_value = "10")]
    kmeans_iters: usize,

    /// Use SPANN-style balanced k-means (lambda-penalized) instead of standard k-means
    #[arg(long, default_value = "false")]
    balanced: bool,

    /// Initial lambda for balanced k-means (higher = stronger balance penalty)
    #[arg(long, default_value = "100.0")]
    initial_lambda: f32,

    /// Dynamic beam (SPFresh-style): include child if dist <= d_best * (1 + tau).
    /// When set, beam is bounded by --beam-min and --beam-max. Omit for fixed beam.
    #[arg(long)]
    beam_tau: Option<f64>,

    /// Minimum children to keep per level (dynamic beam only)
    #[arg(long, default_value = "10")]
    beam_min: usize,

    /// Maximum children to keep per level (dynamic beam only)
    #[arg(long, default_value = "5000")]
    beam_max: usize,

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
    let num_queries = args.num_queries;

    let cfg = TreeBuildConfig {
        branching_factor: args.branching_factor,
        beam_width: args.beam_width,
        expansion_factor: args.expansion_factor,
        max_replicas: args.max_replicas,
        kmeans_iters: args.kmeans_iters,
        balanced: args.balanced,
        initial_lambda: args.initial_lambda,
    };

    if let Some(bits) = centroid_bits {
        assert_eq!(bits, 1, "Only 1-bit quantization is supported for hierarchical tree");
    }

    let (all_vectors, dim, distance_fn) = load_vectors(&args);

    let bits_label = match centroid_bits {
        Some(b) => format!("{}", b),
        None => "f32".to_string(),
    };

    let kmeans_label = if cfg.balanced {
        format!("balanced(lambda={})", cfg.initial_lambda)
    } else {
        format!("{} iters", cfg.kmeans_iters)
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
    let beam_sched = match args.beam_tau {
        Some(tau) => format!(
            "Branching factor: {} | Beam: dynamic (tau={:.2}, min={}, max={}) | K-means: {}",
            cfg.branching_factor, tau, args.beam_min, args.beam_max, kmeans_label
        ),
        None => format!(
            "Branching factor: {} | Beam width: {} | K-means: {}",
            cfg.branching_factor, cfg.beam_width, kmeans_label
        ),
    };
    println!("{}", beam_sched);
    if cfg.expansion_factor > 0.0 && cfg.max_replicas > 1 {
        println!(
            "Expansion: eps={:.1} | Max replicas: {}",
            cfg.expansion_factor, cfg.max_replicas
        );
    }
    println!(
        "Load profile per data vector: {:.2} navigates, {:.4} spawns, {:.4} drops",
        NAVIGATES_PER_ADD, SPAWN_RATE, DROP_RATE
    );

    // =========================================================================
    // Phase 1: Build tree from initial centroids (with disk cache)
    // =========================================================================
    let phase1_start = Instant::now();
    let n = initial_centroids.min(all_vectors.len());

    let cache_dir = PathBuf::from("target/hierarchical_cache");
    let exp_label = if cfg.expansion_factor > 0.0 && cfg.max_replicas > 1 {
        format!("_eps{:.1}_r{}", cfg.expansion_factor, cfg.max_replicas)
    } else {
        String::new()
    };
    let bal_label = if cfg.balanced {
        format!("_bal{}", cfg.initial_lambda)
    } else {
        String::new()
    };
    let cache_file = cache_dir.join(format!(
        "tree_{:?}_{}_bf{}_ki{}{}{}_{:?}_{}.bin",
        args.dataset, initial_centroids, cfg.branching_factor, cfg.kmeans_iters,
        exp_label, bal_label, args.metric, bits_label,
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
            beam_width: cfg.beam_width,
            beam_tau: args.beam_tau,
            beam_min: args.beam_min,
            beam_max: args.beam_max,
            distance_fn: distance_fn.clone(),
            quantization_center,
            code_size: if centroid_bits.is_some() { Code::<1>::size(dim) } else { 0 },
            overflow: parking_lot::Mutex::new(Vec::new()),
            tombstones: parking_lot::Mutex::new(std::collections::HashSet::new()),
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
        let mut idx = HierarchicalCentroidIndex::build(
            &flat_vectors,
            &keys,
            n,
            dim,
            distance_fn.clone(),
            centroid_bits,
            &cfg,
        );
        idx.beam_tau = args.beam_tau;
        idx.beam_min = args.beam_min;
        idx.beam_max = args.beam_max;
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

    print_tree_diagram(&index.root, n, format_count);
    println!("Phase 1 wall clock: {}", format_duration(phase1_start.elapsed()));

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
    let phase3_start = Instant::now();

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
    // Phase 3b: Sweep (beam widths in fixed mode, tau values in dynamic mode)
    // =========================================================================

    let ground_truths: Vec<Vec<u32>> = query_vecs
        .iter()
        .map(|q| brute_force_knn(q, &corpus_vecs, &corpus_keys, k, &distance_fn))
        .collect();

    if args.beam_tau.is_some() {
        let num_levels = tree_depth(&index.root) - 1; // internal levels only

        println!(
            "\n=== Phase 3: Tau sweep ({} queries, min={}, max={}) ===",
            num_queries, args.beam_min, args.beam_max
        );

        let sweep_taus: &[f64] = &[0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0];

        // Build header dynamically based on tree depth
        let mut header = format!(
            "| {:>6} | {:>11} | {:>11} | {:>10}",
            "Tau", "Recall@10", "Recall@100", "Avg lat"
        );
        for l in 1..=num_levels {
            header.push_str(&format!(" | L{} beam | L{} R@100", l, l));
        }
        header.push_str(" | Leaves |   Vecs |");
        println!("{}", header);

        let mut sep = String::from("|--------|-------------|-------------|------------|");
        for _ in 1..=num_levels {
            sep.push_str("---------|---------|");
        }
        sep.push_str("--------|--------|");
        println!("{}", sep);

        for &tau in sweep_taus {
            let mut r10_sum = 0.0f64;
            let mut r100_sum = 0.0f64;
            let mut lat_total = Duration::ZERO;
            // Per-level accumulators: (beam_sum, reach10_sum, count)
            let mut level_accum: Vec<(usize, f64, usize)> = vec![(0, 0.0, 0); num_levels];
            let mut leaves_sum = 0usize;
            let mut vectors_sum = 0usize;

            for (qi, query) in query_vecs.iter().enumerate() {
                let gt = &ground_truths[qi];
                let gt_10: HashSet<u32> = gt.iter().take(10).copied().collect();
                let gt_100: HashSet<u32> = gt.iter().take(k).copied().collect();

                let t = Instant::now();
                let result = index.search_with_beam(query, k, cfg.beam_width, Some(tau));
                lat_total += t.elapsed();

                let predicted: HashSet<u32> =
                    result.iter().map(|&(key, _)| key).collect();

                r10_sum +=
                    predicted.intersection(&gt_10).count() as f64 / gt_10.len().max(1) as f64;
                r100_sum += predicted.intersection(&gt_100).count() as f64
                    / gt_100.len().max(1) as f64;

                let lr = index.diagnose_level_recall(
                    query,
                    cfg.beam_width,
                    &gt_10,
                    &gt_100,
                    Some(tau),
                );
                for entry in &lr {
                    let idx = entry.level - 1;
                    if idx < num_levels {
                        level_accum[idx].0 += entry.beam_size;
                        level_accum[idx].1 += entry.reachable_100;
                        level_accum[idx].2 += 1;
                    }
                    leaves_sum += entry.leaves_scanned;
                    vectors_sum += entry.vectors_scanned;
                }
            }

            let n_q = query_vecs.len() as f64;
            let avg_leaves = leaves_sum / query_vecs.len();
            let avg_vectors = vectors_sum / query_vecs.len();
            let mut row = format!(
                "| {:>6.2} | {:>10.2}% | {:>10.2}% | {:>10}",
                tau,
                r10_sum / n_q * 100.0,
                r100_sum / n_q * 100.0,
                format_duration(lat_total / query_vecs.len() as u32),
            );
            for l in 0..num_levels {
                let (beam_sum, reach_sum, count) = level_accum[l];
                if count > 0 {
                    let avg_beam = beam_sum / count;
                    let avg_reach = reach_sum / count as f64 * 100.0;
                    row.push_str(&format!(" | {:>7} | {:>6.1}%", avg_beam, avg_reach));
                } else {
                    row.push_str(" |       - |      -");
                }
            }
            row.push_str(&format!(" | {:>6} | {:>6} |", avg_leaves, format_count(avg_vectors)));
            println!("{}", row);
        }
    } else {
        println!(
            "\n--- Phase 3b: Beam width sweep ({} queries) ---",
            num_queries
        );

        let sweep_widths: &[usize] = &[5, 10, 20, 50, 100, 200, 500, 1000];

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
                let result = index.search_with_beam(query, k, bw, None);
                lat_total += t.elapsed();

                let predicted: std::collections::HashSet<u32> =
                    result.iter().map(|&(key, _)| key).collect();
                let gt = &ground_truths[qi];
                let gt_10: std::collections::HashSet<u32> =
                    gt.iter().take(10).copied().collect();
                let gt_100: std::collections::HashSet<u32> =
                    gt.iter().take(k).copied().collect();

                r10_sum += predicted.intersection(&gt_10).count() as f64
                    / gt_10.len().max(1) as f64;
                r100_sum += predicted.intersection(&gt_100).count() as f64
                    / gt_100.len().max(1) as f64;
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
    }

    println!(
        "Phase 3 wall clock: {}",
        format_duration(phase3_start.elapsed())
    );

    println!("\n=== Legend ===");
    println!(
        "navigate - beam search the centroid tree (nprobe={})",
        NPROBE
    );
    println!("spawn    - append to overflow buffer (from cluster split)");
    println!("drop     - add to tombstone set (from cluster split/merge)");
    println!("wall     - wall-clock time for the full SPANN simulation phase");
}
