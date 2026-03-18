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
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use datasets::{format_count, Dataset, DatasetType, MetricType};
use hierarchical_index::{
    compute_distance, print_tree_diagram, tree_depth, tree_node_size, CentroidTreeNode,
    HierarchicalCentroidIndex, TreeBuildConfig,
};
use std::collections::{HashMap, HashSet};

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

    /// Number of simulated data vector adds (Phase 2) and recall queries (Phase 3)
    #[arg(long, default_value = "1000000")]
    data_vectors: usize,

    /// Number of threads for workload and search phases
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

    /// Rerank factors to sweep (comma-separated). Only applied when --centroid-bits is set.
    #[arg(long, value_delimiter = ',', default_values_t = vec![1, 4, 16])]
    rerank_factors: Vec<usize>,

    /// Skip Phase 2 (SPANN synthetic workload)
    #[arg(long)]
    skip_phase_2: bool,

    /// Skip Phase 3 (pure search recall sweep)
    #[arg(long)]
    skip_phase_3: bool,

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
    nav_search: MethodStats,
    nav_rerank: MethodStats,
    spawn: MethodStats,
    drop_op: MethodStats,
    wall: Duration,
}

impl PhaseStats {
    fn merge(&mut self, other: &PhaseStats) {
        self.navigate.merge(&other.navigate);
        self.nav_search.merge(&other.nav_search);
        self.nav_rerank.merge(&other.nav_rerank);
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
        + args.data_vectors
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
    let num_queries = data_vectors;

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
        "Dim: {} | Metric: {:?} | Centroid bits: {}",
        dim, args.metric, bits_label
    );
    println!(
        "Initial centroids: {} | Data vectors / queries: {} | Threads: {} | Rerank: {:?}",
        format_count(initial_centroids),
        format_count(data_vectors),
        args.threads,
        args.rerank_factors,
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
    // Precompute ground truths for Phase 2 recall (with disk cache)
    // =========================================================================
    let vec_pool_start = initial_centroids;
    let vec_pool_size = all_vectors.len().saturating_sub(vec_pool_start);
    let num_recall_queries = num_queries.min(vec_pool_size).max(1);

    let recall_query_vecs: Vec<&Vec<f32>> = all_vectors[vec_pool_start..]
        .iter()
        .take(num_recall_queries)
        .collect();

    let gt_cache_file = cache_dir.join(format!(
        "gt_{:?}_{}_{:?}_{}.bin",
        args.dataset, initial_centroids, args.metric, num_recall_queries,
    ));

    let ground_truths: Vec<Vec<u32>> = if gt_cache_file.exists() {
        let data = std::fs::read(&gt_cache_file).expect("Failed to read ground truth cache");
        bincode::deserialize(&data).expect("Failed to deserialize ground truths")
    } else {
        let centroid_vecs: Vec<Vec<f32>> = all_vectors[0..n].to_vec();
        let centroid_keys: Vec<u32> = (0..n as u32).collect();
        println!(
            "\n--- Precomputing ground truths ({} queries, k=100) ---",
            num_recall_queries
        );
        let gt_start = Instant::now();
        let gts: Vec<Vec<u32>> = recall_query_vecs
            .iter()
            .map(|q| brute_force_knn(q, &centroid_vecs, &centroid_keys, 100, &distance_fn))
            .collect();
        std::fs::create_dir_all(&cache_dir).expect("Failed to create cache directory");
        let encoded = bincode::serialize(&gts).expect("Failed to serialize ground truths");
        std::fs::write(&gt_cache_file, &encoded).expect("Failed to write ground truth cache");
        println!(
            "Cached ground truths to {} ({:.1}ms)",
            gt_cache_file.display(),
            gt_start.elapsed().as_secs_f64() * 1000.0,
        );
        gts
    };

    // =========================================================================
    // Shared config for Phases 2 and 3
    // =========================================================================
    let num_threads = args.threads;
    let tau_sweep: &[f64] = &[0.1, 0.5, 1.0];
    let rerank_sweep: Vec<usize> = if centroid_bits.is_some() {
        args.rerank_factors.clone()
    } else {
        vec![1]
    };
    let k = 100;

    // Precompute recall for each (tau, rerank) pair once.
    println!(
        "\n--- Precomputing recall ({} queries per config) ---",
        num_recall_queries
    );
    let mut recall_cache: HashMap<(u64, usize), (f64, f64)> = HashMap::new();
    for &tau in tau_sweep {
        for &rf in &rerank_sweep {
            let mut r10_sum = 0.0f64;
            let mut r100_sum = 0.0f64;
            for (qi, query) in recall_query_vecs.iter().enumerate() {
                let gt = &ground_truths[qi];
                let gt_10: HashSet<u32> = gt.iter().take(10).copied().collect();
                let gt_100: HashSet<u32> = gt.iter().take(k).copied().collect();

                let result = if rf <= 1 || centroid_bits.is_none() {
                    index.search_with_beam(query, NPROBE, index.beam_width, Some(tau))
                } else {
                    index.search_with_rerank(
                        query, NPROBE, rf, index.beam_width, Some(tau),
                    )
                };

                let predicted: HashSet<u32> = result.iter().map(|&(key, _)| key).collect();
                r10_sum += predicted.intersection(&gt_10).count() as f64
                    / gt_10.len().max(1) as f64;
                r100_sum += predicted.intersection(&gt_100).count() as f64
                    / gt_100.len().max(1) as f64;
            }
            let n_q = recall_query_vecs.len() as f64;
            let recall_10 = r10_sum / n_q * 100.0;
            let recall_100 = r100_sum / n_q * 100.0;
            println!(
                "  tau={:.2} rerank={:>2}x => R@10: {:>5.1}%  R@100: {:>5.1}%",
                tau, rf, recall_10, recall_100
            );
            recall_cache.insert((tau.to_bits(), rf), (recall_10, recall_100));
        }
    }

    // =========================================================================
    // Phases 2 & 3: unified tau x rerank sweep
    //   Phase 2 = synthetic SPANN workload (navigate + spawn + drop)
    //   Phase 3 = pure search (navigate only)
    // =========================================================================
    let nav_per_add = NAVIGATES_PER_ADD.floor() as usize;
    let nav_frac = NAVIGATES_PER_ADD - nav_per_add as f64;

    let phases: &[(usize, &str, bool)] = &[
        (2, "SPANN Workload", true),
        (3, "Pure Search", false),
    ];

    let num_levels = tree_depth(&index.root).saturating_sub(1).max(1);
    let max_diag_levels = num_levels.min(4);

    for &(phase_num, phase_label, with_writes) in phases {
        let skip = match phase_num {
            2 => args.skip_phase_2,
            3 => args.skip_phase_3,
            _ => false,
        };
        if skip {
            println!("\nPhase {} skipped (--skip-phase-{})", phase_num, phase_num);
            continue;
        }

        let work_items = if with_writes { data_vectors } else { num_queries };

        print!(
            "\n=== Phase {}: {} ({}", phase_num, phase_label, format_count(work_items),
        );
        if with_writes {
            print!(" data vectors, {} threads) ===", num_threads);
        } else {
            print!(" queries, {} threads) ===", num_threads);
        }
        println!();

        // Table header
        print!(
            "| {:>5} | {:>6} | {:>9} | {:>9} | {:>10} | {:>10} | {:>10} | {:>10}",
            "Tau", "Rerank", "R@10", "R@100", "Wall", "Nav avg", "Srch avg", "Rank avg"
        );
        if with_writes {
            print!(" | {:>10} | {:>10}", "Spn avg", "Drop avg");
        }
        if !with_writes {
            for l in 1..=max_diag_levels {
                print!(" | L{} beam | L{} R@10", l, l);
            }
        }
        println!(" |");

        // Separator
        print!("|-------|--------|----------|----------|------------|------------|------------|------------");
        if with_writes {
            print!("|------------|------------");
        }
        if !with_writes {
            for _ in 1..=max_diag_levels {
                print!("|---------|--------");
            }
        }
        println!("|");

        for &tau in tau_sweep {
            for &rf in &rerank_sweep {
                index.overflow.lock().clear();
                index.tombstones.lock().clear();

                let next_key = AtomicU32::new(initial_centroids as u32);
                let live_entries: Mutex<Vec<(u32, usize)>> = Mutex::new(
                    (0..initial_centroids).map(|i| (i as u32, i)).collect(),
                );

                let phase_start = Instant::now();
                let chunk_size = (work_items + num_threads - 1) / num_threads;
                let tau_override = Some(tau);

                let thread_stats: Vec<PhaseStats> = std::thread::scope(|s| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|thread_id| {
                            let index = &index;
                            let all_vectors = &all_vectors;
                            let next_key = &next_key;
                            let live_entries = &live_entries;
                            s.spawn(move || {
                                let mut local_stats = PhaseStats::default();
                                let mut rng = StdRng::seed_from_u64(123 + thread_id as u64);
                                let start = thread_id * chunk_size;
                                let end = (start + chunk_size).min(work_items);

                                for i in start..end {
                                    let pool_idx = i % vec_pool_size;
                                    let query_vec = &all_vectors[vec_pool_start + pool_idx];

                                    let n_nav = if with_writes {
                                        let extra = if rng.gen::<f64>() < nav_frac { 1 } else { 0 };
                                        nav_per_add + extra
                                    } else {
                                        1
                                    };

                                    for _ in 0..n_nav {
                                        if rf <= 1 || centroid_bits.is_none() {
                                            let t = Instant::now();
                                            let _ = index.search_with_beam(
                                                query_vec, NPROBE, index.beam_width, tau_override,
                                            );
                                            let elapsed = t.elapsed();
                                            local_stats.navigate.record(elapsed);
                                            local_stats.nav_search.record(elapsed);
                                        } else {
                                            let (_, search_dur, rerank_dur) =
                                                index.search_with_rerank_timed(
                                                    query_vec, NPROBE, rf,
                                                    index.beam_width, tau_override,
                                                );
                                            let total = search_dur + rerank_dur;
                                            local_stats.navigate.record(total);
                                            local_stats.nav_search.record(search_dur);
                                            local_stats.nav_rerank.record(rerank_dur);
                                        }
                                    }

                                    if with_writes {
                                        if rng.gen::<f64>() < SPAWN_RATE {
                                            let spawn_idx = (i + 1) % vec_pool_size;
                                            let vec_index = vec_pool_start + spawn_idx;
                                            let spawn_vec = &all_vectors[vec_index];
                                            let key = next_key.fetch_add(1, Ordering::Relaxed);
                                            let t = Instant::now();
                                            index.add(key, spawn_vec.clone());
                                            local_stats.spawn.record(t.elapsed());
                                            live_entries.lock().push((key, vec_index));
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
                                            }
                                        }
                                    }
                                }

                                local_stats
                            })
                        })
                        .collect();

                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });

                let mut stats = PhaseStats::default();
                for ts in &thread_stats {
                    stats.merge(ts);
                }
                stats.wall = phase_start.elapsed();

                let &(recall_10, recall_100) = recall_cache
                    .get(&(tau.to_bits(), rf))
                    .unwrap_or(&(0.0, 0.0));

                print!(
                    "| {:>5.2} | {:>5}x | {:>8.1}% | {:>8.1}% | {:>10} | {:>10} | {:>10} | {:>10}",
                    tau, rf,
                    recall_10, recall_100,
                    format_duration(stats.wall),
                    format_nanos(stats.navigate.avg_nanos()),
                    format_nanos(stats.nav_search.avg_nanos()),
                    format_nanos(stats.nav_rerank.avg_nanos()),
                );
                if with_writes {
                    print!(
                        " | {:>10} | {:>10}",
                        format_nanos(stats.spawn.avg_nanos()),
                        format_nanos(stats.drop_op.avg_nanos()),
                    );
                }
                if !with_writes {
                    // Per-level diagnostics: average beam size and R@10 at each level
                    let mut level_beam_sum = vec![0usize; max_diag_levels];
                    let mut level_r10_sum = vec![0.0f64; max_diag_levels];
                    let mut level_count = vec![0usize; max_diag_levels];

                    for (qi, query) in recall_query_vecs.iter().enumerate() {
                        let gt = &ground_truths[qi];
                        let gt_10: HashSet<u32> = gt.iter().take(10).copied().collect();
                        let gt_100: HashSet<u32> = gt.iter().take(k).copied().collect();
                        let lr = index.diagnose_level_recall(
                            query, cfg.beam_width, &gt_10, &gt_100, Some(tau),
                        );
                        for entry in &lr {
                            let idx = entry.level - 1;
                            if idx < max_diag_levels {
                                level_beam_sum[idx] += entry.beam_size;
                                level_r10_sum[idx] += entry.reachable_10;
                                level_count[idx] += 1;
                            }
                        }
                    }

                    for l in 0..max_diag_levels {
                        if level_count[l] > 0 {
                            let avg_beam = level_beam_sum[l] / level_count[l];
                            let avg_r10 = level_r10_sum[l] / level_count[l] as f64 * 100.0;
                            print!(" | {:>7} | {:>5.1}%", avg_beam, avg_r10);
                        } else {
                            print!(" |       - |     -");
                        }
                    }
                }
                println!(" |");
            }
        }
    }

    println!("\n=== Legend ===");
    println!("R@10/R@100 - recall vs precomputed ground truth (cached)");
    println!(
        "navigate   - search + rerank total (nprobe={})",
        NPROBE
    );
    println!("search     - tree traversal (1-bit or f32)");
    println!("rerank     - f32 rescoring of top candidates");
    println!("spawn      - append to overflow buffer (from cluster split)");
    println!("drop       - add to tombstone set (from cluster split/merge)");
    println!("wall       - wall-clock time for the full phase");
}
