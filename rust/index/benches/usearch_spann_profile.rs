//! Benchmark for USearch centroid index under a realistic SPANN write workload.
//!
//! Phase 1: Bootstrap – add N centroid vectors to the index.
//! Phase 2: Simulate adding 1M data vectors. The centroid index sees:
//!   - navigate (search) ~3.05x per data vector
//!   - spawn (add)        ~1.14% of data vectors
//!   - drop (remove)      ~0.57% of data vectors
//!   These are interleaved to match real SPANN split/merge patterns.
//! Phase 3: Recall – brute-force recall@1/10 against a held-out query set.

#[allow(dead_code)]
mod datasets;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chroma_distance::DistanceFunction;
use chroma_index::{
    usearch::{USearchIndex, USearchIndexConfig},
    VectorIndex,
};
use chroma_types::CollectionUuid;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use simsimd::SpatialSimilarity;
use uuid::Uuid;

use datasets::{format_count, Dataset, DatasetType, MetricType};

// =============================================================================
// CLI Arguments
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "usearch_spann_profile")]
#[command(about = "Benchmark USearch centroid index under SPANN workload")]
#[command(trailing_var_arg = true)]
struct Args {
    /// Dataset to use (or 'random' for synthetic data)
    #[arg(long, default_value = "db-pedia")]
    dataset: DatasetType,

    /// Distance metric
    #[arg(long, default_value = "l2")]
    metric: MetricType,

    /// Quantization bit-width for centroid codes (1 or 4). Omit for full precision.
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

    /// HNSW ef_search (expansion_search) parameter
    #[arg(long, default_value = "128")]
    ef_search: usize,

    /// Number of queries for recall evaluation
    #[arg(long, default_value = "200")]
    num_queries: usize,

    /// Extra arguments (ignored, for compatibility with cargo bench)
    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}
// example:
// cargo bench -p chroma-index --bench usearch_spann_profile -- --dataset wikipedia-en --centroid-bits 4 --initial-centroids 5700 --threads 32 --data-vectors 10000


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
            let d = match distance_fn {
                DistanceFunction::Euclidean => {
                    f32::sqeuclidean(query, vec).unwrap_or(f64::MAX) as f32
                }
                DistanceFunction::InnerProduct => {
                    let ip = f32::inner(query, vec).unwrap_or(0.0) as f32;
                    1.0 - ip
                }
                DistanceFunction::Cosine => {
                    f32::cosine(query, vec).unwrap_or(f64::MAX) as f32
                }
            };
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
    let ef_search = args.ef_search;
    let num_queries = args.num_queries;

    let (all_vectors, dim, distance_fn) = load_vectors(&args);

    let quantization_center: Option<Arc<[f32]>> = centroid_bits.map(|_| {
        let n = all_vectors.len().min(initial_centroids);
        let mut avg = vec![0.0f32; dim];
        for v in &all_vectors[..n] {
            for (a, b) in avg.iter_mut().zip(v.iter()) {
                *a += b;
            }
        }
        let scale = 1.0 / n as f32;
        for a in avg.iter_mut() {
            *a *= scale;
        }
        Arc::from(avg)
    });

    let bits_label = match centroid_bits {
        Some(b) => format!("{}", b),
        None => "f32".to_string(),
    };

    println!("\n=== USearch SPANN Profile Benchmark ===");
    println!(
        "Dim: {} | Metric: {:?} | Centroid bits: {} | ef_search: {} | Threads: {}",
        dim, args.metric, bits_label, ef_search, num_threads
    );
    println!(
        "Initial centroids: {} | Data vectors: {} | Queries: {}",
        format_count(initial_centroids),
        format_count(data_vectors),
        num_queries
    );
    println!(
        "Load profile per data vector: {:.2} navigates, {:.4} spawns, {:.4} drops",
        NAVIGATES_PER_ADD, SPAWN_RATE, DROP_RATE
    );

    // Create USearch index
    let config = USearchIndexConfig {
        collection_id: CollectionUuid(Uuid::new_v4()),
        cmek: None,
        prefix_path: String::new(),
        dimensions: dim,
        distance_function: distance_fn.clone(),
        connectivity: 16,
        expansion_add: 128,
        expansion_search: ef_search,
        quantization_center: quantization_center,
        centroid_quantization_bits: centroid_bits.unwrap_or(4),
    };

    let index = USearchIndex::new_for_benchmark(config.clone())
        .expect("Failed to create index");

    // =========================================================================
    // Phase 1: Bootstrap – add initial centroids (with disk cache)
    // =========================================================================
    let cache_dir = PathBuf::from("target/usearch_cache");
    let cache_file = cache_dir.join(format!(
        "bootstrap_{:?}_{}_{:?}_{}.bin",
        args.dataset,
        initial_centroids,
        args.metric,
        bits_label,
    ));

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let loaded_from_cache = if cache_file.exists() {
        println!("\n--- Phase 1: Loading cached bootstrap from {} ---", cache_file.display());
        let data = std::fs::read(&cache_file).expect("Failed to read cache file");
        rt.block_on(index.load_for_benchmark(Arc::new(data))).expect("Failed to load cached index");
        println!("Loaded {} centroids from cache", index.len().unwrap());
        true
    } else {
        println!("\n--- Phase 1: Bootstrap ({} centroids) ---", format_count(initial_centroids));

        let progress = ProgressBar::new(initial_centroids as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("[Bootstrap] {wide_bar} {pos}/{len} [{elapsed_precise}<{eta_precise}]")
                .unwrap(),
        );

        let bootstrap_start = Instant::now();
        for i in 0..initial_centroids.min(all_vectors.len()) {
            index.add(i as u32, &all_vectors[i]).unwrap();
            progress.inc(1);
        }
        progress.finish_and_clear();
        let bootstrap_time = bootstrap_start.elapsed();

        println!(
            "Added {} centroids in {} ({:.0} vec/s)",
            format_count(initial_centroids),
            format_duration(bootstrap_time),
            initial_centroids as f64 / bootstrap_time.as_secs_f64()
        );
        println!("Index size: {}", index.len().unwrap());

        std::fs::create_dir_all(&cache_dir).expect("Failed to create cache directory");
        let buf = rt.block_on(index.save_for_benchmark()).expect("Failed to serialize index");
        std::fs::write(&cache_file, &buf).expect("Failed to write cache file");
        println!("Cached bootstrap to {}", cache_file.display());
        false
    };
    let _ = loaded_from_cache;

    // =========================================================================
    // Phase 2: Simulated SPANN workload (multi-threaded)
    // =========================================================================
    println!(
        "\n--- Phase 2: SPANN workload ({} data vectors, {} threads) ---",
        format_count(data_vectors),
        num_threads
    );

    let next_key = AtomicU32::new(initial_centroids as u32);
    // Track (usearch_key, index_into_all_vectors) so we can reconstruct the corpus for recall.
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

                        // Navigate (search)
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

                        // Spawn (add) with probability SPAWN_RATE
                        if rng.gen::<f64>() < SPAWN_RATE {
                            let spawn_idx = (i + 1) % vec_pool_size;
                            let vec_index = vec_pool_start + spawn_idx;
                            let spawn_vec = &all_vectors[vec_index];
                            let key = next_key.fetch_add(1, Ordering::Relaxed);

                            let t = Instant::now();
                            index.add(key, spawn_vec).unwrap();
                            local_stats.spawn.record(t.elapsed());
                            live_entries.lock().push((key, vec_index));
                            progress.inc(1);
                        }

                        // Drop (remove) with probability DROP_RATE
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
                                index.remove(key).unwrap();
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
        index.len().unwrap()
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

    // Collect current corpus for brute-force using the tracked (key, vec_index) mapping
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
        let result = index.search(query, k).unwrap();
        total_latency += t.elapsed();

        let predicted: std::collections::HashSet<u32> = result.keys.iter().copied().collect();
        let gt_10: std::collections::HashSet<u32> = gt.iter().take(10).copied().collect();
        let gt_100: std::collections::HashSet<u32> = gt.iter().take(k).copied().collect();
        recall_10_sum += predicted.intersection(&gt_10).count() as f64 / gt_10.len().max(1) as f64;
        recall_100_sum += predicted.intersection(&gt_100).count() as f64 / gt_100.len().max(1) as f64;

        progress.inc(1);
    }
    progress.finish_and_clear();

    let n_q = query_vecs.len() as f64;
    let avg_recall_10 = recall_10_sum / n_q * 100.0;
    let avg_recall_100 = recall_100_sum / n_q * 100.0;
    let avg_latency = total_latency / query_vecs.len() as u32;

    println!("\n=== Recall Summary (no rerank) ===");
    println!("Corpus size: {} | Queries: {} | k: {}", format_count(corpus_keys.len()), query_vecs.len(), k);
    println!(
        "Recall@10: {:.2}% | Recall@100: {:.2}% | Avg latency: {}",
        avg_recall_10, avg_recall_100, format_duration(avg_latency)
    );

    // =========================================================================
    // Phase 3b: Rerank evaluation
    // =========================================================================
    if centroid_bits.is_some() {
        println!("\n--- Phase 3b: Rerank sweep ({} queries) ---", num_queries);

        let corpus_map: std::collections::HashMap<u32, &[f32]> = final_live_entries
            .iter()
            .filter(|&&(_, vi)| vi < all_vectors.len())
            .map(|&(key, vi)| (key, all_vectors[vi].as_slice()))
            .collect();

        let rerank_factors: &[usize] = &[2, 4, 8, 16];

        println!(
            "| {:>7} | {:>10} | {:>11} | {:>11} | {:>10} |",
            "Rerank", "Fetch", "Recall@10", "Recall@100", "Avg lat"
        );
        println!("|---------|------------|-------------|-------------|------------|");

        // Baseline row (1x, no rerank -- already computed)
        println!(
            "| {:>5}x | {:>10} | {:>10.2}% | {:>10.2}% | {:>10} |",
            1, k, avg_recall_10, avg_recall_100, format_duration(avg_latency)
        );

        // Pre-compute ground truths once
        let ground_truths: Vec<Vec<u32>> = query_vecs
            .iter()
            .map(|q| brute_force_knn(q, &corpus_vecs, &corpus_keys, k, &distance_fn))
            .collect();

        for &factor in rerank_factors {
            let fetch_k = factor * k;
            let mut r10_sum = 0.0f64;
            let mut r100_sum = 0.0f64;
            let mut lat_total = Duration::ZERO;

            for (qi, query) in query_vecs.iter().enumerate() {
                let t = Instant::now();

                let result = index.search(query, fetch_k).unwrap();

                let mut scored: Vec<(u32, f32)> = result
                    .keys
                    .iter()
                    .filter_map(|&key| {
                        corpus_map.get(&key).map(|vec| {
                            let d = distance_fn.distance(query, vec);
                            (key, d)
                        })
                    })
                    .collect();
                scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

                lat_total += t.elapsed();

                let reranked: std::collections::HashSet<u32> =
                    scored.iter().take(k).map(|(key, _)| *key).collect();

                let gt = &ground_truths[qi];
                let gt_10: std::collections::HashSet<u32> = gt.iter().take(10).copied().collect();
                let gt_100: std::collections::HashSet<u32> = gt.iter().take(k).copied().collect();

                r10_sum +=
                    reranked.intersection(&gt_10).count() as f64 / gt_10.len().max(1) as f64;
                r100_sum +=
                    reranked.intersection(&gt_100).count() as f64 / gt_100.len().max(1) as f64;
            }

            let n_q = query_vecs.len() as f64;
            println!(
                "| {:>5}x | {:>10} | {:>10.2}% | {:>10.2}% | {:>10} |",
                factor,
                fetch_k,
                r10_sum / n_q * 100.0,
                r100_sum / n_q * 100.0,
                format_duration(lat_total / query_vecs.len() as u32),
            );
        }
    }

    println!("\n=== Legend ===");
    println!("navigate - search() the centroid HNSW index (nprobe={})", NPROBE);
    println!("spawn    - add() a new centroid to the HNSW index (from cluster split)");
    println!("drop     - remove() a centroid from the HNSW index (from cluster split/merge)");
    println!("wall     - wall-clock time for the full SPANN simulation phase");
}
