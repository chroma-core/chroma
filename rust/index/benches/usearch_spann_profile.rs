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

use std::collections::HashMap;
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

    /// HNSW ef_search (expansion_search) parameter
    #[arg(long, default_value = "128")]
    ef_search: usize,

    /// Number of queries for recall evaluation
    #[arg(long, default_value = "200")]
    num_queries: usize,

    /// Comma-separated nprobe values to sweep in Phase 2 (Phase 3 uses the first value)
    #[arg(long, value_delimiter = ',', default_values_t = vec![32, 64, 128, 256])]
    nprobe: Vec<usize>,

    /// Comma-separated rerank factors to sweep in both phases
    #[arg(long, value_delimiter = ',', default_values_t = vec![1, 4, 8, 16])]
    rerank: Vec<usize>,

    /// Enable Phase 2 (synthetic SPANN workload with nprobe x rerank sweep)
    #[arg(long)]
    phase_2: bool,

    /// Enable Phase 3 (search-only recall with rerank sweep)
    #[arg(long)]
    phase_3: bool,

    /// Extra arguments (ignored, for compatibility with cargo bench)
    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}
// example:
// cargo bench -p chroma-index --features usearch --bench usearch_spann_profile -- --dataset wikipedia-en --centroid-bits 1 --initial-centroids 1000000 --phase-2 --phase-3

// =============================================================================
// Load profile ratios (from SPANN CP1 @ 1M data vectors)
// =============================================================================

const NAVIGATES_PER_ADD: f64 = 3.05;
const SPAWN_RATE: f64 = 0.0114;
const DROP_RATE: f64 = 0.0057;

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
    nav_rr_dist: MethodStats,
    nav_rr_sort: MethodStats,
    rr_scored_total: u64,
    rr_scored_calls: u64,
    rr_bytes_total: u64,
    spawn: MethodStats,
    drop_op: MethodStats,
    wall: Duration,
}

impl PhaseStats {
    fn merge(&mut self, other: &PhaseStats) {
        self.navigate.merge(&other.navigate);
        self.nav_search.merge(&other.nav_search);
        self.nav_rerank.merge(&other.nav_rerank);
        self.nav_rr_dist.merge(&other.nav_rr_dist);
        self.nav_rr_sort.merge(&other.nav_rr_sort);
        self.rr_scored_total += other.rr_scored_total;
        self.rr_scored_calls += other.rr_scored_calls;
        self.rr_bytes_total += other.rr_bytes_total;
        self.spawn.merge(&other.spawn);
        self.drop_op.merge(&other.drop_op);
    }

    fn avg_rr_scored(&self) -> f64 {
        if self.rr_scored_calls == 0 {
            0.0
        } else {
            self.rr_scored_total as f64 / self.rr_scored_calls as f64
        }
    }

    fn avg_rr_bytes(&self) -> f64 {
        if self.rr_scored_calls == 0 {
            0.0
        } else {
            self.rr_bytes_total as f64 / self.rr_scored_calls as f64
        }
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

fn format_bytes(bytes: f64) -> String {
    if bytes < 1024.0 {
        format!("{:.0}B", bytes)
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1}KB", bytes / 1024.0)
    } else {
        format!("{:.1}MB", bytes / (1024.0 * 1024.0))
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
// Recall evaluation helpers
// =============================================================================

struct CorpusView<'a> {
    corpus_refs: Vec<&'a [f32]>,
    corpus_keys: Vec<u32>,
    key_to_vec_idx: HashMap<u32, usize>,
}

impl<'a> CorpusView<'a> {
    fn from_live_entries(live_entries: &[(u32, usize)], all_vectors: &'a [Vec<f32>]) -> Self {
        let mut corpus_refs = Vec::with_capacity(live_entries.len());
        let mut corpus_keys = Vec::with_capacity(live_entries.len());
        let mut key_to_vec_idx = HashMap::with_capacity(live_entries.len());
        for &(key, vec_idx) in live_entries {
            if vec_idx < all_vectors.len() {
                corpus_refs.push(all_vectors[vec_idx].as_slice());
                corpus_keys.push(key);
                key_to_vec_idx.insert(key, vec_idx);
            }
        }
        Self {
            corpus_refs,
            corpus_keys,
            key_to_vec_idx,
        }
    }
}

fn brute_force_knn_refs(
    query: &[f32],
    corpus_refs: &[&[f32]],
    corpus_keys: &[u32],
    k: usize,
    distance_fn: &DistanceFunction,
) -> Vec<u32> {
    let mut dists: Vec<(u32, f32)> = corpus_keys
        .iter()
        .zip(corpus_refs.iter())
        .map(|(&key, vec)| {
            let d = match distance_fn {
                DistanceFunction::Euclidean => {
                    f32::sqeuclidean(query, vec).unwrap_or(f64::MAX) as f32
                }
                DistanceFunction::InnerProduct => {
                    let ip = f32::inner(query, vec).unwrap_or(0.0) as f32;
                    1.0 - ip
                }
                DistanceFunction::Cosine => f32::cosine(query, vec).unwrap_or(f64::MAX) as f32,
            };
            (key, d)
        })
        .collect();
    dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    dists.into_iter().take(k).map(|(k, _)| k).collect()
}

fn compute_ground_truths(
    query_vecs: &[&[f32]],
    corpus: &CorpusView,
    k: usize,
    distance_fn: &DistanceFunction,
    num_threads: usize,
) -> Vec<Vec<u32>> {
    std::thread::scope(|s| {
        let chunk_size = (query_vecs.len() + num_threads - 1).max(1) / num_threads.max(1);
        let handles: Vec<_> = query_vecs
            .chunks(chunk_size)
            .map(|chunk| {
                let corpus_refs = &corpus.corpus_refs;
                let corpus_keys = &corpus.corpus_keys;
                s.spawn(move || {
                    chunk
                        .iter()
                        .map(|q| brute_force_knn_refs(q, corpus_refs, corpus_keys, k, distance_fn))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    })
}

fn evaluate_recall(
    index: &USearchIndex,
    query_vecs: &[&[f32]],
    ground_truths: &[Vec<u32>],
    corpus: &CorpusView,
    all_vectors: &[Vec<f32>],
    rerank_factor: usize,
    nprobe: usize,
    distance_fn: &DistanceFunction,
) -> (f64, f64) {
    let mut r10_sum = 0.0f64;
    let mut r100_sum = 0.0f64;

    for (qi, query) in query_vecs.iter().enumerate() {
        let gt = &ground_truths[qi];
        let gt_10: std::collections::HashSet<u32> = gt.iter().take(10).copied().collect();
        let gt_100: std::collections::HashSet<u32> = gt.iter().take(nprobe).copied().collect();

        let fetch_k = rerank_factor * nprobe;
        let result = index.search(query, fetch_k).unwrap();

        let predicted: std::collections::HashSet<u32> = if rerank_factor > 1 {
            let mut scored: Vec<(u32, f32)> = result
                .keys
                .iter()
                .filter_map(|&key| {
                    corpus
                        .key_to_vec_idx
                        .get(&key)
                        .map(|&vi| (key, distance_fn.distance(query, &all_vectors[vi])))
                })
                .collect();
            scored.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            scored.iter().take(nprobe).map(|(key, _)| *key).collect()
        } else {
            result.keys.iter().copied().collect()
        };

        r10_sum += predicted.intersection(&gt_10).count() as f64 / gt_10.len().max(1) as f64;
        r100_sum += predicted.intersection(&gt_100).count() as f64 / gt_100.len().max(1) as f64;
    }

    let n_q = query_vecs.len() as f64;
    (r10_sum / n_q * 100.0, r100_sum / n_q * 100.0)
}

// =============================================================================
// Search + rerank helper
// =============================================================================

struct NavTiming {
    navigate: Duration,
    search: Duration,
    rerank: Duration,
    rr_dist: Duration,
    rr_sort: Duration,
    rr_scored: usize,
    rr_bytes: usize,
}

fn search_and_rerank(
    index: &USearchIndex,
    query: &[f32],
    nprobe: usize,
    rerank_factor: usize,
    corpus: &CorpusView,
    all_vectors: &[Vec<f32>],
    distance_fn: &DistanceFunction,
) -> NavTiming {
    let t_nav = Instant::now();
    let fetch_k = rerank_factor * nprobe;

    let t_search = Instant::now();
    let result = index.search(query, fetch_k).unwrap();
    let search_dur = t_search.elapsed();

    let (rerank_dur, rr_dist, rr_sort, rr_scored, rr_bytes) = if rerank_factor > 1 {
        let t_rr = Instant::now();

        let t_dist = Instant::now();
        let mut scored: Vec<(u32, f32)> = result
            .keys
            .iter()
            .filter_map(|&key| {
                corpus
                    .key_to_vec_idx
                    .get(&key)
                    .map(|&vi| (key, distance_fn.distance(query, &all_vectors[vi])))
            })
            .collect();
        let dist_dur = t_dist.elapsed();

        let t_sort = Instant::now();
        scored.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let sort_dur = t_sort.elapsed();

        let n = scored.len();
        (t_rr.elapsed(), dist_dur, sort_dur, n, n * query.len() * std::mem::size_of::<f32>())
    } else {
        (Duration::ZERO, Duration::ZERO, Duration::ZERO, 0, 0)
    };

    NavTiming {
        navigate: t_nav.elapsed(),
        search: search_dur,
        rerank: rerank_dur,
        rr_dist,
        rr_sort,
        rr_scored,
        rr_bytes,
    }
}

// =============================================================================
// Phase 2 workload runner
// =============================================================================

fn run_phase2_workload(
    index: &USearchIndex,
    all_vectors: &[Vec<f32>],
    corpus: &CorpusView,
    initial_centroids: usize,
    data_vectors: usize,
    num_threads: usize,
    rerank_factor: usize,
    nprobe: usize,
    distance_fn: &DistanceFunction,
) -> (PhaseStats, Vec<(u32, usize)>) {
    let next_key = AtomicU32::new(initial_centroids as u32);
    let live_entries: Mutex<Vec<(u32, usize)>> =
        Mutex::new((0..initial_centroids).map(|i| (i as u32, i)).collect());

    let total_navigates = (data_vectors as f64 * NAVIGATES_PER_ADD) as u64;
    let total_spawns = (data_vectors as f64 * SPAWN_RATE) as u64;
    let total_drops = (data_vectors as f64 * DROP_RATE) as u64;
    let total_ops = total_navigates + total_spawns + total_drops;

    let progress = ProgressBar::new(total_ops);
    progress.set_style(
        ProgressStyle::default_bar()
            .template(&format!(
                "[{}T/{}x] {{wide_bar}} {{pos}}/{{len}} [{{elapsed_precise}}<{{eta_precise}}]",
                num_threads, rerank_factor
            ))
            .unwrap(),
    );

    let nav_per_add = NAVIGATES_PER_ADD.floor() as usize;
    let nav_frac = NAVIGATES_PER_ADD - nav_per_add as f64;
    let vec_pool_start = initial_centroids;
    let vec_pool_size = all_vectors.len() - vec_pool_start;

    let phase_start = Instant::now();

    let chunk_size = (data_vectors + num_threads - 1) / num_threads;
    let thread_stats: Vec<PhaseStats> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let index = index;
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

                        // Navigate (search + optional rerank)
                        let mut n_nav = nav_per_add;
                        if rng.gen::<f64>() < nav_frac {
                            n_nav += 1;
                        }
                        for _ in 0..n_nav {
                            let timing = search_and_rerank(
                                index, query_vec, nprobe, rerank_factor,
                                corpus, all_vectors, distance_fn,
                            );
                            local_stats.navigate.record(timing.navigate);
                            local_stats.nav_search.record(timing.search);
                            local_stats.nav_rerank.record(timing.rerank);
                            local_stats.nav_rr_dist.record(timing.rr_dist);
                            local_stats.nav_rr_sort.record(timing.rr_sort);
                            local_stats.rr_scored_total += timing.rr_scored as u64;
                            local_stats.rr_bytes_total += timing.rr_bytes as u64;
                            local_stats.rr_scored_calls += 1;
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
    stats.wall = phase_start.elapsed();

    (stats, live_entries.into_inner())
}

// =============================================================================
// Main benchmark
// =============================================================================

fn main() {
    let args = Args::parse();
    let centroid_bits = args.centroid_bits;
    let initial_centroids = args.initial_centroids;
    let data_vectors = args.data_vectors;
    let ef_search = args.ef_search;
    let num_queries = args.num_queries;
    let nprobe_values = &args.nprobe;
    let rerank_factors = &args.rerank;
    let num_threads = 32;

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

    let config = USearchIndexConfig {
        collection_id: CollectionUuid(Uuid::new_v4()),
        cmek: None,
        prefix_path: String::new(),
        dimensions: dim,
        distance_function: distance_fn.clone(),
        connectivity: 16,
        expansion_add: 128,
        expansion_search: ef_search,
        quantization_center,
        centroid_quantization_bits: centroid_bits.unwrap_or(4),
    };

    // =========================================================================
    // Phase 1: Bootstrap -- ensure cached index exists
    // =========================================================================
    let cache_dir = PathBuf::from("target/usearch_cache");
    let cache_file = cache_dir.join(format!(
        "bootstrap_{:?}_{}_{:?}_{}.bin",
        args.dataset, initial_centroids, args.metric, bits_label,
    ));

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    if !cache_file.exists() {
        println!(
            "\n--- Phase 1: Bootstrap ({} centroids) ---",
            format_count(initial_centroids)
        );
        let boot_index =
            USearchIndex::new_for_benchmark(config.clone()).expect("Failed to create index");

        let progress = ProgressBar::new(initial_centroids as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("[Bootstrap] {wide_bar} {pos}/{len} [{elapsed_precise}<{eta_precise}]")
                .unwrap(),
        );

        let bootstrap_start = Instant::now();
        for i in 0..initial_centroids.min(all_vectors.len()) {
            boot_index.add(i as u32, &all_vectors[i]).unwrap();
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

        std::fs::create_dir_all(&cache_dir).expect("Failed to create cache directory");
        let buf = rt
            .block_on(boot_index.save_for_benchmark())
            .expect("Failed to serialize index");
        std::fs::write(&cache_file, &buf).expect("Failed to write cache file");
        println!("Cached bootstrap to {}", cache_file.display());
    } else {
        println!(
            "\n--- Phase 1: Using cached bootstrap from {} ---",
            cache_file.display()
        );
    }

    let cached_data = Arc::new(std::fs::read(&cache_file).expect("Failed to read cache file"));

    // Shared setup for recall evaluation
    let vec_pool_start = initial_centroids;
    let query_vecs: Vec<&[f32]> = all_vectors[vec_pool_start..]
        .iter()
        .take(num_queries)
        .map(|v| v.as_slice())
        .collect();

    let bootstrap_entries: Vec<(u32, usize)> =
        (0..initial_centroids).map(|i| (i as u32, i)).collect();
    let corpus = CorpusView::from_live_entries(&bootstrap_entries, &all_vectors);

    // Precompute ground truth once (use largest nprobe for gt_k so it covers all nprobe values)
    let max_nprobe = *nprobe_values.iter().max().unwrap_or(&100);
    eprintln!(
        "  Computing ground truth ({} corpus, {} queries, k={}, {} threads)...",
        corpus.corpus_keys.len(),
        query_vecs.len(),
        max_nprobe,
        num_threads
    );
    let ground_truths =
        compute_ground_truths(&query_vecs, &corpus, max_nprobe, &distance_fn, num_threads);

    // Table header/row helpers
    let print_header = |phase: &str, has_spawn_drop: bool| {
        if has_spawn_drop {
            println!(
                "\n=== {} ===\n| {:>6} | {:>6} | {:>10} | {:>10} | {:>12} | {:>12} | {:>12} | {:>12} | {:>12} | {:>10} | {:>10} | {:>12} | {:>12} | {:>12} |",
                phase, "nprobe", "Rerank", "R@10", "R@100",
                "navigate", "search", "rerank", "rr_dist", "rr_sort", "rr_scored", "rr_bytes",
                "spawn", "drop", "wall"
            );
            println!(
                "|--------|--------|------------|------------|--------------|--------------|--------------|--------------|--------------|------------|------------|--------------|--------------|--------------|"
            );
        } else {
            println!(
                "\n=== {} ===\n| {:>6} | {:>6} | {:>10} | {:>10} | {:>12} | {:>12} | {:>12} | {:>12} | {:>12} | {:>10} | {:>10} |",
                phase, "nprobe", "Rerank", "R@10", "R@100",
                "navigate", "search", "rerank", "rr_dist", "rr_sort", "rr_scored", "rr_bytes"
            );
            println!(
                "|--------|--------|------------|------------|--------------|--------------|--------------|--------------|--------------|------------|------------|"
            );
        }
    };

    // =========================================================================
    // Phase 2: nprobe x rerank sweep with synthetic workload (optional, --phase-2)
    // =========================================================================
    if args.phase_2 {
        print_header(
            &format!(
                "Phase 2: SPANN Workload ({} data vectors, {} threads)",
                format_count(data_vectors),
                num_threads
            ),
            true,
        );

        for &nprobe in nprobe_values {
            for &rerank in rerank_factors {
                let run_index = USearchIndex::new_for_benchmark(config.clone())
                    .expect("Failed to create index");
                rt.block_on(run_index.load_for_benchmark(cached_data.clone()))
                    .expect("Failed to load cached index");

                let (stats, _live_entries) = run_phase2_workload(
                    &run_index,
                    &all_vectors,
                    &corpus,
                    initial_centroids,
                    data_vectors,
                    num_threads,
                    rerank,
                    nprobe,
                    &distance_fn,
                );

                let (r10, r100) = evaluate_recall(
                    &run_index,
                    &query_vecs,
                    &ground_truths,
                    &corpus,
                    &all_vectors,
                    rerank,
                    nprobe,
                    &distance_fn,
                );

                println!(
                    "| {:>6} | {:>4}x | {:>9.2}% | {:>9.2}% | {:>12} | {:>12} | {:>12} | {:>12} | {:>12} | {:>10.1} | {:>10} | {:>12} | {:>12} | {:>12} |",
                    nprobe,
                    rerank,
                    r10,
                    r100,
                    format_nanos(stats.navigate.avg_nanos()),
                    format_nanos(stats.nav_search.avg_nanos()),
                    format_nanos(stats.nav_rerank.avg_nanos()),
                    format_nanos(stats.nav_rr_dist.avg_nanos()),
                    format_nanos(stats.nav_rr_sort.avg_nanos()),
                    stats.avg_rr_scored(),
                    format_bytes(stats.avg_rr_bytes()),
                    format_nanos(stats.spawn.avg_nanos()),
                    format_nanos(stats.drop_op.avg_nanos()),
                    format_duration(stats.wall),
                );
            }
        }
    }

    // =========================================================================
    // Phase 3: Search-only rerank sweep (optional, --phase-3)
    // =========================================================================
    if args.phase_3 {
        let index =
            USearchIndex::new_for_benchmark(config.clone()).expect("Failed to create index");
        rt.block_on(index.load_for_benchmark(cached_data.clone()))
            .expect("Failed to load cached index");

        print_header(
            &format!(
                "Phase 3: Search-Only Recall ({} queries, {} threads)",
                num_queries, num_threads
            ),
            false,
        );

        for &nprobe in nprobe_values {
            for &rerank in rerank_factors {
                let (r10, r100) = evaluate_recall(
                    &index,
                    &query_vecs,
                    &ground_truths,
                    &corpus,
                    &all_vectors,
                    rerank,
                    nprobe,
                    &distance_fn,
                );

                let mut nav_total = Duration::ZERO;
                let mut search_total = Duration::ZERO;
                let mut rerank_total = Duration::ZERO;
                let mut rr_dist_total = Duration::ZERO;
                let mut rr_sort_total = Duration::ZERO;
                let mut rr_scored_total: u64 = 0;
                let mut rr_bytes_total: u64 = 0;

                for query in &query_vecs {
                    let timing = search_and_rerank(
                        &index, query, nprobe, rerank,
                        &corpus, &all_vectors, &distance_fn,
                    );
                    nav_total += timing.navigate;
                    search_total += timing.search;
                    rerank_total += timing.rerank;
                    rr_dist_total += timing.rr_dist;
                    rr_sort_total += timing.rr_sort;
                    rr_scored_total += timing.rr_scored as u64;
                    rr_bytes_total += timing.rr_bytes as u64;
                }

                let n_q = query_vecs.len() as u64;
                let avg_scored = if n_q > 0 { rr_scored_total as f64 / n_q as f64 } else { 0.0 };
                let avg_bytes = if n_q > 0 { rr_bytes_total as f64 / n_q as f64 } else { 0.0 };
                println!(
                    "| {:>6} | {:>4}x | {:>9.2}% | {:>9.2}% | {:>12} | {:>12} | {:>12} | {:>12} | {:>12} | {:>10.1} | {:>10} |",
                    nprobe,
                    rerank,
                    r10,
                    r100,
                    format_nanos(nav_total.as_nanos() as u64 / n_q),
                    format_nanos(search_total.as_nanos() as u64 / n_q),
                    format_nanos(rerank_total.as_nanos() as u64 / n_q),
                    format_nanos(rr_dist_total.as_nanos() as u64 / n_q),
                    format_nanos(rr_sort_total.as_nanos() as u64 / n_q),
                    avg_scored,
                    format_bytes(avg_bytes),
                );
            }
        }
    }

    println!("\n=== Legend ===");
    println!("nprobe   - number of nearest neighbors to retrieve per search");
    println!("navigate - total navigate latency: search + rerank");
    println!("search   - index.search() across the HNSW graph");
    println!("rerank   - re-score candidates with exact f32 distance and sort");
    println!("spawn    - add() a new centroid (from cluster split)");
    println!("drop     - remove() a centroid (from cluster split/merge)");
    println!("wall     - wall-clock time for the full SPANN simulation phase");
}
