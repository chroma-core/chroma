//! Benchmark: centroid vector fetch strategies for navigate rerank.
//!
//! Compares four approaches to retrieving full-precision centroid vectors
//! during the centroid rerank step of navigate():
//!
//!   A. USearch raw_centroid.get(key)  -- current production path
//!   B. Vec<Vec<f32>> indexed by key  -- flat array, O(1) index
//!   C. HashMap<u32, Vec<f32>>        -- stdlib hash map
//!   D. DashMap<u32, Arc<[f32]>>      -- concurrent hash map (writer-style)
//!
//! Each strategy is tested with a simulated rerank loop: fetch K vectors
//! by key, then compute full-precision distance against a query. This
//! mirrors the inner loop of navigate() with centroid_rerank_factor > 1.
//!
//! Usage:
//! ```text
//! cargo bench -p chroma-index --bench centroid_fetch --features chroma-index/usearch -- \
//!     --num-centroids 5000 --fetch-k 128 --num-queries 1000 --dim 1024
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chroma_distance::DistanceFunction;
use chroma_index::{
    usearch::{USearchIndex, USearchIndexConfig},
    VectorIndex,
};
use chroma_types::CollectionUuid;
use clap::Parser;
use dashmap::DashMap;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use uuid::Uuid;

// =============================================================================
// CLI
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "centroid_fetch")]
#[command(about = "Benchmark centroid vector fetch strategies for navigate rerank")]
#[command(trailing_var_arg = true)]
struct Args {
    /// Number of centroid vectors to store.
    #[arg(long, default_value = "5000")]
    num_centroids: usize,

    /// Number of candidates fetched per navigate (rerank_factor * nprobe).
    #[arg(long, default_value = "128")]
    fetch_k: usize,

    /// Number of simulated navigate calls.
    #[arg(long, default_value = "2000")]
    num_queries: usize,

    /// Vector dimensionality.
    #[arg(long, default_value = "1024")]
    dim: usize,

    /// Number of warmup iterations before measurement.
    #[arg(long, default_value = "100")]
    warmup: usize,

    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}

// =============================================================================
// Helpers
// =============================================================================

fn format_duration(d: Duration) -> String {
    let nanos = d.as_nanos() as f64;
    if nanos < 1_000.0 {
        format!("{:.0}ns", nanos)
    } else if nanos < 1_000_000.0 {
        format!("{:.1}us", nanos / 1_000.0)
    } else if nanos < 1_000_000_000.0 {
        format!("{:.2}ms", nanos / 1_000_000.0)
    } else {
        format!("{:.2}s", nanos / 1_000_000_000.0)
    }
}

fn random_vector(rng: &mut impl Rng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect()
}

struct BenchResult {
    name: &'static str,
    total: Duration,
    fetch: Duration,
    distance: Duration,
    queries: usize,
}

impl BenchResult {
    fn avg_total(&self) -> Duration {
        self.total / self.queries as u32
    }
    fn avg_fetch(&self) -> Duration {
        self.fetch / self.queries as u32
    }
    fn avg_distance(&self) -> Duration {
        self.distance / self.queries as u32
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    let args = Args::parse();
    let num_centroids = args.num_centroids;
    let fetch_k = args.fetch_k.min(num_centroids);
    let num_queries = args.num_queries;
    let dim = args.dim;
    let warmup = args.warmup;
    let distance_fn = DistanceFunction::Euclidean;

    println!("=== Centroid Fetch Benchmark ===");
    println!(
        "Centroids: {} | Fetch K: {} | Queries: {} | Dim: {} | Warmup: {}",
        num_centroids, fetch_k, num_queries, dim, warmup,
    );

    // --- Generate random centroid vectors ------------------------------------
    let mut rng = StdRng::seed_from_u64(42);
    let centroids: Vec<Vec<f32>> = (0..num_centroids)
        .map(|_| random_vector(&mut rng, dim))
        .collect();

    // --- Generate queries and per-query fetch key sets -----------------------
    let queries: Vec<Vec<f32>> = (0..num_queries + warmup)
        .map(|_| random_vector(&mut rng, dim))
        .collect();

    let keys: Vec<u32> = (0..num_centroids as u32).collect();
    let fetch_key_sets: Vec<Vec<u32>> = (0..num_queries + warmup)
        .map(|_| {
            let mut ks = keys.clone();
            ks.shuffle(&mut rng);
            ks.truncate(fetch_k);
            ks
        })
        .collect();

    // --- Build all storage backends -----------------------------------------

    // A. USearch raw centroid index (f32, no quantization)
    let usearch_config = USearchIndexConfig {
        collection_id: CollectionUuid(Uuid::new_v4()),
        cmek: None,
        prefix_path: String::new(),
        dimensions: dim,
        distance_function: distance_fn.clone(),
        connectivity: 16,
        expansion_add: 128,
        expansion_search: 64,
        quantization_center: None,
        centroid_quantization_bits: 4,
    };
    let usearch_index =
        USearchIndex::new_for_benchmark(usearch_config).expect("create USearch index");
    for (i, v) in centroids.iter().enumerate() {
        usearch_index.add(i as u32, v).expect("add to USearch");
    }

    // B. Flat Vec<Vec<f32>> indexed by centroid ID
    let flat_vec: Vec<Vec<f32>> = centroids.clone();

    // C. HashMap<u32, Vec<f32>>
    let hash_map: HashMap<u32, Vec<f32>> = centroids
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u32, v.clone()))
        .collect();

    // D. DashMap<u32, Arc<[f32]>>
    let dash_map: DashMap<u32, Arc<[f32]>> = DashMap::new();
    for (i, v) in centroids.iter().enumerate() {
        dash_map.insert(i as u32, Arc::from(v.as_slice()));
    }

    // E. Vec<Arc<[f32]>> indexed by centroid ID (zero-copy reference)
    let flat_arc: Vec<Arc<[f32]>> = centroids
        .iter()
        .map(|v| Arc::from(v.as_slice()))
        .collect();

    println!("\nStorage sizes:");
    println!(
        "  Vectors: {} x {} = {:.1}MB raw",
        num_centroids,
        dim,
        (num_centroids * dim * 4) as f64 / (1024.0 * 1024.0),
    );

    // --- Benchmark each strategy --------------------------------------------

    // A. USearch get()
    let result_a = {
        let mut fetch_total = Duration::ZERO;
        let mut dist_total = Duration::ZERO;
        let t_total = Instant::now();

        for i in 0..num_queries + warmup {
            let query = &queries[i];
            let fetch_keys = &fetch_key_sets[i];

            let t_fetch = Instant::now();
            let fetched: Vec<(u32, Vec<f32>)> = fetch_keys
                .iter()
                .filter_map(|&key| {
                    usearch_index.get(key).ok().flatten().map(|v| (key, v))
                })
                .collect();
            let f_dur = t_fetch.elapsed();

            let t_dist = Instant::now();
            let mut scored: Vec<(u32, f32)> = fetched
                .iter()
                .map(|(key, v)| (*key, distance_fn.distance(query, v)))
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let d_dur = t_dist.elapsed();

            if i >= warmup {
                fetch_total += f_dur;
                dist_total += d_dur;
            }
        }

        BenchResult {
            name: "USearch get()",
            total: t_total.elapsed() - {
                // subtract warmup: re-measure isn't exact, use ratio
                let ratio = warmup as f64 / (num_queries + warmup) as f64;
                Duration::from_nanos((t_total.elapsed().as_nanos() as f64 * ratio) as u64)
            },
            fetch: fetch_total,
            distance: dist_total,
            queries: num_queries,
        }
    };

    // B. Flat Vec<Vec<f32>>
    let result_b = {
        let mut fetch_total = Duration::ZERO;
        let mut dist_total = Duration::ZERO;

        for i in 0..num_queries + warmup {
            let query = &queries[i];
            let fetch_keys = &fetch_key_sets[i];

            let t_fetch = Instant::now();
            let fetched: Vec<(u32, &[f32])> = fetch_keys
                .iter()
                .map(|&key| (key, flat_vec[key as usize].as_slice()))
                .collect();
            let f_dur = t_fetch.elapsed();

            let t_dist = Instant::now();
            let mut scored: Vec<(u32, f32)> = fetched
                .iter()
                .map(|&(key, v)| (key, distance_fn.distance(query, v)))
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let d_dur = t_dist.elapsed();

            if i >= warmup {
                fetch_total += f_dur;
                dist_total += d_dur;
            }
        }

        BenchResult {
            name: "Vec<Vec<f32>>",
            total: fetch_total + dist_total,
            fetch: fetch_total,
            distance: dist_total,
            queries: num_queries,
        }
    };

    // C. HashMap<u32, Vec<f32>>
    let result_c = {
        let mut fetch_total = Duration::ZERO;
        let mut dist_total = Duration::ZERO;

        for i in 0..num_queries + warmup {
            let query = &queries[i];
            let fetch_keys = &fetch_key_sets[i];

            let t_fetch = Instant::now();
            let fetched: Vec<(u32, &[f32])> = fetch_keys
                .iter()
                .filter_map(|&key| hash_map.get(&key).map(|v| (key, v.as_slice())))
                .collect();
            let f_dur = t_fetch.elapsed();

            let t_dist = Instant::now();
            let mut scored: Vec<(u32, f32)> = fetched
                .iter()
                .map(|&(key, v)| (key, distance_fn.distance(query, v)))
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let d_dur = t_dist.elapsed();

            if i >= warmup {
                fetch_total += f_dur;
                dist_total += d_dur;
            }
        }

        BenchResult {
            name: "HashMap",
            total: fetch_total + dist_total,
            fetch: fetch_total,
            distance: dist_total,
            queries: num_queries,
        }
    };

    // D. DashMap<u32, Arc<[f32]>>
    let result_d = {
        let mut fetch_total = Duration::ZERO;
        let mut dist_total = Duration::ZERO;

        for i in 0..num_queries + warmup {
            let query = &queries[i];
            let fetch_keys = &fetch_key_sets[i];

            let t_fetch = Instant::now();
            let fetched: Vec<(u32, Arc<[f32]>)> = fetch_keys
                .iter()
                .filter_map(|&key| dash_map.get(&key).map(|v| (key, v.clone())))
                .collect();
            let f_dur = t_fetch.elapsed();

            let t_dist = Instant::now();
            let mut scored: Vec<(u32, f32)> = fetched
                .iter()
                .map(|(key, v)| (*key, distance_fn.distance(query, v)))
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let d_dur = t_dist.elapsed();

            if i >= warmup {
                fetch_total += f_dur;
                dist_total += d_dur;
            }
        }

        BenchResult {
            name: "DashMap<Arc>",
            total: fetch_total + dist_total,
            fetch: fetch_total,
            distance: dist_total,
            queries: num_queries,
        }
    };

    // E. Vec<Arc<[f32]>>
    let result_e = {
        let mut fetch_total = Duration::ZERO;
        let mut dist_total = Duration::ZERO;

        for i in 0..num_queries + warmup {
            let query = &queries[i];
            let fetch_keys = &fetch_key_sets[i];

            let t_fetch = Instant::now();
            let fetched: Vec<(u32, &[f32])> = fetch_keys
                .iter()
                .map(|&key| (key, flat_arc[key as usize].as_ref()))
                .collect();
            let f_dur = t_fetch.elapsed();

            let t_dist = Instant::now();
            let mut scored: Vec<(u32, f32)> = fetched
                .iter()
                .map(|&(key, v)| (key, distance_fn.distance(query, v)))
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let d_dur = t_dist.elapsed();

            if i >= warmup {
                fetch_total += f_dur;
                dist_total += d_dur;
            }
        }

        BenchResult {
            name: "Vec<Arc<[f32]>>",
            total: fetch_total + dist_total,
            fetch: fetch_total,
            distance: dist_total,
            queries: num_queries,
        }
    };

    // --- Print results -------------------------------------------------------
    let results = [&result_a, &result_b, &result_c, &result_d, &result_e];

    println!("\n=== Results (per navigate call, fetching {} vectors of dim {}) ===", fetch_k, dim);
    println!(
        "| {:>16} | {:>10} | {:>10} | {:>10} | {:>8} |",
        "Strategy", "Total", "Fetch", "Distance", "Speedup",
    );
    println!(
        "|------------------|------------|------------|------------|----------|"
    );

    let baseline_nanos = result_a.avg_total().as_nanos() as f64;
    for r in &results {
        let speedup = baseline_nanos / r.avg_total().as_nanos() as f64;
        println!(
            "| {:>16} | {:>10} | {:>10} | {:>10} | {:>6.1}x |",
            r.name,
            format_duration(r.avg_total()),
            format_duration(r.avg_fetch()),
            format_duration(r.avg_distance()),
            speedup,
        );
    }

    println!("\nPer-vector fetch cost (fetch time / {} vectors):", fetch_k);
    println!(
        "| {:>16} | {:>10} |",
        "Strategy", "Per-vec",
    );
    println!(
        "|------------------|------------|"
    );
    for r in &results {
        let per_vec = r.avg_fetch() / fetch_k as u32;
        println!(
            "| {:>16} | {:>10} |",
            r.name,
            format_duration(per_vec),
        );
    }
}
