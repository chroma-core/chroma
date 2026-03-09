//! Benchmark: centroid rerank factor sweep.
//!
//! Builds (or loads from cache) a quantized centroid HNSW index, then
//! measures recall@10 and recall@100 at rerank factors [1, 2, 4, 8, 16].
//! Reranking fetches `factor * k` candidates from the quantized index and
//! re-scores them by exact f32 distance against the raw centroid vectors
//! held in memory.
//!
//! Skips the SPANN write-path simulation (Phase 2) entirely.
//!
//! Example:
//! ```text
//! cargo bench -p chroma-index --bench centroid_rerank_sweep -- \
//!     --dataset db-pedia --centroid-bits 4 --initial-centroids 5700
//! ```

#[allow(dead_code)]
mod datasets;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
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
use simsimd::SpatialSimilarity;
use uuid::Uuid;

use datasets::{format_count, Dataset, DatasetType, MetricType};

// =============================================================================
// CLI
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "usearch_rerank")]
#[command(about = "Measure centroid recall at different rerank factors")]
#[command(trailing_var_arg = true)]
struct Args {
    #[arg(long, default_value = "db-pedia")]
    dataset: DatasetType,

    #[arg(long, default_value = "l2")]
    metric: MetricType,

    /// Quantization bit-width for centroid codes (1 or 4).
    #[arg(long, default_value = "4")]
    centroid_bits: u8,

    #[arg(long, default_value = "5700")]
    initial_centroids: usize,

    #[arg(long, default_value = "128")]
    ef_search: usize,

    #[arg(long, default_value = "200")]
    num_queries: usize,

    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}

// =============================================================================
// Helpers
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
// Main
// =============================================================================

fn main() {
    let args = Args::parse();
    let centroid_bits = args.centroid_bits;
    let initial_centroids = args.initial_centroids;
    let ef_search = args.ef_search;
    let num_queries = args.num_queries;
    let distance_fn = args.metric.to_distance_function();

    // --- Load dataset ---------------------------------------------------------
    let total_needed = initial_centroids + num_queries + 512;
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let dataset: Box<dyn Dataset> = rt.block_on(async {
        match args.dataset {
            DatasetType::DbPedia => Box::new(
                datasets::dbpedia::DbPedia::load().await.expect("load"),
            ) as Box<dyn Dataset>,
            DatasetType::Arxiv => {
                Box::new(datasets::arxiv::Arxiv::load().await.expect("load"))
            }
            DatasetType::Sec => {
                Box::new(datasets::sec::Sec::load().await.expect("load"))
            }
            DatasetType::MsMarco => {
                Box::new(datasets::msmarco::MsMarco::load().await.expect("load"))
            }
            DatasetType::WikipediaEn => {
                Box::new(datasets::wikipedia::Wikipedia::load().await.expect("load"))
            }
            DatasetType::Synthetic => todo!("synthetic not supported"),
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
    let all_vectors: Vec<Vec<f32>> = pairs.into_iter().map(|(_, v)| v.to_vec()).collect();

    // --- Compute quantization center -----------------------------------------
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
    let quantization_center: Arc<[f32]> = Arc::from(avg);

    let bits_label = format!("{}", centroid_bits);

    println!("\n=== Centroid Rerank Sweep ===");
    println!(
        "Dim: {} | Metric: {:?} | Centroid bits: {} | ef_search: {}",
        dim, args.metric, bits_label, ef_search
    );
    println!(
        "Centroids: {} | Queries: {}",
        format_count(initial_centroids),
        num_queries,
    );

    // --- Build or load quantized centroid index (shared cache) ----------------
    let config = USearchIndexConfig {
        collection_id: CollectionUuid(Uuid::new_v4()),
        cmek: None,
        prefix_path: String::new(),
        dimensions: dim,
        distance_function: distance_fn.clone(),
        connectivity: 16,
        expansion_add: 128,
        expansion_search: ef_search,
        quantization_center: Some(quantization_center),
        centroid_quantization_bits: centroid_bits,
    };

    let index =
        USearchIndex::new_for_benchmark(config.clone()).expect("Failed to create index");

    let cache_dir = PathBuf::from("target/usearch_cache");
    let cache_file = cache_dir.join(format!(
        "bootstrap_{:?}_{}_{:?}_{}.bin",
        args.dataset, initial_centroids, args.metric, bits_label,
    ));

    if cache_file.exists() {
        println!(
            "\n--- Loading cached index from {} ---",
            cache_file.display()
        );
        let data = std::fs::read(&cache_file).expect("read cache");
        rt.block_on(index.load_for_benchmark(Arc::new(data)))
            .expect("load cache");
        println!("Loaded {} centroids from cache", index.len().unwrap());
    } else {
        println!(
            "\n--- Bootstrap ({} centroids) ---",
            format_count(initial_centroids)
        );

        let pb = ProgressBar::new(initial_centroids as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[Bootstrap] {wide_bar} {pos}/{len} [{elapsed_precise}<{eta_precise}]")
                .unwrap(),
        );

        let t0 = Instant::now();
        for i in 0..initial_centroids.min(all_vectors.len()) {
            index.add(i as u32, &all_vectors[i]).unwrap();
            pb.inc(1);
        }
        pb.finish_and_clear();
        let elapsed = t0.elapsed();
        println!(
            "Added {} centroids in {} ({:.0} vec/s)",
            format_count(initial_centroids),
            format_duration(elapsed),
            initial_centroids as f64 / elapsed.as_secs_f64()
        );

        std::fs::create_dir_all(&cache_dir).expect("mkdir");
        let buf = rt
            .block_on(index.save_for_benchmark())
            .expect("serialize");
        std::fs::write(&cache_file, &buf).expect("write cache");
        println!("Cached to {}", cache_file.display());
    }

    // --- Prepare corpus and queries ------------------------------------------
    let corpus_vecs: Vec<Vec<f32>> = all_vectors[..initial_centroids].to_vec();
    let corpus_keys: Vec<u32> = (0..initial_centroids as u32).collect();
    let corpus_map: HashMap<u32, &[f32]> = corpus_keys
        .iter()
        .zip(corpus_vecs.iter())
        .map(|(&k, v)| (k, v.as_slice()))
        .collect();

    let query_vecs: Vec<&Vec<f32>> = all_vectors[initial_centroids..]
        .iter()
        .take(num_queries)
        .collect();

    // --- Pre-compute or load ground truth ------------------------------------
    let k = 100;
    let gt_cache_file = cache_dir.join(format!(
        "gt_{:?}_{}_{:?}_{}.bin",
        args.dataset, initial_centroids, args.metric, num_queries,
    ));

    let ground_truths: Vec<Vec<u32>> = if gt_cache_file.exists() {
        println!(
            "\nLoading cached ground truth from {} ...",
            gt_cache_file.display()
        );
        let data = std::fs::read(&gt_cache_file).expect("read gt cache");
        bincode::deserialize(&data).expect("deserialize gt cache")
    } else {
        println!("\nComputing brute-force ground truth...");

        let pb = ProgressBar::new(query_vecs.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[Ground truth] {wide_bar} {pos}/{len} [{elapsed_precise}]")
                .unwrap(),
        );

        let gts: Vec<Vec<u32>> = query_vecs
            .iter()
            .map(|q| {
                let gt = brute_force_knn(q, &corpus_vecs, &corpus_keys, k, &distance_fn);
                pb.inc(1);
                gt
            })
            .collect();
        pb.finish_and_clear();

        let encoded = bincode::serialize(&gts).expect("serialize gt");
        std::fs::write(&gt_cache_file, &encoded).expect("write gt cache");
        println!("Cached ground truth to {}", gt_cache_file.display());
        gts
    };

    // --- Rerank sweep --------------------------------------------------------
    let rerank_factors: &[usize] = &[1, 2, 4, 8, 16];

    println!("\n=== Rerank Sweep (k={}) ===", k);
    println!(
        "| {:>7} | {:>10} | {:>11} | {:>11} | {:>10} | {:>10} | {:>10} | {:>10} |",
        "Rerank", "Fetch", "Recall@10", "Recall@100", "Avg lat", "search", "fetch", "rerank"
    );
    println!(
        "|---------|------------|-------------|-------------|------------|------------|------------|------------|"
    );

    for &factor in rerank_factors {
        let fetch_k = factor * k;
        let mut r10_sum = 0.0f64;
        let mut r100_sum = 0.0f64;
        let mut search_total = Duration::ZERO;
        let mut fetch_total = Duration::ZERO;
        let mut rerank_total = Duration::ZERO;

        for (qi, query) in query_vecs.iter().enumerate() {
            let t0 = Instant::now();
            let result = index.search(query, fetch_k).unwrap();
            search_total += t0.elapsed();

            let top_keys: HashSet<u32>;

            if factor > 1 {
                let t1 = Instant::now();
                let raw_vecs: Vec<(u32, &[f32])> = result
                    .keys
                    .iter()
                    .filter_map(|&key| corpus_map.get(&key).map(|v| (key, *v)))
                    .collect();
                fetch_total += t1.elapsed();

                let t2 = Instant::now();
                let mut scored: Vec<(u32, f32)> = raw_vecs
                    .iter()
                    .map(|&(key, vec)| (key, distance_fn.distance(query, vec)))
                    .collect();
                scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                rerank_total += t2.elapsed();

                top_keys = scored.iter().take(k).map(|(key, _)| *key).collect();
            } else {
                top_keys = result.keys.iter().copied().collect();
            }

            let gt = &ground_truths[qi];
            let gt_10: HashSet<u32> = gt.iter().take(10).copied().collect();
            let gt_100: HashSet<u32> = gt.iter().take(k).copied().collect();
            r10_sum +=
                top_keys.intersection(&gt_10).count() as f64 / gt_10.len().max(1) as f64;
            r100_sum +=
                top_keys.intersection(&gt_100).count() as f64 / gt_100.len().max(1) as f64;
        }

        let n_q = query_vecs.len() as f64;
        let n_q_u32 = query_vecs.len() as u32;
        let total_lat = search_total + fetch_total + rerank_total;
        println!(
            "| {:>5}x | {:>10} | {:>10.2}% | {:>10.2}% | {:>10} | {:>10} | {:>10} | {:>10} |",
            factor,
            fetch_k,
            r10_sum / n_q * 100.0,
            r100_sum / n_q * 100.0,
            format_duration(total_lat / n_q_u32),
            format_duration(search_total / n_q_u32),
            format_duration(fetch_total / n_q_u32),
            format_duration(rerank_total / n_q_u32),
        );
    }
}
