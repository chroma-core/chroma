//! Benchmark for BinaryQuantizedIndex recall and throughput on DBPedia dataset.
//!
//! This benchmark measures:
//! 1. Recall@K of binary quantization vs exact search
//! 2. Throughput of Hamming distance search (queries per second)
//! 3. Memory efficiency (bytes per vector)

mod datasets;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chroma_index::binary_quantized::{
    binary_code_size, binary_quantize, hamming_distance, BinaryQuantizedConfig,
    BinaryQuantizedIndex,
};
use chroma_index::VectorIndex;
use indicatif::{ProgressBar, ProgressStyle};
use simsimd::SpatialSimilarity;

use datasets::dbpedia::DbPedia;
use datasets::format_count;

fn format_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{}ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:.2}µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2}ms", nanos as f64 / 1_000_000.0)
    } else if d.as_secs() < 60 {
        format!("{:.2}s", d.as_secs_f64())
    } else {
        format!("{:.1}m", d.as_secs_f64() / 60.0)
    }
}

/// Compute exact k-NN using cosine distance.
fn exact_knn_cosine(query: &[f32], vectors: &[(u32, Arc<[f32]>)], k: usize) -> Vec<u32> {
    let mut distances: Vec<(u32, f64)> = vectors
        .iter()
        .map(|(id, v)| {
            let dist = <f32 as SpatialSimilarity>::cos(query, v).unwrap_or(1.0);
            (*id, dist)
        })
        .collect();
    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    distances.iter().take(k).map(|(id, _)| *id).collect()
}

/// Compute recall@k.
fn recall_at_k(predicted: &[u32], ground_truth: &[u32], k: usize) -> f64 {
    let gt: HashSet<u32> = ground_truth.iter().take(k).copied().collect();
    if gt.is_empty() {
        return 0.0;
    }
    let predicted_set: HashSet<u32> = predicted.iter().take(k).copied().collect();
    let found = predicted_set.intersection(&gt).count();
    found as f64 / gt.len() as f64
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Binary Quantized Index Benchmark ===");
    println!();

    // Configuration
    const NUM_VECTORS: usize = 50_000; // Use 50K vectors for reasonable benchmark time
    const NUM_QUERIES: usize = 1_000;
    const K_VALUES: [usize; 4] = [1, 10, 50, 100];

    // Load dataset
    let dataset = DbPedia::load().await?;
    println!(
        "Dataset: {} vectors, {} dimensions",
        format_count(dataset.data_len()),
        dataset.dimension()
    );
    println!(
        "Benchmark: {} vectors, {} queries",
        format_count(NUM_VECTORS),
        format_count(NUM_QUERIES)
    );
    println!();

    // Load vectors
    println!("Loading vectors...");
    let load_start = Instant::now();
    let all_vectors = dataset.load_range(0, NUM_VECTORS + NUM_QUERIES)?;
    let load_time = load_start.elapsed();
    println!(
        "Loaded {} vectors in {}",
        format_count(all_vectors.len()),
        format_duration(load_time)
    );

    // Split into index vectors and query vectors
    let index_vectors: Vec<(u32, Arc<[f32]>)> = all_vectors[..NUM_VECTORS].to_vec();
    let query_vectors: Vec<(u32, Arc<[f32]>)> = all_vectors[NUM_VECTORS..].to_vec();

    // Compute center (mean of all index vectors)
    println!("Computing center...");
    let dim = dataset.dimension();
    let mut center = vec![0.0f32; dim];
    for (_, v) in &index_vectors {
        for (i, &val) in v.iter().enumerate() {
            center[i] += val;
        }
    }
    for c in &mut center {
        *c /= index_vectors.len() as f32;
    }

    // Memory statistics
    let raw_bytes_per_vector = dim * 4; // f32
    let binary_bytes_per_vector = binary_code_size(dim);
    let compression_ratio = raw_bytes_per_vector as f64 / binary_bytes_per_vector as f64;
    println!();
    println!("=== Memory ===");
    println!("Raw: {} bytes/vector", raw_bytes_per_vector);
    println!("Binary: {} bytes/vector", binary_bytes_per_vector);
    println!("Compression ratio: {:.1}x", compression_ratio);

    // Build binary index
    println!();
    println!("=== Index Build ===");
    let config = BinaryQuantizedConfig {
        dimensions: dim,
        center: Some(center.clone().into()),
    };
    let index = BinaryQuantizedIndex::new(&config);

    let build_start = Instant::now();
    let progress = ProgressBar::new(index_vectors.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[Building] {wide_bar} {pos}/{len} [{elapsed_precise}]")
            .unwrap(),
    );

    for (id, v) in &index_vectors {
        index.add(*id, v).unwrap();
        progress.inc(1);
    }
    progress.finish_and_clear();

    let build_time = build_start.elapsed();
    let build_throughput = index_vectors.len() as f64 / build_time.as_secs_f64();
    println!(
        "Build time: {} ({:.0} vec/s)",
        format_duration(build_time),
        build_throughput
    );

    // Pre-compute ground truth for all queries
    println!();
    println!("=== Computing Ground Truth ===");
    let gt_start = Instant::now();
    let progress = ProgressBar::new(query_vectors.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[Ground Truth] {wide_bar} {pos}/{len} [{elapsed_precise}]")
            .unwrap(),
    );

    let max_k = *K_VALUES.iter().max().unwrap();
    let ground_truths: Vec<Vec<u32>> = query_vectors
        .iter()
        .map(|(_, q)| {
            let gt = exact_knn_cosine(q, &index_vectors, max_k);
            progress.inc(1);
            gt
        })
        .collect();
    progress.finish_and_clear();

    let gt_time = gt_start.elapsed();
    println!(
        "Ground truth computed in {} ({:.0} q/s)",
        format_duration(gt_time),
        query_vectors.len() as f64 / gt_time.as_secs_f64()
    );

    // Evaluate recall and throughput with per-query latency tracking
    println!();
    println!("=== Search Evaluation ===");

    let progress = ProgressBar::new(query_vectors.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[Searching] {wide_bar} {pos}/{len} [{elapsed_precise}]")
            .unwrap(),
    );

    let mut results: Vec<Vec<u32>> = Vec::with_capacity(query_vectors.len());
    let mut latencies: Vec<Duration> = Vec::with_capacity(query_vectors.len());

    let search_start = Instant::now();
    for (_, q) in &query_vectors {
        let query_start = Instant::now();
        let result = index.search(q, max_k).unwrap();
        latencies.push(query_start.elapsed());
        results.push(result.keys);
        progress.inc(1);
    }
    progress.finish_and_clear();
    let search_time = search_start.elapsed();

    // Compute latency statistics
    latencies.sort();
    let total_latency: Duration = latencies.iter().sum();
    let avg_latency = total_latency / latencies.len() as u32;
    let p50_latency = latencies[latencies.len() * 50 / 100];
    let p95_latency = latencies[latencies.len() * 95 / 100];
    let p99_latency = latencies[latencies.len() * 99 / 100];
    let min_latency = latencies.first().unwrap();
    let max_latency = latencies.last().unwrap();

    let qps = query_vectors.len() as f64 / search_time.as_secs_f64();

    println!(
        "Total search time: {} ({:.0} queries/sec)",
        format_duration(search_time),
        qps
    );
    println!();
    println!("Query Latency Statistics:");
    println!("  Min:    {:>10}", format_duration(*min_latency));
    println!("  Avg:    {:>10}", format_duration(avg_latency));
    println!("  P50:    {:>10}", format_duration(p50_latency));
    println!("  P95:    {:>10}", format_duration(p95_latency));
    println!("  P99:    {:>10}", format_duration(p99_latency));
    println!("  Max:    {:>10}", format_duration(*max_latency));

    // Compute recall for each K (pure Hamming)
    println!();
    println!("=== Recall Results (Pure Hamming) ===");
    println!("{:<12} {:<12} {:<12}", "K", "Recall@K", "Recall%");
    println!("{}", "-".repeat(36));

    for &k in &K_VALUES {
        let mut total_recall = 0.0;
        for (result, gt) in results.iter().zip(ground_truths.iter()) {
            total_recall += recall_at_k(result, gt, k);
        }
        let avg_recall = total_recall / query_vectors.len() as f64;
        println!(
            "{:<12} {:<12.4} {:<12.2}%",
            k,
            avg_recall,
            avg_recall * 100.0
        );
    }

    // === RERANKING EVALUATION ===
    println!();
    println!("=== Recall with Reranking ===");
    println!("Two-stage retrieval: Hamming candidates -> exact cosine rerank");
    println!();

    let oversample_factors = [2, 5, 10, 20, 50];
    let test_k = 10;

    println!(
        "{:<15} {:<12} {:<12} {:<15}",
        "Oversample", "Recall@10", "Recall%", "Latency (avg)"
    );
    println!("{}", "-".repeat(55));

    for &oversample in &oversample_factors {
        let mut rerank_latencies: Vec<Duration> = Vec::with_capacity(query_vectors.len());
        let mut rerank_results: Vec<Vec<u32>> = Vec::with_capacity(query_vectors.len());

        for (_, q) in &query_vectors {
            let start = Instant::now();
            let result = index.search_with_rerank(q, test_k, oversample).unwrap();
            rerank_latencies.push(start.elapsed());
            rerank_results.push(result.keys);
        }

        let total_latency: Duration = rerank_latencies.iter().sum();
        let avg_latency = total_latency / rerank_latencies.len() as u32;

        let mut total_recall = 0.0;
        for (result, gt) in rerank_results.iter().zip(ground_truths.iter()) {
            total_recall += recall_at_k(result, gt, test_k);
        }
        let avg_recall = total_recall / query_vectors.len() as f64;

        println!(
            "{:<15} {:<12.4} {:<12.2}% {:<15}",
            format!("{}x", oversample),
            avg_recall,
            avg_recall * 100.0,
            format_duration(avg_latency)
        );
    }

    // Benchmark raw Hamming distance throughput
    println!();
    println!("=== Hamming Distance Throughput ===");

    // Pre-quantize all vectors
    let binary_vectors: Vec<Vec<u8>> = index_vectors
        .iter()
        .map(|(_, v)| binary_quantize(v, &center))
        .collect();

    let query_binary: Vec<Vec<u8>> = query_vectors
        .iter()
        .map(|(_, v)| binary_quantize(v, &center))
        .collect();

    // Measure pure Hamming distance computation
    let hamming_start = Instant::now();
    let mut _dummy: u64 = 0; // Prevent optimization

    for qb in &query_binary {
        for vb in &binary_vectors {
            _dummy += hamming_distance(qb, vb) as u64;
        }
    }

    let hamming_time = hamming_start.elapsed();
    let total_comparisons = query_binary.len() * binary_vectors.len();
    let comparisons_per_sec = total_comparisons as f64 / hamming_time.as_secs_f64();
    let bytes_per_sec =
        (total_comparisons * binary_bytes_per_vector * 2) as f64 / hamming_time.as_secs_f64();

    println!(
        "Hamming comparisons: {} in {}",
        format_count(total_comparisons),
        format_duration(hamming_time)
    );
    println!(
        "Throughput: {:.2}M comparisons/sec",
        comparisons_per_sec / 1e6
    );
    println!("Bandwidth: {:.2} GB/s", bytes_per_sec / 1e9);

    println!();
    println!("=== Summary ===");
    println!(
        "Binary quantization on {} DBPedia vectors ({}D):",
        format_count(NUM_VECTORS),
        dim
    );
    println!(
        "  - Compression: {:.1}x ({} -> {} bytes)",
        compression_ratio, raw_bytes_per_vector, binary_bytes_per_vector
    );
    println!("  - Build: {:.0} vec/s", build_throughput);
    println!(
        "  - Search: {:.0} q/s (brute force over {} vectors)",
        qps,
        format_count(NUM_VECTORS)
    );
    println!(
        "  - Recall@10: {:.2}%",
        results
            .iter()
            .zip(ground_truths.iter())
            .map(|(r, g)| recall_at_k(r, g, 10))
            .sum::<f64>()
            / query_vectors.len() as f64
            * 100.0
    );

    println!("\nDone!");
    Ok(())
}
