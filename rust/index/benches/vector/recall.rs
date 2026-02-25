//! Recall benchmark for RaBitQ quantization on real embedding datasets.
//!
//! Measures recall@K for three implementations:
//!
//!   4bit_float   — 4-bit data quantization, f32 query (quality ceiling)
//!   1bit_float   — 1-bit data quantization, f32 query (signed_dot)
//!   1bit_bitwise — 1-bit data quantization, quantized query (AND+popcount, §3.3.1)
//!
//! For each, the entire database is quantized, then every query is scored
//! against all codes.  The top-(K * rerank_factor) candidates are selected
//! and intersected with the brute-force ground truth to measure recall.
//!
//! Data preparation (one-time):
//!   cd rust/index/benches/vector/recall
//!   pip install datasets numpy tqdm
//!   python prepare_dataset.py                       # cohere_wiki, all sizes
//!   python prepare_dataset.py --dataset msmarco    # msmarco dataset
//!   python prepare_dataset.py --dataset beir       # BEIR msmarco
//!   python prepare_dataset.py --dataset sec_filings # SEC filings
//!
//! Run:
//!   cargo bench -p chroma-index --bench recall
//!   cargo bench -p chroma-index --bench recall -- --dataset cohere_wiki --size 10000
//!   cargo bench -p chroma-index --bench recall -- --size 10000 --k 100
//!   cargo bench -p chroma-index --bench recall -- --size 100000 --k 100 --rerank 1,2,4,8,16
//!
//! CLI flags:
//!   --dataset D       dataset slug (default: cohere_wiki; also: msmarco, beir, sec_filings)
//!   --size N          database size to test (may repeat; default: all available)
//!   --k K             recall@K to compute (default: 10; max: ground-truth K from prepare_dataset.py, default 100)
//!   --rerank r1,r2,…  comma-separated rerank factors (default: 1,2,4,8,16,32,64)
//!
//! The benchmark prints a recall table to stdout (no criterion timing).

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code1Bit, Code4Bit, QuantizedQuery};

// ── Configuration ─────────────────────────────────────────────────────────────

const DEFAULT_DIM: usize = 1024;
const DEFAULT_K: usize = 10;
const DEFAULT_RERANK_FACTORS: &[usize] = &[1, 2, 4, 8, 16];
const AVAILABLE_SIZES: &[usize] = &[10_000, 100_000, 1_000_000];
const DEFAULT_DATASET: &str = "cohere_wiki";

fn data_dir(dataset: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("vector")
        .join("recall")
        .join("data__nogit")
        .join(dataset)
}

// ── Binary file I/O ──────────────────────────────────────────────────────────

fn load_f32_matrix(path: &std::path::Path, dim: usize) -> Vec<Vec<f32>> {
    let bytes = std::fs::read(path).unwrap_or_else(|e| {
        eprintln!("ERROR: Cannot read {}: {e}", path.display());
        eprintln!("       Run `python prepare_dataset.py` first.  See benches/vector/recall/README.");
        std::process::exit(1);
    });
    assert_eq!(
        bytes.len() % (dim * 4),
        0,
        "File size not a multiple of dim*4"
    );
    let n = bytes.len() / (dim * 4);
    let floats: &[f32] =
        unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const f32, n * dim) };
    floats.chunks_exact(dim).map(|c| c.to_vec()).collect()
}

fn load_ground_truth(path: &std::path::Path, k: usize) -> Vec<Vec<u32>> {
    let bytes = std::fs::read(path).unwrap_or_else(|e| {
        eprintln!("ERROR: Cannot read {}: {e}", path.display());
        eprintln!("       Run `python prepare_dataset.py` first.");
        std::process::exit(1);
    });
    assert_eq!(bytes.len() % (k * 4), 0, "File size not a multiple of k*4");
    let n = bytes.len() / (k * 4);
    let vals: &[u32] = unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const u32, n * k) };
    vals.chunks_exact(k).map(|c| c.to_vec()).collect()
}

fn load_meta(path: &std::path::Path) -> serde_json::Value {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("ERROR: Cannot read {}: {e}", path.display());
        std::process::exit(1);
    });
    serde_json::from_str(&text).unwrap()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn c_norm(centroid: &[f32]) -> f32 {
    centroid.iter().map(|x| x * x).sum::<f32>().sqrt()
}

fn c_dot_q(centroid: &[f32], r_q: &[f32]) -> f32 {
    centroid.iter().zip(r_q).map(|(c, r)| c * (r + c)).sum()
}

fn q_norm(centroid: &[f32], r_q: &[f32]) -> f32 {
    r_q.iter()
        .zip(centroid)
        .map(|(r, c)| (r + c) * (r + c))
        .sum::<f32>()
        .sqrt()
}

/// Compute a simple centroid as the mean of all vectors.
fn compute_centroid(vectors: &[Vec<f32>]) -> Vec<f32> {
    let dim = vectors[0].len();
    let n = vectors.len() as f32;
    let mut centroid = vec![0.0f32; dim];
    for v in vectors {
        for (c, &x) in centroid.iter_mut().zip(v) {
            *c += x;
        }
    }
    for c in &mut centroid {
        *c /= n;
    }
    centroid
}

/// Score all codes against one query, return top-R indices.
fn top_r_by_estimated_distance(scores: &[f32], r: usize) -> Vec<usize> {
    let mut indexed: Vec<(usize, f32)> = scores.iter().copied().enumerate().collect();
    indexed.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    indexed.iter().take(r).map(|&(i, _)| i).collect()
}

/// recall@k with R candidates = |top_R_candidates ∩ true_top_K| / K
///
/// `candidates` has R >= K elements (the top-R by estimated distance).
/// We check how many of the true top-K appear anywhere in those R candidates.
/// With R = K this is standard recall@K; with R = K * rerank_factor, it
/// measures what recall you'd get if you reranked R candidates with exact
/// distances and kept the best K.
fn recall_at_k(candidates: &[usize], ground_truth: &[u32], k: usize) -> f32 {
    let candidate_set: HashSet<usize> = candidates.iter().copied().collect();
    let hit_count = ground_truth
        .iter()
        .take(k)
        .filter(|&&gt_idx| candidate_set.contains(&(gt_idx as usize)))
        .count();
    hit_count as f32 / k as f32
}

// ── Benchmark runner ─────────────────────────────────────────────────────────

struct RecallResult {
    method: &'static str,
    rerank_factor: usize,
    k: usize,
    recall_mean: f32,
    recall_min: f32,
    recall_max: f32,
    score_time_ms: f64,
}

fn run_recall(dataset: &str, n: usize, k: usize, rerank_factors: &[usize]) -> Vec<RecallResult> {
    let dir = data_dir(dataset);
    let meta = load_meta(&dir.join(format!("meta_{n}.json")));
    let dim = meta["dim"].as_u64().unwrap_or(DEFAULT_DIM as u64) as usize;
    let vectors = load_f32_matrix(&dir.join(format!("vectors_{n}.bin")), dim);
    let queries = load_f32_matrix(&dir.join(format!("queries_{n}.bin")), dim);
    let gt_k = meta["k"].as_u64().unwrap() as usize;
    let ground_truth = load_ground_truth(&dir.join(format!("ground_truth_{n}.bin")), gt_k);

    assert!(
        k <= gt_k,
        "requested --k {k} exceeds the ground-truth K={gt_k} stored in meta_{n}.json.\n\
         Re-run prepare_dataset.py with --k {k} to rebuild ground truth."
    );
    assert_eq!(vectors.len(), n);
    assert_eq!(queries.len(), ground_truth.len());

    let centroid = compute_centroid(&vectors);
    let cn = c_norm(&centroid);
    let df = DistanceFunction::Euclidean;
    let padded_bytes = Code1Bit::packed_len(dim);

    println!("  Quantizing {n} vectors ...");

    let t0 = Instant::now();
    let codes_4: Vec<Vec<u8>> = vectors
        .iter()
        .map(|v| Code4Bit::quantize(v, &centroid).as_ref().to_vec())
        .collect();
    let t_q4 = t0.elapsed();

    let t0 = Instant::now();
    let codes_1: Vec<Vec<u8>> = vectors
        .iter()
        .map(|v| Code1Bit::quantize(v, &centroid).as_ref().to_vec())
        .collect();
    let t_q1 = t0.elapsed();

    println!(
        "  Quantized in {:.1}s (4bit) / {:.1}s (1bit)",
        t_q4.as_secs_f64(),
        t_q1.as_secs_f64()
    );

    let n_queries = queries.len();
    let mut results = Vec::new();

    // ── Score all queries with each method ────────────────────────────────
    let methods: Vec<(&str, Box<dyn Fn(&[f32]) -> Vec<f32>>)> = vec![
        (
            "4bit_float",
            Box::new(|r_q: &[f32]| -> Vec<f32> {
                let cdq = c_dot_q(&centroid, r_q);
                let qn = q_norm(&centroid, r_q);
                codes_4
                    .iter()
                    .map(|cb| Code4Bit::new(cb.as_slice()).distance_query(&df, r_q, cn, cdq, qn))
                    .collect()
            }),
        ),
        (
            "1bit_float",
            Box::new(|r_q: &[f32]| -> Vec<f32> {
                let cdq = c_dot_q(&centroid, r_q);
                let qn = q_norm(&centroid, r_q);
                codes_1
                    .iter()
                    .map(|cb| {
                        Code1Bit::new(cb.as_slice())
                            .distance_query_full_precision(&df, r_q, cn, cdq, qn)
                    })
                    .collect()
            }),
        ),
        (
            "1bit_bitwise",
            Box::new(|r_q: &[f32]| -> Vec<f32> {
                let cdq = c_dot_q(&centroid, r_q);
                let qn = q_norm(&centroid, r_q);
                let qq = QuantizedQuery::new(r_q, 4, padded_bytes, cn, cdq, qn);
                codes_1
                    .iter()
                    .map(|cb| Code1Bit::new(cb.as_slice()).distance_query(&df, &qq))
                    .collect()
            }),
        ),
    ];

    for (method_name, score_fn) in &methods {
        println!("  Scoring {n_queries} queries with {method_name} ...");
        let t0 = Instant::now();

        // Collect scores for all queries.
        let all_scores: Vec<Vec<f32>> = queries
            .iter()
            .map(|q| {
                let r_q: Vec<f32> = q.iter().zip(&centroid).map(|(q, c)| q - c).collect();
                score_fn(&r_q)
            })
            .collect();

        let score_time = t0.elapsed();
        let score_ms = score_time.as_secs_f64() * 1000.0;

        for &rf in rerank_factors {
            let r = k * rf;
            if r > n {
                continue;
            }
            let mut recalls = Vec::with_capacity(n_queries);
            for (qi, scores) in all_scores.iter().enumerate() {
                let top = top_r_by_estimated_distance(scores, r);
                recalls.push(recall_at_k(&top, &ground_truth[qi], k));
            }
            let recall_mean = recalls.iter().sum::<f32>() / recalls.len() as f32;
            let recall_min = recalls.iter().copied().fold(f32::INFINITY, f32::min);
            let recall_max = recalls.iter().copied().fold(f32::NEG_INFINITY, f32::max);

            results.push(RecallResult {
                method: method_name,
                rerank_factor: rf,
                k,
                recall_mean,
                recall_min,
                recall_max,
                score_time_ms: score_ms,
            });
        }
    }

    results
}

// ── CLI + pretty-print ───────────────────────────────────────────────────────

fn parse_args() -> (String, Vec<usize>, usize, Vec<usize>) {
    let args: Vec<String> = std::env::args().collect();

    let mut dataset = DEFAULT_DATASET.to_string();
    let mut sizes = Vec::new();
    let mut k = DEFAULT_K;
    let mut rerank_factors = DEFAULT_RERANK_FACTORS.to_vec();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dataset" | "-d" => {
                i += 1;
                if i < args.len() {
                    dataset = args[i].clone();
                }
            }
            "--size" | "-s" => {
                i += 1;
                if i < args.len() {
                    sizes.push(args[i].replace('_', "").parse::<usize>().unwrap());
                }
            }
            "--k" => {
                i += 1;
                if i < args.len() {
                    k = args[i].parse().unwrap();
                }
            }
            "--rerank" => {
                i += 1;
                if i < args.len() {
                    rerank_factors = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().unwrap())
                        .collect();
                }
            }
            _ => {}
        }
        i += 1;
    }

    if sizes.is_empty() {
        let dir = data_dir(&dataset);
        for &sz in AVAILABLE_SIZES {
            if dir.join(format!("vectors_{sz}.bin")).exists() {
                sizes.push(sz);
            }
        }
        if sizes.is_empty() {
            eprintln!("ERROR: No dataset files found in {}", dir.display());
            eprintln!("       Run `python prepare_dataset.py --dataset {dataset}` first.");
            eprintln!("       See rust/index/benches/vector/recall/prepare_dataset.py");
            std::process::exit(1);
        }
    }

    (dataset, sizes, k, rerank_factors)
}

fn print_results(dataset: &str, size: usize, results: &[RecallResult]) {
    let hr = "═".repeat(94);
    let sep = "─".repeat(94);

    println!("\n{hr}");
    println!("  Recall@{k} on {dataset}  (N={size})", k = results[0].k);
    println!("{sep}");
    println!(
        "  {:<16} {:>8} {:>10} {:>12} {:>12} {:>12} {:>14}",
        "method", "rerank", "candidates", "recall_mean", "recall_min", "recall_max", "score_ms"
    );
    println!("{sep}");

    let mut prev_method = "";
    for r in results {
        if !prev_method.is_empty() && r.method != prev_method {
            println!("{sep}");
        }
        prev_method = r.method;
        println!(
            "  {:<16} {:>5}x {:>10} {:>11.4} {:>11.4} {:>11.4} {:>13.1}",
            r.method,
            r.rerank_factor,
            r.k * r.rerank_factor,
            r.recall_mean,
            r.recall_min,
            r.recall_max,
            r.score_time_ms,
        );
    }
    println!("{hr}\n");
}

fn main() {
    let (dataset, sizes, k, rerank_factors) = parse_args();

    println!("\n=== RaBitQ Recall Benchmark ===");
    println!("  dataset={dataset}, K={k}, rerank_factors={rerank_factors:?}");
    println!("  sizes={sizes:?}\n");

    for &size in &sizes {
        let results = run_recall(&dataset, size, k, &rerank_factors);
        print_results(&dataset, size, &results);
    }
}
