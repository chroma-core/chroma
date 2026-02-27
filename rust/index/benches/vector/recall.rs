//! Recall benchmark for RaBitQ quantization on real embedding datasets.
//!
//! Measures recall@K for three scoring methods:
//!
//!   4bit-code-full-query  — 4-bit data quantization, f32 query (quality ceiling)
//!   1bit-code-4bit-query  — 1-bit data quantization, 4-bit quantized query (AND+popcount, §3.3.1)
//!   1bit-code-1bit-query  — 1-bit data quantization, 1-bit quantized query (distance_code)
//!
//! All methods apply a random orthogonal rotation P before quantization, matching the
//! production SPANN pipeline.  The rotation is what makes the RaBitQ estimator unbiased
//! (Theorem 3.2) and is what the O(1/√D) error bound relies on.  Without it, sign
//! quantization error is correlated with the data direction and the bound does not apply.
//!
//! Ground truth is computed in-process from the pre-processed vectors (after
//! normalization for cosine, before rotation), so it is correct for any distance
//! function.  For N=1M this adds ~10–30s depending on machine speed.
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
//!   cargo bench -p chroma-index --bench quantization_recall
//!   cargo bench -p chroma-index --bench quantization_recall -- --dataset cohere_wiki --size 10000
//!   cargo bench -p chroma-index --bench quantization_recall -- --size 10000 --k 100
//!   cargo bench -p chroma-index --bench quantization_recall -- --size 100000 --k 100 --rerank 1,2,4,8,16
//!   cargo bench -p chroma-index --bench quantization_recall -- --distance cosine --size 10000
//!   cargo bench -p chroma-index --bench quantization_recall -- --distance ip
//!
//! CLI flags:
//!   --dataset D       dataset slug (default: cohere_wiki; also: msmarco, beir, sec_filings)
//!   --size N          database size to test (may repeat; default: all available)
//!   --k K             recall@K to compute (default: 10)
//!   --rerank r1,r2,…  comma-separated rerank factors (default: 1,2,4,8,16)
//!   --distance D      distance function: euclidean (default), cosine, ip

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use chroma_distance::{normalize, DistanceFunction};
use chroma_index::quantization::{Code1Bit, Code4Bit, QuantizedQuery};
use faer::{
    stats::{
        prelude::{Distribution, StandardNormal, ThreadRng},
        UnitaryMat,
    },
    Mat,
};

// ── Configuration ─────────────────────────────────────────────────────────────

const DEFAULT_DIM: usize = 1024;
const DEFAULT_K: usize = 10;
const DEFAULT_RERANK_FACTORS: &[usize] = &[1, 2, 4, 8, 16];
const AVAILABLE_SIZES: &[usize] = &[10_000, 100_000, 1_000_000];
const DEFAULT_DATASET: &str = "cohere_wiki";

// Vectors are rotated in chunks of this many to keep peak extra memory bounded.
// At D=1024 and f32: ROTATE_CHUNK * D * 4 bytes ≈ 200 MB per input/output buffer.
const ROTATE_CHUNK: usize = 50_000;

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
        eprintln!(
            "       Run `python prepare_dataset.py` first.  See benches/vector/recall/README."
        );
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

/// Apply the distance function's pre-processing to a raw embedding.
///
/// For cosine, normalizes to unit length before quantization, matching the production
/// SPANN pipeline (`rotate` in `quantized_spann.rs` normalizes before rotating for
/// cosine distance).  Other distance functions return a copy of the input unchanged.
fn preprocess(v: &[f32], df: &DistanceFunction) -> Vec<f32> {
    match df {
        DistanceFunction::Cosine => normalize(v),
        _ => v.to_vec(),
    }
}

/// Exact distance between two pre-processed vectors under the given function.
///
/// Cosine and IP both compute `1 - dot(a, b)`.  For cosine, vectors are already
/// unit-normalized by `preprocess`, so this equals `1 - cos(a, b)`.
fn exact_distance(a: &[f32], b: &[f32], df: &DistanceFunction) -> f32 {
    match df {
        DistanceFunction::Euclidean => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
        DistanceFunction::Cosine | DistanceFunction::InnerProduct => {
            1.0 - a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>()
        }
    }
}

/// Brute-force KNN ground truth.
///
/// Returns for each query the indices of its K nearest neighbors in `db`,
/// computed from pre-processed (normalized for cosine, but not yet rotated)
/// vectors.  Rotation is not applied here because it preserves all distances,
/// so the ground truth indices are the same with or without rotation.
fn compute_ground_truth(
    db: &[Vec<f32>],
    queries: &[Vec<f32>],
    k: usize,
    df: &DistanceFunction,
) -> Vec<Vec<u32>> {
    queries
        .iter()
        .map(|q| {
            let mut dists: Vec<(usize, f32)> = db
                .iter()
                .enumerate()
                .map(|(i, v)| (i, exact_distance(v, q, df)))
                .collect();
            dists.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
            dists.iter().take(k).map(|(i, _)| *i as u32).collect()
        })
        .collect()
}

/// Sample a random D×D orthogonal matrix (the RaBitQ rotation P).
fn random_rotation(dim: usize) -> Mat<f32> {
    let dist = UnitaryMat {
        dim,
        standard_normal: StandardNormal,
    };
    dist.sample(&mut ThreadRng::default())
}

/// Rotate a batch of vectors by P: returns [P × v for each v in vectors].
///
/// Processes ROTATE_CHUNK vectors at a time as a single BLAS GEMM so that faer's
/// multi-threaded matrix multiply is used while peak extra memory stays bounded
/// (~400 MB per chunk at D=1024).
fn rotate_batch(p: &Mat<f32>, vectors: &[Vec<f32>]) -> Vec<Vec<f32>> {
    let n = vectors.len();
    if n == 0 {
        return vec![];
    }
    let dim = vectors[0].len();
    let mut result = Vec::with_capacity(n);
    for chunk in vectors.chunks(ROTATE_CHUNK) {
        let m = chunk.len();
        // Build m×dim matrix with one embedding per row.
        let v_mat = Mat::from_fn(m, dim, |i, j| chunk[i][j]);
        // (m×D) × (D×D)^T = (m×D) × (D×D) since P is orthogonal.
        // Each row vi rotates to (P × vi^T)^T = vi × P^T.
        let rotated = v_mat * p.transpose();
        for i in 0..m {
            result.push(rotated.row(i).iter().copied().collect::<Vec<f32>>());
        }
    }
    result
}

/// Score all codes against one query; return top-R indices sorted ascending.
fn top_r_by_estimated_distance(scores: &[f32], r: usize) -> Vec<usize> {
    let mut indexed: Vec<(usize, f32)> = scores.iter().copied().enumerate().collect();
    indexed.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    indexed.iter().take(r).map(|&(i, _)| i).collect()
}

/// recall@k with R candidates = |top_R_candidates ∩ true_top_K| / K.
///
/// With R = K this is standard recall@K.  With R = K * rerank_factor it measures
/// what recall a reranker achieves if it receives R candidates and re-scores them
/// exactly (picking the best K).
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

fn run_recall(
    dataset: &str,
    n: usize,
    k: usize,
    rerank_factors: &[usize],
    df: &DistanceFunction,
) -> Vec<RecallResult> {
    let dir = data_dir(dataset);
    let meta = load_meta(&dir.join(format!("meta_{n}.json")));
    let dim = meta["dim"].as_u64().unwrap_or(DEFAULT_DIM as u64) as usize;
    let vectors_raw = load_f32_matrix(&dir.join(format!("vectors_{n}.bin")), dim);
    let queries_raw = load_f32_matrix(&dir.join(format!("queries_{n}.bin")), dim);
    assert_eq!(vectors_raw.len(), n);
    let n_queries = queries_raw.len();

    // Step 1: pre-process (normalize for cosine; no-op for euclidean/ip).
    // This mirrors the production SPANN pipeline which normalizes before rotating.
    print!("  Pre-processing {n} vectors + {n_queries} queries ...");
    let t0 = Instant::now();
    let vectors: Vec<Vec<f32>> = vectors_raw.iter().map(|v| preprocess(v, df)).collect();
    let queries: Vec<Vec<f32>> = queries_raw.iter().map(|q| preprocess(q, df)).collect();
    println!(" {:.2}s", t0.elapsed().as_secs_f64());

    // Step 2: centroid of pre-processed vectors (before rotation).
    let centroid_unrotated = compute_centroid(&vectors);

    // Step 3: random orthogonal rotation.  This is the key piece absent from the
    // old benchmark.  Without it, sign quantization errors are correlated with the
    // data direction and the O(1/√D) error bound does not apply.
    println!("  Generating {dim}×{dim} random rotation P ...");
    let t0 = Instant::now();
    let p = random_rotation(dim);
    println!("  Generated in {:.2}s", t0.elapsed().as_secs_f64());

    // Step 4: rotate centroid, vectors, and queries.
    println!("  Rotating centroid + {n} vectors + {n_queries} queries ...");
    let t0 = Instant::now();
    let centroid = rotate_batch(&p, std::slice::from_ref(&centroid_unrotated))
        .into_iter()
        .next()
        .unwrap();
    let rotated_vectors = rotate_batch(&p, &vectors);
    let rotated_queries = rotate_batch(&p, &queries);
    println!("  Rotated in {:.2}s", t0.elapsed().as_secs_f64());

    // Step 5: ground truth from pre-processed unrotated vectors.
    // Rotation preserves all distances so ground truth indices are rotation-invariant.
    println!("  Computing ground truth ({n_queries} queries × {n} vectors) ...");
    let t0 = Instant::now();
    let ground_truth = compute_ground_truth(&vectors, &queries, k, df);
    println!("  Ground truth in {:.2}s", t0.elapsed().as_secs_f64());

    // Step 6: quantize the rotated vectors.
    let cn = c_norm(&centroid);
    let padded_bytes = Code1Bit::packed_len(dim);
    println!("  Quantizing {n} rotated vectors ...");
    let t0 = Instant::now();
    let codes_4: Vec<Vec<u8>> = rotated_vectors
        .iter()
        .map(|v| Code4Bit::quantize(v, &centroid).as_ref().to_vec())
        .collect();
    let t_q4 = t0.elapsed();
    let t0 = Instant::now();
    let codes_1: Vec<Vec<u8>> = rotated_vectors
        .iter()
        .map(|v| Code1Bit::quantize(v, &centroid).as_ref().to_vec())
        .collect();
    let t_q1 = t0.elapsed();
    println!(
        "  Quantized in {:.1}s (4bit) / {:.1}s (1bit)",
        t_q4.as_secs_f64(),
        t_q1.as_secs_f64()
    );

    // Step 7: score.  Each closure takes the rotated query residual
    // r_q = rotated_q - rotated_centroid and returns per-code estimated distances.
    // DistanceFunction is Clone-only, so clone once per closure.
    let df4f = df.clone();
    let df1b = df.clone();
    let df1c = df.clone();

    let methods: Vec<(&'static str, Box<dyn Fn(&[f32]) -> Vec<f32>>)> = vec![
        (
            "4bit-code-full-query",
            Box::new(|r_q: &[f32]| -> Vec<f32> {
                let cdq = c_dot_q(&centroid, r_q);
                let qn = q_norm(&centroid, r_q);
                codes_4
                    .iter()
                    .map(|cb| Code4Bit::new(cb.as_slice()).distance_query(&df4f, r_q, cn, cdq, qn))
                    .collect()
            }),
        ),
        (
            "1bit-code-4bit-query",
            Box::new(|r_q: &[f32]| -> Vec<f32> {
                let cdq = c_dot_q(&centroid, r_q);
                let qn = q_norm(&centroid, r_q);
                let qq = QuantizedQuery::new(r_q, 4, padded_bytes, cn, cdq, qn);
                codes_1
                    .iter()
                    .map(|cb| Code1Bit::new(cb.as_slice()).distance_4bit_query(&df1b, &qq))
                    .collect()
            }),
        ),
        (
            "1bit-code-1bit-query",
            Box::new(|r_q: &[f32]| -> Vec<f32> {
                let full_q: Vec<f32> = r_q.iter().zip(&centroid).map(|(r, c)| r + c).collect();
                let cq = Code1Bit::quantize(&full_q, &centroid);
                codes_1
                    .iter()
                    .map(|cb| Code1Bit::new(cb.as_slice()).distance_code(&df1c, &cq, cn, dim))
                    .collect()
            }),
        ),
    ];

    let mut results = Vec::new();

    for (method_name, score_fn) in &methods {
        println!("  Scoring {n_queries} queries with {method_name} ...");
        let t0 = Instant::now();

        // For each query: use the pre-rotated query vector, compute residual to
        // the rotated centroid, then score all codes.
        let all_scores: Vec<Vec<f32>> = rotated_queries
            .iter()
            .map(|rq| {
                let r_q: Vec<f32> = rq.iter().zip(&centroid).map(|(q, c)| q - c).collect();
                score_fn(&r_q)
            })
            .collect();

        let score_ms = t0.elapsed().as_secs_f64() * 1000.0;

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

fn df_name(df: &DistanceFunction) -> &'static str {
    match df {
        DistanceFunction::Euclidean => "euclidean",
        DistanceFunction::Cosine => "cosine",
        DistanceFunction::InnerProduct => "ip",
    }
}

fn parse_args() -> (String, Vec<usize>, usize, Vec<usize>, DistanceFunction) {
    let args: Vec<String> = std::env::args().collect();

    let mut dataset = DEFAULT_DATASET.to_string();
    let mut sizes = Vec::new();
    let mut k = DEFAULT_K;
    let mut rerank_factors = DEFAULT_RERANK_FACTORS.to_vec();
    let mut distance_str = "euclidean".to_string();

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
            "--distance" | "--dist" => {
                i += 1;
                if i < args.len() {
                    distance_str = args[i].to_lowercase();
                }
            }
            _ => {}
        }
        i += 1;
    }

    let df = match distance_str.as_str() {
        "cosine" => DistanceFunction::Cosine,
        "ip" | "inner_product" | "innerproduct" => DistanceFunction::InnerProduct,
        _ => DistanceFunction::Euclidean,
    };

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

    (dataset, sizes, k, rerank_factors, df)
}

fn print_results(dataset: &str, size: usize, df: &DistanceFunction, results: &[RecallResult]) {
    let hr = "═".repeat(96);
    let sep = "─".repeat(96);

    println!("\n{hr}");
    println!(
        "  Recall@{k} on {dataset}  (N={size}, distance={dist})",
        k = results[0].k,
        dist = df_name(df),
    );
    println!("{sep}");
    println!(
        "  {:<20} {:>8} {:>10} {:>12} {:>12} {:>12} {:>14}",
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
            "  {:<20} {:>5}x {:>10} {:>11.4} {:>11.4} {:>11.4} {:>13.1}",
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
    let (dataset, sizes, k, rerank_factors, df) = parse_args();

    println!("\n=== RaBitQ Recall Benchmark ===");
    println!(
        "  dataset={dataset}, distance={dist}, K={k}, rerank_factors={rerank_factors:?}",
        dist = df_name(&df),
    );
    println!("  sizes={sizes:?}\n");

    for &size in &sizes {
        let results = run_recall(&dataset, size, k, &rerank_factors, &df);
        print_results(&dataset, size, &df, &results);
    }
}
