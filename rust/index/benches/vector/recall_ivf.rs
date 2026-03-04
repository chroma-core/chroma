//! IVF recall benchmark: production-like SPANN pipeline with per-cluster centroids.
//!
//! Simulates the full quantized SPANN query path:
//!
//!   1. KMeans clusters the database into C clusters (each with its own centroid).
//!   2. A random orthogonal rotation P is applied before quantization.
//!   3. Centroids are quantized relative to a global centroid (centroid-of-centroids).
//!   4. Data vectors are quantized relative to their cluster's rotated centroid.
//!   5. At query time the query is rotated, then:
//!      a. The closest `nprobe * centroid_rerank_factor` centroids are found via
//!         **quantized** brute-force distance (simulating the production quantized HNSW).
//!      b. If centroid_rerank_factor > 1, those candidates are re-scored with exact
//!         centroid distance and the top `nprobe` are kept.
//!   6. The quantized codes in the selected clusters are scored; the top
//!      `k * vector_rerank_factor` candidates are collected and optionally reranked
//!      with exact distances.
//!
//! This measures end-to-end recall@K and decomposes it into:
//!   - centroid_recall_cieling: ceiling -- what exact centroid search achieves at nprobe
//!   - centroid_recall: what the quantized centroid pipeline achieves at this centroid_rerank_factor
//!   - vector_recall: within-cluster quantized shortlist recall
//!   - vector_recall_reranked: final recall after exact-distance vector reranking
//!
//! Data preparation: same as `recall.rs` (see `recall/prepare_dataset.py`).
//!
//! Run:
//!   cargo bench -p chroma-index --bench recall_ivf
//!   cargo bench -p chroma-index --bench recall_ivf -- --size 100000
//!   cargo bench -p chroma-index --bench recall_ivf -- --distance cosine --clusters 64
//!   cargo bench -p chroma-index --bench recall_ivf -- --nprobe 1,2,4,8 --rerank 1,4,16
//!   cargo bench -p chroma-index --bench recall_ivf -- --centroid-rerank 1,2,4,8
//!   cargo bench -p chroma-index --bench recall_ivf -- --centroid-bits 4
//!
//! CLI flags:
//!   --dataset D              dataset slug (default: cohere_wiki)
//!   --size N                 database size (may repeat; default: all available)
//!   --k K                    recall@K (default: 10)
//!   --clusters C             number of IVF clusters (default: sqrt(N))
//!   --nprobe n1,n2,...        probed clusters per query (default: 1,2,4,8,16)
//!   --rerank r1,r2,...        vector rerank factors (default: 1,4)
//!   --centroid-rerank r,...   centroid rerank factors (default: 1,2,4,8,16)
//!   --centroid-bits B         quantization bits for centroids: 1 (default) or 4
//!   --data-bits B             quantization bits for data vectors: 1 (default) or 4
//!   --cluster-bits B          quantization bits for KMeans assignment: 1 or 4 (default: exact)
//!   --distance D              euclidean (default), cosine, ip
//!
//! When --cluster-bits is set, KMeans uses code-vs-code distance for the
//! assignment step instead of exact f32 distance. Vectors are quantized once
//! up front (relative to the global centroid of all rotated vectors).
//! Centroids are re-quantized each iteration (since they change). The update
//! step still uses raw f32 vectors to recompute centroid means.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use chroma_distance::{normalize, DistanceFunction};
use chroma_index::quantization::{Code, QuantizedQuery, RabitqCode};
use faer::{
    col::ColRef,
    stats::{
        prelude::{Distribution, StandardNormal, ThreadRng},
        UnitaryMat,
    },
    Mat,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::prelude::*;

// ── Configuration ─────────────────────────────────────────────────────────────

const DEFAULT_DIM: usize = 1024;
const DEFAULT_K: usize = 10;
const DEFAULT_NPROBE: &[usize] = &[16, 32, 64, 128];
const DEFAULT_VECTOR_RERANK_FACTORS: &[usize] = &[1, 2, 4];
const DEFAULT_CENTROID_RERANK_FACTORS: &[usize] = &[1, 2, 4];
const AVAILABLE_SIZES: &[usize] = &[10_000, 100_000, 1_000_000, 10_000_000];
const DEFAULT_DATASET: &str = "cohere_wiki";
const DEFAULT_DATA_BITS: u8 = 1;
const DEFAULT_CENTROID_BITS: u8 = 1;

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
        eprintln!("       Run `python prepare_dataset.py` first.");
        std::process::exit(1);
    });
    assert_eq!(bytes.len() % (dim * 4), 0);
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

// ── Vector helpers ───────────────────────────────────────────────────────────

fn preprocess(v: &[f32], df: &DistanceFunction) -> Vec<f32> {
    match df {
        DistanceFunction::Cosine => normalize(v),
        _ => v.to_vec(),
    }
}

fn exact_distance(a: &[f32], b: &[f32], df: &DistanceFunction) -> f32 {
    match df {
        DistanceFunction::Euclidean => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
        DistanceFunction::Cosine | DistanceFunction::InnerProduct => {
            1.0 - a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>()
        }
    }
}

fn compute_ground_truth(
    db: &[Vec<f32>],
    queries: &[Vec<f32>],
    k: usize,
    df: &DistanceFunction,
) -> Vec<Vec<u32>> {
    queries
        .par_iter()
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

fn rotate_vec(p: &Mat<f32>, v: &[f32]) -> Vec<f32> {
    let result = p * ColRef::from_slice(v);
    result.iter().copied().collect()
}

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

fn compute_mean(vectors: &[Vec<f32>]) -> Vec<f32> {
    let dim = vectors[0].len();
    let n = vectors.len() as f32;
    let mut mean = vec![0.0f32; dim];
    for v in vectors {
        for (m, &x) in mean.iter_mut().zip(v) {
            *m += x;
        }
    }
    for m in &mut mean {
        *m /= n;
    }
    mean
}

// ── Simple KMeans ────────────────────────────────────────────────────────────

struct KMeansResult {
    centroids: Vec<Vec<f32>>,
    assignments: Vec<usize>,
}

fn simple_kmeans(vectors: &[Vec<f32>], n_clusters: usize, df: &DistanceFunction) -> KMeansResult {
    let n = vectors.len();
    let dim = vectors[0].len();
    let mut rng = StdRng::seed_from_u64(42);

    let mut indices: Vec<usize> = (0..n).collect();
    for i in 0..n_clusters.min(n) {
        let j = rng.gen_range(i..n);
        indices.swap(i, j);
    }
    let mut centroids: Vec<Vec<f32>> = indices[..n_clusters.min(n)]
        .iter()
        .map(|&i| vectors[i].clone())
        .collect();

    let mut assignments = vec![0usize; n];

    for _ in 0..50 {
        let new_assignments: Vec<usize> = vectors
            .par_iter()
            .map(|v| {
                centroids
                    .iter()
                    .enumerate()
                    .map(|(c, cent)| (c, exact_distance(v, cent, df)))
                    .min_by(|a, b| a.1.total_cmp(&b.1))
                    .unwrap()
                    .0
            })
            .collect();

        let changed = assignments
            .iter()
            .zip(&new_assignments)
            .any(|(old, new)| old != new);
        assignments = new_assignments;

        if !changed {
            break;
        }

        let mut sums = vec![vec![0.0f32; dim]; n_clusters];
        let mut counts = vec![0usize; n_clusters];
        for (i, v) in vectors.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for (j, &x) in v.iter().enumerate() {
                sums[c][j] += x;
            }
        }
        for c in 0..n_clusters {
            if counts[c] > 0 {
                for j in 0..dim {
                    centroids[c][j] = sums[c][j] / counts[c] as f32;
                }
            }
        }
    }

    KMeansResult {
        centroids,
        assignments,
    }
}

fn quantized_kmeans(
    vectors: &[Vec<f32>],
    n_clusters: usize,
    df: &DistanceFunction,
    global_centroid: &[f32],
    cluster_bits: u8,
) -> KMeansResult {
    let n = vectors.len();
    let dim = vectors[0].len();
    let gc_norm = c_norm(global_centroid);
    let mut rng = StdRng::seed_from_u64(42);

    let mut indices: Vec<usize> = (0..n).collect();
    for i in 0..n_clusters.min(n) {
        let j = rng.gen_range(i..n);
        indices.swap(i, j);
    }
    let mut centroids: Vec<Vec<f32>> = indices[..n_clusters.min(n)]
        .iter()
        .map(|&i| vectors[i].clone())
        .collect();

    println!("    Quantizing {n} vectors for KMeans ({cluster_bits}-bit) ...");
    let t0 = Instant::now();
    let vector_codes: Vec<Vec<u8>> = vectors
        .par_iter()
        .map(|v| match cluster_bits {
            4 => Code::<4>::quantize(v, global_centroid).as_ref().to_vec(),
            _ => Code::<1>::quantize(v, global_centroid).as_ref().to_vec(),
        })
        .collect();
    println!("    Quantized vectors in {:.2}s", t0.elapsed().as_secs_f64());

    let mut assignments = vec![0usize; n];

    for iter in 0..50 {
        let centroid_codes: Vec<Vec<u8>> = centroids
            .iter()
            .map(|c| match cluster_bits {
                4 => Code::<4>::quantize(c, global_centroid).as_ref().to_vec(),
                _ => Code::<1>::quantize(c, global_centroid).as_ref().to_vec(),
            })
            .collect();

        let new_assignments: Vec<usize> = vector_codes
            .par_iter()
            .map(|vc| {
                centroid_codes
                    .iter()
                    .enumerate()
                    .map(|(c, cc)| {
                        let d = match cluster_bits {
                            4 => Code::<4, _>::new(vc.as_slice())
                                .distance_code(&Code::<4, _>::new(cc.as_slice()), df, gc_norm, dim),
                            _ => Code::<1, _>::new(vc.as_slice())
                                .distance_code(&Code::<1, _>::new(cc.as_slice()), df, gc_norm, dim),
                        };
                        (c, d)
                    })
                    .min_by(|a, b| a.1.total_cmp(&b.1))
                    .unwrap()
                    .0
            })
            .collect();

        let changed = assignments
            .iter()
            .zip(&new_assignments)
            .any(|(old, new)| old != new);
        assignments = new_assignments;

        if !changed {
            println!("    KMeans converged at iteration {iter}");
            break;
        }

        let mut sums = vec![vec![0.0f32; dim]; n_clusters];
        let mut counts = vec![0usize; n_clusters];
        for (i, v) in vectors.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for (j, &x) in v.iter().enumerate() {
                sums[c][j] += x;
            }
        }
        for c in 0..n_clusters {
            if counts[c] > 0 {
                for j in 0..dim {
                    centroids[c][j] = sums[c][j] / counts[c] as f32;
                }
            }
        }
    }

    KMeansResult {
        centroids,
        assignments,
    }
}

// ── Per-cluster quantized data ───────────────────────────────────────────────

struct QuantizedCluster {
    centroid: Vec<f32>,
    member_indices: Vec<usize>,
    codes: Vec<u8>,
    code_size: usize,
}

// ── Scoring ──────────────────────────────────────────────────────────────────

struct ScoredCandidate {
    global_idx: usize,
    distance: f32,
}

fn score_cluster(
    cluster: &QuantizedCluster,
    rotated_query: &[f32],
    df: &DistanceFunction,
    data_bits: u8,
) -> Vec<ScoredCandidate> {
    let centroid = &cluster.centroid;
    let cn = c_norm(centroid);
    let r_q: Vec<f32> = rotated_query
        .iter()
        .zip(centroid.iter())
        .map(|(q, c)| q - c)
        .collect();
    let cdq = c_dot_q(centroid, &r_q);
    let qn = q_norm(centroid, &r_q);
    let dim = centroid.len();

    match data_bits {
        1 => {
            let padded_bytes = Code::<1>::packed_len(dim);
            let qq = QuantizedQuery::new(&r_q, 4, padded_bytes, cn, cdq, qn);
            cluster
                .member_indices
                .iter()
                .zip(cluster.codes.chunks(cluster.code_size))
                .map(|(&global_idx, code_bytes)| {
                    let distance = Code::<1, _>::new(code_bytes).distance_4bit_query(df, &qq);
                    ScoredCandidate {
                        global_idx,
                        distance,
                    }
                })
                .collect()
        }
        _ => cluster
            .member_indices
            .iter()
            .zip(cluster.codes.chunks(cluster.code_size))
            .map(|(&global_idx, code_bytes)| {
                let distance = Code::<4, _>::new(code_bytes).distance_query(df, &r_q, cn, cdq, qn);
                ScoredCandidate {
                    global_idx,
                    distance,
                }
            })
            .collect(),
    }
}

// ── Centroid recall helper ───────────────────────────────────────────────────

fn centroid_recall(
    probed_per_query: &[&[usize]],
    cluster_members: &[Vec<usize>],
    gt_sets: &[HashSet<u32>],
    k: usize,
) -> f32 {
    let n_queries = probed_per_query.len();
    let mut total = 0.0f32;
    for (qi, probed) in probed_per_query.iter().enumerate() {
        let probed_members: HashSet<u32> = probed
            .iter()
            .flat_map(|&c| cluster_members[c].iter().map(|&i| i as u32))
            .collect();
        let hits = gt_sets[qi]
            .iter()
            .filter(|idx| probed_members.contains(idx))
            .count();
        total += hits as f32 / k as f32;
    }
    total / n_queries as f32
}

// ── Benchmark runner ─────────────────────────────────────────────────────────

struct IvfRecallResult {
    nprobe: usize,
    centroid_rerank_factor: usize,
    vector_rerank_factor: usize,
    centroid_recall_cieling: f32,
    centroid_recall: f32,
    vector_recall: f32,
    vector_recall_reranked: f32,
}

fn run_ivf_recall(
    dataset: &str,
    n: usize,
    k: usize,
    n_clusters: usize,
    nprobes: &[usize],
    centroid_rerank_factors: &[usize],
    vector_rerank_factors: &[usize],
    df: &DistanceFunction,
    data_bits: u8,
    centroid_bits: u8,
    cluster_bits: Option<u8>,
) -> Vec<IvfRecallResult> {
    let dir = data_dir(dataset);
    let meta = load_meta(&dir.join(format!("meta_{n}.json")));
    let dim = meta["dim"].as_u64().unwrap_or(DEFAULT_DIM as u64) as usize;
    let vectors_raw = load_f32_matrix(&dir.join(format!("vectors_{n}.bin")), dim);
    let queries_raw = load_f32_matrix(&dir.join(format!("queries_{n}.bin")), dim);
    assert_eq!(vectors_raw.len(), n);
    let n_queries = queries_raw.len();

    // Step 1: pre-process
    print!("  Pre-processing {n} vectors + {n_queries} queries ...");
    let t0 = Instant::now();
    let vectors: Vec<Vec<f32>> = vectors_raw.iter().map(|v| preprocess(v, df)).collect();
    let queries: Vec<Vec<f32>> = queries_raw.iter().map(|q| preprocess(q, df)).collect();
    println!(" {:.2}s", t0.elapsed().as_secs_f64());

    // Step 2: ground truth (before rotation -- rotation preserves distances)
    println!("  Computing ground truth ({n_queries} queries x {n} vectors) ...");
    let t0 = Instant::now();
    let ground_truth = compute_ground_truth(&vectors, &queries, k, df);
    println!("  Ground truth in {:.2}s", t0.elapsed().as_secs_f64());

    // Step 3: random orthogonal rotation (before KMeans so quantized KMeans can use it)
    println!("  Generating {dim}x{dim} random rotation P ...");
    let t0 = Instant::now();
    let p = {
        let dist = UnitaryMat {
            dim,
            standard_normal: StandardNormal,
        };
        dist.sample(&mut ThreadRng::default())
    };
    println!("  Generated in {:.2}s", t0.elapsed().as_secs_f64());

    // Step 3b: rotate all vectors (needed for both quantized KMeans and cluster building)
    println!("  Rotating {n} vectors ...");
    let t0 = Instant::now();
    let rotated_vectors: Vec<Vec<f32>> = vectors
        .par_iter()
        .map(|v| rotate_vec(&p, v))
        .collect();
    println!("  Rotated in {:.2}s", t0.elapsed().as_secs_f64());

    // Global centroid of rotated vectors (used for quantized KMeans and centroid quantization)
    let global_centroid = compute_mean(&rotated_vectors);

    // Step 4: KMeans clustering on rotated vectors
    let cluster_mode = match cluster_bits {
        Some(b) => format!("{b}-bit quantized"),
        None => "exact".to_string(),
    };
    println!("  KMeans clustering into {n_clusters} clusters ({cluster_mode} distances) ...");
    let t0 = Instant::now();
    let km = match cluster_bits {
        Some(bits) => quantized_kmeans(&rotated_vectors, n_clusters, df, &global_centroid, bits),
        None => simple_kmeans(&rotated_vectors, n_clusters, df),
    };
    let mut cluster_sizes: Vec<usize> = vec![0; n_clusters];
    for &a in &km.assignments {
        cluster_sizes[a] += 1;
    }
    let non_empty = cluster_sizes.iter().filter(|&&c| c > 0).count();
    let avg_size = n as f32 / non_empty as f32;
    let max_size = cluster_sizes.iter().max().unwrap();
    let min_size = cluster_sizes.iter().filter(|&&c| c > 0).min().unwrap();
    println!(
        "  Clustered in {:.2}s: {non_empty} non-empty clusters, avg={avg_size:.0}, min={min_size}, max={max_size}",
        t0.elapsed().as_secs_f64()
    );

    let mut cluster_members: Vec<Vec<usize>> = vec![Vec::new(); n_clusters];
    for (i, &c) in km.assignments.iter().enumerate() {
        cluster_members[c].push(i);
    }

    // Centroids from KMeans are already in rotated space (no separate rotation needed)
    let rotated_centroids = km.centroids;

    // Step 5: quantize centroids relative to global centroid
    println!("  Quantizing {n_clusters} centroids ({centroid_bits}-bit) ...");
    let t0 = Instant::now();
    let gc_norm = c_norm(&global_centroid);
    let centroid_codes: Vec<Vec<u8>> = rotated_centroids
        .iter()
        .map(|c| match centroid_bits {
            4 => Code::<4>::quantize(c, &global_centroid).as_ref().to_vec(),
            _ => Code::<1>::quantize(c, &global_centroid).as_ref().to_vec(),
        })
        .collect();
    println!(
        "  Centroid quantization in {:.4}s",
        t0.elapsed().as_secs_f64()
    );

    // Step 6: build per-cluster quantized data codes (vectors already rotated)
    println!("  Building {n_clusters} quantized clusters ({data_bits}-bit data) ...");
    let t0 = Instant::now();
    let code_size = match data_bits {
        1 => Code::<1>::size(dim),
        _ => Code::<4>::size(dim),
    };
    let quantized_clusters: Vec<QuantizedCluster> = (0..n_clusters)
        .into_par_iter()
        .map(|c| {
            let centroid = &rotated_centroids[c];
            let members = &cluster_members[c];
            let mut codes = Vec::with_capacity(members.len() * code_size);
            for &idx in members {
                let code_bytes = match data_bits {
                    1 => Code::<1>::quantize(&rotated_vectors[idx], centroid)
                        .as_ref()
                        .to_vec(),
                    _ => Code::<4>::quantize(&rotated_vectors[idx], centroid)
                        .as_ref()
                        .to_vec(),
                };
                codes.extend_from_slice(&code_bytes);
            }
            QuantizedCluster {
                centroid: centroid.clone(),
                member_indices: members.clone(),
                codes,
                code_size,
            }
        })
        .collect();
    println!(
        "  Built quantized clusters in {:.2}s",
        t0.elapsed().as_secs_f64()
    );

    // Step 7: pre-rotate queries and build centroid rankings
    println!("  Scoring {n_queries} queries ...");
    let t0 = Instant::now();

    let rotated_queries: Vec<Vec<f32>> = queries.iter().map(|q| rotate_vec(&p, q)).collect();

    // Exact centroid ranking (baseline / ceiling)
    let exact_centroid_rankings: Vec<Vec<usize>> = rotated_queries
        .iter()
        .map(|rq| {
            let mut dists: Vec<(usize, f32)> = rotated_centroids
                .iter()
                .enumerate()
                .map(|(c, cent)| (c, exact_distance(rq, cent, df)))
                .collect();
            dists.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
            dists.iter().map(|(c, _)| *c).collect()
        })
        .collect();

    // Quantized centroid ranking
    let quantized_centroid_rankings: Vec<Vec<usize>> = rotated_queries
        .iter()
        .map(|rq| {
            let r_q: Vec<f32> = rq
                .iter()
                .zip(&global_centroid)
                .map(|(q, c)| q - c)
                .collect();
            let cdq = c_dot_q(&global_centroid, &r_q);
            let qn = q_norm(&global_centroid, &r_q);
            let mut dists: Vec<(usize, f32)> = match centroid_bits {
                4 => centroid_codes
                    .iter()
                    .enumerate()
                    .map(|(c, code_bytes)| {
                        let d = Code::<4, _>::new(code_bytes.as_slice())
                            .distance_query(df, &r_q, gc_norm, cdq, qn);
                        (c, d)
                    })
                    .collect(),
                _ => {
                    let padded = Code::<1>::packed_len(dim);
                    let qq = QuantizedQuery::new(&r_q, 4, padded, gc_norm, cdq, qn);
                    centroid_codes
                        .iter()
                        .enumerate()
                        .map(|(c, code_bytes)| {
                            let d = Code::<1, _>::new(code_bytes.as_slice())
                                .distance_4bit_query(df, &qq);
                            (c, d)
                        })
                        .collect()
                }
            };
            dists.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
            dists.iter().map(|(c, _)| *c).collect()
        })
        .collect();

    let gt_sets: Vec<HashSet<u32>> = ground_truth
        .iter()
        .map(|gt| gt.iter().copied().collect())
        .collect();

    let mut results = Vec::new();

    for &nprobe in nprobes {
        // Exact centroid recall baseline at this nprobe
        let exact_cr = centroid_recall(
            &exact_centroid_rankings
                .iter()
                .map(|r| &r[..nprobe.min(r.len())])
                .collect::<Vec<_>>(),
            &cluster_members,
            &gt_sets,
            k,
        );

        for &crf in centroid_rerank_factors {
            let n_candidates = nprobe * crf;

            // Select clusters: quantized search for n_candidates, rerank to nprobe
            let probed_per_query: Vec<Vec<usize>> = if crf <= 1 {
                quantized_centroid_rankings
                    .iter()
                    .map(|r| r[..nprobe.min(r.len())].to_vec())
                    .collect()
            } else {
                rotated_queries
                    .iter()
                    .zip(quantized_centroid_rankings.iter())
                    .map(|(rq, qr)| {
                        let candidates = &qr[..n_candidates.min(qr.len())];
                        let mut reranked: Vec<(usize, f32)> = candidates
                            .iter()
                            .map(|&c| (c, exact_distance(rq, &rotated_centroids[c], df)))
                            .collect();
                        reranked.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
                        reranked.iter().take(nprobe).map(|(c, _)| *c).collect()
                    })
                    .collect()
            };

            // Centroid recall after the quantized + rerank pipeline
            let cr = centroid_recall(
                &probed_per_query
                    .iter()
                    .map(|v| v.as_slice())
                    .collect::<Vec<_>>(),
                &cluster_members,
                &gt_sets,
                k,
            );

            // Score all probed clusters for each query
            let per_query_scored: Vec<Vec<ScoredCandidate>> = rotated_queries
                .iter()
                .zip(probed_per_query.iter())
                .map(|(rq, probed)| {
                    let mut all_candidates: Vec<ScoredCandidate> = Vec::new();
                    for &cluster_id in probed {
                        let cluster = &quantized_clusters[cluster_id];
                        if cluster.member_indices.is_empty() {
                            continue;
                        }
                        all_candidates.extend(score_cluster(cluster, rq, df, data_bits));
                    }
                    all_candidates.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));
                    all_candidates
                })
                .collect();

            for &vrf in vector_rerank_factors {
                let r = k * vrf;

                let vector_recall = {
                    let mut total = 0.0f32;
                    for (qi, candidates) in per_query_scored.iter().enumerate() {
                        let shortlist: HashSet<u32> = candidates
                            .iter()
                            .take(r)
                            .map(|c| c.global_idx as u32)
                            .collect();
                        let hits = gt_sets[qi]
                            .iter()
                            .filter(|idx| shortlist.contains(idx))
                            .count();
                        total += hits as f32 / k as f32;
                    }
                    total / n_queries as f32
                };

                let vector_recall_reranked = if vrf <= 1 {
                    vector_recall
                } else {
                    let mut total = 0.0f32;
                    for (qi, candidates) in per_query_scored.iter().enumerate() {
                        let mut reranked: Vec<(usize, f32)> = candidates
                            .iter()
                            .take(r)
                            .map(|c| {
                                let exact =
                                    exact_distance(&vectors[c.global_idx], &queries[qi], df);
                                (c.global_idx, exact)
                            })
                            .collect();
                        reranked.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
                        let final_set: HashSet<u32> = reranked
                            .iter()
                            .take(k)
                            .map(|(idx, _)| *idx as u32)
                            .collect();
                        let hits = gt_sets[qi]
                            .iter()
                            .filter(|idx| final_set.contains(idx))
                            .count();
                        total += hits as f32 / k as f32;
                    }
                    total / n_queries as f32
                };

                results.push(IvfRecallResult {
                    nprobe,
                    centroid_rerank_factor: crf,
                    vector_rerank_factor: vrf,
                    centroid_recall_cieling: exact_cr,
                    centroid_recall: cr,
                    vector_recall,
                    vector_recall_reranked,
                });
            }
        }
    }

    println!("  Scored in {:.2}s", t0.elapsed().as_secs_f64());
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

struct CliArgs {
    dataset: String,
    sizes: Vec<usize>,
    k: usize,
    clusters: Option<usize>,
    nprobes: Vec<usize>,
    centroid_rerank_factors: Vec<usize>,
    vector_rerank_factors: Vec<usize>,
    df: DistanceFunction,
    data_bits: u8,
    centroid_bits: u8,
    cluster_bits: Option<u8>,
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = std::env::args().collect();

    let mut dataset = DEFAULT_DATASET.to_string();
    let mut sizes = Vec::new();
    let mut k = DEFAULT_K;
    let mut clusters: Option<usize> = None;
    let mut nprobes = DEFAULT_NPROBE.to_vec();
    let mut centroid_rerank_factors = DEFAULT_CENTROID_RERANK_FACTORS.to_vec();
    let mut vector_rerank_factors = DEFAULT_VECTOR_RERANK_FACTORS.to_vec();
    let mut distance_str = "euclidean".to_string();
    let mut data_bits = DEFAULT_DATA_BITS;
    let mut centroid_bits = DEFAULT_CENTROID_BITS;
    let mut cluster_bits: Option<u8> = None;

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
            "--clusters" => {
                i += 1;
                if i < args.len() {
                    clusters = Some(args[i].parse().unwrap());
                }
            }
            "--nprobe" => {
                i += 1;
                if i < args.len() {
                    nprobes = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().unwrap())
                        .collect();
                }
            }
            "--centroid-rerank" => {
                i += 1;
                if i < args.len() {
                    centroid_rerank_factors = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().unwrap())
                        .collect();
                }
            }
            "--rerank" => {
                i += 1;
                if i < args.len() {
                    vector_rerank_factors = args[i]
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
            "--data-bits" | "--bits" => {
                i += 1;
                if i < args.len() {
                    data_bits = args[i].parse().unwrap();
                }
            }
            "--centroid-bits" => {
                i += 1;
                if i < args.len() {
                    centroid_bits = args[i].parse().unwrap();
                }
            }
            "--cluster-bits" => {
                i += 1;
                if i < args.len() {
                    cluster_bits = Some(args[i].parse().unwrap());
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
            std::process::exit(1);
        }
    }

    CliArgs {
        dataset,
        sizes,
        k,
        clusters,
        nprobes,
        centroid_rerank_factors,
        vector_rerank_factors,
        df,
        data_bits,
        centroid_bits,
        cluster_bits,
    }
}

fn print_results(
    dataset: &str,
    n: usize,
    n_clusters: usize,
    k: usize,
    df: &DistanceFunction,
    data_bits: u8,
    centroid_bits: u8,
    results: &[IvfRecallResult],
) {
    let hr = "=".repeat(116);
    let sep = "-".repeat(116);

    println!("\n{hr}");
    println!(
        "  IVF Recall@{k} on {dataset}  (N={n}, clusters={n_clusters}, distance={dist}, data={data_bits}bit, centroid={centroid_bits}bit)",
        dist = df_name(df),
    );
    println!("{sep}");
    println!("  Legend:");
    println!("    nprobe                    clusters probed per query");
    println!("    centroid_rerank           centroid rerank factor (retrieve nprobe*c_rerank by quantized distance, rerank to nprobe with exact)");
    println!("    vector_rerank             vector rerank factor (shortlist k*v_rerank candidates, rerank to k with exact)");
    println!("    centroid_recall           centroid recall after quantized search + rerank pipeline");
    println!("    centroid_recall_cieling   what an exact centroid search would achieve at nprobe");
    println!("    vector_recall             end-to-end recall using quantized within-cluster scoring (no vector rerank)");
    println!("    vector_recall_reranked    end-to-end recall after exact vector reranking of top k*v_rerank candidates");
    println!("{sep}");
    println!(
        "  {:>8} {:>15} {:>13} {:>15} {:>22} {:>13} {:>22}",
        "nprobe",
        "centroid_rerank",
        "vector_rerank",
        "centroid_recall",
        "centroid_recall_cieling",
        "vector_recall",
        "vector_recall_reranked"
    );
    println!("{sep}");

    let mut prev_nprobe = 0;
    for r in results {
        if prev_nprobe != 0 && r.nprobe != prev_nprobe {
            println!("{sep}");
        }
        prev_nprobe = r.nprobe;
        println!(
            "  {:>8} {:>14}x {:>12}x {:>15.4} {:>22.4} {:>13.4} {:>22.4}",
            r.nprobe,
            r.centroid_rerank_factor,
            r.vector_rerank_factor,
            r.centroid_recall,
            r.centroid_recall_cieling,
            r.vector_recall,
            r.vector_recall_reranked,
        );
    }
    println!("{hr}\n");
}

fn main() {
    let args = parse_args();

    println!("\n=== IVF RaBitQ Recall Benchmark ===");
    println!(
        "  dataset={}, distance={}, K={}, data_bits={}, centroid_bits={}, cluster_bits={}",
        args.dataset,
        df_name(&args.df),
        args.k,
        args.data_bits,
        args.centroid_bits,
        args.cluster_bits.map_or("exact".to_string(), |b| format!("{b}")),
    );
    println!(
        "  nprobes={:?}, centroid_rerank={:?}, vector_rerank={:?}",
        args.nprobes, args.centroid_rerank_factors, args.vector_rerank_factors,
    );
    println!("  sizes={:?}\n", args.sizes);

    for &size in &args.sizes {
        let n_clusters = args
            .clusters
            .unwrap_or_else(|| (size as f64).sqrt() as usize);
        let results = run_ivf_recall(
            &args.dataset,
            size,
            args.k,
            n_clusters,
            &args.nprobes,
            &args.centroid_rerank_factors,
            &args.vector_rerank_factors,
            &args.df,
            args.data_bits,
            args.centroid_bits,
            args.cluster_bits,
        );
        print_results(
            &args.dataset,
            size,
            n_clusters,
            args.k,
            &args.df,
            args.data_bits,
            args.centroid_bits,
            &results,
        );
    }
}
