//! Two-stage reranking recall benchmark.
//!
//! Evaluates a two-stage approach to reducing full-precision vector fetches:
//!
//!   1. Store both 1-bit and 4-bit quantized codes for every data vector.
//!   2. Initial scan: score all vectors in probed clusters with cheap 1-bit codes.
//!   3. Intermediate rerank: re-score the top M candidates using their stored 4-bit codes.
//!   4. Final rerank: fetch full-precision embeddings for the top R candidates.
//!
//! The hypothesis: the 4-bit intermediate rerank produces a better-ordered shortlist
//! than 1-bit alone, allowing a smaller R (fewer FP fetches) for the same recall.
//!
//! Three pipelines are compared at each (nprobe, rerank_factor):
//!
//!   4bit         Score all with 4-bit codes -> top R -> exact rerank (most expensive scoring)
//!   1bit         Score all with 1-bit codes -> top R -> exact rerank (cheapest scoring)
//!   1bit->4bit   Score all with 1-bit -> top M -> 4-bit rescore -> top R -> exact rerank
//!
//! Run:
//!   cargo bench -p chroma-index --bench recall_twostage
//!   cargo bench -p chroma-index --bench recall_twostage -- --size 100000
//!   cargo bench -p chroma-index --bench recall_twostage -- --nprobe 8,16,32 --rerank 1,2,4,8
//!   cargo bench -p chroma-index --bench recall_twostage -- --first-stage 8,16,32,64
//!
//! What it does: Compares three vector scoring pipelines at each combination of
//! (nprobe, rerank_factor), where rerank_factor determines how many full-precision
//! vectors are fetched
//!
//! Each row of the output shows
//! - recall_quantized (before FP reranking) and
//! - recall_reranked (after FP reranking)
//! so you can directly compare: at the same number of full-precision fetches,
//! which pipeline achieves higher recall?
//!
//! Key design choices:
//! - Both 1-bit and 4-bit codes are stored per data vector in each cluster (DualCodeCluster)
//! - The 4-bit rescore in the two-stage pipeline uses Code::<4>::distance_query
//!   with full-precision query residual, which unpacks the 4-bit grid and
//!   computes the dot product -- same as the 4-bit baseline but only applied to the shortlisted candidates
//! - Centroid search uses exact distances to isolate the vector pipeline comparison
//! - The two-stage pipeline skips configurations where
//!   first_stage_factor <= rerank_factor (would be identical to 1-bit baseline)

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

use chroma_distance::{normalize, DistanceFunction};
use chroma_index::quantization::{Code, QuantizedQuery};
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
const DEFAULT_NPROBE: &[usize] = &[16, 32, 64];
const DEFAULT_RERANK_FACTORS: &[usize] = &[1, 2, 4, 8, 16];
const DEFAULT_FIRST_STAGE_FACTORS: &[usize] = &[4, 8, 16, 32, 64];
const AVAILABLE_SIZES: &[usize] = &[10_000, 100_000, 1_000_000, 10_000_000];
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

// ── Dual-code cluster (stores both 1-bit and 4-bit codes) ───────────────────

struct DualCodeCluster {
    centroid: Vec<f32>,
    member_indices: Vec<usize>,
    codes_1bit: Vec<u8>,
    code_size_1bit: usize,
    codes_4bit: Vec<u8>,
    code_size_4bit: usize,
}

// ── Scoring ──────────────────────────────────────────────────────────────────

struct ScoredCandidate {
    global_idx: usize,
    cluster_id: usize,
    member_offset: usize,
    distance: f32,
}

fn score_cluster_1bit(
    cluster_id: usize,
    cluster: &DualCodeCluster,
    rotated_query: &[f32],
    df: &DistanceFunction,
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
    let padded_bytes = Code::<1>::packed_len(dim);
    let qq = QuantizedQuery::new(&r_q, padded_bytes, cn, cdq, qn);

    cluster
        .member_indices
        .iter()
        .enumerate()
        .zip(cluster.codes_1bit.chunks(cluster.code_size_1bit))
        .map(|((offset, &global_idx), code_bytes)| {
            let distance = Code::<1, _>::new(code_bytes).distance_quantized_query(df, &qq);
            ScoredCandidate {
                global_idx,
                cluster_id,
                member_offset: offset,
                distance,
            }
        })
        .collect()
}

fn score_cluster_4bit(
    cluster_id: usize,
    cluster: &DualCodeCluster,
    rotated_query: &[f32],
    df: &DistanceFunction,
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

    cluster
        .member_indices
        .iter()
        .enumerate()
        .zip(cluster.codes_4bit.chunks(cluster.code_size_4bit))
        .map(|((offset, &global_idx), code_bytes)| {
            let distance = Code::<4, _>::new(code_bytes).distance_query(df, &r_q, cn, cdq, qn);
            ScoredCandidate {
                global_idx,
                cluster_id,
                member_offset: offset,
                distance,
            }
        })
        .collect()
}

struct ClusterQueryParams {
    r_q: Vec<f32>,
    cn: f32,
    cdq: f32,
    qn: f32,
}

fn rescore_4bit(
    candidates: &[ScoredCandidate],
    clusters: &[DualCodeCluster],
    rotated_query: &[f32],
    df: &DistanceFunction,
) -> Vec<ScoredCandidate> {
    let cluster_ids: HashSet<usize> = candidates.iter().map(|c| c.cluster_id).collect();
    let params: HashMap<usize, ClusterQueryParams> = cluster_ids
        .iter()
        .map(|&cid| {
            let centroid = &clusters[cid].centroid;
            let cn = c_norm(centroid);
            let r_q: Vec<f32> = rotated_query
                .iter()
                .zip(centroid.iter())
                .map(|(q, c)| q - c)
                .collect();
            let cdq = c_dot_q(centroid, &r_q);
            let qn = q_norm(centroid, &r_q);
            (cid, ClusterQueryParams { r_q, cn, cdq, qn })
        })
        .collect();

    candidates
        .iter()
        .map(|c| {
            let cluster = &clusters[c.cluster_id];
            let p = &params[&c.cluster_id];
            let offset = c.member_offset * cluster.code_size_4bit;
            let code_bytes = &cluster.codes_4bit[offset..offset + cluster.code_size_4bit];
            let distance =
                Code::<4, _>::new(code_bytes).distance_query(df, &p.r_q, p.cn, p.cdq, p.qn);
            ScoredCandidate {
                global_idx: c.global_idx,
                cluster_id: c.cluster_id,
                member_offset: c.member_offset,
                distance,
            }
        })
        .collect()
}

// ── Recall computation helpers ──────────────────────────────────────────────

fn recall_at_k(
    candidates: &[ScoredCandidate],
    gt_set: &HashSet<u32>,
    k: usize,
    take: usize,
) -> f32 {
    let shortlist: HashSet<u32> = candidates
        .iter()
        .take(take)
        .map(|c| c.global_idx as u32)
        .collect();
    let hits = gt_set.iter().filter(|idx| shortlist.contains(idx)).count();
    hits as f32 / k as f32
}

fn reranked_recall(
    candidates: &[ScoredCandidate],
    take: usize,
    vectors: &[Vec<f32>],
    query: &[f32],
    df: &DistanceFunction,
    gt_set: &HashSet<u32>,
    k: usize,
) -> f32 {
    if take <= 1 {
        return recall_at_k(candidates, gt_set, k, take.max(k));
    }
    let mut reranked: Vec<(usize, f32)> = candidates
        .iter()
        .take(take)
        .map(|c| {
            (
                c.global_idx,
                exact_distance(&vectors[c.global_idx], query, df),
            )
        })
        .collect();
    reranked.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
    let final_set: HashSet<u32> = reranked
        .iter()
        .take(k)
        .map(|(idx, _)| *idx as u32)
        .collect();
    let hits = gt_set.iter().filter(|idx| final_set.contains(idx)).count();
    hits as f32 / k as f32
}

fn centroid_recall_fraction(
    probed: &[usize],
    cluster_members: &[Vec<usize>],
    gt_set: &HashSet<u32>,
    k: usize,
) -> f32 {
    let probed_members: HashSet<u32> = probed
        .iter()
        .flat_map(|&c| cluster_members[c].iter().map(|&i| i as u32))
        .collect();
    let hits = gt_set
        .iter()
        .filter(|idx| probed_members.contains(idx))
        .count();
    hits as f32 / k as f32
}

// ── Result types ─────────────────────────────────────────────────────────────

struct PipelineResult {
    pipeline: String,
    nprobe: usize,
    first_stage: usize,
    fp_fetched: usize,
    recall_quantized: f32,
    recall_reranked: f32,
}

// ── Benchmark runner ─────────────────────────────────────────────────────────

fn run_benchmark(
    dataset: &str,
    n: usize,
    k: usize,
    n_clusters: usize,
    nprobes: &[usize],
    rerank_factors: &[usize],
    first_stage_factors: &[usize],
    df: &DistanceFunction,
) -> (f32, Vec<PipelineResult>) {
    let dir = data_dir(dataset);
    let meta = load_meta(&dir.join(format!("meta_{n}.json")));
    let dim = meta["dim"].as_u64().unwrap_or(DEFAULT_DIM as u64) as usize;
    let vectors_raw = load_f32_matrix(&dir.join(format!("vectors_{n}.bin")), dim);
    let queries_raw = load_f32_matrix(&dir.join(format!("queries_{n}.bin")), dim);
    assert_eq!(vectors_raw.len(), n);
    let n_queries = queries_raw.len();

    print!("  Pre-processing {n} vectors + {n_queries} queries ...");
    let t0 = Instant::now();
    let vectors: Vec<Vec<f32>> = vectors_raw.iter().map(|v| preprocess(v, df)).collect();
    let queries: Vec<Vec<f32>> = queries_raw.iter().map(|q| preprocess(q, df)).collect();
    println!(" {:.2}s", t0.elapsed().as_secs_f64());

    println!("  Computing ground truth ({n_queries} queries x {n} vectors) ...");
    let t0 = Instant::now();
    let ground_truth = compute_ground_truth(&vectors, &queries, k, df);
    println!("  Ground truth in {:.2}s", t0.elapsed().as_secs_f64());

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

    println!("  Rotating {n} vectors ...");
    let t0 = Instant::now();
    let rotated_vectors: Vec<Vec<f32>> = vectors.par_iter().map(|v| rotate_vec(&p, v)).collect();
    println!("  Rotated in {:.2}s", t0.elapsed().as_secs_f64());

    println!("  KMeans clustering into {n_clusters} clusters ...");
    let t0 = Instant::now();
    let km = simple_kmeans(&rotated_vectors, n_clusters, df);
    let mut cluster_members: Vec<Vec<usize>> = vec![Vec::new(); n_clusters];
    for (i, &c) in km.assignments.iter().enumerate() {
        cluster_members[c].push(i);
    }
    let non_empty = cluster_members.iter().filter(|m| !m.is_empty()).count();
    let avg_size = n as f32 / non_empty as f32;
    println!(
        "  Clustered in {:.2}s: {non_empty} non-empty, avg={avg_size:.0}",
        t0.elapsed().as_secs_f64()
    );

    let rotated_centroids = km.centroids;

    println!("  Building dual-code clusters (1-bit + 4-bit) ...");
    let t0 = Instant::now();
    let code_size_1bit = Code::<1>::size(dim);
    let code_size_4bit = Code::<4>::size(dim);
    let clusters: Vec<DualCodeCluster> = (0..n_clusters)
        .into_par_iter()
        .map(|c| {
            let centroid = &rotated_centroids[c];
            let members = &cluster_members[c];
            let mut codes_1bit = Vec::with_capacity(members.len() * code_size_1bit);
            let mut codes_4bit = Vec::with_capacity(members.len() * code_size_4bit);
            for &idx in members {
                codes_1bit.extend_from_slice(
                    Code::<1>::quantize(&rotated_vectors[idx], centroid).as_ref(),
                );
                codes_4bit.extend_from_slice(
                    Code::<4>::quantize(&rotated_vectors[idx], centroid).as_ref(),
                );
            }
            DualCodeCluster {
                centroid: centroid.clone(),
                member_indices: members.clone(),
                codes_1bit,
                code_size_1bit,
                codes_4bit,
                code_size_4bit,
            }
        })
        .collect();
    let bytes_1bit: usize = clusters.iter().map(|c| c.codes_1bit.len()).sum();
    let bytes_4bit: usize = clusters.iter().map(|c| c.codes_4bit.len()).sum();
    println!(
        "  Built in {:.2}s (1-bit: {:.1} MB, 4-bit: {:.1} MB, total: {:.1} MB)",
        t0.elapsed().as_secs_f64(),
        bytes_1bit as f64 / 1e6,
        bytes_4bit as f64 / 1e6,
        (bytes_1bit + bytes_4bit) as f64 / 1e6,
    );

    println!("  Scoring {n_queries} queries ...");
    let t0 = Instant::now();

    let rotated_queries: Vec<Vec<f32>> = queries.iter().map(|q| rotate_vec(&p, q)).collect();

    // Exact centroid rankings (isolate the vector pipeline comparison)
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

    let gt_sets: Vec<HashSet<u32>> = ground_truth
        .iter()
        .map(|gt| gt.iter().copied().collect())
        .collect();

    let max_nprobe = *nprobes.iter().max().unwrap();
    // Compute mean centroid recall at max_nprobe for reporting
    let mean_centroid_recall: f32 = (0..n_queries)
        .map(|qi| {
            let probed = &exact_centroid_rankings[qi][..max_nprobe.min(n_clusters)];
            centroid_recall_fraction(probed, &cluster_members, &gt_sets[qi], k)
        })
        .sum::<f32>()
        / n_queries as f32;

    // Pre-compute per-query scoring for all probed clusters at max_nprobe.
    // For each query, score all vectors with both 1-bit and 4-bit.
    let per_query_1bit: Vec<Vec<ScoredCandidate>> = (0..n_queries)
        .map(|qi| {
            let rq = &rotated_queries[qi];
            let probed = &exact_centroid_rankings[qi][..max_nprobe.min(n_clusters)];
            let mut all: Vec<ScoredCandidate> = Vec::new();
            for &cluster_id in probed {
                let cluster = &clusters[cluster_id];
                if cluster.member_indices.is_empty() {
                    continue;
                }
                all.extend(score_cluster_1bit(cluster_id, cluster, rq, df));
            }
            all.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));
            all
        })
        .collect();

    let per_query_4bit: Vec<Vec<ScoredCandidate>> = (0..n_queries)
        .map(|qi| {
            let rq = &rotated_queries[qi];
            let probed = &exact_centroid_rankings[qi][..max_nprobe.min(n_clusters)];
            let mut all: Vec<ScoredCandidate> = Vec::new();
            for &cluster_id in probed {
                let cluster = &clusters[cluster_id];
                if cluster.member_indices.is_empty() {
                    continue;
                }
                all.extend(score_cluster_4bit(cluster_id, cluster, rq, df));
            }
            all.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));
            all
        })
        .collect();

    // Two-stage: rescore 1-bit top-M with 4-bit, for each first_stage_factor
    let per_query_twostage: Vec<Vec<(usize, Vec<ScoredCandidate>)>> = (0..n_queries)
        .map(|qi| {
            let rq = &rotated_queries[qi];
            let candidates_1bit = &per_query_1bit[qi];
            let mut results_by_fs = Vec::new();
            for &fsf in first_stage_factors {
                let m = k * fsf;
                let top_m = &candidates_1bit[..m.min(candidates_1bit.len())];
                let mut rescored = rescore_4bit(top_m, &clusters, rq, df);
                rescored.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));
                results_by_fs.push((fsf, rescored));
            }
            results_by_fs
        })
        .collect();

    let mut results = Vec::new();

    for &nprobe in nprobes {
        for &rerank_factor in rerank_factors {
            let fp_fetched = k * rerank_factor;

            // --- 4-bit baseline ---
            {
                let mut total_q = 0.0f32;
                let mut total_r = 0.0f32;
                for qi in 0..n_queries {
                    let candidates = &per_query_4bit[qi];
                    total_q += recall_at_k(candidates, &gt_sets[qi], k, fp_fetched);
                    total_r += reranked_recall(
                        candidates,
                        fp_fetched,
                        &vectors,
                        &queries[qi],
                        df,
                        &gt_sets[qi],
                        k,
                    );
                }
                results.push(PipelineResult {
                    pipeline: "4bit".to_string(),
                    nprobe,
                    first_stage: 0,
                    fp_fetched,
                    recall_quantized: total_q / n_queries as f32,
                    recall_reranked: total_r / n_queries as f32,
                });
            }

            // --- 1-bit baseline ---
            {
                let mut total_q = 0.0f32;
                let mut total_r = 0.0f32;
                for qi in 0..n_queries {
                    let candidates = &per_query_1bit[qi];
                    total_q += recall_at_k(candidates, &gt_sets[qi], k, fp_fetched);
                    total_r += reranked_recall(
                        candidates,
                        fp_fetched,
                        &vectors,
                        &queries[qi],
                        df,
                        &gt_sets[qi],
                        k,
                    );
                }
                results.push(PipelineResult {
                    pipeline: "1bit".to_string(),
                    nprobe,
                    first_stage: 0,
                    fp_fetched,
                    recall_quantized: total_q / n_queries as f32,
                    recall_reranked: total_r / n_queries as f32,
                });
            }

            // --- Two-stage pipelines ---
            for &fsf in first_stage_factors {
                if fsf <= rerank_factor {
                    continue;
                }
                let mut total_q = 0.0f32;
                let mut total_r = 0.0f32;
                for qi in 0..n_queries {
                    let fs_results = &per_query_twostage[qi];
                    let rescored = &fs_results.iter().find(|(f, _)| *f == fsf).unwrap().1;
                    total_q += recall_at_k(rescored, &gt_sets[qi], k, fp_fetched);
                    total_r += reranked_recall(
                        rescored,
                        fp_fetched,
                        &vectors,
                        &queries[qi],
                        df,
                        &gt_sets[qi],
                        k,
                    );
                }
                results.push(PipelineResult {
                    pipeline: format!("1bit->4bit(x{fsf})"),
                    nprobe,
                    first_stage: k * fsf,
                    fp_fetched,
                    recall_quantized: total_q / n_queries as f32,
                    recall_reranked: total_r / n_queries as f32,
                });
            }
        }
    }

    println!("  Scored in {:.2}s", t0.elapsed().as_secs_f64());
    (mean_centroid_recall, results)
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
    rerank_factors: Vec<usize>,
    first_stage_factors: Vec<usize>,
    df: DistanceFunction,
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = std::env::args().collect();

    let mut dataset = DEFAULT_DATASET.to_string();
    let mut sizes = Vec::new();
    let mut k = DEFAULT_K;
    let mut clusters: Option<usize> = None;
    let mut nprobes = DEFAULT_NPROBE.to_vec();
    let mut rerank_factors = DEFAULT_RERANK_FACTORS.to_vec();
    let mut first_stage_factors = DEFAULT_FIRST_STAGE_FACTORS.to_vec();
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
            "--rerank" => {
                i += 1;
                if i < args.len() {
                    rerank_factors = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().unwrap())
                        .collect();
                }
            }
            "--first-stage" => {
                i += 1;
                if i < args.len() {
                    first_stage_factors = args[i]
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
            std::process::exit(1);
        }
    }

    CliArgs {
        dataset,
        sizes,
        k,
        clusters,
        nprobes,
        rerank_factors,
        first_stage_factors,
        df,
    }
}

fn print_results(
    dataset: &str,
    n: usize,
    n_clusters: usize,
    k: usize,
    df: &DistanceFunction,
    centroid_recall: f32,
    results: &[PipelineResult],
) {
    let w = 110;
    let hr = "=".repeat(w);
    let sep = "-".repeat(w);

    println!("\n{hr}");
    println!(
        "  Two-Stage Reranking Recall@{k}   N={n}, clusters={n_clusters}, distance={}, dataset={dataset}",
        df_name(df),
    );
    println!("  centroid_recall={centroid_recall:.4} (exact centroid search, fixed across all pipelines)");
    println!("{sep}");
    println!("  Pipeline descriptions:");
    println!("    4bit           Score all vectors with 4-bit codes -> top R -> exact rerank");
    println!("    1bit           Score all vectors with 1-bit codes -> top R -> exact rerank");
    println!("    1bit->4bit(xM) Score all with 1-bit -> top k*M -> rescore with 4-bit -> top R -> exact rerank");
    println!("{sep}");
    println!(
        "  {:>20} {:>12} {:>12} {:>16} {:>18}",
        "pipeline", "shortlist", "fp_fetched", "recall_quantized", "recall_reranked",
    );
    println!("{sep}");

    let mut prev_nprobe = 0;
    let mut prev_fp = 0;
    for r in results {
        if prev_nprobe != 0 && r.nprobe != prev_nprobe {
            println!("{hr}");
            println!("  nprobe = {}", r.nprobe);
            println!("{sep}");
        } else if prev_nprobe == 0 {
            println!("  nprobe = {}", r.nprobe);
            println!("{sep}");
        }
        if prev_fp != r.fp_fetched && prev_nprobe == r.nprobe && prev_fp != 0 {
            println!("  {}", ".".repeat(w - 2));
        }
        prev_nprobe = r.nprobe;
        prev_fp = r.fp_fetched;

        let shortlist_str = if r.first_stage > 0 {
            format!("{}", r.first_stage)
        } else {
            "-".to_string()
        };

        println!(
            "  {:>20} {:>12} {:>12} {:>16.4} {:>18.4}",
            r.pipeline, shortlist_str, r.fp_fetched, r.recall_quantized, r.recall_reranked,
        );
    }
    println!("{hr}");

    // Summary: for selected recall targets, show min fp_fetched per pipeline type
    println!("\n  Summary: minimum fp_fetched to reach target recall (reranked)");
    println!("  {sep}");

    let pipeline_types: Vec<String> = {
        let mut seen = Vec::new();
        for r in results {
            if !seen.contains(&r.pipeline) {
                seen.push(r.pipeline.clone());
            }
        }
        seen
    };

    let targets = [0.90, 0.92, 0.95, 0.97, 0.99];
    let nprobe_for_summary = results.last().map(|r| r.nprobe).unwrap_or(0);

    print!("  {:>8}", "target");
    for pt in &pipeline_types {
        print!("  {:>20}", pt);
    }
    println!();
    println!("  {sep}");

    for &target in &targets {
        print!("  {:>8.2}", target);
        for pt in &pipeline_types {
            let min_fp = results
                .iter()
                .filter(|r| r.pipeline == *pt && r.nprobe == nprobe_for_summary)
                .filter(|r| r.recall_reranked >= target)
                .map(|r| r.fp_fetched)
                .min();
            match min_fp {
                Some(fp) => print!("  {:>20}", fp),
                None => print!("  {:>20}", "-"),
            }
        }
        println!();
    }
    println!();
}

fn main() {
    let args = parse_args();

    println!("\n=== Two-Stage Reranking Recall Benchmark ===");
    println!(
        "  dataset={}, distance={}, K={}",
        args.dataset,
        df_name(&args.df),
        args.k,
    );
    println!(
        "  nprobes={:?}, rerank_factors={:?}, first_stage_factors={:?}",
        args.nprobes, args.rerank_factors, args.first_stage_factors,
    );
    println!("  sizes={:?}\n", args.sizes);

    for &size in &args.sizes {
        let n_clusters = args
            .clusters
            .unwrap_or_else(|| (size as f64).sqrt() as usize);
        let (centroid_recall, results) = run_benchmark(
            &args.dataset,
            size,
            args.k,
            n_clusters,
            &args.nprobes,
            &args.rerank_factors,
            &args.first_stage_factors,
            &args.df,
        );
        print_results(
            &args.dataset,
            size,
            n_clusters,
            args.k,
            &args.df,
            centroid_recall,
            &results,
        );
    }
}
