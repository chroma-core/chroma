//! Benchmarks for RaBitQ quantization performance characteristics.
//!
//! - Quantization throughput (bytes/s, varying dimension)
//! - Distance estimation throughput: code-vs-code and code-vs-query (bytes/s, varying dimension)
//! - Primitive kernel throughput: hamming_distance, signed_dot (1bit_kernel)
//! - Thread scaling: parallel quantization and distance estimation
//! - Rerank budget: for a fixed K, what rerank factor guarantees the true
//!   top-K is in the candidate set (accuracy analysis, printed to stdout)
//!
//! Run with:
//!   # All benchmarks
//!   cargo bench -p chroma-index --bench quantization
//!
//!   # Just one group
//!   cargo bench -p chroma-index --bench quantization -- quantize
//!
//!   # Just 1-bit at dim=1024
//!   cargo bench -p chroma-index --bench quantization -- "1bit/1024"
//!
//! For native CPU (enables POPCNT, AVX2, etc.):
//!   RUSTFLAGS="-C target-cpu=native" cargo bench -p chroma-index --bench quantization

use std::hint::black_box;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::prelude::*;

// ── Dimensions from the description: 128-dim (small), 1024-dim (target),
//    4096-dim (maximum stated in the doc: "up to 4096dim embeddings")
const DIMS: &[usize] = &[128, 1024, 4096];

// Number of code-pairs / query pairs per benchmark iteration.
const BATCH: usize = 512;

// Number of threads to test for thread-scaling.
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_rng() -> StdRng {
    StdRng::seed_from_u64(0xdeadbeef)
}

fn random_vec(rng: &mut impl Rng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0_f32..1.0)).collect()
}

/// Pre-build a batch of codes and query residuals for a given dim and BITS.
fn make_codes<const BITS: u8>(
    dim: usize,
    n: usize,
) -> (Vec<f32>, Vec<Vec<u8>>, Vec<Vec<f32>>) {
    let mut rng = make_rng();
    let centroid = random_vec(&mut rng, dim);
    let codes: Vec<Vec<u8>> = (0..n)
        .map(|_| {
            let emb = random_vec(&mut rng, dim);
            Code::<Vec<u8>, BITS>::quantize(&emb, &centroid)
                .as_ref()
                .to_vec()
        })
        .collect();
    let queries: Vec<Vec<f32>> = (0..n)
        .map(|_| {
            let q = random_vec(&mut rng, dim);
            q.iter().zip(&centroid).map(|(q, c)| q - c).collect()
        })
        .collect();
    (centroid, codes, queries)
}

fn c_norm(centroid: &[f32]) -> f32 {
    centroid.iter().map(|x| x * x).sum::<f32>().sqrt()
}

fn c_dot_q(centroid: &[f32], query_residual: &[f32]) -> f32 {
    centroid
        .iter()
        .zip(query_residual)
        .map(|(c, r)| c * (r + c)) // r_q = q - c  =>  q = c + r_q
        .sum()
}

fn q_norm(centroid: &[f32], query_residual: &[f32]) -> f32 {
    query_residual
        .iter()
        .zip(centroid)
        .map(|(r, c)| (r + c) * (r + c))
        .sum::<f32>()
        .sqrt()
}

// ── 1. Quantization throughput ────────────────────────────────────────────────

fn bench_quantize(c: &mut Criterion) {
    let mut group = c.benchmark_group("quantize");

    for &dim in DIMS {
        let mut rng = make_rng();
        let centroid = random_vec(&mut rng, dim);
        let embeddings: Vec<Vec<f32>> =
            (0..BATCH).map(|_| random_vec(&mut rng, dim)).collect();

        // Throughput = BATCH * dim * 4 bytes (f32 input) per iteration.
        group.throughput(Throughput::Bytes((BATCH * dim * 4) as u64));

        group.bench_with_input(BenchmarkId::new("1bit", dim), &dim, |b, _| {
            b.iter(|| {
                for emb in &embeddings {
                    black_box(Code::<Vec<u8>, 1>::quantize(emb, &centroid));
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("4bit", dim), &dim, |b, _| {
            b.iter(|| {
                for emb in &embeddings {
                    black_box(Code::<Vec<u8>, 4>::quantize(emb, &centroid));
                }
            });
        });
    }

    group.finish();
}

// ── 2. distance_code throughput (code vs code) ───────────────────────────────

fn bench_distance_code(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_code");
    let df = DistanceFunction::Euclidean;

    for &dim in DIMS {
        let (centroid, codes_1, _) = make_codes::<1>(dim, BATCH);
        let (_, codes_4, _) = make_codes::<4>(dim, BATCH);
        let cn = c_norm(&centroid);

        // Input bytes = BATCH * code_size (the data being read per iteration).
        let code_bytes_1 = Code::<&[u8], 1>::size(dim);
        let code_bytes_4 = Code::<&[u8], 4>::size(dim);
        let pairs = (BATCH / 2) as u64;

        group.throughput(Throughput::Bytes(pairs * 2 * code_bytes_1 as u64));
        group.bench_with_input(BenchmarkId::new("1bit", dim), &dim, |b, _| {
            b.iter(|| {
                for i in (0..BATCH).step_by(2) {
                    let a = Code::<&[u8], 1>::new(codes_1[i].as_slice());
                    let bb = Code::<&[u8], 1>::new(codes_1[i + 1].as_slice());
                    black_box(a.distance_code(&df, &bb, cn, dim));
                }
            });
        });

        group.throughput(Throughput::Bytes(pairs * 2 * code_bytes_4 as u64));
        group.bench_with_input(BenchmarkId::new("4bit", dim), &dim, |b, _| {
            b.iter(|| {
                for i in (0..BATCH).step_by(2) {
                    let a = Code::<&[u8], 4>::new(codes_4[i].as_slice());
                    let bb = Code::<&[u8], 4>::new(codes_4[i + 1].as_slice());
                    black_box(a.distance_code(&df, &bb, cn, dim));
                }
            });
        });
    }

    group.finish();
}

// ── 3. distance_query throughput (code vs full-precision query) ───────────────
//
// Two variants with deliberately different memory access patterns:
//
//   "1bit/<dim>" / "4bit/<dim>"  — cold-query variant
//     BATCH=512 (code, query) pairs, each with a distinct r_q.
//     The query vector is never cache-warm between iterations; this models
//     the worst-case where every lookup touches a fresh query.
//
//   "cluster_scan_1bit" / "cluster_scan_4bit"  — hot-query variant (dim=1024)
//     N=2048 codes scored against a single fixed query.  The query vector
//     stays hot in L1/registers while the codes stream through cache.  This
//     is the actual hot-path pattern: one query probing one posting list
//     (cluster) of ~2048 vectors.  Throughput here is what matters most for
//     end-to-end query latency.

fn bench_distance_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_query");
    let df = DistanceFunction::Euclidean;

    // Cold-query variant: BATCH pairs each with a distinct r_q.
    for &dim in DIMS {
        let (centroid, codes_1, queries_1) = make_codes::<1>(dim, BATCH);
        let (_, codes_4, queries_4) = make_codes::<4>(dim, BATCH);
        let cn = c_norm(&centroid);

        // Throughput = (code_bytes + query_bytes) * BATCH per iteration.
        let code_bytes_1 = Code::<&[u8], 1>::size(dim);
        let query_bytes = dim * 4;
        let code_bytes_4 = Code::<&[u8], 4>::size(dim);

        group.throughput(Throughput::Bytes(
            BATCH as u64 * (code_bytes_1 + query_bytes) as u64,
        ));
        group.bench_with_input(BenchmarkId::new("1bit", dim), &dim, |b, _| {
            b.iter(|| {
                for i in 0..BATCH {
                    let code = Code::<&[u8], 1>::new(codes_1[i].as_slice());
                    let r_q = &queries_1[i];
                    let cdq = c_dot_q(&centroid, r_q);
                    let qn = q_norm(&centroid, r_q);
                    black_box(code.distance_query(&df, r_q, cn, cdq, qn));
                }
            });
        });

        group.throughput(Throughput::Bytes(
            BATCH as u64 * (code_bytes_4 + query_bytes) as u64,
        ));
        group.bench_with_input(BenchmarkId::new("4bit", dim), &dim, |b, _| {
            b.iter(|| {
                for i in 0..BATCH {
                    let code = Code::<&[u8], 4>::new(codes_4[i].as_slice());
                    let r_q = &queries_4[i];
                    let cdq = c_dot_q(&centroid, r_q);
                    let qn = q_norm(&centroid, r_q);
                    black_box(code.distance_query(&df, r_q, cn, cdq, qn));
                }
            });
        });
    }

    // Hot-query / cluster-scan variant: one fixed query, N=2048 codes, dim=1024.
    // This matches what query_quantized_cluster does per probed cluster.
    {
        const SCAN_DIM: usize = 1024;
        const SCAN_N: usize = 2048;

        let mut rng = make_rng();
        let centroid = random_vec(&mut rng, SCAN_DIM);
        let r_q: Vec<f32> = {
            let query = random_vec(&mut rng, SCAN_DIM);
            query.iter().zip(&centroid).map(|(q, c)| q - c).collect()
        };
        let cdq = c_dot_q(&centroid, &r_q);
        let qn = q_norm(&centroid, &r_q);
        let cn = c_norm(&centroid);

        let (_, codes_1, _) = make_codes::<1>(SCAN_DIM, SCAN_N);
        let (_, codes_4, _) = make_codes::<4>(SCAN_DIM, SCAN_N);

        // Throughput = code_bytes * N (the query is reused and stays in L1).
        let code_bytes_1 = Code::<&[u8], 1>::size(SCAN_DIM);
        let code_bytes_4 = Code::<&[u8], 4>::size(SCAN_DIM);

        group.throughput(Throughput::Bytes(SCAN_N as u64 * code_bytes_1 as u64));
        group.bench_function("cluster_scan_1bit", |b| {
            b.iter(|| {
                let _: f32 = codes_1
                    .iter()
                    .map(|cb| {
                        let code = Code::<&[u8], 1>::new(cb.as_slice());
                        black_box(code.distance_query(&df, &r_q, cn, cdq, qn))
                    })
                    .sum();
            });
        });

        group.throughput(Throughput::Bytes(SCAN_N as u64 * code_bytes_4 as u64));
        group.bench_function("cluster_scan_4bit", |b| {
            b.iter(|| {
                let _: f32 = codes_4
                    .iter()
                    .map(|cb| {
                        let code = Code::<&[u8], 4>::new(cb.as_slice());
                        black_box(code.distance_query(&df, &r_q, cn, cdq, qn))
                    })
                    .sum();
            });
        });
    }

    group.finish();
}

// ── 4. Thread scaling ─────────────────────────────────────────────────────────
//
// We measure parallel quantization and parallel distance_query to see whether
// throughput scales linearly with thread count (cache-friendly) or flattens
// (LLC thrashing).

fn bench_thread_scaling(c: &mut Criterion) {
    // Use a fixed 1024-dim (the primary target dimension).
    const DIM: usize = 1024;
    const N: usize = 1024; // larger batch so parallelism is meaningful

    let mut rng = make_rng();
    let centroid: Vec<f32> = random_vec(&mut rng, DIM);
    let embeddings: Vec<Vec<f32>> = (0..N).map(|_| random_vec(&mut rng, DIM)).collect();

    let (_, codes_1, queries_1) = make_codes::<1>(DIM, N);
    let (_, codes_4, queries_4) = make_codes::<4>(DIM, N);
    let cn = c_norm(&centroid);
    let df = DistanceFunction::Euclidean;

    let mut group = c.benchmark_group("thread_scaling");

    for &threads in THREAD_COUNTS {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();

        // 1-bit quantize
        group.throughput(Throughput::Bytes((N * DIM * 4) as u64));
        group.bench_with_input(
            BenchmarkId::new("quantize_1bit", threads),
            &threads,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        embeddings.par_iter().for_each(|emb| {
                            black_box(Code::<Vec<u8>, 1>::quantize(emb, &centroid));
                        });
                    });
                });
            },
        );

        // 4-bit quantize
        group.bench_with_input(
            BenchmarkId::new("quantize_4bit", threads),
            &threads,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        embeddings.par_iter().for_each(|emb| {
                            black_box(Code::<Vec<u8>, 4>::quantize(emb, &centroid));
                        });
                    });
                });
            },
        );

        // 1-bit distance_query
        group.throughput(Throughput::Bytes(
            (N * (Code::<&[u8], 1>::size(DIM) + DIM * 4)) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::new("distance_query_1bit", threads),
            &threads,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        codes_1.par_iter().zip(queries_1.par_iter()).for_each(
                            |(code_bytes, r_q)| {
                                let code = Code::<&[u8], 1>::new(code_bytes.as_slice());
                                let cdq = c_dot_q(&centroid, r_q);
                                let qn = q_norm(&centroid, r_q);
                                black_box(code.distance_query(&df, r_q, cn, cdq, qn));
                            },
                        );
                    });
                });
            },
        );

        // 4-bit distance_query
        group.throughput(Throughput::Bytes(
            (N * (Code::<&[u8], 4>::size(DIM) + DIM * 4)) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::new("distance_query_4bit", threads),
            &threads,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        codes_4.par_iter().zip(queries_4.par_iter()).for_each(
                            |(code_bytes, r_q)| {
                                let code = Code::<&[u8], 4>::new(code_bytes.as_slice());
                                let cdq = c_dot_q(&centroid, r_q);
                                let qn = q_norm(&centroid, r_q);
                                black_box(code.distance_query(&df, r_q, cn, cdq, qn));
                            },
                        );
                    });
                });
            },
        );
    }

    group.finish();
}

// ── 5. Rerank budget analysis ─────────────────────────────────────────────────
//
// This is a recall/accuracy analysis, not a throughput benchmark.
//
// For a ground-truth top-K ranked by true Euclidean distance, we ask:
// "At what rerank factor R does the estimated top-(K*R) always contain the
//  true top-K?"  The answer tells you how large to make the candidate set
//  before re-scoring with full-precision distances.
//
// The result is printed as a table to stdout when `cargo bench` runs.
// No criterion timings are registered here.

/// Returns true if the estimated top-(k * rerank_factor) candidate set
/// covers all of the true top-k vectors.
fn rerank_factor_needed(
    true_distances: &[f32],         // true distance for each data point by index
    est_distances: &[(usize, f32)], // (original_idx, estimated_dist), unsorted
    k: usize,
    rerank_factor: usize,
) -> bool {
    // Candidate set: top-(k * rerank_factor) by estimated distance.
    let mut est_sorted: Vec<(usize, f32)> = est_distances.to_vec();
    est_sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let candidate_ids: std::collections::HashSet<usize> =
        est_sorted.iter().take(k * rerank_factor).map(|&(i, _)| i).collect();

    // Recover which indices are the true top-K.
    let mut true_sorted_idx: Vec<usize> = est_distances.iter().map(|&(i, _)| i).collect();
    true_sorted_idx.sort_by(|&a, &b| {
        true_distances[a].partial_cmp(&true_distances[b]).unwrap()
    });

    // All true top-K indices must appear in the candidate set.
    true_sorted_idx.iter().take(k).all(|idx| candidate_ids.contains(idx))
}

/// Builds test data, scores with both 1-bit and 4-bit estimated distances,
/// and prints the rerank-budget table to stdout.
fn print_rerank_budget() {
    const DIM: usize = 1024;
    const N: usize = 2048;
    const K: usize = 100;
    const RERANK_FACTORS: &[usize] = &[1, 2, 4, 8];

    let mut rng = make_rng();
    let centroid = random_vec(&mut rng, DIM);
    let query = random_vec(&mut rng, DIM);
    let embeddings: Vec<Vec<f32>> = (0..N).map(|_| random_vec(&mut rng, DIM)).collect();

    let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
    let cdq: f32 = centroid.iter().zip(&query).map(|(c, q)| c * q).sum();
    let qn: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
    let cn = c_norm(&centroid);
    let df = DistanceFunction::Euclidean;

    let true_distances: Vec<f32> = embeddings
        .iter()
        .map(|emb| emb.iter().zip(&query).map(|(e, q)| (e - q).powi(2)).sum::<f32>())
        .collect();

    let codes_1: Vec<Vec<u8>> = embeddings
        .iter()
        .map(|emb| Code::<Vec<u8>, 1>::quantize(emb, &centroid).as_ref().to_vec())
        .collect();
    let codes_4: Vec<Vec<u8>> = embeddings
        .iter()
        .map(|emb| Code::<Vec<u8>, 4>::quantize(emb, &centroid).as_ref().to_vec())
        .collect();

    let score = |codes: &[Vec<u8>], bits: u8| -> Vec<(usize, f32)> {
        codes
            .iter()
            .enumerate()
            .map(|(i, cb)| {
                let dist = if bits == 1 {
                    Code::<&[u8], 1>::new(cb.as_slice()).distance_query(&df, &r_q, cn, cdq, qn)
                } else {
                    Code::<&[u8], 4>::new(cb.as_slice()).distance_query(&df, &r_q, cn, cdq, qn)
                };
                (i, dist)
            })
            .collect()
    };

    let est_1 = score(&codes_1, 1);
    let est_4 = score(&codes_4, 4);

    println!("\n=== Rerank budget (dim={DIM}, N={N}, K={K}) ===");
    println!("{:<10} {:<8} {:<12} {:<12}", "bits", "factor", "candidates", "top-K covered");
    for &rf in RERANK_FACTORS {
        for (label, est) in [("1", &est_1), ("4", &est_4)] {
            let covered = rerank_factor_needed(&true_distances, est, K, rf);
            println!(
                "{:<10} {:<8} {:<12} {:<12}",
                label, rf, K * rf,
                if covered { "YES" } else { "NO" }
            );
        }
    }
    println!();
}

/// Criterion entry-point that triggers the rerank-budget analysis.  No
/// benchmarks are registered; this exists solely so `print_rerank_budget`
/// runs once as part of `cargo bench`, placing the accuracy table in the
/// output alongside the timing results.
fn bench_rerank_budget(c: &mut Criterion) {
    let _ = c;
    print_rerank_budget();
}

// ── 6. SIMD primitive kernels ─────────────────────────────────────────────────

fn bench_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives");

    for &dim in DIMS {
        // Build packed byte slices (length = dim/8, padded to multiple of 8).
        let padded = dim.div_ceil(64) * 64;
        let bytes = padded / 8;
        let mut rng = make_rng();
        let a: Vec<u8> = (0..bytes).map(|_| rng.gen()).collect();
        let b: Vec<u8> = (0..bytes).map(|_| rng.gen()).collect();
        let values: Vec<f32> = (0..padded).map(|_| rng.gen_range(-1.0f32..1.0)).collect();

        // hamming_distance: reads 2 * bytes per call.
        group.throughput(Throughput::Bytes(2 * bytes as u64));
        group.bench_with_input(BenchmarkId::new("hamming_distance", dim), &dim, |b_cr, _| {
            b_cr.iter(|| {
                // hamming_distance is private so we exercise it via distance_code.
                // Build two trivial 1-bit codes around the raw packed bytes.
                let header = [0u8; 12]; // correction=0, norm=0, radial=0
                let mut code_a = header.to_vec();
                code_a.extend_from_slice(&a);
                let mut code_b = header.to_vec();
                code_b.extend_from_slice(&b);
                let ca = Code::<&[u8], 1>::new(code_a.as_slice());
                let cb_code = Code::<&[u8], 1>::new(code_b.as_slice());
                // distance_code with correction=0 gives NaN/inf — we only care
                // about timing the XOR+popcount path, not the result.
                black_box(ca.distance_code(&DistanceFunction::InnerProduct, &cb_code, 0.0, padded));
            });
        });

        // 1bit_kernel: exercises signed_dot, the hot kernel for 1-bit distance_query.
        // Throughput = packed bytes + f32 values read per call.
        group.throughput(Throughput::Bytes((bytes + padded * 4) as u64));
        group.bench_with_input(BenchmarkId::new("1bit_kernel", dim), &dim, |b_cr, _| {
            b_cr.iter(|| {
                // signed_dot is private; exercise it via distance_query on a
                // pre-built 1-bit code with a zero centroid so the code
                // reflects the raw embedding signs.
                let mut rng2 = make_rng();
                let centroid = vec![0.0f32; padded];
                let embedding: Vec<f32> = (0..padded)
                    .map(|_| rng2.gen_range(-1.0f32..1.0))
                    .collect();
                let code_owned = Code::<Vec<u8>, 1>::quantize(&embedding, &centroid);
                let code = Code::<&[u8], 1>::new(code_owned.as_ref());
                black_box(code.distance_query(
                    &DistanceFunction::InnerProduct,
                    &values[..padded],
                    0.0,
                    0.0,
                    1.0,
                ));
            });
        });

        // vector subtraction (residual computation: r = embedding - centroid)
        // This is the first step of quantize.
        let embedding: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        group.bench_with_input(BenchmarkId::new("vec_sub", dim), &dim, |b_cr, _| {
            b_cr.iter(|| {
                let r: Vec<f32> = embedding
                    .iter()
                    .zip(&centroid)
                    .map(|(e, c)| e - c)
                    .collect();
                black_box(r);
            });
        });
    }

    group.finish();
}

// ── Entry point ───────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_quantize,
    bench_distance_code,
    bench_distance_query,
    bench_thread_scaling,
    bench_rerank_budget,
    bench_primitives,
);
criterion_main!(benches);
