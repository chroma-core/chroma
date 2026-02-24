//! Benchmarks for RaBitQ quantization performance characteristics.
//!
//! Groups:
//!   quantize         — encoding throughput (1-bit, 4-bit) vs dim
//!   distance_code    — code-vs-code distance (1-bit, 4-bit) vs dim
//!   distance_query   — code-vs-query distance, all implementations and dims:
//!                        cold (BATCH=512 distinct queries):
//!                          1bit_float, 1bit_bitwise, 1bit_lut, 4bit_float
//!                        hot / cluster-scan (1 query × N=2048 codes, dim=1024):
//!                          cluster_scan_1bit_float, _bitwise, _lut, _4bit_float
//!   thread_scaling   — parallel quantize and distance_query vs thread count
//!   primitives       — raw kernel throughput: hamming_distance, 1bit_kernel, vec_sub
//!   rerank_budget    — accuracy table (not a timing benchmark; printed to stdout)
//!   error_analysis   — error distribution and histograms for all 4 implementations
//!
//! Implementations compared in distance_query:
//!   float   — signed_dot: expand bits to ±1.0 f32, simsimd dot product
//!   bitwise — paper §3.3.1: quantize query to B_q bits, AND + popcount
//!   lut     — paper §3.3.2: precompute nibble LUTs, table lookup
//!
//! Run with:
//!   cargo bench -p chroma-index --bench quantization
//!   cargo bench -p chroma-index --bench quantization -- distance_query
//!   cargo bench -p chroma-index --bench quantization -- "1bit_float/1024"
//!
//! For native CPU (POPCNT, AVX2, etc.):
//!   RUSTFLAGS="-C target-cpu=native" cargo bench -p chroma-index --bench quantization

use std::hint::black_box;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{BatchQueryLuts, Code, QuantizedQuery};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::prelude::*;

/// Print a one-line description of the benchmark that follows.
/// Appears immediately before criterion's own "Benchmarking …" line.
macro_rules! desc {
    ($id:expr, $text:expr) => {
        println!("  [{:48}] {}", $id, $text);
    };
}

const DIMS: &[usize] = &[1024];
// const DIMS: &[usize] = &[128, 1024, 4096];
const BATCH: usize = 512;
const THREAD_COUNTS: &[usize] = &[1, 8];
// const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_rng() -> StdRng {
    StdRng::seed_from_u64(0xdeadbeef)
}

fn random_vec(rng: &mut impl Rng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0_f32..1.0)).collect()
}

fn make_codes<const BITS: u8>(dim: usize, n: usize) -> (Vec<f32>, Vec<Vec<u8>>, Vec<Vec<f32>>) {
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

// ── 1. Quantization throughput ────────────────────────────────────────────────
//
// For each dimension, 1-bit and 4-bit are benchmarked back-to-back so their
// times appear adjacent in criterion's output.

fn bench_quantize(c: &mut Criterion) {
    let mut group = c.benchmark_group("quantize");
    // Throughput = f32 input bytes read per iteration (same for both BITS).
    // This makes GiB/s comparable across bit widths at the same dim.

    for &dim in DIMS {
        let mut rng = make_rng();
        let centroid = random_vec(&mut rng, dim);
        let embeddings: Vec<Vec<f32>> = (0..BATCH).map(|_| random_vec(&mut rng, dim)).collect();

        group.throughput(Throughput::Bytes((BATCH * dim * 4) as u64));

        desc!(
            format!("quantize/4bit/{dim}"),
            format!("{BATCH} embeddings → 4-bit ray-walk codes")
        );
        group.bench_with_input(BenchmarkId::new("4bit", dim), &dim, |b, _| {
            b.iter(|| {
                for emb in &embeddings {
                    black_box(Code::<Vec<u8>, 4>::quantize(emb, &centroid));
                }
            });
        });

        desc!(
            format!("quantize/1bit/{dim}"),
            format!("{BATCH} embeddings → sign-bit codes")
        );
        group.bench_with_input(BenchmarkId::new("1bit", dim), &dim, |b, _| {
            b.iter(|| {
                for emb in &embeddings {
                    black_box(Code::<Vec<u8>, 1>::quantize(emb, &centroid));
                }
            });
        });
    }

    group.finish();
}

// ── 2. distance_code throughput (code vs code) ────────────────────────────────

fn bench_distance_code(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_code");
    let df = DistanceFunction::Euclidean;
    let pairs = (BATCH / 2) as u64;

    for &dim in DIMS {
        let (centroid, codes_1, _) = make_codes::<1>(dim, BATCH);
        let (_, codes_4, _) = make_codes::<4>(dim, BATCH);
        let cn = c_norm(&centroid);
        let code_bytes_1 = Code::<&[u8], 1>::size(dim);
        let code_bytes_4 = Code::<&[u8], 4>::size(dim);

        group.throughput(Throughput::Bytes(pairs * 2 * code_bytes_1 as u64));
        desc!(
            format!("distance_code/1bit/{dim}"),
            format!("{pairs} pairs; XOR + popcount")
        );
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
        desc!(
            format!("distance_code/4bit/{dim}"),
            format!("{pairs} pairs; nibble unpack + dot")
        );
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

// ── 3. distance_query: all implementations in one group ──────────────────────
//
// All three 1-bit implementations and the 4-bit baseline appear together per
// dimension, so criterion's output lets you compare them at a glance.
//
// Implementations:
//   1bit_float   — signed_dot: expand bit vector to ±1.0 signs, simsimd dot
//   1bit_bitwise — paper §3.3.1: quantize query to B_q bits, AND + popcount
//   1bit_lut     — paper §3.3.2: precompute nibble LUTs, table lookup
//   4bit_float   — grid unpack + f32 dot product (reference quality ceiling)
//
// Access patterns:
//   cold (BATCH=512 distinct queries) — each iteration touches a fresh query
//     vector; models worst-case where every lookup has a cache-cold query.
//     Query quantization / LUT build cost is paid for every call.
//   hot / cluster-scan (N=2048 codes, 1 fixed query, dim=1024) — the query
//     stays in L1 while codes stream through cache.  Query setup is paid
//     once before the iter loop, so only the inner scoring loop is timed.
//     This is the realistic hot path: one query probing one cluster.

fn bench_distance_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_query");
    let df = DistanceFunction::Euclidean;

    // ── Cold-query variant ────────────────────────────────────────────────────
    for &dim in DIMS {
        let (centroid, codes_1, queries_1) = make_codes::<1>(dim, BATCH);
        let (_, codes_4, queries_4) = make_codes::<4>(dim, BATCH);
        let cn = c_norm(&centroid);
        let code_bytes_1 = Code::<&[u8], 1>::size(dim);
        let code_bytes_4 = Code::<&[u8], 4>::size(dim);
        let query_bytes = dim * 4;
        let padded_bytes = Code::<&[u8], 1>::packed_len(dim);

        // All 1-bit variants use the same throughput so GiB/s is comparable.
        let throughput_1bit = BATCH as u64 * (code_bytes_1 + query_bytes) as u64;
        let throughput_4bit = BATCH as u64 * (code_bytes_4 + query_bytes) as u64;

        group.throughput(Throughput::Bytes(throughput_4bit));
        desc!(
            format!("distance_query/4bit_float/{dim}"),
            format!("cold {BATCH} queries; grid unpack + f32 dot (reference quality ceiling)")
        );
        group.bench_with_input(BenchmarkId::new("4bit_float", dim), &dim, |b, _| {
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
        group.throughput(Throughput::Bytes(throughput_1bit));
        desc!(
            format!("distance_query/1bit_float/{dim}"),
            format!("cold {BATCH} queries; signed_dot (bits→±1.0 f32, simsimd dot)")
        );
        group.bench_with_input(BenchmarkId::new("1bit_float", dim), &dim, |b, _| {
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

        group.throughput(Throughput::Bytes(throughput_1bit));
        desc!(
            format!("distance_query/1bit_bitwise/{dim}"),
            format!("cold {BATCH} queries; QuantizedQuery build + AND+popcount (§3.3.1)")
        );
        group.bench_with_input(BenchmarkId::new("1bit_bitwise", dim), &dim, |b, _| {
            b.iter(|| {
                for i in 0..BATCH {
                    let code = Code::<&[u8], 1>::new(codes_1[i].as_slice());
                    let r_q = &queries_1[i];
                    let cdq = c_dot_q(&centroid, r_q);
                    let qn = q_norm(&centroid, r_q);
                    let qq = QuantizedQuery::new(r_q, 4, padded_bytes, cn, cdq, qn);
                    black_box(code.distance_query_bitwise(&df, &qq, dim));
                }
            });
        });

        group.throughput(Throughput::Bytes(throughput_1bit));
        desc!(
            format!("distance_query/1bit_lut/{dim}"),
            format!("cold {BATCH} queries; BatchQueryLuts build + nibble lookup (§3.3.2)")
        );
        group.bench_with_input(BenchmarkId::new("1bit_lut", dim), &dim, |b, _| {
            b.iter(|| {
                for i in 0..BATCH {
                    let code = Code::<&[u8], 1>::new(codes_1[i].as_slice());
                    let r_q = &queries_1[i];
                    let cdq = c_dot_q(&centroid, r_q);
                    let qn = q_norm(&centroid, r_q);
                    let luts = BatchQueryLuts::new(r_q, cn, cdq, qn);
                    black_box(luts.distance_query(&code, &df));
                }
            });
        });
    }

    // ── Hot-query / cluster-scan variant ─────────────────────────────────────
    // Query setup (QuantizedQuery / BatchQueryLuts build) is done once outside
    // the iter loop, so only the inner per-code scoring loop is timed.
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
        let padded_bytes = Code::<&[u8], 1>::packed_len(SCAN_DIM);

        // Throughput counts code bytes only; the query is amortized and stays in L1.
        let tput_1bit = SCAN_N as u64 * Code::<&[u8], 1>::size(SCAN_DIM) as u64;
        let tput_4bit = SCAN_N as u64 * Code::<&[u8], 4>::size(SCAN_DIM) as u64;

        group.throughput(Throughput::Bytes(tput_4bit));
        desc!(
            "distance_query/cluster_scan_4bit_float",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; grid unpack + f32 dot (quality ceiling)")
        );
        group.bench_function("cluster_scan_4bit_float", |b| {
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

        group.throughput(Throughput::Bytes(tput_1bit));
        desc!(
            "distance_query/cluster_scan_1bit_float",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; signed_dot; query in L1 (baseline)")
        );
        group.bench_function("cluster_scan_1bit_float", |b| {
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

        group.throughput(Throughput::Bytes(tput_1bit));
        desc!(
            "distance_query/cluster_scan_1bit_bitwise",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; QuantizedQuery built once, AND+popcount (§3.3.1)")
        );
        group.bench_function("cluster_scan_1bit_bitwise", |b| {
            let qq = QuantizedQuery::new(&r_q, 4, padded_bytes, cn, cdq, qn);
            b.iter(|| {
                let _: f32 = codes_1
                    .iter()
                    .map(|cb| {
                        let code = Code::<&[u8], 1>::new(cb.as_slice());
                        black_box(code.distance_query_bitwise(&df, &qq, SCAN_DIM))
                    })
                    .sum();
            });
        });

        group.throughput(Throughput::Bytes(tput_1bit));
        desc!(
            "distance_query/cluster_scan_1bit_lut",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; BatchQueryLuts built once, nibble lookup (§3.3.2)")
        );
        group.bench_function("cluster_scan_1bit_lut", |b| {
            let luts = BatchQueryLuts::new(&r_q, cn, cdq, qn);
            b.iter(|| {
                let _: f32 = codes_1
                    .iter()
                    .map(|cb| {
                        let code = Code::<&[u8], 1>::new(cb.as_slice());
                        black_box(luts.distance_query(&code, &df))
                    })
                    .sum();
            });
        });
    }

    group.finish();
}

// ── 4. Thread scaling ─────────────────────────────────────────────────────────
//
// Parallel quantization and distance_query at dim=1024.  Shows whether
// throughput scales linearly (compute-bound) or flattens (LLC / memory-bound).

fn bench_thread_scaling(c: &mut Criterion) {
    const DIM: usize = 1024;
    const N: usize = 1024;

    let mut rng = make_rng();
    let centroid: Vec<f32> = random_vec(&mut rng, DIM);
    let embeddings: Vec<Vec<f32>> = (0..N).map(|_| random_vec(&mut rng, DIM)).collect();

    let (_, codes_1, queries_1) = make_codes::<1>(DIM, N);
    let (_, codes_4, queries_4) = make_codes::<4>(DIM, N);
    let cn = c_norm(&centroid);
    let df = DistanceFunction::Euclidean;
    let padded_bytes = Code::<&[u8], 1>::packed_len(DIM);

    let mut group = c.benchmark_group("thread_scaling");

    for &threads in THREAD_COUNTS {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();

        group.throughput(Throughput::Bytes((N * DIM * 4) as u64));

        desc!(
            format!("thread_scaling/quantize_4bit/{threads}"),
            format!("{N} embeddings → 4-bit, {threads} thread(s)")
        );
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

        desc!(
            format!("thread_scaling/quantize_1bit/{threads}"),
            format!("{N} embeddings → 1-bit, {threads} thread(s)")
        );
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



        group.throughput(Throughput::Bytes(
            (N * (Code::<&[u8], 4>::size(DIM) + DIM * 4)) as u64,
        ));
        desc!(
            format!("thread_scaling/distance_query_4bit/{threads}"),
            format!("{N} cold 4-bit queries, {threads} thread(s)")
        );
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
        group.throughput(Throughput::Bytes(
            (N * (Code::<&[u8], 1>::size(DIM) + DIM * 4)) as u64,
        ));
        desc!(
            format!("thread_scaling/distance_query_1bit/{threads}"),
            format!("{N} cold 1-bit queries, {threads} thread(s)")
        );
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

        desc!(
            format!("thread_scaling/distance_query_1bit_bitwise/{threads}"),
            format!("{N} cold 1-bit queries (AND+popcount §3.3.1), {threads} thread(s)")
        );
        group.bench_with_input(
            BenchmarkId::new("distance_query_1bit_bitwise", threads),
            &threads,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        codes_1.par_iter().zip(queries_1.par_iter()).for_each(
                            |(code_bytes, r_q)| {
                                let code = Code::<&[u8], 1>::new(code_bytes.as_slice());
                                let cdq = c_dot_q(&centroid, r_q);
                                let qn = q_norm(&centroid, r_q);
                                let qq = QuantizedQuery::new(r_q, 4, padded_bytes, cn, cdq, qn);
                                black_box(code.distance_query_bitwise(&df, &qq, DIM));
                            },
                        );
                    });
                });
            },
        );

        desc!(
            format!("thread_scaling/distance_query_1bit_lut/{threads}"),
            format!("{N} cold 1-bit queries (nibble LUT §3.3.2), {threads} thread(s)")
        );
        group.bench_with_input(
            BenchmarkId::new("distance_query_1bit_lut", threads),
            &threads,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        codes_1.par_iter().zip(queries_1.par_iter()).for_each(
                            |(code_bytes, r_q)| {
                                let code = Code::<&[u8], 1>::new(code_bytes.as_slice());
                                let cdq = c_dot_q(&centroid, r_q);
                                let qn = q_norm(&centroid, r_q);
                                let luts = BatchQueryLuts::new(r_q, cn, cdq, qn);
                                black_box(luts.distance_query(&code, &df));
                            },
                        );
                    });
                });
            },
        );
    }
    group.finish();
}

// ── 6. Error distribution analysis ───────────────────────────────────────────
//
// Not a timing benchmark.  Measures how accurately each implementation
// estimates the true squared Euclidean distance.
//
// For every (embedding, query) pair we compute two metrics:
//
//   relative_error = (d_est − d_true) / d_true   [dimensionless, scale-free]
//   absolute_error = d_est − d_true               [in units of squared distance]
//
// where d_true = Σ(eᵢ − qᵢ)² (true squared L2 from original floats) and
// d_est comes from each quantized estimator.  Comparing the four methods
// isolates two distinct error sources:
//
//   data quantization alone : 4bit_float  vs  1bit_float
//   + query quantization    : 1bit_float  vs  1bit_bitwise / 1bit_lut
//
// WHY THE RELATIVE-ERROR MEAN IS NON-ZERO (even for an unbiased estimator)
// ─────────────────────────────────────────────────────────────────────────
// The RaBitQ paper claims the estimator is unbiased in the ABSOLUTE sense:
//
//   E[d_est − d_true] = 0
//
// The 1-bit estimator approximates ⟨r, r_q⟩ ≈ ‖r‖ · ⟨g, r_q⟩ / ⟨g, n⟩
// where g = sign(r)/√D and n = r/‖r‖.  Averaging over random r_q with
// E[r_q] = 0, both the true inner product and its estimate have expectation
// zero, so the absolute error E[d_est − d_true] is indeed zero.
//
// The RELATIVE error ε/d_true is a different story:
//
//   E[ε / d_true] = Cov(ε, d_true⁻¹)  ≠ 0  in general
//
// because d_true = ‖r − r_q‖² ≥ 0 is bounded below by zero, which makes
// the relative-error distribution inherently right-skewed:
//
//   • (d_est − d_true) / d_true ≥ −1  (hard floor: d_est ≥ 0, so d_est/d_true − 1 ≥ −1)
//   • no corresponding upper bound: d_est can overshoot by any amount
//
// In practice: for near pairs (small d_true), even a modest absolute
// overestimate gets divided by a small denominator, producing a large
// positive relative error.  For far pairs (large d_true), the same absolute
// overestimate is a tiny relative error.  The net result is a positive mean
// relative error even though the absolute error is zero on average.
//
// The 4-bit estimator shows near-zero mean relative error because its
// absolute errors are ~8× smaller, making this asymmetry negligible.
//
// WHAT THE ABSOLUTE-ERROR TABLE ACTUALLY SHOWS
// ─────────────────────────────────────────────
// The paper's unbiasedness guarantee is: for a fixed query r_q, the
// expected estimator over many random rotations P equals the true inner
// product, i.e., E_P[⟨ĝ(Pr), r_q⟩ · ‖r‖ / ⟨ĝ(Pr), n̂⟩] = ⟨r, r_q⟩.
//
// Our test does NOT average over rotations.  Instead, we use ONE fixed
// quantization (no rotation) and average over many RANDOM queries drawn
// from U(-1,1)^D.  Because the queries are centered at the ORIGIN and the
// centroid c is non-zero, the query residuals satisfy E[r_q] = -c ≠ 0.
// That non-zero mean propagates differently through the true inner product
// (⟨r, r_q⟩ → -⟨r, c⟩) and through the 1-bit estimator
// (‖r‖²/‖r‖₁ · ⟨sign(r), r_q⟩ → -‖r‖²/‖r‖₁ · ⟨sign(r), c⟩), causing
// a small systematic gap whenever |r_k| ≠ ‖r‖²/‖r‖₁ (i.e., always).
//
// In a real ANN workload queries are specific fixed vectors, not random,
// so this is a test-setup artefact only.  The non-zero absolute mean is
// small (≈ 0.3 % of d_true) compared to the std (≈ 2 %), confirming the
// estimator is useful in practice.
//
// Results are printed as a relative-error stats table, an absolute-error
// summary, and per-method histograms (shared x-axis, directly comparable).

fn print_error_analysis() {
    const DIM: usize = 1024;
    const N: usize = 2048;       // codes per cluster
    const N_QUERIES: usize = 64; // queries to average over
    const N_BINS: usize = 20;    // histogram bins
    const BAR_W: usize = 48;     // max histogram bar width in chars

    let mut rng = make_rng();
    let centroid = random_vec(&mut rng, DIM);
    let df = DistanceFunction::Euclidean;
    let cn = c_norm(&centroid);
    let padded_bytes = Code::<&[u8], 1>::packed_len(DIM);

    // Generate embeddings and keep the originals to compute d_true.
    let embeddings: Vec<Vec<f32>> = (0..N).map(|_| random_vec(&mut rng, DIM)).collect();
    let codes_1: Vec<Vec<u8>> = embeddings
        .iter()
        .map(|emb| Code::<Vec<u8>, 1>::quantize(emb, &centroid).as_ref().to_vec())
        .collect();
    let codes_4: Vec<Vec<u8>> = embeddings
        .iter()
        .map(|emb| Code::<Vec<u8>, 4>::quantize(emb, &centroid).as_ref().to_vec())
        .collect();

    let total = N * N_QUERIES;
    let mut err_4bit   = Vec::with_capacity(total);
    let mut err_1float = Vec::with_capacity(total);
    let mut err_1bitw  = Vec::with_capacity(total);
    let mut err_1lut   = Vec::with_capacity(total);
    // distance_code: both the data vector and the query are quantized codes.
    // This stacks the error from quantizing both sides, isolating the combined
    // code-vs-code estimation error vs. the one-sided code-vs-query methods.
    let mut err_code4  = Vec::with_capacity(total);
    let mut err_code1  = Vec::with_capacity(total);

    // Absolute errors collected in parallel; E[abs] ≈ 0 per the paper's
    // unbiasedness claim.  Comparing against the relative-error means above
    // shows that the non-zero relative mean is a metric artefact, not a bug.
    let mut abs_4bit   = Vec::with_capacity(total);
    let mut abs_1float = Vec::with_capacity(total);
    let mut abs_1bitw  = Vec::with_capacity(total);
    let mut abs_1lut   = Vec::with_capacity(total);
    let mut abs_code4  = Vec::with_capacity(total);
    let mut abs_code1  = Vec::with_capacity(total);

    for _ in 0..N_QUERIES {
        let query = random_vec(&mut rng, DIM);
        let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
        let cdq = c_dot_q(&centroid, &r_q);
        let qn  = q_norm(&centroid, &r_q);
        // QuantizedQuery and LUTs are built once per query, amortized over all N codes.
        let qq   = QuantizedQuery::new(&r_q, 4, padded_bytes, cn, cdq, qn);
        let luts = BatchQueryLuts::new(&r_q, cn, cdq, qn);
        // Quantize the query itself so distance_code can treat it as another data code.
        let cq1_bytes = Code::<Vec<u8>, 1>::quantize(&query, &centroid).as_ref().to_vec();
        let cq4_bytes = Code::<Vec<u8>, 4>::quantize(&query, &centroid).as_ref().to_vec();
        let cq1 = Code::<&[u8], 1>::new(cq1_bytes.as_slice());
        let cq4 = Code::<&[u8], 4>::new(cq4_bytes.as_slice());

        for i in 0..N {
            // True squared Euclidean distance from original unquantized embedding.
            let d_true: f32 = embeddings[i]
                .iter()
                .zip(&query)
                .map(|(e, q)| (e - q) * (e - q))
                .sum();
            if d_true < f32::EPSILON {
                continue;
            }

            let c1 = Code::<&[u8], 1>::new(codes_1[i].as_slice());
            let c4 = Code::<&[u8], 4>::new(codes_4[i].as_slice());

            let d4  = c4.distance_query(&df, &r_q, cn, cdq, qn);
            let df1 = c1.distance_query(&df, &r_q, cn, cdq, qn);
            let db  = c1.distance_query_bitwise(&df, &qq, DIM);
            let dl  = luts.distance_query(&c1, &df);
            // distance_code: both vectors quantized; error comes from both sides.
            let dc4 = c4.distance_code(&df, &cq4, cn, DIM);
            let dc1 = c1.distance_code(&df, &cq1, cn, DIM);

            // Relative error: positive = overestimate, negative = underestimate.
            err_4bit.push((d4  - d_true) / d_true);
            err_1float.push((df1 - d_true) / d_true);
            err_1bitw.push((db  - d_true) / d_true);
            err_1lut.push((dl  - d_true) / d_true);
            err_code4.push((dc4 - d_true) / d_true);
            err_code1.push((dc1 - d_true) / d_true);

            abs_4bit.push(d4  - d_true);
            abs_1float.push(df1 - d_true);
            abs_1bitw.push(db  - d_true);
            abs_1lut.push(dl  - d_true);
            abs_code4.push(dc4 - d_true);
            abs_code1.push(dc1 - d_true);
        }
    }

    // ── Descriptive statistics ────────────────────────────────────────────────
    struct Stats {
        mean: f32, std: f32, rmse: f32,
        p5: f32, p25: f32, p50: f32, p75: f32, p95: f32,
    }

    let compute_stats = |v: &mut Vec<f32>| -> Stats {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = v.len() as f32;
        let mean = v.iter().sum::<f32>() / n;
        let var  = v.iter().map(|x| (x - mean) * (x - mean)).sum::<f32>() / n;
        let rmse = (v.iter().map(|x| x * x).sum::<f32>() / n).sqrt();
        let pct  = |p: f32| v[((p * n) as usize).min(v.len() - 1)];
        Stats { mean, std: var.sqrt(), rmse,
                p5: pct(0.05), p25: pct(0.25), p50: pct(0.50),
                p75: pct(0.75), p95: pct(0.95) }
    };

    let s4  = compute_stats(&mut err_4bit);
    let sf  = compute_stats(&mut err_1float);
    let sb  = compute_stats(&mut err_1bitw);
    let sl  = compute_stats(&mut err_1lut);
    let sc4 = compute_stats(&mut err_code4);
    let sc1 = compute_stats(&mut err_code1);

    let hr  = "═".repeat(92);
    let sep = "─".repeat(92);

    // Per-method descriptions printed in the header for quick reference.
    let methods_desc: &[(&str, &str)] = &[
        ("4bit_float",   "distance_query, 4-bit data code, raw f32 query (most accurate)"),
        ("1bit_float",   "distance_query, 1-bit data code, raw f32 query"),
        ("1bit_bitwise", "distance_query_bitwise, 1-bit data + 4-bit quantized query (QuantizedQuery)"),
        ("1bit_lut",     "BatchQueryLuts::distance_query, 1-bit data + nibble LUT query"),
        ("4bit_code",    "distance_code, 4-bit data code vs 4-bit query code (both sides quantized)"),
        ("1bit_code",    "distance_code, 1-bit data code vs 1-bit query code (both sides quantized)"),
    ];

    println!("\n{hr}");
    println!("  Error analysis: relative_error = (d_est − d_true) / d_true");
    println!("  dim={DIM}, N={N} codes, {N_QUERIES} queries, {} samples/method", N * N_QUERIES);
    println!("  d_true = true squared L2 between original embedding and query");
    println!("{sep}");
    println!("  Methods:");
    for (name, desc) in methods_desc {
        println!("    {:<20} {}", name, desc);
    }
    println!("{sep}");
    println!("  {:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
             "method", "mean", "std", "RMSE", "p5", "p25", "p50", "p75", "p95");
    println!("{sep}");

    let row = |name: &str, s: &Stats| {
        println!("  {:<20} {:>+8.3} {:>8.3} {:>8.3} {:>+8.3} {:>+8.3} {:>+8.3} {:>+8.3} {:>+8.3}",
                 name, s.mean, s.std, s.rmse, s.p5, s.p25, s.p50, s.p75, s.p95);
    };
    row("4bit_float",   &s4);
    row("1bit_float",   &sf);
    row("1bit_bitwise", &sb);
    row("1bit_lut",     &sl);
    println!("{sep}");
    row("4bit_code",    &sc4);
    row("1bit_code",    &sc1);

    // ── Absolute error summary ────────────────────────────────────────────────
    // See the block comment above the function for a full explanation of why
    // this mean may be non-zero in our random-query test setup.  In practice
    // (fixed queries, real data) the bias is not present.
    println!("{sep}");
    println!("  Absolute error (d_est − d_true)  [see comment re: test-setup bias]");
    println!("{sep}");
    println!("  {:<20} {:>12} {:>12}", "method", "mean", "std");
    println!("{sep}");

    let abs_summary = |v: &[f32]| -> (f32, f32) {
        let n = v.len() as f32;
        let mean = v.iter().sum::<f32>() / n;
        let std  = (v.iter().map(|x| (x - mean) * (x - mean)).sum::<f32>() / n).sqrt();
        (mean, std)
    };

    let abs_row = |name: &str, v: &[f32]| {
        let (mean, std) = abs_summary(v);
        println!("  {:<20} {:>+12.4} {:>12.4}", name, mean, std);
    };
    abs_row("4bit_float",   &abs_4bit);
    abs_row("1bit_float",   &abs_1float);
    abs_row("1bit_bitwise", &abs_1bitw);
    abs_row("1bit_lut",     &abs_1lut);
    abs_row("4bit_code",    &abs_code4);
    abs_row("1bit_code",    &abs_code1);

    // ── Histograms ────────────────────────────────────────────────────────────
    // All four methods share the same bin edges and the same bar scale
    // (global_max across all bins and methods), so bar lengths are directly
    // comparable across histograms.
    //
    // The range is auto-detected from the data: we take the 99th-percentile
    // absolute relative-error across all methods combined, then round up to
    // the nearest 0.01.  This zooms the x-axis to where the data actually
    // lives instead of wasting bins on an empty ±100% range.
    let range = {
        let p99_abs = |v: &[f32]| -> f32 {
            let mut abs: Vec<f32> = v.iter().map(|x| x.abs()).collect();
            abs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let idx = ((abs.len() as f32 * 0.99) as usize).min(abs.len() - 1);
            abs[idx]
        };
        let max_p99 = p99_abs(&err_4bit)
            // .max(p99_abs(&err_1float))
            // .max(p99_abs(&err_1bitw))
            // .max(p99_abs(&err_1lut))
            .max(p99_abs(&err_code4))
            .max(p99_abs(&err_code1));
        // Round up to nearest 0.01 so bin edges are clean numbers.
        ((max_p99 / 0.01).ceil() * 0.01).clamp(0.01, 1.0)
    };

    println!("{sep}");
    println!("  Histograms  (±{:.0}% range, {N_BINS} bins, bars scaled to global max)", range * 100.0);
    println!("  Range auto-detected from p99 of |relative_error| across all methods.");
    println!("  Values outside ±{:.0}% are counted in the ≤ / + edge bins.", range * 100.0);
    println!("{sep}");

    let bin_w = 2.0 * range / N_BINS as f32;

    let make_hist = |v: &[f32]| -> Vec<usize> {
        let mut counts = vec![0usize; N_BINS];
        for &e in v {
            let idx = ((e + range) / bin_w) as isize;
            counts[idx.clamp(0, (N_BINS - 1) as isize) as usize] += 1;
        }
        counts
    };

    let h4  = make_hist(&err_4bit);
    let hf  = make_hist(&err_1float);
    let hb  = make_hist(&err_1bitw);
    let hl  = make_hist(&err_1lut);
    let hc4 = make_hist(&err_code4);
    let hc1 = make_hist(&err_code1);

    let global_max = h4.iter().chain(&hf).chain(&hb).chain(&hl)
        .chain(&hc4).chain(&hc1)
        .copied().max().unwrap_or(1);

    let bar = |count: usize| -> String {
        // Use eighth-block characters for sub-character precision.
        let eighths = count * BAR_W * 8 / global_max;
        let full    = eighths / 8;
        let frac    = eighths % 8;
        let frac_ch = [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉'][frac];
        format!("{}{}", "█".repeat(full), if frac > 0 { frac_ch.to_string() } else { String::new() })
    };

    let methods: &[(&str, &[usize])] = &[
        ("4bit_float",   &h4),
        ("1bit_float",   &hf),
        ("1bit_bitwise", &hb),
        ("1bit_lut",     &hl),
        ("4bit_code",    &hc4),
        ("1bit_code",    &hc1),
    ];

    for (name, hist) in methods {
        println!("\n  {name}:");
        for (i, &count) in hist.iter().enumerate() {
            let lo = -range + i as f32 * bin_w;
            let hi = lo + bin_w;
            let lo_mark = if i == 0            { "≤" } else { " " };
            let hi_mark = if i == N_BINS - 1   { "+" } else { " " };
            println!("  {lo_mark}[{lo:+.3},{hi:+.3}){hi_mark} {:7} | {}",
                     count, bar(count));
        }
    }

    println!("\n{hr}\n");
}

fn bench_error_analysis(c: &mut Criterion) {
    let _ = c;
    print_error_analysis();
}

// ── 7. SIMD primitive kernels ─────────────────────────────────────────────────

fn bench_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives");

    for &dim in DIMS {
        let padded = dim.div_ceil(64) * 64;
        let bytes = padded / 8;
        let mut rng = make_rng();
        let a: Vec<u8> = (0..bytes).map(|_| rng.gen()).collect();
        let b: Vec<u8> = (0..bytes).map(|_| rng.gen()).collect();
        let values: Vec<f32> = (0..padded).map(|_| rng.gen_range(-1.0f32..1.0)).collect();

        group.throughput(Throughput::Bytes(2 * bytes as u64));
        desc!(
            format!("primitives/hamming_distance/{dim}"),
            "XOR + popcount over two packed bit vectors"
        );
        group.bench_with_input(BenchmarkId::new("hamming_distance", dim), &dim, |b_cr, _| {
            b_cr.iter(|| {
                let header = [0u8; 12];
                let mut code_a = header.to_vec();
                code_a.extend_from_slice(&a);
                let mut code_b = header.to_vec();
                code_b.extend_from_slice(&b);
                let ca = Code::<&[u8], 1>::new(code_a.as_slice());
                let cb_code = Code::<&[u8], 1>::new(code_b.as_slice());
                black_box(ca.distance_code(&DistanceFunction::InnerProduct, &cb_code, 0.0, padded));
            });
        });

        group.throughput(Throughput::Bytes((bytes + padded * 4) as u64));
        desc!(
            format!("primitives/1bit_kernel/{dim}"),
            "signed_dot: bits → ±1.0 f32, simsimd dot product"
        );
        group.bench_with_input(BenchmarkId::new("1bit_kernel", dim), &dim, |b_cr, _| {
            b_cr.iter(|| {
                let mut rng2 = make_rng();
                let centroid = vec![0.0f32; padded];
                let embedding: Vec<f32> =
                    (0..padded).map(|_| rng2.gen_range(-1.0f32..1.0)).collect();
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

        let embedding: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        desc!(
            format!("primitives/vec_sub/{dim}"),
            "r = embedding − centroid (residual formation, first step of quantize)"
        );
        group.bench_with_input(BenchmarkId::new("vec_sub", dim), &dim, |b_cr, _| {
            b_cr.iter(|| {
                let r: Vec<f32> = embedding.iter().zip(&centroid).map(|(e, c)| e - c).collect();
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
    bench_error_analysis,
    bench_primitives,
);
criterion_main!(benches);
