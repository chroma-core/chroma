//! Benchmarks for RaBitQ quantization performance characteristics.
//!
//! Groups:
//!   quantize         — encoding throughput (1-bit, 4-bit) and query setup
//!   distance_code    — code-vs-code distance (1-bit, 4-bit) vs dim
//!   distance_query   — code-vs-query distance (bitwise 1-bit, float 4-bit):
//!                        cold (BATCH=512 distinct queries per dim)
//!                        hot / cluster-scan (1 query x N=2048 codes, dim=1024)
//!   thread_scaling   — parallel quantize and distance_query vs thread count
//!   primitives       — raw kernel throughput for each function's primitives
//!   error_analysis   — error distribution and histograms for all implementations
//!
//! ── Function tags ─────────────────────────────────────────────────────────
//!
//! Every benchmark ID contains a short function tag.  Tags are designed so
//! that substring matching (the only filter Criterion supports) yields
//! exactly the right set of benchmarks for two common scenarios:
//!
//!   Scenario 1 — compare all variants of a function family:
//!     cargo bench ... -- dq-            # all distance_query variants
//!     cargo bench ... -- dc-            # all distance_code variants
//!     cargo bench ... -- q-             # all data encoding + query setup
//!
//!   Scenario 2 — one specific function, across all groups:
//!     cargo bench ... -- dq-bw          # bitwise: cold, hot, thread, primitives
//!     cargo bench ... -- quant-1bit         # 1-bit encode: quantize, thread, primitives
//!     cargo bench ... -- quant-query         # QuantizedQuery::new: quantize, primitives
//!
//!   Tag        Function                                 Prefix
//!   ─────────  ────────────────────────────────────     ──────
//!   quant-1bit     Code::<1>::quantize                       q-
//!   quant-4bit     Code::<4>::quantize                       q-
//!   quant-query    QuantizedQuery::new                       q-
//!   dq-fp          DistanceFunction::distance (f32×f32)      dq-
//!   dq-bw          Code::<1>::distance_quantized_query       dq-
//!   dq-4f          Code::<4>::distance_query                 dq-
//!   dc-1bit        Code::<1>::distance_code                  dc-
//!   dc-4bit        Code::<4>::distance_code                  dc-
//!
//! Run with:
//!   cargo bench -p chroma-index --bench quantization_performance
//!   cargo bench -p chroma-index --bench quantization_performance -- dq-
//!   cargo bench -p chroma-index --bench quantization_performance -- dq-bw
//!
//! For native CPU (POPCNT, AVX2, etc.):
//!   RUSTFLAGS="-C target-cpu=native" cargo bench -p chroma-index --bench quantization_performance

use std::hint::black_box;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery, rabitq_distance_code_public};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use itertools::izip;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::prelude::*;
use simsimd::{BinarySimilarity, SpatialSimilarity};

/// Criterion's filter: the first positional argument after `--`, if any.
/// Benchmarks whose ID doesn't contain the filter are skipped by Criterion,
/// so we skip the desc! print too.
fn bench_filter() -> Option<String> {
    // argv: <binary> [criterion-opts...] [filter]
    // The filter is the last positional arg (no leading `-`).
    std::env::args().skip(1).find(|a| !a.starts_with('-'))
}

/// Print a one-line description of the benchmark that follows, but only when
/// the benchmark ID matches the active filter (or no filter is set).
macro_rules! desc {
    ($id:expr, $text:expr) => {
        if bench_filter().map_or(true, |f| $id.contains(f.as_str())) {
            println!("  [{:48}] {}", $id, $text);
        }
    };
}

const DIMS: &[usize] = &[1024];
// const DIMS: &[usize] = &[128, 1024, 4096];
const BATCH: usize = 512;
// const THREAD_COUNTS: &[usize] = &[1, 8];
const THREAD_COUNTS: &[usize] = &[1, 16, 32];

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_rng() -> StdRng {
    StdRng::seed_from_u64(0xdeadbeef)
}

fn random_vec(rng: &mut impl Rng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0_f32..1.0)).collect()
}

fn make_codes_1bit(dim: usize, n: usize) -> (Vec<f32>, Vec<Vec<u8>>, Vec<Vec<f32>>) {
    let mut rng = make_rng();
    let centroid = random_vec(&mut rng, dim);
    let codes: Vec<Vec<u8>> = (0..n)
        .map(|_| {
            Code::<1>::quantize(&random_vec(&mut rng, dim), &centroid)
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

fn make_codes_4bit(dim: usize, n: usize) -> (Vec<f32>, Vec<Vec<u8>>, Vec<Vec<f32>>) {
    let mut rng = make_rng();
    let centroid = random_vec(&mut rng, dim);
    let codes: Vec<Vec<u8>> = (0..n)
        .map(|_| {
            Code::<4>::quantize(&random_vec(&mut rng, dim), &centroid)
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

    for &dim in DIMS {
        let mut rng = make_rng();
        let centroid = random_vec(&mut rng, dim);
        let embedding = random_vec(&mut rng, dim);
        let padded_bytes = Code::<1>::packed_len(dim);
        let cn = c_norm(&centroid);

        group.throughput(Throughput::Bytes((2 * dim * 4) as u64));
        desc!(
            format!("quant-4bit/{dim}"),
            "per-call; 4-bit ray-walk codes"
        );
        group.bench_with_input(BenchmarkId::new("quant-4bit", dim), &dim, |b, _| {
            b.iter(|| black_box(Code::<4>::quantize(&embedding, &centroid)));
        });

        group.throughput(Throughput::Bytes((2 * dim * 4) as u64));
        desc!(
            format!("quant-1bit/{dim}"),
            "per-call; sign-bit codes"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit", dim), &dim, |b, _| {
            b.iter(|| black_box(Code::<1>::quantize(&embedding, &centroid)));
        });

        group.throughput(Throughput::Bytes((dim * 4) as u64));
        desc!(
            format!("quant-query/{dim}"),
            "per-call; residual alloc + c_dot_q + q_norm + QuantizedQuery::new"
        );
        group.bench_with_input(BenchmarkId::new("quant-query", dim), &dim, |b, _| {
            b.iter(|| {
                let r_q: Vec<f32> = embedding.iter().zip(&centroid).map(|(q, c)| q - c).collect();
                let cdq = c_dot_q(&centroid, &r_q);
                let qn = q_norm(&centroid, &r_q);
                black_box(QuantizedQuery::new(&r_q, padded_bytes, cn, cdq, qn));
            });
        });
    }

    group.finish();
}

fn bench_quantize_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("quantize_scan");

    for &dim in DIMS {
        let mut rng = make_rng();
        let centroid = random_vec(&mut rng, dim);
        let embeddings: Vec<Vec<f32>> = (0..BATCH).map(|_| random_vec(&mut rng, dim)).collect();
        let padded_bytes = Code::<1>::packed_len(dim);
        let cn = c_norm(&centroid);

        group.throughput(Throughput::Bytes((2 * dim * 4) as u64));
        desc!(
            format!("quant-4bit/{dim}"),
            format!("per-call avg over {BATCH} embeddings; 4-bit ray-walk codes")
        );
        group.bench_with_input(BenchmarkId::new("quant-4bit", dim), &dim, |b, _| {
            let mut idx = 0usize;
            b.iter(|| {
                let emb = &embeddings[idx];
                let d = black_box(Code::<4>::quantize(emb, &centroid));
                idx = (idx + 1) % BATCH;
                d
            });
        });

        group.throughput(Throughput::Bytes((2 * dim * 4) as u64));
        desc!(
            format!("quant-1bit/{dim}"),
            format!("per-call avg over {BATCH} embeddings; sign-bit codes")
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit", dim), &dim, |b, _| {
            let mut idx = 0usize;
            b.iter(|| {
                let emb = &embeddings[idx];
                let d = black_box(Code::<1>::quantize(emb, &centroid));
                idx = (idx + 1) % BATCH;
                d
            });
        });

        group.throughput(Throughput::Bytes((dim * 4) as u64));
        desc!(
            format!("quant-query/{dim}"),
            format!(
                "per-call avg over {BATCH} queries; residual alloc + c_dot_q + q_norm + QuantizedQuery::new"
            )
        );
        group.bench_with_input(BenchmarkId::new("quant-query", dim), &dim, |b, _| {
            let mut idx = 0usize;
            b.iter(|| {
                let query = &embeddings[idx];
                let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
                let cdq = c_dot_q(&centroid, &r_q);
                let qn = q_norm(&centroid, &r_q);
                let d = black_box(QuantizedQuery::new(&r_q, padded_bytes, cn, cdq, qn));
                idx = (idx + 1) % BATCH;
                d
            });
        });
    }

    group.finish();
}

// ── 2. distance_code throughput (code vs code) ────────────────────────────────

fn bench_distance(c: &mut Criterion) {
    // Compare the same two codes over and over again. Measure the latency of each distance_code call
    let mut group = c.benchmark_group("distance");
    let df = DistanceFunction::Euclidean;

    for &dim in DIMS {
        let (centroid, codes_1, queries_1) = make_codes_1bit(dim, 2);
        let (_, codes_4, queries_4) = make_codes_4bit(dim, 2);
        let cn = c_norm(&centroid);
        let code_bytes_1 = Code::<1>::size(dim);
        let code_bytes_4 = Code::<4>::size(dim);

        group.throughput(Throughput::Bytes(2 * code_bytes_1 as u64));
        desc!(
            format!("dc-1bit/{dim}"),
            "per-call; simsimd hamming (NEON CNT / AVX-512 VPOPCNTDQ)"
        );
        let a = Code::<1, _>::new(codes_1[0].as_slice());
        let bb = Code::<1, _>::new(codes_1[1].as_slice());
        group.bench_with_input(BenchmarkId::new("dc-1bit", dim), &dim, |b, _| {
            b.iter(|| black_box(a.distance_code(&bb, &df, cn, dim)));
        });

        group.throughput(Throughput::Bytes(2 * code_bytes_1 as u64));
        desc!(
            format!("dc-1bit/simsimd_hamming/{dim}"),
            "per-call; <u8 as BinarySimilarity>::hamming (SIMD popcount)"
        );
        group.bench_with_input(BenchmarkId::new("dc-1bit/simsimd_hamming", dim), &dim, |b, _| {
            let a = codes_1[0].as_slice();
            let bb = codes_1[1].as_slice();
            b.iter(|| black_box(<u8 as BinarySimilarity>::hamming(a, bb).unwrap_or(0.0)));
        });

        group.throughput(Throughput::Bytes(2 * code_bytes_1 as u64));
        desc!(
            format!("dc-1bit/rabitq_distance_code/{dim}"),
            "per-call; rabitq_distance_code"
        );
        group.bench_with_input(BenchmarkId::new("dc-1bit/rabitq_distance_code", dim), &dim, |b, _| {
            let a = Code::<1, _>::new(codes_1[0].as_slice());
            let bb = Code::<1, _>::new(codes_1[1].as_slice());
            b.iter(|| black_box(rabitq_distance_code_public(0.0, a.correction(), a.norm(), a.radial(), bb.correction(), bb.norm(), bb.radial(), cn, &df)));
        });

        group.throughput(Throughput::Bytes(2 * code_bytes_4 as u64));
        desc!(
            format!("dc-4bit/{dim}"),
            "per-call; nibble unpack + dot"
        );
        group.bench_with_input(BenchmarkId::new("dc-4bit", dim), &dim, |b, _| {
            let a = Code::<4, _>::new(codes_4[0].as_slice());
            let bb = Code::<4, _>::new(codes_4[1].as_slice());
            b.iter(|| black_box(a.distance_code(&bb, &df, cn, dim)));
        });

        let query_bytes = dim * 4;

        let mut rng_exact = StdRng::seed_from_u64(0xcafe);
        let raw_vec = random_vec(&mut rng_exact, dim);
        let raw_query = random_vec(&mut rng_exact, dim);

        group.throughput(Throughput::Bytes((2 * dim * 4) as u64));
        desc!(
            format!("dq-fp/{dim}"),
            "per-call; DistanceFunction::distance f32*f32 (ground truth)"
        );
        group.bench_with_input(BenchmarkId::new("dq-fp", dim), &dim, |b, _| {
            b.iter(|| black_box(df.distance(&raw_vec, &raw_query)));
        });

        group.throughput(Throughput::Bytes((code_bytes_4 + query_bytes) as u64));
        desc!(
            format!("dq-4f/{dim}"),
            "per-call; grid unpack + f32 dot (reference quality ceiling)"
        );
        group.bench_with_input(BenchmarkId::new("dq-4f", dim), &dim, |b, _| {
            let code = Code::<4, _>::new(codes_4[0].as_slice());
            let r_q = &queries_4[0];
            let cdq = c_dot_q(&centroid, r_q);
            let qn = q_norm(&centroid, r_q);
            b.iter(|| black_box(code.distance_query(&df, r_q, cn, cdq, qn)));
        });

        let query_bytes = dim * 4 / 8;
        let padded_bytes = Code::<1>::packed_len(dim);
        group.throughput(Throughput::Bytes((code_bytes_1 + query_bytes) as u64));
        desc!(
            format!("dq-bw/{dim}"),
            "per-call; QuantizedQuery built once, AND+popcount (§3.3.1)"
        );
        group.bench_with_input(BenchmarkId::new("dq-bw", dim), &dim, |b, _| {
            let code = Code::<1, _>::new(codes_1[0].as_slice());
            let r_q = &queries_1[0];
            let cdq = c_dot_q(&centroid, r_q);
            let qn = q_norm(&centroid, r_q);
            let qq = QuantizedQuery::new(r_q, padded_bytes, cn, cdq, qn);
            b.iter(|| black_box(code.distance_quantized_query(&df, &qq)));
        });
    }
    group.finish();
}

// ── Hot-query / cluster-scan variant ─────────────────────────────────────
// 1M codes, (1 query for distance_query functions).
// loop over all codes and compute the distance to the query/code.
// Do this as many times as Criterion chooses to. I've seen 1,065,641,261 function calls.
//
// Query setup (QuantizedQuery build) is done once outside the iter loop,
// so only the inner per-code scoring loop is timed.
// -------------------------------------------------------------------------
fn bench_distance_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_scan");
    const SCAN_DIM: usize = 1024;
    const SCAN_N: usize = 100000;

    let mut rng = make_rng();
    let centroid = random_vec(&mut rng, SCAN_DIM);
    let r_q: Vec<f32> = {
        let query = random_vec(&mut rng, SCAN_DIM);
        query.iter().zip(&centroid).map(|(q, c)| q - c).collect()
    };
    let cdq = c_dot_q(&centroid, &r_q);
    let qn = q_norm(&centroid, &r_q);
    let cn = c_norm(&centroid);
    let df = DistanceFunction::Euclidean;

    let (_, codes_1, _) = make_codes_1bit(SCAN_DIM, SCAN_N);
    let (_, codes_4, _) = make_codes_4bit(SCAN_DIM, SCAN_N);
    let padded_bytes = Code::<1>::packed_len(SCAN_DIM);

    let mut rng_exact = StdRng::seed_from_u64(0xcafe);
    let scan_raw_vecs: Vec<Vec<f32>> = (0..SCAN_N)
        .map(|_| random_vec(&mut rng_exact, SCAN_DIM))
        .collect();
    let scan_raw_query = random_vec(&mut rng_exact, SCAN_DIM);

    group.throughput(Throughput::Bytes((SCAN_DIM * 4) as u64));
    desc!(
        "dq-fp/scan",
        format!("per-call avg over {SCAN_N} vectors @ dim={SCAN_DIM}; DistanceFunction::distance f32*f32 (ground truth)")
    );
    {
        let mut idx = 0usize;
        group.bench_function("dq-fp/scan", |b| {
            b.iter(|| {
                let d = black_box(df.distance(&scan_raw_vecs[idx], &scan_raw_query));
                idx = (idx + 1) % SCAN_N;
                d
            });
        });
    }

    group.throughput(Throughput::Bytes(Code::<4>::size(SCAN_DIM) as u64));
    desc!(
        "dq-4f/scan",
        format!("per-call avg over {SCAN_N} codes @ dim={SCAN_DIM}; grid unpack + f32 dot (quality ceiling)")
    );
    {
        let mut idx = 0usize;
        group.bench_function("dq-4f/scan", |b| {
            b.iter(|| {
                let code = Code::<4, _>::new(codes_4[idx].as_slice());
                let d = black_box(code.distance_query(&df, &r_q, cn, cdq, qn));
                idx = (idx + 1) % SCAN_N;
                d
            });
        });
    }

    group.throughput(Throughput::Bytes(Code::<1>::size(SCAN_DIM) as u64));
    desc!(
        "dq-bw/scan",
        format!("per-call avg over {SCAN_N} codes @ dim={SCAN_DIM}; QuantizedQuery built once, AND+popcount (§3.3.1)")
    );
    {
        let qq = QuantizedQuery::new(&r_q, padded_bytes, cn, cdq, qn);
        let mut idx = 0usize;
        group.bench_function("dq-bw/scan", |b| {
            b.iter(|| {
                let code = Code::<1, _>::new(codes_1[idx].as_slice());
                let d = black_box(code.distance_quantized_query(&df, &qq));
                idx = (idx + 1) % SCAN_N;
                d
            });
        });
    }

    group.throughput(Throughput::Bytes(2 * Code::<1>::size(SCAN_DIM) as u64));
    desc!(
        "dc-1bit/scan",
        format!(
            "per-pair avg over {} pairs @ dim={SCAN_DIM}; simsimd hamming code-vs-code",
            SCAN_N / 2
        )
    );
    {
        let mut idx = 0usize;
        group.bench_function("dc-1bit/scan", |b| {
            b.iter(|| {
                let a = Code::<1, _>::new(codes_1[idx].as_slice());
                let bb = Code::<1, _>::new(codes_1[idx + 1].as_slice());
                let d = black_box(a.distance_code(&bb, &df, cn, SCAN_DIM));
                idx = (idx + 2) % SCAN_N;
                d
            });
        });
    }

    group.throughput(Throughput::Bytes(2 * Code::<1>::size(SCAN_DIM) as u64));
    desc!(
        "dc-1bit/scan/simsimd_hamming",
        format!(
            "per-pair avg over {} pairs @ dim={SCAN_DIM}; <u8 as BinarySimilarity>::hamming",
            SCAN_N / 2
        )
    );
    {
        let mut idx = 0usize;
        group.bench_function("dc-1bit/scan/simsimd_hamming", |b| {
            b.iter(|| {
                let a = codes_1[idx].as_slice();
                let bb = codes_1[idx + 1].as_slice();
                let d = black_box(<u8 as BinarySimilarity>::hamming(a, bb).unwrap_or(0.0));
                idx = (idx + 2) % SCAN_N;
                d
            });
        });
    }

    group.throughput(Throughput::Bytes(2 * Code::<1>::size(SCAN_DIM) as u64));
    desc!(
        "dc-1bit/scan/rabitq_distance_code",
        format!("per-pair avg over {} pairs @ dim={SCAN_DIM}; rabitq_distance_code", SCAN_N / 2)
    );
    {
        let mut idx = 0usize;
        group.bench_function("dc-1bit/scan/rabitq_distance_code", |b| {
            b.iter(|| {
                let a = Code::<1, _>::new(codes_1[idx].as_slice());
                let bb = Code::<1, _>::new(codes_1[idx + 1].as_slice());
                let d = black_box(rabitq_distance_code_public(0.0, a.correction(), a.norm(), a.radial(), bb.correction(), bb.norm(), bb.radial(), cn, &df));
                idx = (idx + 2) % SCAN_N;
                black_box(d);
            });
        });
    }

    group.throughput(Throughput::Bytes(2 * Code::<4>::size(SCAN_DIM) as u64));
    desc!(
        "dc-4bit/scan",
        format!(
            "per-pair avg over {} pairs @ dim={SCAN_DIM}; nibble unpack + dot code-vs-code",
            SCAN_N / 2
        )
    );
    {
        let mut idx = 0usize;
        group.bench_function("dc-4bit/scan", |b| {
            b.iter(|| {
                let a = Code::<4, _>::new(codes_4[idx].as_slice());
                let bb = Code::<4, _>::new(codes_4[idx + 1].as_slice());
                let d = black_box(a.distance_code(&bb, &df, cn, SCAN_DIM));
                idx = (idx + 2) % SCAN_N;
                d
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

    let (_, codes_1, queries_1) = make_codes_1bit(DIM, N);
    let (_, codes_4, queries_4) = make_codes_4bit(DIM, N);
    let cn = c_norm(&centroid);
    let df = DistanceFunction::Euclidean;
    let padded_bytes = Code::<1>::packed_len(DIM);

    let mut group = c.benchmark_group("thread_scaling");

    for &threads in THREAD_COUNTS {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();

        group.throughput(Throughput::Bytes((N * DIM * 4) as u64));

        desc!(
            format!("quant-4bit/{threads}t"),
            format!("{N} embeddings → 4-bit, {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("quant-4bit", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    embeddings.par_iter().for_each(|emb| {
                        black_box(Code::<4>::quantize(emb, &centroid));
                    });
                });
            });
        });

        desc!(
            format!("quant-1bit/{threads}t"),
            format!("{N} embeddings → 1-bit, {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    embeddings.par_iter().for_each(|emb| {
                        black_box(Code::<1>::quantize(emb, &centroid));
                    });
                });
            });
        });

        group.throughput(Throughput::Bytes(
            (N * (Code::<4>::size(DIM) + DIM * 4)) as u64,
        ));
        desc!(
            format!("dq-4f/{threads}t"),
            format!("{N} cold 4-bit queries, {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("dq-4f", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    codes_4
                        .par_iter()
                        .zip(queries_4.par_iter())
                        .for_each(|(code_bytes, r_q)| {
                            let code = Code::<4, _>::new(code_bytes.as_slice());
                            let cdq = c_dot_q(&centroid, r_q);
                            let qn = q_norm(&centroid, r_q);
                            black_box(code.distance_query(&df, r_q, cn, cdq, qn));
                        });
                });
            });
        });
        desc!(
            format!("dq-bw/{threads}t"),
            format!("{N} cold 1-bit queries (AND+popcount §3.3.1), {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("dq-bw", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    codes_1
                        .par_iter()
                        .zip(queries_1.par_iter())
                        .for_each(|(code_bytes, r_q)| {
                            let code = Code::<1, _>::new(code_bytes.as_slice());
                            let cdq = c_dot_q(&centroid, r_q);
                            let qn = q_norm(&centroid, r_q);
                            let qq = QuantizedQuery::new(r_q, padded_bytes, cn, cdq, qn);
                            black_box(code.distance_quantized_query(&df, &qq));
                        });
                });
            });
        });
        group.throughput(Throughput::Bytes(
            (N * 2 * Code::<1>::size(DIM)) as u64,
        ));
        desc!(
            format!("dc-1bit/{threads}t"),
            format!("{} pairs, 1-bit distance_code, {threads} thread(s)", N / 2)
        );
        group.bench_with_input(BenchmarkId::new("dc-1bit", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    codes_1.par_chunks(2).for_each(|pair| {
                        let a = Code::<1, _>::new(pair[0].as_slice());
                        let bb = Code::<1, _>::new(pair[1].as_slice());
                        black_box(a.distance_code(&bb, &df, cn, DIM));
                    });
                });
            });
        });

        group.throughput(Throughput::Bytes(
            (N * 2 * Code::<4>::size(DIM)) as u64,
        ));
        desc!(
            format!("dc-4bit/{threads}t"),
            format!("{} pairs, 4-bit distance_code, {threads} thread(s)", N / 2)
        );
        group.bench_with_input(BenchmarkId::new("dc-4bit", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    codes_4.par_chunks(2).for_each(|pair| {
                        let a = Code::<4, _>::new(pair[0].as_slice());
                        let bb = Code::<4, _>::new(pair[1].as_slice());
                        black_box(a.distance_code(&bb, &df, cn, DIM));
                    });
                });
            });
        });
    }
    group.finish();
}

// ── 7. Primitive kernel benchmarks ─────────────────────────────────────────────
//
// Each primitive benchmark ID contains the parent function's tag (see header),
// so filtering by tag pulls in both full-function and primitive benchmarks:
//   cargo bench ... -- quant-1bit           # full quant-1bit + all its primitives
//   cargo bench ... -- quant-query          # full quant-query + min_max, quantize_elements, ...
//   cargo bench ... -- primitives           # ALL primitives only (no full-function groups)
//
// Isolate every major computational primitive used by Code::<1>::quantize,
// Code::<1>::distance_quantized_query, Code::<1>::distance_code,
// and QuantizedQuery::new.
//
// Comparing a function's wall-clock time against the sum of its primitives
// reveals overhead (allocation, scalar math, cache effects) that the
// "full function" benchmarks in earlier groups cannot isolate.
//
fn bench_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives");

    for &dim in DIMS {
        let padded = dim.div_ceil(64) * 64;
        let bytes = padded / 8;
        let mut rng = make_rng();

        let packed_a: Vec<u8> = (0..bytes).map(|_| rng.gen()).collect();
        let packed_b: Vec<u8> = (0..bytes).map(|_| rng.gen()).collect();
        let values: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
        let values2: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
        let bit_planes: Vec<Vec<u8>> = (0..4)
            .map(|_| (0..bytes).map(|_| rng.gen()).collect())
            .collect();

        // ── Code::<1>::quantize primitives ────────────────────────────────────
        //
        // These measure each logical operation in isolation.  The production
        // quantize() fuses all of them into a single pass with dual
        // accumulators over chunks_exact(16).  Compare the sum of these
        // against quant-1bit/full to see the fusion benefit.

        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        desc!(
            format!("quant-1bit/vec_sub/{dim}"),
            "r = emb − centroid  [reference: 8 KB read baseline, not used in fused path]"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit/vec_sub", dim), &dim, |b, _| {
            b.iter(|| {
                let r: Vec<f32> = values.iter().zip(&values2).map(|(e, c)| e - c).collect();
                black_box(r);
            });
        });

        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        desc!(
            format!("quant-1bit/simsimd_dot/{dim}"),
            "⟨a, b⟩ via simsimd  [reference: SIMD ceiling for a single dot product]"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-1bit/simsimd_dot", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    black_box(f32::dot(&values, &values2).unwrap_or(0.0) as f32);
                });
            },
        );

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-1bit/abs_sum/{dim}"),
            "Σ|r[i]| single accumulator (sequential dep chain, does NOT auto-vectorize)"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit/abs_sum", dim), &dim, |b, _| {
            b.iter(|| {
                let s: f32 = values.iter().map(|v| v.abs()).sum();
                black_box(s);
            });
        });

        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        desc!(
            format!("quant-1bit/fused_reductions/{dim}"),
            "sub + abs_sum + norm² + radial, dual accumulators over chunks_exact(16)"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-1bit/fused_reductions", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut abs_a = 0.0f32;
                    let mut nsq_a = 0.0f32;
                    let mut rad_a = 0.0f32;
                    let mut abs_b = 0.0f32;
                    let mut nsq_b = 0.0f32;
                    let mut rad_b = 0.0f32;
                    for (emb16, cen16) in values.chunks_exact(16).zip(values2.chunks_exact(16)) {
                        for j in 0..4 {
                            let v = emb16[j] - cen16[j];
                            abs_a += v.abs();
                            nsq_a += v * v;
                            rad_a += v * cen16[j];
                        }
                        for j in 4..8 {
                            let v = emb16[j] - cen16[j];
                            abs_b += v.abs();
                            nsq_b += v * v;
                            rad_b += v * cen16[j];
                        }
                        for j in 8..12 {
                            let v = emb16[j] - cen16[j];
                            abs_a += v.abs();
                            nsq_a += v * v;
                            rad_a += v * cen16[j];
                        }
                        for j in 12..16 {
                            let v = emb16[j] - cen16[j];
                            abs_b += v.abs();
                            nsq_b += v * v;
                            rad_b += v * cen16[j];
                        }
                    }
                    black_box((abs_a + abs_b, nsq_a + nsq_b, rad_a + rad_b));
                });
            },
        );

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-1bit/sign_pack/{dim}"),
            "sign-bit extract + byte pack via chunks_exact(16), 16 f32 → 2 bytes"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-1bit/sign_pack", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut packed = vec![0u8; bytes];
                    for (out, chunk) in packed.chunks_exact_mut(2).zip(values.chunks_exact(16)) {
                        let mut b0 = 0u8;
                        let mut b1 = 0u8;
                        for j in 0..8 {
                            b0 |= ((chunk[j].to_bits() >> 31) as u8 ^ 1) << j;
                        }
                        for j in 0..8 {
                            b1 |= ((chunk[j + 8].to_bits() >> 31) as u8 ^ 1) << j;
                        }
                        out[0] = b0;
                        out[1] = b1;
                    }
                    black_box(packed);
                });
            },
        );

        group.throughput(Throughput::Bytes(bytes as u64));
        desc!(
            format!("quant-1bit/popcount/{dim}"),
            "u64 popcount over packed bytes  [quantize: signed_sum]"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-1bit/popcount", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let count: u32 = packed_a
                        .chunks_exact(8)
                        .map(|c| u64::from_le_bytes(c.try_into().unwrap()).count_ones())
                        .sum();
                    black_box(count);
                });
            },
        );

        // ── Code::<1>::distance_code primitives ───────────────────────────────

        group.throughput(Throughput::Bytes(2 * bytes as u64));
        desc!(
            format!("dc-1bit/hamming/scalar/{dim}"),
            "scalar u64 XOR + POPCNT  [distance_code kernel, baseline]"
        );
        group.bench_with_input(
            BenchmarkId::new("dc-1bit/hamming/scalar", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut count = 0u32;
                    for i in (0..packed_a.len()).step_by(8) {
                        let a_word = u64::from_le_bytes(packed_a[i..i + 8].try_into().unwrap());
                        let b_word = u64::from_le_bytes(packed_b[i..i + 8].try_into().unwrap());
                        count += (a_word ^ b_word).count_ones();
                    }
                    black_box(count);
                });
            },
        );

        desc!(
            format!("dc-1bit/hamming/simsimd/{dim}"),
            "simsimd::BinarySimilarity::hamming (NEON CNT / AVX-512 VPOPCNTDQ)  [C1]"
        );
        group.bench_with_input(
            BenchmarkId::new("dc-1bit/hamming/simsimd", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let d = <u8 as BinarySimilarity>::hamming(&packed_a, &packed_b).unwrap_or(0.0);
                    black_box(d);
                });
            },
        );

        // ── Code::<1>::distance_query_bitwise primitives ──────────────────────

        group.throughput(Throughput::Bytes((bytes + 4 * bytes) as u64));
        desc!(
            format!("dq-bw/and_popcount/sequential/{dim}"),
            "4 sequential passes over x_b (baseline)  [distance_query_bitwise kernel]"
        );
        group.bench_with_input(
            BenchmarkId::new("dq-bw/and_popcount/sequential", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut xb_dot_qu = 0u32;
                    for (j, plane) in bit_planes.iter().enumerate() {
                        let mut plane_pop = 0u32;
                        for i in (0..packed_a.len()).step_by(8) {
                            let x_word = u64::from_le_bytes(packed_a[i..i + 8].try_into().unwrap());
                            let q_word = u64::from_le_bytes(plane[i..i + 8].try_into().unwrap());
                            plane_pop += (x_word & q_word).count_ones();
                        }
                        xb_dot_qu += plane_pop << j;
                    }
                    black_box(xb_dot_qu);
                });
            },
        );

        desc!(
            format!("dq-bw/and_popcount/interleaved/{dim}"),
            "[B1] 1 pass over x_b, 4 planes per word  [distance_query_bitwise kernel]"
        );
        group.bench_with_input(
            BenchmarkId::new("dq-bw/and_popcount/interleaved", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut pops = [0u32; 4];
                    for i in (0..packed_a.len()).step_by(8) {
                        let x_word = u64::from_le_bytes(packed_a[i..i + 8].try_into().unwrap());
                        for (j, plane) in bit_planes.iter().enumerate() {
                            let q_word = u64::from_le_bytes(plane[i..i + 8].try_into().unwrap());
                            pops[j] += (x_word & q_word).count_ones();
                        }
                    }
                    let xb_dot_qu = pops[0] + (pops[1] << 1) + (pops[2] << 2) + (pops[3] << 3);
                    black_box(xb_dot_qu);
                });
            },
        );

        desc!(
            format!("dq-bw/and_popcount/chunks/{dim}"),
            "[B2] chunks_exact(8) — eliminates index bounds check  [distance_query_bitwise kernel]"
        );
        group.bench_with_input(
            BenchmarkId::new("dq-bw/and_popcount/chunks", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut xb_dot_qu = 0u32;
                    for (j, plane) in bit_planes.iter().enumerate() {
                        let mut plane_pop = 0u32;
                        for (x_chunk, q_chunk) in
                            packed_a.chunks_exact(8).zip(plane.chunks_exact(8))
                        {
                            let x_word = u64::from_le_bytes(x_chunk.try_into().unwrap());
                            let q_word = u64::from_le_bytes(q_chunk.try_into().unwrap());
                            plane_pop += (x_word & q_word).count_ones();
                        }
                        xb_dot_qu += plane_pop << j;
                    }
                    black_box(xb_dot_qu);
                });
            },
        );

        desc!(
            format!("dq-bw/and_popcount/interleaved_chunks/{dim}"),
            "[B1+B2] interleaved + chunks_exact  [distance_query_bitwise kernel]"
        );
        group.bench_with_input(
            BenchmarkId::new("dq-bw/and_popcount/interleaved_chunks", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut pops = [0u32; 4];
                    let plane_chunks: [_; 4] =
                        std::array::from_fn(|j| bit_planes[j].chunks_exact(8));
                    for (x_chunk, (p0, p1, p2, p3)) in packed_a.chunks_exact(8).zip(izip!(
                        plane_chunks[0].clone(),
                        plane_chunks[1].clone(),
                        plane_chunks[2].clone(),
                        plane_chunks[3].clone(),
                    )) {
                        let x = u64::from_le_bytes(x_chunk.try_into().unwrap());
                        pops[0] += (x & u64::from_le_bytes(p0.try_into().unwrap())).count_ones();
                        pops[1] += (x & u64::from_le_bytes(p1.try_into().unwrap())).count_ones();
                        pops[2] += (x & u64::from_le_bytes(p2.try_into().unwrap())).count_ones();
                        pops[3] += (x & u64::from_le_bytes(p3.try_into().unwrap())).count_ones();
                    }
                    let xb_dot_qu = pops[0] + (pops[1] << 1) + (pops[2] << 2) + (pops[3] << 3);
                    black_box(xb_dot_qu);
                });
            },
        );

        // ── QuantizedQuery::new primitives ───────────────────────────────────

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-query/min_max/two_pass/{dim}"),
            "two separate fold passes for min and max (baseline)"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/min_max/two_pass", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let v_l = values.iter().copied().fold(f32::INFINITY, f32::min);
                    let v_r = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
                    black_box((v_l, v_r));
                });
            },
        );

        desc!(
            format!("quant-query/min_max/fused/{dim}"),
            "[P1] single fold pass for min+max simultaneously"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/min_max/fused", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let (v_l, v_r) = values
                        .iter()
                        .copied()
                        .fold((f32::INFINITY, f32::NEG_INFINITY), |(lo, hi), v| {
                            (lo.min(v), hi.max(v))
                        });
                    black_box((v_l, v_r));
                });
            },
        );

        let v_l = values.iter().copied().fold(f32::INFINITY, f32::min);
        let v_r = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let delta = (v_r - v_l) / 15.0;
        let _inv_delta = 1.0 / delta;

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-query/quantize_elements/{dim}"),
            "round((v-v_l)/delta), clamp to 0..15 — allocates Vec<u32>  [baseline]"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/quantize_elements", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let q_u: Vec<u32> = values
                        .iter()
                        .map(|&v| (((v - v_l) / delta).round() as u32).min(15))
                        .collect();
                    black_box(q_u);
                });
            },
        );

        let q_u: Vec<u32> = values
            .iter()
            .map(|&v| (((v - v_l) / delta).round() as u32).min(15))
            .collect();

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-query/bit_plane_decompose/baseline/{dim}"),
            "4 × Vec alloc + scatter with branch  [baseline]"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/bit_plane_decompose/baseline", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut planes = vec![vec![0u8; bytes]; 4];
                    for (i, &qu) in q_u.iter().enumerate() {
                        for j in 0..4usize {
                            if (qu >> j) & 1 == 1 {
                                planes[j][i / 8] |= 1 << (i % 8);
                            }
                        }
                    }
                    black_box(planes);
                });
            },
        );

        desc!(
            format!("quant-query/bit_plane_decompose/flat_alloc/{dim}"),
            "[P4] 1 flat Vec alloc + scatter with branch"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/bit_plane_decompose/flat_alloc", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut planes = vec![0u8; 4 * bytes];
                    for (i, &qu) in q_u.iter().enumerate() {
                        let byte = i / 8;
                        let bit = i % 8;
                        for j in 0..4usize {
                            planes[j * bytes + byte] |= (((qu >> j) & 1) as u8) << bit;
                        }
                    }
                    black_box(planes);
                });
            },
        );

        desc!(
            format!("quant-query/bit_plane_decompose/byte_chunks/{dim}"),
            "[P4+] flat alloc + process 8 elements→1 byte per plane"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/bit_plane_decompose/byte_chunks", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut planes = vec![0u8; 4 * bytes];
                    for (byte_idx, chunk) in q_u.chunks(8).enumerate() {
                        let (mut b0, mut b1, mut b2, mut b3) = (0u8, 0u8, 0u8, 0u8);
                        for (bit, &qu) in chunk.iter().enumerate() {
                            b0 |= (((qu >> 0) & 1) as u8) << bit;
                            b1 |= (((qu >> 1) & 1) as u8) << bit;
                            b2 |= (((qu >> 2) & 1) as u8) << bit;
                            b3 |= (((qu >> 3) & 1) as u8) << bit;
                        }
                        planes[0 * bytes + byte_idx] = b0;
                        planes[1 * bytes + byte_idx] = b1;
                        planes[2 * bytes + byte_idx] = b2;
                        planes[3 * bytes + byte_idx] = b3;
                    }
                    black_box(planes);
                });
            },
        );

        desc!(
            format!("quant-query/full/fused/{dim}"),
            "[P1+P2+P4] single pass: fused min/max + quantize + scatter, flat alloc"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/full/fused", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    // P1: fused min+max in one pass
                    let (v_l, v_r) = values
                        .iter()
                        .copied()
                        .fold((f32::INFINITY, f32::NEG_INFINITY), |(lo, hi), v| {
                            (lo.min(v), hi.max(v))
                        });
                    let range = v_r - v_l;
                    let delta = if range > f32::EPSILON {
                        range / 15.0
                    } else {
                        1.0
                    };
                    let inv_delta = 1.0 / delta;

                    // P2+P4: flat planes, fuse quantize + sum + scatter
                    let mut planes = vec![0u8; 4 * bytes];
                    let mut sum_q_u = 0u32;
                    for (byte_idx, chunk) in values.chunks(8).enumerate() {
                        let (mut b0, mut b1, mut b2, mut b3) = (0u8, 0u8, 0u8, 0u8);
                        for (bit, &v) in chunk.iter().enumerate() {
                            let qu = (((v - v_l) * inv_delta).round() as u32).min(15);
                            sum_q_u += qu;
                            b0 |= (((qu >> 0) & 1) as u8) << bit;
                            b1 |= (((qu >> 1) & 1) as u8) << bit;
                            b2 |= (((qu >> 2) & 1) as u8) << bit;
                            b3 |= (((qu >> 3) & 1) as u8) << bit;
                        }
                        planes[0 * bytes + byte_idx] = b0;
                        planes[1 * bytes + byte_idx] = b1;
                        planes[2 * bytes + byte_idx] = b2;
                        planes[3 * bytes + byte_idx] = b3;
                    }
                    black_box((planes, sum_q_u));
                });
            },
        );

        desc!(
            format!("quant-query/full/two_pass_fused/{dim}"),
            "[P2+P4] two-pass min/max + fused quantize+sum+scatter via chunks_exact(8)"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/full/two_pass_fused", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let v_l = values.iter().copied().fold(f32::INFINITY, f32::min);
                    let v_r = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
                    let range = v_r - v_l;
                    let delta = if range > f32::EPSILON {
                        range / 15.0
                    } else {
                        1.0
                    };
                    let inv_delta = 1.0 / delta;

                    let mut flat_planes = vec![0u8; 4 * bytes];
                    let mut sum_q_u = 0u32;
                    for (byte_idx, chunk) in values.chunks_exact(8).enumerate() {
                        let (mut b0, mut b1, mut b2, mut b3) = (0u8, 0u8, 0u8, 0u8);
                        for (bit, &v) in chunk.iter().enumerate() {
                            let qu = (((v - v_l) * inv_delta).round() as u32).min(15);
                            sum_q_u += qu;
                            b0 |= (((qu >> 0) & 1) as u8) << bit;
                            b1 |= (((qu >> 1) & 1) as u8) << bit;
                            b2 |= (((qu >> 2) & 1) as u8) << bit;
                            b3 |= (((qu >> 3) & 1) as u8) << bit;
                        }
                        flat_planes[0 * bytes + byte_idx] = b0;
                        flat_planes[1 * bytes + byte_idx] = b1;
                        flat_planes[2 * bytes + byte_idx] = b2;
                        flat_planes[3 * bytes + byte_idx] = b3;
                    }
                    black_box((flat_planes, sum_q_u));
                });
            },
        );

        // ── Full-function reference (for comparison against sum of primitives)

        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();

        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        desc!(
            format!("quant-1bit/full/{dim}"),
            "Code::<1>::quantize end-to-end  [compare vs sum of primitives]"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit/full", dim), &dim, |b, _| {
            b.iter(|| {
                black_box(Code::<1>::quantize(&values, &centroid));
            });
        });

        let r_q = &values2;
        let cn = c_norm(&centroid);
        let cdq = c_dot_q(&centroid, r_q);
        let qn = q_norm(&centroid, r_q);
        let padded_bytes = Code::<1>::packed_len(dim);

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-query/full/{dim}"),
            "QuantizedQuery::new alone, single vector, hot cache. \
             Lower latency than quantize/quant-query (which includes residual alloc + \
             c_dot_q + q_norm + cache-cold batch effects)."
        );
        group.bench_with_input(BenchmarkId::new("quant-query/full", dim), &dim, |b, _| {
            b.iter(|| {
                black_box(QuantizedQuery::new(r_q, padded_bytes, cn, cdq, qn));
            });
        });
    }

    group.finish();
}

// ── Entry point ───────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_quantize,
    bench_quantize_scan,
    bench_distance,
    bench_distance_scan,
    bench_thread_scaling,
    bench_primitives,
);
criterion_main!(benches);
