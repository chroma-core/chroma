//! Benchmarks for RaBitQ quantization performance characteristics.
//!
//! Groups:
//!   quantize         — encoding throughput (1-bit, 4-bit) and query setup
//!   distance_code    — code-vs-code distance (1-bit, 4-bit) vs dim
//!   distance_query   — code-vs-query distance, all implementations and dims:
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
//!   quant-1bit     Code1Bit::quantize                       q-
//!   quant-4bit     Code4Bit::quantize                       q-
//!   quant-query    QuantizedQuery::new                      q-
//!   quant-lut      BatchQueryLuts::new                      q-
//!   dq-float       Code1Bit::distance_query_full_precision  dq-
//!   dq-bw          Code1Bit::distance_query                 dq-
//!   d-lut          BatchQueryLuts::distance_query           dq-
//!   dq-4f          Code4Bit::distance_query                 dq-
//!   dc-1bit        Code1Bit::distance_code                  dc-
//!   dc-4bit        Code4Bit::distance_code                  dc-
//!
//! Run with:
//!   cargo bench -p chroma-index --bench quantization
//!   cargo bench -p chroma-index --bench quantization -- dq-
//!   cargo bench -p chroma-index --bench quantization -- dq-bw
//!
//! For native CPU (POPCNT, AVX2, etc.):
//!   RUSTFLAGS="-C target-cpu=native" cargo bench -p chroma-index --bench quantization

use std::hint::black_box;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{BatchQueryLuts, Code1Bit, Code4Bit, QuantizedQuery};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::prelude::*;
use simsimd::SpatialSimilarity;

/// Criterion's filter: the first positional argument after `--`, if any.
/// Benchmarks whose ID doesn't contain the filter are skipped by Criterion,
/// so we skip the desc! print too.
fn bench_filter() -> Option<String> {
    // argv: <binary> [criterion-opts...] [filter]
    // The filter is the last positional arg (no leading `-`).
    std::env::args()
        .skip(1)
        .find(|a| !a.starts_with('-'))
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
const THREAD_COUNTS: &[usize] = &[1, 8];
// const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

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
            Code1Bit::quantize(&random_vec(&mut rng, dim), &centroid)
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
            Code4Bit::quantize(&random_vec(&mut rng, dim), &centroid)
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
        let padded_bytes = Code1Bit::packed_len(dim);
        let cn = c_norm(&centroid);

        group.throughput(Throughput::Bytes((BATCH * dim * 4) as u64));

        desc!(
            format!("quant-4bit/{dim}"),
            format!("{BATCH} embeddings → 4-bit ray-walk codes")
        );
        group.bench_with_input(BenchmarkId::new("quant-4bit", dim), &dim, |b, _| {
            b.iter(|| {
                for emb in &embeddings {
                    black_box(Code4Bit::quantize(emb, &centroid));
                }
            });
        });

        desc!(
            format!("quant-1bit/{dim}"),
            format!("{BATCH} embeddings → sign-bit codes")
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit", dim), &dim, |b, _| {
            b.iter(|| {
                for emb in &embeddings {
                    black_box(Code1Bit::quantize(emb, &centroid));
                }
            });
        });

        // QuantizedQuery::new — builds 4 bit-planes from a quantized query residual.
        // This is the per-query setup cost for the bitwise distance path (§3.3.1).
        // Throughput accounts for the f32 query vector read (same denominator as Code::quantize).
        desc!(
            format!("quant-query/{dim}"),
            format!("{BATCH} queries → QuantizedQuery (4-bit planes, §3.3.1)")
        );
        group.bench_with_input(BenchmarkId::new("quant-query", dim), &dim, |b, _| {
            b.iter(|| {
                for query in &embeddings {
                    let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
                    let cdq = c_dot_q(&centroid, &r_q);
                    let qn = q_norm(&centroid, &r_q);
                    black_box(QuantizedQuery::new(&r_q, 4, padded_bytes, cn, cdq, qn));
                }
            });
        });

        // BatchQueryLuts::new — precomputes D/4 nibble LUTs for the LUT distance path (§3.3.2).
        // Compared against QuantizedQuery::new to isolate which setup is cheaper.
        desc!(
            format!("quant-lut/{dim}"),
            format!("{BATCH} queries → BatchQueryLuts (nibble LUTs, §3.3.2)")
        );
        group.bench_with_input(BenchmarkId::new("quant-lut", dim), &dim, |b, _| {
            b.iter(|| {
                for query in &embeddings {
                    let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
                    let cdq = c_dot_q(&centroid, &r_q);
                    let qn = q_norm(&centroid, &r_q);
                    black_box(BatchQueryLuts::new(&r_q, cn, cdq, qn));
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
        let (centroid, codes_1, _) = make_codes_1bit(dim, BATCH);
        let (_, codes_4, _) = make_codes_4bit(dim, BATCH);
        let cn = c_norm(&centroid);
        let code_bytes_1 = Code1Bit::size(dim);
        let code_bytes_4 = Code4Bit::size(dim);

        group.throughput(Throughput::Bytes(pairs * 2 * code_bytes_1 as u64));
        desc!(
            format!("dc-1bit/{dim}"),
            format!("{pairs} pairs; XOR + popcount")
        );
        group.bench_with_input(BenchmarkId::new("dc-1bit", dim), &dim, |b, _| {
            b.iter(|| {
                for i in (0..BATCH).step_by(2) {
                    let a = Code1Bit::new(codes_1[i].as_slice());
                    let bb = Code1Bit::new(codes_1[i + 1].as_slice());
                    black_box(a.distance_code(&df, &bb, cn, dim));
                }
            });
        });

        group.throughput(Throughput::Bytes(pairs * 2 * code_bytes_4 as u64));
        desc!(
            format!("dc-4bit/{dim}"),
            format!("{pairs} pairs; nibble unpack + dot")
        );
        group.bench_with_input(BenchmarkId::new("dc-4bit", dim), &dim, |b, _| {
            b.iter(|| {
                for i in (0..BATCH).step_by(2) {
                    let a = Code4Bit::new(codes_4[i].as_slice());
                    let bb = Code4Bit::new(codes_4[i + 1].as_slice());
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
        let (centroid, codes_1, queries_1) = make_codes_1bit(dim, BATCH);
        let (_, codes_4, queries_4) = make_codes_4bit(dim, BATCH);
        let cn = c_norm(&centroid);
        let code_bytes_1 = Code1Bit::size(dim);
        let code_bytes_4 = Code4Bit::size(dim);
        let query_bytes = dim * 4;
        let padded_bytes = Code1Bit::packed_len(dim);

        // All 1-bit variants use the same throughput so GiB/s is comparable.
        let throughput_1bit = BATCH as u64 * (code_bytes_1 + query_bytes) as u64;
        let throughput_4bit = BATCH as u64 * (code_bytes_4 + query_bytes) as u64;

        group.throughput(Throughput::Bytes(throughput_4bit));
        desc!(
            format!("dq-4f/{dim}"),
            format!("cold {BATCH} queries; grid unpack + f32 dot (reference quality ceiling)")
        );
        group.bench_with_input(BenchmarkId::new("dq-4f", dim), &dim, |b, _| {
            b.iter(|| {
                for i in 0..BATCH {
                    let code = Code4Bit::new(codes_4[i].as_slice());
                    let r_q = &queries_4[i];
                    let cdq = c_dot_q(&centroid, r_q);
                    let qn = q_norm(&centroid, r_q);
                    black_box(code.distance_query(&df, r_q, cn, cdq, qn));
                }
            });
        });
        group.throughput(Throughput::Bytes(throughput_1bit));
        desc!(
            format!("dq-float/{dim}"),
            format!("cold {BATCH} queries; signed_dot (bits→±1.0 f32, simsimd dot)")
        );
        group.bench_with_input(BenchmarkId::new("dq-float", dim), &dim, |b, _| {
            b.iter(|| {
                for i in 0..BATCH {
                    let code = Code1Bit::new(codes_1[i].as_slice());
                    let r_q = &queries_1[i];
                    let cdq = c_dot_q(&centroid, r_q);
                    let qn = q_norm(&centroid, r_q);
                    black_box(code.distance_query_full_precision(&df, r_q, cn, cdq, qn));
                }
            });
        });

        group.throughput(Throughput::Bytes(throughput_1bit));
        desc!(
            format!("dq-bw/{dim}"),
            format!("cold {BATCH} queries; QuantizedQuery build + AND+popcount (§3.3.1)")
        );
        group.bench_with_input(BenchmarkId::new("dq-bw", dim), &dim, |b, _| {
            b.iter(|| {
                for i in 0..BATCH {
                    let code = Code1Bit::new(codes_1[i].as_slice());
                    let r_q = &queries_1[i];
                    let cdq = c_dot_q(&centroid, r_q);
                    let qn = q_norm(&centroid, r_q);
                    let qq = QuantizedQuery::new(r_q, 4, padded_bytes, cn, cdq, qn);
                    black_box(code.distance_query(&df, &qq));
                }
            });
        });

        group.throughput(Throughput::Bytes(throughput_1bit));
        desc!(
            format!("d-lut/{dim}"),
            format!("cold {BATCH} queries; BatchQueryLuts build + nibble lookup (§3.3.2)")
        );
        group.bench_with_input(BenchmarkId::new("d-lut", dim), &dim, |b, _| {
            b.iter(|| {
                for i in 0..BATCH {
                    let code = Code1Bit::new(codes_1[i].as_slice());
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

        let (_, codes_1, _) = make_codes_1bit(SCAN_DIM, SCAN_N);
        let (_, codes_4, _) = make_codes_4bit(SCAN_DIM, SCAN_N);
        let padded_bytes = Code1Bit::packed_len(SCAN_DIM);

        // Throughput counts code bytes only; the query is amortized and stays in L1.
        let tput_1bit = SCAN_N as u64 * Code1Bit::size(SCAN_DIM) as u64;
        let tput_4bit = SCAN_N as u64 * Code4Bit::size(SCAN_DIM) as u64;

        group.throughput(Throughput::Bytes(tput_4bit));
        desc!(
            "dq-4f/scan",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; grid unpack + f32 dot (quality ceiling)")
        );
        group.bench_function("dq-4f/scan", |b| {
            b.iter(|| {
                let _: f32 = codes_4
                    .iter()
                    .map(|cb| {
                        let code = Code4Bit::new(cb.as_slice());
                        black_box(code.distance_query(&df, &r_q, cn, cdq, qn))
                    })
                    .sum();
            });
        });

        group.throughput(Throughput::Bytes(tput_1bit));
        desc!(
            "dq-float/scan",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; signed_dot; query in L1 (baseline)")
        );
        group.bench_function("dq-float/scan", |b| {
            b.iter(|| {
                let _: f32 = codes_1
                    .iter()
                    .map(|cb| {
                        let code = Code1Bit::new(cb.as_slice());
                        black_box(code.distance_query_full_precision(&df, &r_q, cn, cdq, qn))
                    })
                    .sum();
            });
        });

        group.throughput(Throughput::Bytes(tput_1bit));
        desc!(
            "dq-bw/scan",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; QuantizedQuery built once, AND+popcount (§3.3.1)")
        );
        group.bench_function("dq-bw/scan", |b| {
            let qq = QuantizedQuery::new(&r_q, 4, padded_bytes, cn, cdq, qn);
            b.iter(|| {
                let _: f32 = codes_1
                    .iter()
                    .map(|cb| {
                        let code = Code1Bit::new(cb.as_slice());
                        black_box(code.distance_query(&df, &qq))
                    })
                    .sum();
            });
        });

        group.throughput(Throughput::Bytes(tput_1bit));
        desc!(
            "d-lut/scan",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; BatchQueryLuts built once, nibble lookup (§3.3.2)")
        );
        group.bench_function("d-lut/scan", |b| {
            let luts = BatchQueryLuts::new(&r_q, cn, cdq, qn);
            b.iter(|| {
                let _: f32 = codes_1
                    .iter()
                    .map(|cb| {
                        let code = Code1Bit::new(cb.as_slice());
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

    let (_, codes_1, queries_1) = make_codes_1bit(DIM, N);
    let (_, codes_4, queries_4) = make_codes_4bit(DIM, N);
    let cn = c_norm(&centroid);
    let df = DistanceFunction::Euclidean;
    let padded_bytes = Code1Bit::packed_len(DIM);

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
                        black_box(Code4Bit::quantize(emb, &centroid));
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
                        black_box(Code1Bit::quantize(emb, &centroid));
                    });
                });
            });
        });

        group.throughput(Throughput::Bytes(
            (N * (Code4Bit::size(DIM) + DIM * 4)) as u64,
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
                            let code = Code4Bit::new(code_bytes.as_slice());
                            let cdq = c_dot_q(&centroid, r_q);
                            let qn = q_norm(&centroid, r_q);
                            black_box(code.distance_query(&df, r_q, cn, cdq, qn));
                        });
                });
            });
        });
        group.throughput(Throughput::Bytes(
            (N * (Code1Bit::size(DIM) + DIM * 4)) as u64,
        ));
        desc!(
            format!("dq-float/{threads}t"),
            format!("{N} cold 1-bit queries, {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("dq-float", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    codes_1
                        .par_iter()
                        .zip(queries_1.par_iter())
                        .for_each(|(code_bytes, r_q)| {
                            let code = Code1Bit::new(code_bytes.as_slice());
                            let cdq = c_dot_q(&centroid, r_q);
                            let qn = q_norm(&centroid, r_q);
                            black_box(code.distance_query_full_precision(&df, r_q, cn, cdq, qn));
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
                            let code = Code1Bit::new(code_bytes.as_slice());
                            let cdq = c_dot_q(&centroid, r_q);
                            let qn = q_norm(&centroid, r_q);
                            let qq = QuantizedQuery::new(r_q, 4, padded_bytes, cn, cdq, qn);
                            black_box(code.distance_query(&df, &qq));
                        });
                });
            });
        });

        desc!(
            format!("d-lut/{threads}t"),
            format!("{N} cold 1-bit queries (nibble LUT §3.3.2), {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("d-lut", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    codes_1
                        .par_iter()
                        .zip(queries_1.par_iter())
                        .for_each(|(code_bytes, r_q)| {
                            let code = Code1Bit::new(code_bytes.as_slice());
                            let cdq = c_dot_q(&centroid, r_q);
                            let qn = q_norm(&centroid, r_q);
                            let luts = BatchQueryLuts::new(r_q, cn, cdq, qn);
                            black_box(luts.distance_query(&code, &df));
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
//   cargo bench ... -- dq-float             # full dq-float + signed_dot, sign_expand
//   cargo bench ... -- quant-query          # full quant-query + min_max, quantize_elements, ...
//   cargo bench ... -- primitives           # ALL primitives only (no full-function groups)
//
// Isolate every major computational primitive used by Code1Bit::quantize,
// Code1Bit::distance_query, Code1Bit::distance_query_bitwise,
// Code1Bit::distance_code, and QuantizedQuery::new.
//
// Comparing a function's wall-clock time against the sum of its primitives
// reveals overhead (allocation, scalar math, cache effects) that the
// "full function" benchmarks in earlier groups cannot isolate.
//
// ── Performance improvement opportunities ────────────────────────────────────
//
// Target hardware:
//   Query:  r6id.8xlarge  (32 vCPU Ice Lake, AVX-512 + VPOPCNTDQ + VNNI)
//   Index:  r6id.32xlarge (128 vCPU Ice Lake)
//   Future: Graviton 3/4  (ARM Neoverse V1/V2, NEON + SVE)
//
// Code1Bit::quantize
// ──────────────────
//   Current cost breakdown (dim=1024):
//     1. vec_sub:   r = emb − centroid                    (read 8 KB, write 4 KB)
//     2. dot × 2:   norm² = ⟨r,r⟩, radial = ⟨r,c⟩       (read 4 KB each)
//     3. sign_pack: 8 f32 → 1 byte via IEEE bit extract   (read 4 KB, write 128 B)
//     4. abs_sum:   Σ|r[i]|                                (read 4 KB)
//     5. popcount:  over 128 packed bytes                  (read 128 B)
//     6. alloc:     Vec::with_capacity + extend_from_slice
//
//   Improvements:
//     [Q1] Fuse vec_sub + sign_pack + abs_sum into one pass. Today we read `r`
//          three times (sign_pack, abs_sum, dot) and write it once. A single-pass
//          kernel that computes residual, extracts sign, and accumulates |r[i]|
//          would cut memory traffic from ~20 KB to ~12 KB and eliminate the 4 KB
//          intermediate `r` allocation. The two simsimd dots (norm², radial) can
//          be piggybacked onto the same pass with dual accumulators; however
//          simsimd's SIMD dispatch is hard to beat, so benchmark before fusing.
//
//     [Q2] Avoid `r` heap allocation entirely. Currently `let r: Vec<f32> = ...`
//          allocates 4 KB. If the caller provides a scratch buffer or the output
//          buffer is pre-allocated, this can be eliminated. For batch quantize
//          paths (indexing), a thread-local arena (e.g. bumpalo) removes all
//          per-call malloc overhead.
//
//     [Q3] Write output directly. The final buffer assembly copies the header
//          (16 B) and packed bytes (128 B) into a fresh Vec. If quantize wrote
//          into a caller-provided `&mut [u8]` slice, the alloc + copy disappears.
//          Relevant for indexing where millions of codes are produced.
//
//     [Q4] ARM/Graviton: sign_pack's IEEE bit-trick (val.to_bits() >> 31) is
//          portable, but NEON offers CMLT (compare-less-than-zero) → bitwise
//          select which may be faster. abs_sum maps to FABS + FADD which is
//          well-supported. The main risk is `f32::dot` — ensure simsimd
//          dispatches to NEON FMLA or SVE FMA, not scalar.
//
// Code1Bit::distance_query
// ────────────────────────
//   The sole hot primitive is signed_dot (bits → ±1.0 expansion + simsimd dot).
//
//   Improvements:
//     [D1] Eliminate sign expansion entirely. Instead of building a ±1.0 f32
//          array and calling simsimd dot, operate directly on the packed bits
//          and the f32 values. For each u64 word of packed bits:
//            - Compute positive_sum = Σ values[i] where bit=1
//            - result = 2 * positive_sum - total_sum
//          where total_sum = Σ values[i] is precomputed once for the query.
//          This replaces 1024 f32::from_bits + 1024-element dot with 16 iterations
//          of masked sum. On AVX-512 with VMASKMOV, this is ~2 instructions per
//          64 values. On ARM, NEON BSL (bitwise select) serves the same role.
//
//     [D2] Alternative: use SIMD sign-flip via XOR. For each lane, XOR the f32
//          value with 0x80000000 when the corresponding bit is 0 (negate), then
//          sum all values. No expansion needed, no intermediate array. This is
//          the most promising single optimization for distance_query:
//            sign_mask = broadcast packed bit to each f32 lane's sign position
//            flipped = values XOR sign_mask
//            sum += horizontal_add(flipped)
//
//     [D3] Graviton: ARM NEON lacks a direct equivalent to AVX-512 mask ops,
//          but NEON BSL (Bit Select) + FNEG is a clean alternative. The signed_dot
//          IEEE bit trick (0x3F800000 | sign << 31) compiles to BFI + FMOV on ARM
//          which is reasonable but not optimal; [D2]'s XOR approach is better.
//
// Code1Bit::distance_code
// ───────────────────────
//   Primitive: hamming_distance (XOR + popcount on 128 bytes).
//
//   Improvements:
//     [C1] Use simsimd's hamming distance function (simsimd::BinarySimilarity::hamming)
//          which has AVX-512 VPOPCNTDQ and ARM NEON CNT backends. Our scalar
//          loop processes 64 bits/iteration; VPOPCNTDQ processes 512 bits.
//          For dim=1024 that's 2 iterations vs 16.
//
//     [C2] Graviton: ARM NEON has `vcnt` (byte-level popcount) but no u64
//          popcount instruction. The scalar `count_ones()` may compile to a
//          software implementation. simsimd's binary backend should handle this.
//
// Code1Bit::distance_query_bitwise
// ────────────────────────────────
//   Primitive: 4 rounds of AND+popcount over 128-byte strings.
//
//   Improvements:
//     [B1] Interleave bit planes: instead of 4 sequential passes over x_b,
//          process all 4 planes per u64 word. Reduces x_b cache reads from 4
//          to 1. At dim=1024, x_b is 128 bytes (fits L1 regardless), so this
//          matters more at higher dims or under cache pressure from concurrent
//          queries on the same core.
//
//     [B2] The inner loop uses `try_into().unwrap()` for u64 conversion. With
//          `unsafe { *(ptr as *const u64) }` or `u64::from_ne_bytes` with known
//          alignment, the bounds check + panic path is eliminated. Profile first;
//          LLVM often elides these.
//
//     [B3] Graviton: same `vcnt` concern as [C2]. Also, the `<< j` shift per
//          plane is a dependent chain; ARM's barrel shifter handles it in 1 cycle
//          but interleaving [B1] would help hide latency.
//
// QuantizedQuery::new
// ───────────────────
//   Current cost breakdown (dim=1024):
//     1. min_max:             2 passes over r_q (8 KB read)
//     2. quantize_elements:   1 pass, allocates Vec<u32> (4 KB)
//     3. sum_q_u:             1 pass (4 KB read)
//     4. bit_plane_decompose: 1 pass, allocates 4 × Vec<u8> (4 × 128 B)
//
//   Improvements:
//     [P1] Fuse min + max into a single pass. Trivial but currently separate
//          folds. Saves one 4 KB read.
//
//     [P2] Fuse quantize + sum + bit_plane_decompose into one pass. Quantize
//          each element, accumulate sum, and scatter bits immediately, eliminating
//          the intermediate Vec<u32> allocation. The scatter has poor spatial
//          locality (writing to 4 different arrays), but at 128 bytes each they
//          all fit in L1.
//
//     [P3] SIMD quantize: the per-element `round((v - v_l) / delta)` is a
//          classic SIMD-friendly operation (subtract, multiply, round, clamp).
//          On AVX-512: VSUBPS, VMULPS, VRNDSCALEPS, VMINPS — 4 instructions
//          for 16 elements. Currently scalar.
//
//     [P4] Flat bit_planes allocation: 4 separate Vec<u8> → one Vec<u8> of
//          4 × padded_bytes with index arithmetic. Eliminates 3 of 4 allocations.
//
//     [P5] Graviton: NEON has FRINTN (round-to-nearest) and FMIN/FMAX for
//          the quantization. The bit scatter is the same cost on both platforms.
//
// Cross-cutting
// ─────────────
//     [X1] Batch API for cluster scan: provide `distance_query_batch` that takes
//          a slice of code byte slices and returns distances. Enables software
//          prefetching of the next code while processing the current one, hiding
//          LLC latency (relevant at N > L1-capacity / code_size ≈ 200 codes).
//
//     [X2] Alignment: ensure packed byte arrays are 64-byte aligned for optimal
//          AVX-512 loads. Currently heap-allocated with default alignment (16B
//          on most allocators). `aligned_vec` crate or manual Layout allocation.
//
//     [X3] Thread-local scratch buffers: for the indexing path, a thread-local
//          arena (bumpalo) eliminates all per-quantize allocation overhead.
//          At 128 vCPUs on r6id.32xlarge, malloc contention can be significant.
//
//     [X4] Graviton migration checklist:
//          - Verify simsimd dispatches to NEON/SVE (not scalar fallback) for
//            f32::dot, hamming. Build with RUSTFLAGS="-C target-cpu=neoverse-v1".
//          - Benchmark count_ones() on Graviton; if slow, use NEON vcnt intrinsic.
//          - signed_dot [D2] XOR approach is better on ARM than the current
//            IEEE bit trick.
//          - r6id ICE LAKE has 2 × 512-bit FMA units; Graviton 3 has 4 × 128-bit
//            NEON units. Per-core throughput may be lower but Graviton has more
//            cores per dollar — measure end-to-end QPS, not single-core ns/op.

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

        // ── Code1Bit::quantize primitives ────────────────────────────────────

        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        desc!(
            format!("quant-1bit/vec_sub/{dim}"),
            "r = emb − centroid  [quantize step 1]"
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
            "⟨a, b⟩ via simsimd  [quantize: norm², radial]"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit/simsimd_dot", dim), &dim, |b, _| {
            b.iter(|| {
                black_box(f32::dot(&values, &values2).unwrap_or(0.0) as f32);
            });
        });

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-1bit/abs_sum/{dim}"),
            "Σ|r[i]|, auto-vectorizes to VABSPS+VADDPS  [quantize: correction]"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit/abs_sum", dim), &dim, |b, _| {
            b.iter(|| {
                let s: f32 = values.iter().map(|v| v.abs()).sum();
                black_box(s);
            });
        });

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-1bit/sign_pack/{dim}"),
            "IEEE sign-bit extract + byte pack, 8 f32 → 1 byte  [quantize: packed codes]"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit/sign_pack", dim), &dim, |b, _| {
            b.iter(|| {
                let mut packed = vec![0u8; bytes];
                for (byte_ref, chunk) in packed.iter_mut().zip(values.chunks(8)) {
                    let mut byte = 0u8;
                    for (j, &val) in chunk.iter().enumerate() {
                        let sign = (val.to_bits() >> 31) as u8;
                        byte |= (sign ^ 1) << j;
                    }
                    *byte_ref = byte;
                }
                black_box(packed);
            });
        });

        group.throughput(Throughput::Bytes(bytes as u64));
        desc!(
            format!("quant-1bit/popcount/{dim}"),
            "u64 popcount over packed bytes  [quantize: signed_sum]"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit/popcount", dim), &dim, |b, _| {
            b.iter(|| {
                let count: u32 = packed_a
                    .chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().unwrap()).count_ones())
                    .sum();
                black_box(count);
            });
        });

        // ── Code1Bit::distance_query primitives ──────────────────────────────

        group.throughput(Throughput::Bytes((bytes + dim * 4) as u64));
        desc!(
            format!("dq-float/signed_dot/{dim}"),
            "bits→±1.0 expand + simsimd dot  [distance_query: THE hot kernel]"
        );
        group.bench_with_input(
            BenchmarkId::new("dq-float/signed_dot", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    const CHUNK: usize = 8;
                    let mut signs = [0.0f32; CHUNK * 8];
                    let mut sum = 0.0f32;
                    for (pc, vc) in packed_a.chunks(CHUNK).zip(values.chunks(CHUNK * 8)) {
                        let n = vc.len();
                        for (i, &byte) in pc.iter().enumerate() {
                            let base = i * 8;
                            let bb = byte as u32;
                            signs[base] = f32::from_bits(0x3F800000 | (((bb >> 0) & 1) ^ 1) << 31);
                            signs[base + 1] =
                                f32::from_bits(0x3F800000 | (((bb >> 1) & 1) ^ 1) << 31);
                            signs[base + 2] =
                                f32::from_bits(0x3F800000 | (((bb >> 2) & 1) ^ 1) << 31);
                            signs[base + 3] =
                                f32::from_bits(0x3F800000 | (((bb >> 3) & 1) ^ 1) << 31);
                            signs[base + 4] =
                                f32::from_bits(0x3F800000 | (((bb >> 4) & 1) ^ 1) << 31);
                            signs[base + 5] =
                                f32::from_bits(0x3F800000 | (((bb >> 5) & 1) ^ 1) << 31);
                            signs[base + 6] =
                                f32::from_bits(0x3F800000 | (((bb >> 6) & 1) ^ 1) << 31);
                            signs[base + 7] =
                                f32::from_bits(0x3F800000 | (((bb >> 7) & 1) ^ 1) << 31);
                        }
                        sum += f32::dot(&signs[..n], vc).unwrap_or(0.0) as f32;
                    }
                    black_box(sum);
                });
            },
        );

        // signed_dot breakdown: just the bit-expansion part, no dot product.
        // Comparing this vs signed_dot shows how much time is expansion vs SIMD dot.
        group.throughput(Throughput::Bytes(bytes as u64));
        desc!(
            format!("dq-float/sign_expand/{dim}"),
            "bits→±1.0 expansion only (no dot)  [signed_dot step 1 of 2]"
        );
        group.bench_with_input(
            BenchmarkId::new("dq-float/sign_expand", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut signs = vec![0.0f32; dim];
                    for (i, &byte) in packed_a.iter().enumerate() {
                        let base = i * 8;
                        let bb = byte as u32;
                        if base + 7 < dim {
                            signs[base] = f32::from_bits(0x3F800000 | (((bb >> 0) & 1) ^ 1) << 31);
                            signs[base + 1] =
                                f32::from_bits(0x3F800000 | (((bb >> 1) & 1) ^ 1) << 31);
                            signs[base + 2] =
                                f32::from_bits(0x3F800000 | (((bb >> 2) & 1) ^ 1) << 31);
                            signs[base + 3] =
                                f32::from_bits(0x3F800000 | (((bb >> 3) & 1) ^ 1) << 31);
                            signs[base + 4] =
                                f32::from_bits(0x3F800000 | (((bb >> 4) & 1) ^ 1) << 31);
                            signs[base + 5] =
                                f32::from_bits(0x3F800000 | (((bb >> 5) & 1) ^ 1) << 31);
                            signs[base + 6] =
                                f32::from_bits(0x3F800000 | (((bb >> 6) & 1) ^ 1) << 31);
                            signs[base + 7] =
                                f32::from_bits(0x3F800000 | (((bb >> 7) & 1) ^ 1) << 31);
                        }
                    }
                    black_box(signs);
                });
            },
        );

        // ── Code1Bit::distance_code primitives ───────────────────────────────

        group.throughput(Throughput::Bytes(2 * bytes as u64));
        desc!(
            format!("dc-1bit/hamming/{dim}"),
            "raw XOR + popcount  [distance_code kernel]"
        );
        group.bench_with_input(BenchmarkId::new("dc-1bit/hamming", dim), &dim, |b, _| {
            b.iter(|| {
                let mut count = 0u32;
                for i in (0..packed_a.len()).step_by(8) {
                    let a_word = u64::from_le_bytes(packed_a[i..i + 8].try_into().unwrap());
                    let b_word = u64::from_le_bytes(packed_b[i..i + 8].try_into().unwrap());
                    count += (a_word ^ b_word).count_ones();
                }
                black_box(count);
            });
        });

        // ── Code1Bit::distance_query_bitwise primitives ──────────────────────

        group.throughput(Throughput::Bytes((bytes + 4 * bytes) as u64));
        desc!(
            format!("dq-bw/and_popcount/{dim}"),
            "4 rounds AND+popcount on D-bit strings  [distance_query_bitwise kernel]"
        );
        group.bench_with_input(BenchmarkId::new("dq-bw/and_popcount", dim), &dim, |b, _| {
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
        });

        // ── QuantizedQuery::new primitives ───────────────────────────────────

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-query/min_max/{dim}"),
            "min + max scan over f32 slice  [QuantizedQuery::new step 1]"
        );
        group.bench_with_input(BenchmarkId::new("quant-query/min_max", dim), &dim, |b, _| {
            b.iter(|| {
                let v_l = values.iter().copied().fold(f32::INFINITY, f32::min);
                let v_r = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
                black_box((v_l, v_r));
            });
        });

        let v_l = values.iter().copied().fold(f32::INFINITY, f32::min);
        let v_r = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let delta = (v_r - v_l) / 15.0;

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-query/quantize_elements/{dim}"),
            "round((v-v_l)/delta), clamp to 0..15  [QuantizedQuery::new step 2]"
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
            format!("quant-query/bit_plane_decompose/{dim}"),
            "scatter q_u into 4 bit planes  [QuantizedQuery::new step 3]"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/bit_plane_decompose", dim),
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

        // ── Full-function reference (for comparison against sum of primitives)

        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();

        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        desc!(
            format!("quant-1bit/full/{dim}"),
            "Code1Bit::quantize end-to-end  [compare vs sum of primitives]"
        );
        group.bench_with_input(BenchmarkId::new("quant-1bit/full", dim), &dim, |b, _| {
            b.iter(|| {
                black_box(Code1Bit::quantize(&values, &centroid));
            });
        });

        let code_owned = Code1Bit::quantize(&values, &centroid);
        let code = Code1Bit::new(code_owned.as_ref());
        let r_q = &values2;
        let cn = c_norm(&centroid);
        let cdq = c_dot_q(&centroid, r_q);
        let qn = q_norm(&centroid, r_q);

        group.throughput(Throughput::Bytes((Code1Bit::size(dim) + dim * 4) as u64));
        desc!(
            format!("dq-float/full/{dim}"),
            "Code1Bit::distance_query end-to-end  [compare vs signed_dot]"
        );
        group.bench_with_input(BenchmarkId::new("dq-float/full", dim), &dim, |b, _| {
            b.iter(|| {
                black_box(code.distance_query_full_precision(
                    &DistanceFunction::Euclidean,
                    r_q,
                    cn,
                    cdq,
                    qn,
                ));
            });
        });

        let padded_bytes = Code1Bit::packed_len(dim);

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("quant-query/full/{dim}"),
            "QuantizedQuery::new end-to-end  [compare vs sum of primitives]"
        );
        group.bench_with_input(BenchmarkId::new("quant-query/full", dim), &dim, |b, _| {
            b.iter(|| {
                black_box(QuantizedQuery::new(r_q, 4, padded_bytes, cn, cdq, qn));
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
    bench_primitives,
);
criterion_main!(benches);
