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
//!     cargo bench ... -- q-1bit         # 1-bit encode: quantize, thread, primitives
//!     cargo bench ... -- q-query         # QuantizedQuery::new: quantize, primitives
//!
//!   Tag        Function                              Prefix
//!   ─────────  ────────────────────────────────────   ──────
//!   q-1bit     Code1Bit::quantize                     q-
//!   q-4bit     Code4Bit::quantize                     q-
//!   q-query    QuantizedQuery::new                    q-
//!   q-lut      BatchQueryLuts::new                    q-
//!   dq-float   Code1Bit::distance_query (signed_dot)  dq-
//!   dq-bw      Code1Bit::distance_query_bitwise       dq-
//!   dq-lut     BatchQueryLuts::distance_query         dq-
//!   dq-4f      Code4Bit::distance_query               dq-
//!   dc-1bit    Code1Bit::distance_code                dc-
//!   dc-4bit    Code4Bit::distance_code                dc-
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
            format!("q-4bit/{dim}"),
            format!("{BATCH} embeddings → 4-bit ray-walk codes")
        );
        group.bench_with_input(BenchmarkId::new("q-4bit", dim), &dim, |b, _| {
            b.iter(|| {
                for emb in &embeddings {
                    black_box(Code4Bit::quantize(emb, &centroid));
                }
            });
        });

        desc!(
            format!("q-1bit/{dim}"),
            format!("{BATCH} embeddings → sign-bit codes")
        );
        group.bench_with_input(BenchmarkId::new("q-1bit", dim), &dim, |b, _| {
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
            format!("q-query/{dim}"),
            format!("{BATCH} queries → QuantizedQuery (4-bit planes, §3.3.1)")
        );
        group.bench_with_input(BenchmarkId::new("q-query", dim), &dim, |b, _| {
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
            format!("q-lut/{dim}"),
            format!("{BATCH} queries → BatchQueryLuts (nibble LUTs, §3.3.2)")
        );
        group.bench_with_input(BenchmarkId::new("q-lut", dim), &dim, |b, _| {
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
                    black_box(code.distance_query(&df, r_q, cn, cdq, qn));
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
                    black_box(code.distance_query_bitwise(&df, &qq));
                }
            });
        });

        group.throughput(Throughput::Bytes(throughput_1bit));
        desc!(
            format!("dq-lut/{dim}"),
            format!("cold {BATCH} queries; BatchQueryLuts build + nibble lookup (§3.3.2)")
        );
        group.bench_with_input(BenchmarkId::new("dq-lut", dim), &dim, |b, _| {
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
                        black_box(code.distance_query(&df, &r_q, cn, cdq, qn))
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
                        black_box(code.distance_query_bitwise(&df, &qq))
                    })
                    .sum();
            });
        });

        group.throughput(Throughput::Bytes(tput_1bit));
        desc!(
            "dq-lut/scan",
            format!("hot {SCAN_N} codes @ dim={SCAN_DIM}; BatchQueryLuts built once, nibble lookup (§3.3.2)")
        );
        group.bench_function("dq-lut/scan", |b| {
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
            format!("q-4bit/{threads}t"),
            format!("{N} embeddings → 4-bit, {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("q-4bit", threads), &threads, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    embeddings.par_iter().for_each(|emb| {
                        black_box(Code4Bit::quantize(emb, &centroid));
                    });
                });
            });
        });

        desc!(
            format!("q-1bit/{threads}t"),
            format!("{N} embeddings → 1-bit, {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("q-1bit", threads), &threads, |b, _| {
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
                            let code = Code1Bit::new(code_bytes.as_slice());
                            let cdq = c_dot_q(&centroid, r_q);
                            let qn = q_norm(&centroid, r_q);
                            let qq = QuantizedQuery::new(r_q, 4, padded_bytes, cn, cdq, qn);
                            black_box(code.distance_query_bitwise(&df, &qq));
                        });
                });
            });
        });

        desc!(
            format!("dq-lut/{threads}t"),
            format!("{N} cold 1-bit queries (nibble LUT §3.3.2), {threads} thread(s)")
        );
        group.bench_with_input(BenchmarkId::new("dq-lut", threads), &threads, |b, _| {
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
    const N: usize = 2048; // codes per cluster
    const N_QUERIES: usize = 64; // queries to average over
    const N_BINS: usize = 20; // histogram bins
    const BAR_W: usize = 48; // max histogram bar width in chars

    let mut rng = make_rng();
    let centroid = random_vec(&mut rng, DIM);
    let df = DistanceFunction::Euclidean;
    let cn = c_norm(&centroid);
    let padded_bytes = Code1Bit::packed_len(DIM);

    // Generate embeddings and keep the originals to compute d_true.
    let embeddings: Vec<Vec<f32>> = (0..N).map(|_| random_vec(&mut rng, DIM)).collect();
    let codes_1: Vec<Vec<u8>> = embeddings
        .iter()
        .map(|emb| Code1Bit::quantize(emb, &centroid).as_ref().to_vec())
        .collect();
    let codes_4: Vec<Vec<u8>> = embeddings
        .iter()
        .map(|emb| Code4Bit::quantize(emb, &centroid).as_ref().to_vec())
        .collect();

    let total = N * N_QUERIES;
    let mut err_4bit = Vec::with_capacity(total);
    let mut err_1float = Vec::with_capacity(total);
    let mut err_1bitw = Vec::with_capacity(total);
    let mut err_1lut = Vec::with_capacity(total);
    // distance_code: both the data vector and the query are quantized codes.
    // This stacks the error from quantizing both sides, isolating the combined
    // code-vs-code estimation error vs. the one-sided code-vs-query methods.
    let mut err_code4 = Vec::with_capacity(total);
    let mut err_code1 = Vec::with_capacity(total);

    // Absolute errors collected in parallel; E[abs] ≈ 0 per the paper's
    // unbiasedness claim.  Comparing against the relative-error means above
    // shows that the non-zero relative mean is a metric artefact, not a bug.
    let mut abs_4bit = Vec::with_capacity(total);
    let mut abs_1float = Vec::with_capacity(total);
    let mut abs_1bitw = Vec::with_capacity(total);
    let mut abs_1lut = Vec::with_capacity(total);
    let mut abs_code4 = Vec::with_capacity(total);
    let mut abs_code1 = Vec::with_capacity(total);

    for _ in 0..N_QUERIES {
        let query = random_vec(&mut rng, DIM);
        let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
        let cdq = c_dot_q(&centroid, &r_q);
        let qn = q_norm(&centroid, &r_q);
        // QuantizedQuery and LUTs are built once per query, amortized over all N codes.
        let qq = QuantizedQuery::new(&r_q, 4, padded_bytes, cn, cdq, qn);
        let luts = BatchQueryLuts::new(&r_q, cn, cdq, qn);
        // Quantize the query itself so distance_code can treat it as another data code.
        let cq1_bytes = Code1Bit::quantize(&query, &centroid).as_ref().to_vec();
        let cq4_bytes = Code4Bit::quantize(&query, &centroid).as_ref().to_vec();
        let cq1 = Code1Bit::new(cq1_bytes.as_slice());
        let cq4 = Code4Bit::new(cq4_bytes.as_slice());

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

            let c1 = Code1Bit::new(codes_1[i].as_slice());
            let c4 = Code4Bit::new(codes_4[i].as_slice());

            let d4 = c4.distance_query(&df, &r_q, cn, cdq, qn);
            let df1 = c1.distance_query(&df, &r_q, cn, cdq, qn);
            let db = c1.distance_query_bitwise(&df, &qq);
            let dl = luts.distance_query(&c1, &df);
            // distance_code: both vectors quantized; error comes from both sides.
            let dc4 = c4.distance_code(&df, &cq4, cn, DIM);
            let dc1 = c1.distance_code(&df, &cq1, cn, DIM);

            // Relative error: positive = overestimate, negative = underestimate.
            err_4bit.push((d4 - d_true) / d_true);
            err_1float.push((df1 - d_true) / d_true);
            err_1bitw.push((db - d_true) / d_true);
            err_1lut.push((dl - d_true) / d_true);
            err_code4.push((dc4 - d_true) / d_true);
            err_code1.push((dc1 - d_true) / d_true);

            abs_4bit.push(d4 - d_true);
            abs_1float.push(df1 - d_true);
            abs_1bitw.push(db - d_true);
            abs_1lut.push(dl - d_true);
            abs_code4.push(dc4 - d_true);
            abs_code1.push(dc1 - d_true);
        }
    }

    // ── Descriptive statistics ────────────────────────────────────────────────
    struct Stats {
        mean: f32,
        std: f32,
        rmse: f32,
        p5: f32,
        p25: f32,
        p50: f32,
        p75: f32,
        p95: f32,
    }

    let compute_stats = |v: &mut Vec<f32>| -> Stats {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = v.len() as f32;
        let mean = v.iter().sum::<f32>() / n;
        let var = v.iter().map(|x| (x - mean) * (x - mean)).sum::<f32>() / n;
        let rmse = (v.iter().map(|x| x * x).sum::<f32>() / n).sqrt();
        let pct = |p: f32| v[((p * n) as usize).min(v.len() - 1)];
        Stats {
            mean,
            std: var.sqrt(),
            rmse,
            p5: pct(0.05),
            p25: pct(0.25),
            p50: pct(0.50),
            p75: pct(0.75),
            p95: pct(0.95),
        }
    };

    let s4 = compute_stats(&mut err_4bit);
    let sf = compute_stats(&mut err_1float);
    let sb = compute_stats(&mut err_1bitw);
    let sl = compute_stats(&mut err_1lut);
    let sc4 = compute_stats(&mut err_code4);
    let sc1 = compute_stats(&mut err_code1);

    let hr = "═".repeat(92);
    let sep = "─".repeat(92);

    // Per-method descriptions printed in the header for quick reference.
    let methods_desc: &[(&str, &str)] = &[
        (
            "4bit_float",
            "distance_query, 4-bit data code, raw f32 query (most accurate)",
        ),
        (
            "1bit_float",
            "distance_query, 1-bit data code, raw f32 query",
        ),
        (
            "1bit_bitwise",
            "distance_query_bitwise, 1-bit data + 4-bit quantized query (QuantizedQuery)",
        ),
        (
            "1bit_lut",
            "BatchQueryLuts::distance_query, 1-bit data + nibble LUT query",
        ),
        (
            "4bit_code",
            "distance_code, 4-bit data code vs 4-bit query code (both sides quantized)",
        ),
        (
            "1bit_code",
            "distance_code, 1-bit data code vs 1-bit query code (both sides quantized)",
        ),
    ];

    println!("\n{hr}");
    println!("  Error analysis: relative_error = (d_est − d_true) / d_true");
    println!(
        "  dim={DIM}, N={N} codes, {N_QUERIES} queries, {} samples/method",
        N * N_QUERIES
    );
    println!("  d_true = true squared L2 between original embedding and query");
    println!("{sep}");
    println!("  Methods:");
    for (name, desc) in methods_desc {
        println!("    {:<20} {}", name, desc);
    }
    println!("{sep}");
    println!(
        "  {:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "method", "mean", "std", "RMSE", "p5", "p25", "p50", "p75", "p95"
    );
    println!("{sep}");

    let row = |name: &str, s: &Stats| {
        println!(
            "  {:<20} {:>+8.3} {:>8.3} {:>8.3} {:>+8.3} {:>+8.3} {:>+8.3} {:>+8.3} {:>+8.3}",
            name, s.mean, s.std, s.rmse, s.p5, s.p25, s.p50, s.p75, s.p95
        );
    };
    row("4bit_float", &s4);
    row("1bit_float", &sf);
    row("1bit_bitwise", &sb);
    row("1bit_lut", &sl);
    println!("{sep}");
    row("4bit_code", &sc4);
    row("1bit_code", &sc1);

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
        let std = (v.iter().map(|x| (x - mean) * (x - mean)).sum::<f32>() / n).sqrt();
        (mean, std)
    };

    let abs_row = |name: &str, v: &[f32]| {
        let (mean, std) = abs_summary(v);
        println!("  {:<20} {:>+12.4} {:>12.4}", name, mean, std);
    };
    abs_row("4bit_float", &abs_4bit);
    abs_row("1bit_float", &abs_1float);
    abs_row("1bit_bitwise", &abs_1bitw);
    abs_row("1bit_lut", &abs_1lut);
    abs_row("4bit_code", &abs_code4);
    abs_row("1bit_code", &abs_code1);

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
    println!(
        "  Histograms  (±{:.0}% range, {N_BINS} bins, bars scaled to global max)",
        range * 100.0
    );
    println!("  Range auto-detected from p99 of |relative_error| across all methods.");
    println!(
        "  Values outside ±{:.0}% are counted in the ≤ / + edge bins.",
        range * 100.0
    );
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

    let h4 = make_hist(&err_4bit);
    let hf = make_hist(&err_1float);
    let hb = make_hist(&err_1bitw);
    let hl = make_hist(&err_1lut);
    let hc4 = make_hist(&err_code4);
    let hc1 = make_hist(&err_code1);

    let global_max = h4
        .iter()
        .chain(&hf)
        .chain(&hb)
        .chain(&hl)
        .chain(&hc4)
        .chain(&hc1)
        .copied()
        .max()
        .unwrap_or(1);

    let bar = |count: usize| -> String {
        // Use eighth-block characters for sub-character precision.
        let eighths = count * BAR_W * 8 / global_max;
        let full = eighths / 8;
        let frac = eighths % 8;
        let frac_ch = [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉'][frac];
        format!(
            "{}{}",
            "█".repeat(full),
            if frac > 0 {
                frac_ch.to_string()
            } else {
                String::new()
            }
        )
    };

    let methods: &[(&str, &[usize])] = &[
        ("4bit_float", &h4),
        ("1bit_float", &hf),
        ("1bit_bitwise", &hb),
        ("1bit_lut", &hl),
        ("4bit_code", &hc4),
        ("1bit_code", &hc1),
    ];

    for (name, hist) in methods {
        println!("\n  {name}:");
        for (i, &count) in hist.iter().enumerate() {
            let lo = -range + i as f32 * bin_w;
            let hi = lo + bin_w;
            let lo_mark = if i == 0 { "≤" } else { " " };
            let hi_mark = if i == N_BINS - 1 { "+" } else { " " };
            println!(
                "  {lo_mark}[{lo:+.3},{hi:+.3}){hi_mark} {:7} | {}",
                count,
                bar(count)
            );
        }
    }

    println!("\n{hr}\n");
}

fn bench_error_analysis(c: &mut Criterion) {
    let _ = c;
    print_error_analysis();
}

// ── 7. Primitive kernel benchmarks ─────────────────────────────────────────────
//
// Each primitive benchmark ID contains the parent function's tag (see header),
// so filtering by tag pulls in both full-function and primitive benchmarks:
//   cargo bench ... -- q-1bit           # full q-1bit + all its primitives
//   cargo bench ... -- dq-float         # full dq-float + signed_dot, sign_expand
//   cargo bench ... -- q-query           # full q-query + min_max, quantize_elements, ...
//   cargo bench ... -- primitives       # ALL primitives only (no full-function groups)
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
            format!("q-1bit/vec_sub/{dim}"),
            "r = emb − centroid  [quantize step 1]"
        );
        group.bench_with_input(BenchmarkId::new("q-1bit/vec_sub", dim), &dim, |b, _| {
            b.iter(|| {
                let r: Vec<f32> = values.iter().zip(&values2).map(|(e, c)| e - c).collect();
                black_box(r);
            });
        });

        group.throughput(Throughput::Bytes(2 * dim as u64 * 4));
        desc!(
            format!("q-1bit/simsimd_dot/{dim}"),
            "⟨a, b⟩ via simsimd  [quantize: norm², radial]"
        );
        group.bench_with_input(BenchmarkId::new("q-1bit/simsimd_dot", dim), &dim, |b, _| {
            b.iter(|| {
                black_box(f32::dot(&values, &values2).unwrap_or(0.0) as f32);
            });
        });

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("q-1bit/abs_sum/{dim}"),
            "Σ|r[i]|, auto-vectorizes to VABSPS+VADDPS  [quantize: correction]"
        );
        group.bench_with_input(BenchmarkId::new("q-1bit/abs_sum", dim), &dim, |b, _| {
            b.iter(|| {
                let s: f32 = values.iter().map(|v| v.abs()).sum();
                black_box(s);
            });
        });

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("q-1bit/sign_pack/{dim}"),
            "IEEE sign-bit extract + byte pack, 8 f32 → 1 byte  [quantize: packed codes]"
        );
        group.bench_with_input(BenchmarkId::new("q-1bit/sign_pack", dim), &dim, |b, _| {
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
            format!("q-1bit/popcount/{dim}"),
            "u64 popcount over packed bytes  [quantize: signed_sum]"
        );
        group.bench_with_input(BenchmarkId::new("q-1bit/popcount", dim), &dim, |b, _| {
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
            format!("q-query/min_max/{dim}"),
            "min + max scan over f32 slice  [QuantizedQuery::new step 1]"
        );
        group.bench_with_input(BenchmarkId::new("q-query/min_max", dim), &dim, |b, _| {
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
            format!("q-query/quantize_elements/{dim}"),
            "round((v-v_l)/delta), clamp to 0..15  [QuantizedQuery::new step 2]"
        );
        group.bench_with_input(
            BenchmarkId::new("q-query/quantize_elements", dim),
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
            format!("q-query/bit_plane_decompose/{dim}"),
            "scatter q_u into 4 bit planes  [QuantizedQuery::new step 3]"
        );
        group.bench_with_input(
            BenchmarkId::new("q-query/bit_plane_decompose", dim),
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
            format!("q-1bit/full/{dim}"),
            "Code1Bit::quantize end-to-end  [compare vs sum of primitives]"
        );
        group.bench_with_input(BenchmarkId::new("q-1bit/full", dim), &dim, |b, _| {
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
                black_box(code.distance_query(&DistanceFunction::Euclidean, r_q, cn, cdq, qn));
            });
        });

        let padded_bytes = Code1Bit::packed_len(dim);

        group.throughput(Throughput::Bytes(dim as u64 * 4));
        desc!(
            format!("q-query/full/{dim}"),
            "QuantizedQuery::new end-to-end  [compare vs sum of primitives]"
        );
        group.bench_with_input(BenchmarkId::new("q-query/full", dim), &dim, |b, _| {
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
    bench_error_analysis,
    bench_primitives,
);
criterion_main!(benches);
