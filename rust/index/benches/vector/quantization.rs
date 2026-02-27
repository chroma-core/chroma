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
//!   cargo bench -p chroma-index --bench quantization_performance
//!   cargo bench -p chroma-index --bench quantization_performance -- dq-
//!   cargo bench -p chroma-index --bench quantization_performance -- dq-bw
//!
//! For native CPU (POPCNT, AVX2, etc.):
//!   RUSTFLAGS="-C target-cpu=native" cargo bench -p chroma-index --bench quantization_performance

use std::hint::black_box;

use chroma_distance::DistanceFunction;

/// Lookup table for signed_dot sign expansion (benchmark variant only).
const fn sign_table_entry(b: u8) -> [f32; 8] {
    let bb = b as u32;
    [
        f32::from_bits(0x3F800000 | (((bb >> 0) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((bb >> 1) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((bb >> 2) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((bb >> 3) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((bb >> 4) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((bb >> 5) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((bb >> 6) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((bb >> 7) & 1) ^ 1) << 31),
    ]
}

static SIGN_LUT: [[f32; 8]; 256] = {
    let mut t = [[0.0f32; 8]; 256];
    let mut i = 0u8;
    while i < 255 {
        t[i as usize] = sign_table_entry(i);
        i += 1;
    }
    t[255] = sign_table_entry(255);
    t
};

use chroma_index::quantization::{BatchQueryLuts, Code1Bit, Code4Bit, QuantizedQuery};
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
            format!("{pairs} pairs; simsimd hamming (NEON CNT / AVX-512 VPOPCNTDQ)")
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
                    black_box(code.distance_4bit_query(&df, &qq));
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
                        black_box(code.distance_4bit_query(&df, &qq))
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
                            black_box(code.distance_4bit_query(&df, &qq));
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
        group.bench_with_input(
            BenchmarkId::new("quant-1bit/sign_pack", dim),
            &dim,
            |b, _| {
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

        // ── Code1Bit::distance_query primitives ──────────────────────────────

        group.throughput(Throughput::Bytes((bytes + dim * 4) as u64));
        desc!(
            format!("dq-float/signed_dot/{dim}"),
            "SIGN_TABLE lookup + simsimd dot  [distance_query: THE hot kernel]"
        );
        group.bench_with_input(
            BenchmarkId::new("dq-float/signed_dot", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    let mut signs = [0.0f32; 64];
                    let mut sum = 0.0f32;
                    for (pc, vc) in packed_a.chunks(8).zip(values.chunks(64)) {
                        let n = vc.len();
                        for (i, &byte) in pc.iter().enumerate() {
                            signs[i * 8..(i + 1) * 8].copy_from_slice(&SIGN_LUT[byte as usize]);
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

        // ── Code1Bit::distance_query_bitwise primitives ──────────────────────

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
            "[P2+P4] two-pass min/max + fused quantize+sum+scatter  [expected production]"
        );
        group.bench_with_input(
            BenchmarkId::new("quant-query/full/two_pass_fused", dim),
            &dim,
            |b, _| {
                b.iter(|| {
                    // Two-pass min/max — vectorizes independently
                    let v_l = values.iter().copied().fold(f32::INFINITY, f32::min);
                    let v_r = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
                    let range = v_r - v_l;
                    let delta = if range > f32::EPSILON {
                        range / 15.0
                    } else {
                        1.0
                    };
                    let inv_delta = 1.0 / delta;

                    // P2+P4: flat planes, fuse quantize + sum + byte_chunks scatter
                    let mut flat_planes = vec![0u8; 4 * bytes];
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
                black_box(code.distance_query(&DistanceFunction::Euclidean, r_q, cn, cdq, qn));
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
