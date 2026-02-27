
# Implemented Optimizations

## `Code1Bit::quantize`: single fused pass

Replaced the four-pass approach with a single fused pass over the input data
and the centroid. This eliminates the 4 KB intermediate `r` allocation and three
re-reads of it. The four scalar accumulators (abs_sum, norm_sq, radial, popcount)
are independent across iterations and auto-vectorize to VABSPS/VFMADD on AVX2/AVX-512.

Current cost breakdown (dim=1024):
  1. fused pass: emb−centroid, sign_pack, abs_sum, norm², radial, popcount
                 (read 8 KB, write 128 B — no intermediate r allocation)
  2. alloc:      Vec::with_capacity for the output code buffer (144 B)

Prior cost (before Q1): vec_sub alloc (4 KB), simsimd dot ×2 (8 KB reads),
sign_pack (4 KB read), abs_sum (4 KB read), popcount (128 B read) — ~5 passes.

Measurements on Apple M-series (`-C target-cpu=native`):

| Benchmark | Before | After | Delta |
|---|---|---|---|
| `primitives/quant-1bit/full/1024` | ~1,934 ns, ~3.95 GiB/s | 988 ns, 7.72 GiB/s | **−49% / +96%** |

---

## `Code1Bit::distance_code`: simsimd hamming distance

Replaced the scalar `u64` XOR + POPCNT loop in `hamming_distance` with
`simsimd::BinarySimilarity::hamming`, which dispatches at runtime to:

- **x86_64 (r6id production):** AVX-512 VPOPCNTDQ — 512 bits/cycle
- **ARM (Graviton / Apple Silicon):** NEON CNT — 128 bits/cycle

Measurements on Apple M-series (`-C target-cpu=native`):

| Benchmark | Before | After | Delta |
|---|---|---|---|
| `primitives/dc-1bit/hamming/1024` (single pair) | 10.60 ns | 6.69 ns | **−37% / +58%** |
| `distance_code/dc-1bit/1024` (256 pairs) | 3.66 µs | 2.58 µs | **−30% / +42%** |

On x86 production the gain will be larger: VPOPCNTDQ packs 8 u64 pops into
one instruction vs our prior scalar loop (16 iterations of 3 instructions each).

A scalar fallback is retained for `None` returns (unsupported targets / tests).

---

## `Code1Bit::distance_query`: interleaved + `chunks_exact` for AND+popcount

1. Read each x_b word once and AND with all 4 bit planes simultaneously,
accumulating into separate per-plane counters. Avoids 3 redundant re-reads of
x_b (128 B × 3 = 384 B at dim=1024; grows linearly with dim).

2. Replaced `step_by(8)+slice[i..i+8]` with `chunks_exact(8)`. Exposes
the iteration stride to LLVM cleanly, enabling auto-vectorization of the inner
loop. The speedup (2.4× from 1. alone) was far larger than expected from just
eliminating a bounds check — LLVM was not vectorizing the step_by version at all.

The production code unrolls the 4-plane loop entirely

Measurements on Apple M-series (`-C target-cpu=native`):

| Benchmark | Baseline | B2 only | B1+B2 |
|---|---|---|---|
| `primitives/dq-bw/and_popcount/1024` | 44.4 ns | 18.8 ns (**2.4×**) | 16.7 ns (**2.7×**) |
| `distance_query/dq-bw/scan` (2048 codes) | 98.0 µs | 43.9 µs (**+126%**) | 41.0 µs (**+139%**) |

Cold query throughput (`dq-bw/1024`, 512 queries) was unchanged — dominated by
`QuantizedQuery::new` build cost and cache-miss latency on fresh queries.

## `QuantizedQuery::new`: flat allocation + fused scatter

### Background

`QuantizedQuery::new` had five passes over the input data and four separate
heap allocations, making it the dominant cost in cold-query workloads.

```
Prior breakdown (dim=1024):
  min_max scan ×2:    163 ns each (8 KB total reads)
  quantize_elements:  165 ns + Vec<u32> alloc (4 KB)
  sum_q_u:            separate fold pass
  bit_plane_decompose: 2 339 ns + 4 × Vec<u8> alloc (4 × 128 B)
  Total: ~5 500 ns
```

### Flat allocation + fused scatter (IMPLEMENTED)

Replaced the branchy element-by-element scatter into 4 separate `Vec<u8>`
allocations with a byte_chunks approach:

- One flat `Vec<u8>` of `b_q × padded_bytes` (1 alloc instead of b_q)
- Quantize, accumulate sum, and scatter bits in a **single pass** over r_q
- Process 8 elements → 1 byte per plane per iteration (branchless)
- b_q=4 fast path: hardcoded plane accumulators enable LLVM to fully unroll

`QuantizedQuery::bit_planes` changed from `Vec<Vec<u8>>` to flat `Vec<u8>`,
with a new `padded_bytes: usize` field for indexing. `distance_query` updated
to slice the flat buffer: plane j at `&bit_planes[j*pb..(j+1)*pb]`.

Measurements on Apple M-series:

| Benchmark | Time | vs. Baseline |
|---|---|---|
| `bit_plane_decompose/baseline` | 2 339 ns | — |
| `bit_plane_decompose/flat_alloc` (P4 alone) | 2 351 ns | no change |
| `bit_plane_decompose/byte_chunks` | 707 ns | **3.3×** |
| `quant-query/full/1024` (before) | 5 537 ns | — |
| `quant-query/full/1024` (after) | 1 034 ns | **5.35× / −81%** |

---

## `Code1Bit::distance_query_full_precision`: lookup table

The hot kernel for the float query path computes `Σ sign[i]·values[i]` where
sign[i] ∈ {−1, +1} from packed bits. Prior approach: expand bits to ±1.0 f32s
via 8 `f32::from_bits` calls per byte (IEEE bit trick), then simsimd dot.

**Implemented:** Precomputed lookup table `SIGN_TABLE[256][8]` mapping each byte
to its 8 f32 signs. One table lookup + `copy_from_slice` replaces the 8
`f32::from_bits` calls per byte. The 8 KB table stays in L1.

Measurements on Apple M-series:

| Benchmark | Before | After | Delta |
|---|---|---|---|
| `primitives/dq-float/signed_dot/1024` | ~316 ns | ~194 ns | **−39% / +63%** |

**Future options** (see benches/vector/quantization.rs § Code1Bit::distance_query_full_precision):
- [D1] Masked sum: `2·Σ(values where bit=1) − total_sum`; needs VMASKMOV/BSL.
- [D2] XOR sign-flip: XOR values with 0x80000000 where bit=0, then sum; no expansion.
