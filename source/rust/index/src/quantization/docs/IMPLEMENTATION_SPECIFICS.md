
# RaBitQ Implementation Specifics

Findings from reviewing the Chroma RaBitQ implementation against the original
paper.

Reference implementations examined:

- **gaoj0017/RaBitQ** — original author's C++/Python code
  (`data/rabitq.py`, `src/space.h`, `src/ivf_rabitq.h`)
---

## Distance-Estimation Formula

Our implementation uses the same formula as the original paper. Starting from Euclidean
distance in the original space:

```
‖d − q‖² = ‖c‖² + ‖r_d‖² + ‖r_q‖² + 2⟨r_d, c⟩ + 2⟨r_q, c⟩ − 2⟨d, q⟩
```

where `r_d = d − c` and `r_q = q − c` are the data and query residuals relative
to the cluster centroid `c`.  The inner product `⟨r_d, r_q⟩` is estimated via
the 1-bit approximation (Theorem 3.2 of the paper):

```
⟨r_d, r_q⟩ ≈ ‖r_d‖ · ⟨g_d, r_q⟩ / ⟨g_d, n_d⟩
```

- `g_d[i] = +0.5` when bit `i` is 1, `−0.5` when bit `i` is 0 (our scaling)
- `n_d = r_d / ‖r_d‖` is the unit-norm data residual
- `⟨g_d, n_d⟩` is the **correction factor**, stored in `CodeHeader::correction`

---

## Correction Factor Algebra

The implementations use different scaling conventions but are algebraically
equivalent.

**Our implementation** (`Code::quantize`, BITS=1):

```
correction = 0.5 · Σ|r[i]| / ‖r‖     (= GRID_OFFSET · abs_sum / norm)
```

**gaoj0017** uses normalised vectors and defines:

```
x0 = ‖XP‖₁ / (√D · ‖XP‖)
```

For unit-norm `XP` this simplifies to `‖XP‖₁ / √D`.  With `g[i] = ±1/√D`
(their scaling), their correction equals `Σ|r̂[i]| / √D = Σ|r[i]| / (√D · ‖r‖)`.
Our `GRID_OFFSET = 0.5 = 1/(2·1) ≠ 1/√D`, but the `0.5` factors in `⟨g, r_q⟩`
and `⟨g, n_d⟩` cancel in the ratio, so the estimated distance is identical.

---

## Random Orthogonal Rotation (P)

The paper applies a random orthogonal rotation `P` before quantization to obtain
its theoretical error guarantees (the `O(1/√D)` bound holds in expectation over
random `P`).

| Implementation | Rotation applied? |
|---|---|
| gaoj0017 | Yes — `XP` computed once at index time |
| **Our production code** (`quantized_spann.rs::rotate`) | **Yes** — `self.rotation` matrix applied before `Code::quantize` |

---

## Storage of ⟨r, c⟩ (Radial Component)

The term `⟨r, c⟩` is required at query time for every code.

| Implementation | How ⟨r, c⟩ is stored |
|---|---|
| gaoj0017 | Exact f32, precomputed at index time |
| **Ours** (`CodeHeader::radial`) | **Exact** f32, precomputed at index time |

Storing the exact value is a strict accuracy advantage over the NTU library,
which introduces additional quantization error in this term.

---

## Precomputed Signed Sum (`factor_ppc` / `signed_sum`)

The signed sum `Σ sign[i] = 2·popcount(x_b) − D` appears in the bitwise
distance estimator for any query that uses `QuantizedQuery` or `BatchQueryLuts`.
It depends only on the data code and is constant across all queries.

**gaoj0017** precomputes this as `factor_ppc` at index time.

In our implementation, `CodeHeader` stores the value as `signed_sum: i32`,
computed once in `Code::quantize`:

```rust
let popcount: i32 = packed.iter().map(|b| b.count_ones() as i32).sum();
let signed_sum = 2 * popcount - dim as i32;
```

---

## Query Quantization: Deterministic vs. Stochastic Dithering

`QuantizedQuery::new` uses deterministic rounding:

```rust
((v - v_l) / delta).round()
```

The original paper (gaoj0017) uses stochastic dithering (random rounding) for
query quantization.  At `B_q = 4` bits the accuracy difference is negligible;
deterministic rounding is simpler and removes a source of non-determinism.

---

## 4-Bit Codebook Structure

Our 4-bit implementation uses a ray-walk algorithm to find the optimal grid
point along `r` that maximises cosine similarity.

---

## Error Analysis

The `print_error_analysis` benchmark (`benches/quantization.rs`) measures
relative and absolute error of the distance estimator for 4-bit float,
1-bit float, and 1-bit bitwise (QuantizedQuery) methods.

### Why relative error has a non-zero mean

Relative error `ε_rel = (d̂ − d) / d` has a strictly positive mean even for an
unbiased estimator, due to **Jensen's inequality**: `E[1/X] > 1/E[X]` when `X`
is a positive random variable.  The distribution of `d̂` is approximately
symmetric around `d`, but `1/d` is convex, so dividing by `d` distorts the
symmetry upward.  This is a property of the metric, not a flaw in the
implementation.

---

## Summary of Differences

| Aspect | gaoj0017 (original paper) |  Ours |
|---|---|---|
| Random rotation | Yes | Yes (production); No (benchmarks) |
| `⟨r, c⟩` storage | Exact | Exact |
| `signed_sum` precomputed | Yes (`factor_ppc`) | Yes |
| Query dithering | Stochastic | Deterministic |
| 4-bit codebook | N/A | Ray-walk |
| Multi-bit query scoring | Bit-plane (same) | Bit-plane (same) |


# Optimizations

## `Code::<1>::quantize`: fused pass with dual accumulators

Replaced the original five-pass approach with a single fused pass over
`(embedding, centroid)`.  No intermediate `r` vector is allocated (saves 4 KB
for dim=1024).  The output buffer is allocated once and the header + packed
bytes are written directly into it.

Three optimizations on top of the naive fused loop:

1. **`chunks_exact(16)`** — processes 16 elements (2 output bytes) per outer
   iteration.  Guarantees the chunk length to LLVM, enabling bounds-check
   elimination and wider code generation.  `sign_pack` alone went from 334 ns
   to 152 ns (2.2×) from this change.

2. **Dual accumulators** — each FP reduction (`abs_sum`, `norm_sq`, `radial`)
   is split into two independent chains (elements 0..3 into `_a`, 4..7 into
   `_b`).  This breaks the sequential dependency chain that prevents the OoO
   core from pipelining the additions.  The single-accumulator `abs_sum`
   primitive runs at 4.4 GiB/s; the dual-accumulator `fused_reductions`
   primitive (which also computes `norm_sq` and `radial`) runs at 10.4 GiB/s.

3. **Single allocation** — the packed bytes are written directly into the
   final `Vec<u8>` (header region filled last via `copy_from_slice`),
   eliminating one 128-byte `Vec` allocation and memcpy per code.

Current cost breakdown (dim=1024):
  1. fused pass: emb−centroid, sign_pack, abs_sum, norm², radial, popcount
                 (read 8 KB, write 128 B — no intermediate r allocation)
  2. alloc:      single `vec![0u8; 144]` for header + packed bytes

Approaches tried and rejected:

- **Split loop** (separate SIMD reductions from bit-packing): +26% regression.
  The extra 8 KB L1 re-read costs more than the SIMD vectorization gains.
- **`abs_sum` via `signed_dot` after packing**: +33% regression.  Two
  `signed_dot` calls (~390 ns) cost more than the inline accumulation.
- **4 accumulators** instead of 2: +3% regression.  Extra register pressure
  outweighs deeper pipelining on tested hardware.

Measurements on Apple M-series (`-C target-cpu=native`):

| Benchmark | Original (5-pass) | Fused (v1) | Fused + dual accum (v2) |
|---|---|---|---|
| `primitives/quant-1bit/full/1024` | ~1,934 ns, 3.95 GiB/s | 988 ns, 7.72 GiB/s | **712 ns, 10.7 GiB/s** |

---

## `Code::<1>::distance_code`: simsimd hamming distance

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

## `Code::<1>::distance_query`: interleaved + `chunks_exact` for AND+popcount

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
| `quant-query/full/1024` (after flat+fused) | 1 034 ns | **5.35× / −81%** |
| `quant-query/full/1024` (after +chunks_exact) | 567 ns | **9.77× / −90%** |

### `chunks_exact(8)` upgrade

Switching the scatter loop from `chunks(8)` to `chunks_exact(8)` gave a further
44% speedup (1020 → 567 ns).  `chunks_exact` guarantees the chunk length to
LLVM, enabling bounds-check elimination and tighter code generation for the
inner 8-element loop.  This is the same pattern that improved `Code::<1>::quantize`.

Approaches tried and rejected:

- **Replace `.round()` with `(x + 0.5) as u32`**: no improvement.  ARM NEON
  compiles `round()` to a single `VCVTNS` instruction; adding 0.5 + truncation
  is two instructions.
- **Process 16 elements (2 bytes per plane)**: 2.35× regression.  The
  `step_by(2)` index pattern generates poor code compared to the natural
  `chunks_exact(8).enumerate()` iterator.

---

## `Code::<1>::distance_query_full_precision`: lookup table

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

**Future options** (see benches/vector/quantization.rs § Code::<1>::distance_query_full_precision):
- [D1] Masked sum: `2·Σ(values where bit=1) − total_sum`; needs VMASKMOV/BSL.
- [D2] XOR sign-flip: XOR values with 0x80000000 where bit=0, then sum; no expansion.
