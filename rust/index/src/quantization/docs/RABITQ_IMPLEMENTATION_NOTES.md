This document contains notes on the 1-bit RaBitQ implementation and performance.

# RaBitQ Implementation Notes

Findings from reviewing the Chroma RaBitQ implementation against the original
paper.

Reference implementations examined:

- **gaoj0017/RaBitQ** — original author's C++/Python code
  (`data/rabitq.py`, `src/space.h`, `src/ivf_rabitq.h`)
---

## Distance-Estimation Formula

All three implementations compute the same formula.  Starting from Euclidean
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
| **Our benchmarks** (`benches/quantization.rs`) | **No** — `random_vec` produces unrotated inputs |

The rotation is not absent — it is correctly applied in production.  The
benchmarks intentionally omit it for simplicity; this does not affect the
performance measurements (timing is dominated by the inner-product arithmetic)
but does mean the benchmark error-distribution results carry a slight
test-setup bias (see Error Analysis section below).

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

**Our previous implementation** recomputed `popcount(x_b)` on every call to
`distance_query_bitwise` (16 `popcnt` instructions for 1024-d) and on every
nibble iteration in `BatchQueryLuts::distance_query`.

**After this change**, `CodeHeader` stores the value as `signed_sum: i32`,
computed once in `Code::quantize`:

```rust
let popcount: i32 = packed.iter().map(|b| b.count_ones() as i32).sum();
let signed_sum = 2 * popcount - dim as i32;
```

`distance_query_bitwise` and `BatchQueryLuts::distance_query` now read
`code.signed_sum()` instead of running a popcount loop.  For 1024-d codes this
eliminates 16 `popcnt` + 16 additions per distance estimate in the bitwise path.

As a side-effect, `distance_query_bitwise` no longer needs the `dim: usize`
argument, which has been removed from the public API.

**Header size change:** `CodeHeader` grew from 12 bytes to 16 bytes
(`signed_sum: i32` added at offset 12).  Persisted codes (blockfiles) written
before this change are **not** compatible with the updated reader.

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

### Why absolute error has a non-zero mean in the benchmarks

The paper's unbiasedness guarantee is: for a fixed query `q`, the estimator is
unbiased in expectation over random orthogonal rotations `P`.  In the benchmarks
the rotation is omitted and queries are drawn from `Uniform(−1, 1)^D` centred at
the origin, not at the centroid.  This means query residuals `r_q = q − c` have
a non-zero expected value (`−c`), introducing a small systematic bias in the
test.  The absolute error mean is approximately 0.3 % of `d_true` with a
standard deviation of ≈ 2 %, making it negligible for ranking purposes.

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

---

# Reranking

Quantized distance estimates are approximate. Reranking re-scores candidates with
exact (full-precision) distances to improve recall. Two rerank steps exist in the
quantized SPANN query path, controlled by `centroid_rerank_factor` and
`vector_rerank_factor` in `InternalSpannConfiguration` (defaults: 1 = disabled).

## Lifecycle / Trace

```
Query
  |
  v
1. rotate(query)                                    [segment reader]
  |
  v
2. quantized_centroid.search(query, nprobe * centroid_rerank_factor)
   [segment reader]  Returns nprobe * factor cluster IDs (estimated distances)
  |
  v
3. RERANK 1 (centroid): if centroid_rerank_factor > 1
   - raw_centroid.get(id) for each candidate        [in-memory, no network]
   - exact distance(query, centroid)
   - sort, keep top nprobe
  |
  v
4. For each cluster_id: load cluster from blockfile [network: S3 block fetch]
  |
  v
5. query_quantized_cluster(cluster, query)
   [index utils]  Score all codes (estimated distances), sort, keep top K * vector_rerank_factor
  |
  v
6. Merge { k: K * vector_rerank_factor }            [worker: merge per-cluster batches]
  |
  v
7. RERANK 2 (vector): if vector_rerank_factor > 1
   - RecordSegmentReader::from_segment()            [opens blockfile metadata; no full data yet]
   - for each candidate: get_data_for_offset_id()   [network: S3 block fetch on cache miss]
   - exact distance(query, embedding)
   - sort, truncate to K
  |
  v
8. Return K results (exact distances)
```

## Rerank Step 1: Centroids

**Where:** `QuantizedSpannSegmentReader::navigate` in
`rust/segment/src/quantized_spann.rs`.

**What:** After the quantized centroid index returns `nprobe * centroid_rerank_factor`
candidates, we load each candidate's full-precision centroid from `raw_centroid`
(USearch index), compute exact distance in rotated space, sort, and keep top
`nprobe`.

**Network:** None. `raw_centroid` is an in-memory USearch index. `.get(id)` is an
in-process lookup (usearch `export`). The index is loaded once at reader creation
from S3; subsequent queries do not hit the network for centroid rerank.

**Memory:** The raw centroid index is only loaded when `centroid_rerank_factor > 1`.
If loaded: `num_clusters × dim × 4 bytes` (e.g. 100K clusters × 1536 × 4 ≈ 600 MB).
When factor is 1 (default), `raw_centroid` is `None` and no extra memory is used.

## Rerank Step 2: Vectors

**Where:** `QuantizedSpannRerankOperator::run` in
`rust/worker/src/execution/operators/quantized_spann_rerank.rs`.

**What:** After the merge step produces the top `K * vector_rerank_factor`
candidates across all clusters (approximate distances), we open a
`RecordSegmentReader`, fetch each candidate's full embedding from the record
segment blockfile, compute exact distance, sort, and keep top K.

**Network:** Each `get_data_for_offset_id(offset_id)` triggers a blockfile lookup.
If the block containing that offset is not in the foyer cache, the blockfile
provider fetches it from S3. Multiple embeddings share a block (~330 per 2 MB at
dim=1536), so one S3 GET can satisfy several lookups. Fetches are sequential in
the current implementation.

**Memory:** The record segment blockfile reader holds metadata and a cache of
recently accessed blocks. Rerank fetches `K * vector_rerank_factor` embeddings
(e.g. 40 for K=10, factor=4); each is `dim × 4 bytes`. The blockfile cache
(foyer) is shared across the process and sized by configuration.

## Summary of Impact

| Step        | When active              | Network (per query)              | Memory (extra)                          |
|-------------|--------------------------|----------------------------------|----------------------------------------|
| Centroid    | factor > 1               | None (index loaded at init)      | ~600 MB if loaded; 0 if factor=1        |
| Vector      | factor > 1               | 1–4 S3 GETs (blocks, cache-dependent) | Blockfile cache (shared)            |

---

# 4-Bit vs 1-Bit Performance Comparison

Benchmark data from `cargo bench -p chroma-index --bench quantization` (dim=1024,
BATCH=512 for quantize/distance_code, SCAN_N=2048 for scan).

| Benchmark | What it measures | 4-bit | 1-bit | Speedup |
|-----------|------------------|-------|-------|---------|
| quantize/quant-4bit/1024 vs quantize/quant-1bit/1024 | Data vector quantization | 39–45 ms, 44–51 MiB/s | 520–550 µs, 3.55–3.76 GiB/s | ~80x faster, ~77x higher throughput |
| distance_code/dc-4bit/1024 vs distance_code/dc-1bit/1024 | Code-vs-code distance: 256 pairs | 174 µs, 1.43 GiB/s | 2.45 µs, 28 GiB/s | ~71x faster, ~19.5x higher throughput |
| distance_query/dq-4f/scan vs distance_query/dq-bw/scan | Batched distance query: 2048 codes, 1 hot query | 1.01 ms, 965–1012 MiB/s | 39 µs, 6.5–6.9 GiB/s | ~25x faster, ~6.7x higher throughput |
| quantize/quant-query/1024 | Query quantization | N/A (4-bit uses raw f32 query) | 1.52 ms, 1.25–1.29 GiB/s | — |

**Summary:** 1-bit RaBitQ is 25–80x faster than 4-bit across data quantization, code-vs-code distance, and batched query distance. The 1-bit path uses sign-bit packing, simsimd hamming/AND+popcount, and QuantizedQuery bit-planes; the 4-bit path uses ray-walk codes, nibble unpack, and f32 dot products.

---

# Thread Scaling

Benchmark data from `cargo bench -p chroma-index --bench quantization -- thread_scaling` (N=1024, dim=1024).

| Operation | What it does | 1 thread | 8 threads | Speedup |
|-----------|--------------|----------|-----------|---------|
| quant-4bit | 4-bit data encode (ray-walk) | 54 ms, 74 MiB/s | 8.0 ms, 500 MiB/s | ~6.7x |
| quant-1bit | 1-bit data encode (sign-bit) | 1.0 ms, 3.8 GiB/s | 0.21 ms, 18 GiB/s | ~4.8x |
| dq-4f (cold) | 4-bit code vs f32 query (grid unpack + dot) | 2.3 ms, 1.9 GiB/s | 0.37 ms, 12 GiB/s | ~6.3x |
| dq-float (cold) | 1-bit code vs f32 query (signed_dot) | 2.1 ms, 2.0 GiB/s | 0.32 ms, 13 GiB/s | ~6.5x |
| dq-bw (cold) | 1-bit code vs QuantizedQuery (AND+popcount) | 2.9 ms, 1.4 GiB/s | 0.49 ms, 8.3 GiB/s | ~6.0x |
| d-lut (cold) | 1-bit code vs BatchQueryLuts (nibble LUT) | 10.2 ms, 405 MiB/s | 1.6 ms, 2.5 GiB/s | ~6.2x |

4-bit quantization scales near-linearly (~6.7x with 8 threads). 1-bit quantize scales ~4.8x, likely memory-bandwidth bound (sign_pack, abs_sum dominate). Distance-query methods scale ~6x; dq-bw and d-lut benefit from parallel QuantizedQuery/LUT build amortized across threads. Full raw output in `benchmark_results.txt`.

---

# Recall at 1M Vectors

Benchmark data from `cargo bench -p chroma-index --bench recall -- --dataset <dataset> --size 1000000` (K=10) and `--k 100` (K=100).
Full output in `recall_1M_results.txt` and `recall_1M_results_k100.txt`.

**4-bit**

| rerank | cohere_wiki@10 | msmarco@10 | beir@10 | cohere_wiki@100 | msmarco@100 | beir@100 |
|--------|----------------|------------|---------|----------------|-------------|----------|
| 1x | 0.896 | 0.935 | 0.930 | 0.928 | 0.947 | 0.944 |
| 2x | 1.000 | 0.997 | 0.999 | 1.000 | 1.000 | 1.000 |
| 4x | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| 8x | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| 16x | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |

**1-bit**

| rerank | cohere_wiki@10 | msmarco@10 | beir@10 | cohere_wiki@100 | msmarco@100 | beir@100 |
|--------|----------------|------------|---------|----------------|-------------|----------|
| 1x | 0.581 | 0.682 | 0.711 | 0.646 | 0.730 | 0.741 |
| 2x | 0.769 | 0.865 | 0.900 | 0.838 | 0.912 | 0.921 |
| 4x | 0.899 | 0.951 | 0.973 | 0.943 | 0.980 | 0.981 |
| 8x | 0.968 | 0.988 | 0.991 | 0.986 | 0.997 | 0.996 |
| 16x | 0.994 | 0.994 | 0.999 | 0.998 | 1.000 | 1.000 |

4-bit reaches recall_mean 1.0 at rerank 2x–4x on all datasets. 1-bit needs rerank 8x–16x for recall_mean > 0.99; at rerank 4x, 1-bit recall_mean is 0.90–0.98.
