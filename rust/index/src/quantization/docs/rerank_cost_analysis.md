# Reranking Cost Analysis

## What reranking adds to the query path

The SPANN query path has two rerank opportunities. Both are disabled by default
(`centroid_rerank_factor = 1`, `vector_rerank_factor = 1`) and activated by setting
those values in `InternalSpannConfiguration`.

---

## Centroid rerank

### What happens

1. `quantized_centroid.search(query, search_nprobe * centroid_rerank_factor)` returns
   `n = search_nprobe * centroid_rerank_factor` candidates (estimated distances).
2. For each of those `n` candidates, `raw_centroid.get(id)` retrieves the full-precision
   centroid embedding (in-memory USearch `.export()` call — O(1), no I/O).
3. `distance_function.distance(rotated_query, &centroid)` computes the exact distance —
   one SIMD dot product over `dim` f32s.
4. Sort `n` floats, keep top `search_nprobe`.

### Compute cost

| Factor | Extra centroid lookups | Extra dot products (dim=1536) | Extra sort |
|--------|------------------------|-------------------------------|------------|
| 1x     | 0                      | 0                             | none       |
| 2x     | 64 (= search_nprobe)   | 64 × ~0.5 µs ≈ 32 µs          | 128 floats |
| 4x     | 192                    | 192 × ~0.5 µs ≈ 96 µs         | 256 floats |
| 8x     | 448                    | 448 × ~0.5 µs ≈ 224 µs        | 512 floats |

Figures above assume `search_nprobe = 64` and dim = 1536 on an M-series/Graviton core.
Sorting `n` floats is negligible (nanoseconds).

The `.get()` calls are in-memory (no S3 round-trips). `raw_centroid` is loaded once at
reader creation time and held in the in-process USearch cache. Memory footprint of the
raw centroid index: `num_clusters × dim × 4 bytes`. For 100 K clusters and dim = 1536:
`100 000 × 1536 × 4 ≈ 600 MB` — kept in the same cache as the quantized index.

### I/O cost

None beyond the one-time index load at reader creation.

### Recall improvement (single-centroid benchmark)

The centroid rerank is not directly measured in the single-centroid recall benchmarks
(which already score all vectors in a cluster perfectly). Its benefit is in multi-centroid
SPANN: selecting the right clusters dominates recall when `search_nprobe` is small relative
to the number of clusters.

---

## Vector rerank

### What happens

1. Each bruteforce operator returns `K * vector_rerank_factor` approximate candidates per
   cluster (instead of K).
2. The merge operator collects the top `K * vector_rerank_factor` across all clusters
   (approximate distances).
3. `QuantizedSpannRerankOperator` runs:
   - Opens a `RecordSegmentReader` (one-time blockfile header read).
   - For each of the `K * vector_rerank_factor` merged candidates, calls
     `reader.get_data_for_offset_id(offset_id)` to fetch the full embedding.
   - Computes exact distance, sorts, truncates to K.

### Compute cost

One dot product per candidate: `K * vector_rerank_factor × dim × 4 ns ≈ negligible`.
For K=10, factor=4, dim=1536: 40 × ~0.6 µs ≈ 24 µs of compute.

### I/O cost

This dominates. Each `get_data_for_offset_id` call fetches one embedding from the record
segment blockfile.

**Warm cache (foyer L2 in-process):**
If the embedding blocks are already resident (common for hot collections), each lookup is
a hash map probe + memcpy: ~1–5 µs per embedding.

| K   | Factor | Candidates | Warm-cache latency |
|-----|--------|------------|--------------------|
| 10  | 2x     | 20         | ~20–100 µs         |
| 10  | 4x     | 40         | ~40–200 µs         |
| 10  | 8x     | 80         | ~80–400 µs         |
| 100 | 2x     | 200        | ~200 µs – 1 ms     |
| 100 | 4x     | 400        | ~400 µs – 2 ms     |

**Cold cache (S3 GET):**
Each uncached embedding block requires one S3 GET. Block size is configurable
(default 2 MB), so multiple embeddings share a block. With dim = 1536, one
f32 embedding is 6 KB; a 2 MB block holds ~330 embeddings. For K=10, factor=4:
40 embeddings likely fit in 1–2 blocks → 1–2 S3 GETs at ~10–30 ms each.
For K=100, factor=4: 400 embeddings → 2–4 blocks → 20–120 ms cold.

Chroma's admissions-controlled S3 client and foyer cache mitigate cold misses on
repeat queries.

### Recall improvement (1M vector benchmark, single centroid)

| Quantization | Factor | recall@10  | recall@100 |
|--------------|--------|------------|------------|
| 4-bit        | 1x     | 0.90       | 0.93       |
| 4-bit        | 2x     | 1.00       | 1.00       |
| 4-bit        | 4x     | 1.00       | 1.00       |
| 1-bit        | 1x     | 0.58–0.71  | 0.65–0.74  |
| 1-bit        | 4x     | 0.90–0.97  | 0.94–0.98  |
| 1-bit        | 8x     | 0.97–0.99  | 0.99–1.00  |
| 1-bit        | 16x    | 0.99–1.00  | 1.00       |

4-bit reaches 100% recall at factor 2x. 1-bit needs 8x–16x for recall > 0.99.

---

## Combined cost budget

Query latency = base_latency + centroid_rerank_cost + merge_overhead + vector_rerank_cost

For a typical production query (K=10, search_nprobe=64, dim=1536, warm cache):

| centroid_factor | vector_factor | Extra latency (warm) | recall@10 (4-bit) | recall@10 (1-bit) |
|----------------|---------------|----------------------|-------------------|-------------------|
| 1              | 1             | 0                    | 0.90              | ~0.65             |
| 2              | 2             | ~50–150 µs           | 1.00              | ~0.87             |
| 2              | 4             | ~100–300 µs          | 1.00              | ~0.93             |
| 4              | 8             | ~200–600 µs          | 1.00              | ~0.99             |

The dominant cost is vector embedding I/O. Centroid rerank is nearly free relative to
cluster loading (which already involves blockfile I/O per cluster).

---

## Recommended defaults by use case

| Use case                  | centroid_rerank_factor | vector_rerank_factor | Notes                              |
|---------------------------|------------------------|----------------------|------------------------------------|
| 4-bit, latency-sensitive  | 1                      | 2                    | Full recall, minimal overhead      |
| 4-bit, recall-first       | 2                      | 4                    | Redundant at 4-bit but safe        |
| 1-bit, balanced           | 2                      | 8                    | recall@10 ~0.97–0.99               |
| 1-bit, recall-first       | 4                      | 16                   | recall@10 ~1.00, +500 µs warm      |

Current defaults: both = 1 (no reranking, backwards-compatible).
