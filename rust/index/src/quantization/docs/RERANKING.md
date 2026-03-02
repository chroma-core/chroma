
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

## Centroid Rerank

**Where:** `QuantizedSpannSegmentReader::navigate` in
`rust/segment/src/quantized_spann.rs`.

**What:**
1. `quantized_centroid.search(query, search_nprobe * centroid_rerank_factor)` returns
   `n = search_nprobe * centroid_rerank_factor` candidates (estimated distances).
2. For each candidate, `raw_centroid.get(id)` retrieves the full-precision centroid
   (in-memory USearch `.export()` -- O(1), no I/O).
3. `distance_function.distance(rotated_query, &centroid)` computes exact distance --
   one SIMD dot product over `dim` f32s.
4. Sort `n` floats, keep top `search_nprobe`.

**I/O:** None beyond the one-time index load at reader creation.

**Memory:** `raw_centroid` is only loaded when `centroid_rerank_factor > 1`.
Cost: `num_clusters * dim * 4 bytes` (e.g. 100K clusters * 1536 * 4 = 600 MB).
When factor is 1 (default), `raw_centroid` is `None` and no extra memory is used.

**Compute cost** (search_nprobe = 64, dim = 1536, M-series/Graviton):

| Factor | Extra centroid lookups | Extra dot products          | Extra sort |
|--------|------------------------|-----------------------------|------------|
| 1x     | 0                      | 0                           | none       |
| 2x     | 64                     | 64 * ~0.5 us = 32 us        | 128 floats |
| 4x     | 192                    | 192 * ~0.5 us = 96 us       | 256 floats |
| 8x     | 448                    | 448 * ~0.5 us = 224 us      | 512 floats |

Sorting `n` floats is negligible (nanoseconds). The centroid rerank is not
directly measured in single-centroid recall benchmarks. Its benefit is in
multi-centroid SPANN: selecting the right clusters dominates recall when
`search_nprobe` is small relative to the number of clusters.

## Vector Rerank

**Where:** `QuantizedSpannRerankOperator::run` in
`rust/worker/src/execution/operators/quantized_spann_rerank.rs`.

**What:**
1. Each bruteforce operator returns `K * vector_rerank_factor` approximate
   candidates per cluster (instead of K).
2. The merge operator collects the top `K * vector_rerank_factor` across all
   clusters (approximate distances).
3. `QuantizedSpannRerankOperator` opens a `RecordSegmentReader`, fetches each
   candidate's full embedding via `get_data_for_offset_id()`, computes exact
   distance, sorts, and truncates to K.

**Compute cost:** One dot product per candidate. For K=10, factor=4, dim=1536:
40 * ~0.6 us = 24 us -- negligible.

**I/O cost (dominates):**

*Warm cache (foyer L2 in-process):* Each lookup is a hash map probe + memcpy
(~1-5 us per embedding).

| K   | Factor | Candidates | Warm-cache latency |
|-----|--------|------------|--------------------|
| 10  | 2x     | 20         | ~20-100 us         |
| 10  | 4x     | 40         | ~40-200 us         |
| 10  | 8x     | 80         | ~80-400 us         |
| 100 | 2x     | 200        | ~200 us - 1 ms     |
| 100 | 4x     | 400        | ~400 us - 2 ms     |

*Cold cache (S3 GET):* Block size is configurable (default 2 MB). With dim=1536,
one f32 embedding is 6 KB; a 2 MB block holds ~330 embeddings. For K=10,
factor=4: 40 embeddings likely fit in 1-2 blocks (1-2 S3 GETs at ~10-30 ms each).
For K=100, factor=4: 400 embeddings -> 2-4 blocks -> 20-120 ms cold.
Chroma's admissions-controlled S3 client and foyer cache mitigate cold misses on
repeat queries. Fetches are sequential in the current implementation.

**Recall improvement** (1M vector benchmark, single centroid):

| Quantization | Factor | recall@10  | recall@100 |
|--------------|--------|------------|------------|
| 4-bit        | 1x     | 0.90       | 0.93       |
| 4-bit        | 2x     | 1.00       | 1.00       |
| 4-bit        | 4x     | 1.00       | 1.00       |
| 1-bit        | 1x     | 0.58-0.71  | 0.65-0.74  |
| 1-bit        | 4x     | 0.90-0.97  | 0.94-0.98  |
| 1-bit        | 8x     | 0.97-0.99  | 0.99-1.00  |
| 1-bit        | 16x    | 0.99-1.00  | 1.00       |

4-bit reaches 100% recall at factor 2x. 1-bit needs 8x-16x for recall > 0.99.

## Combined Cost Budget

Query latency = base_latency + centroid_rerank_cost + merge_overhead + vector_rerank_cost

For a typical production query (K=10, search_nprobe=64, dim=1536, warm cache):

| centroid_factor | vector_factor | Extra latency (warm) | recall@10 (4-bit) | recall@10 (1-bit) |
|----------------|---------------|----------------------|-------------------|-------------------|
| 1              | 1             | 0                    | 0.90              | ~0.65             |
| 2              | 2             | ~50-150 us           | 1.00              | ~0.87             |
| 2              | 4             | ~100-300 us          | 1.00              | ~0.93             |
| 4              | 8             | ~200-600 us          | 1.00              | ~0.99             |

The dominant cost is vector embedding I/O. Centroid rerank is nearly free relative
to cluster loading (which already involves blockfile I/O per cluster).

## Recommended Defaults

| Use case                  | centroid_rerank_factor | vector_rerank_factor | Notes                              |
|---------------------------|------------------------|----------------------|------------------------------------|
| 4-bit, latency-sensitive  | 1                      | 2                    | Full recall, minimal overhead      |
| 4-bit, recall-first       | 2                      | 4                    | Redundant at 4-bit but safe        |
| 1-bit, balanced           | 2                      | 8                    | recall@10 ~0.97-0.99               |
| 1-bit, recall-first       | 4                      | 16                   | recall@10 ~1.00, +500 us warm      |

Current defaults: both = 1 (no reranking, backwards-compatible).
