# 1-Bit (vs 4-Bit) Performance Comparison

Benchmark data from `cargo bench -p chroma-index --bench quantization` (dim=1024,
BATCH=512 for quantize/distance_code, SCAN_N=2048 for scan).
Throughput for quantize benchmarks counts both input arrays (embedding + centroid =
`2 * dim * 4` bytes per call).

| Benchmark | What it measures | 4-bit | 1-bit | Speedup |
|-----------|------------------|-------|-------|---------|
| quantize/quant-4bit/1024 vs quantize/quant-1bit/1024 | Data vector quantization | 28 ms, 144 MiB/s | 365–390 µs, 9.9–10.1 GiB/s | ~71x faster |
| distance_code/dc-4bit/1024 vs distance_code/dc-1bit/1024 | Code-vs-code distance: 256 pairs | 174 µs, 1.43 GiB/s | 2.45 µs, 28 GiB/s | ~71x faster |
| distance_query/dq-4f/scan vs distance_query/dq-bw/scan | Batched distance query: 2048 codes, 1 hot query | 1.01 ms, 965–1012 MiB/s | 39 µs, 6.5–6.9 GiB/s | ~25x faster |
| primitives/quant-query/full/1024 | QuantizedQuery::new alone | N/A | 568 ns, 6.73 GiB/s | — |

The batch `quant-query` includes residual allocation, `c_dot_q`, `q_norm`, and cache-cold
effects from cycling 512 distinct queries (~2.55 us/query). `quant-query/full` isolates
`QuantizedQuery::new` with a single hot-cache vector (568 ns). The 4.5x per-query gap
is the cost of the preparation pipeline and cache pressure, not the quantization itself.

**Summary:** 1-bit RaBitQ is 25--71x faster than 4-bit across data quantization, code-vs-code distance, and batched query distance. The 1-bit path uses sign-bit packing with dual-accumulator fused reductions, simsimd hamming/AND+popcount, and QuantizedQuery bit-planes (fused quantize+scatter via `chunks_exact(8)`); the 4-bit path uses ray-walk codes, nibble unpack, and f32 dot products.

---

# Thread Scaling

Benchmark data from `cargo bench -p chroma-index --bench quantization -- thread_scaling`
(N=1024, dim=1024) on r6i.8xlarge (16 physical cores / 32 vCPUs, Intel Ice Lake).
Full raw output in `saved_benchmarks/thread_scaling_r6i.8xlarge.txt`.

| Operation | What it does | 1 thread | 16 threads | 32 threads | 1->16 | 16->32 (HT) |
|-----------|--------------|----------|------------|------------|-------|-------------|
| quant-4bit | 4-bit data encode (ray-walk) | 86.9 ms, 46 MiB/s | 6.09 ms, 656 MiB/s | 4.54 ms, 880 MiB/s | 14.3x | 1.34x |
| quant-1bit | 1-bit data encode (dual accum) | 1.17 ms, 3.35 GiB/s | 108 us, 36.1 GiB/s | 114 us, 34.2 GiB/s | 10.8x | **0.95x** |
| dq-4f | 4-bit code vs f32 query | 3.48 ms, 1.27 GiB/s | 261 us, 16.9 GiB/s | 168 us, 26.2 GiB/s | 13.3x | 1.55x |
| dq-float | 1-bit code vs f32 query (signed_dot) | 2.94 ms, 1.38 GiB/s | 224 us, 18.1 GiB/s | 143 us, 28.3 GiB/s | 13.1x | 1.57x |
| dq-bw | 1-bit code vs QuantizedQuery (AND+popcount) | 4.84 ms, 855 MiB/s | 345 us, 11.7 GiB/s | 250 us, 16.1 GiB/s | 14.0x | 1.38x |
| d-lut | 1-bit code vs BatchQueryLuts (nibble LUT) | 7.02 ms, 589 MiB/s | 490 us, 8.24 GiB/s | 401 us, 10.1 GiB/s | 14.3x | 1.22x |

**Scaling shape:** All operations scale near-linearly from 1 to 16 threads (physical cores).
Beyond 16 threads, hyperthreading (HT) behaviour diverges by workload type:

- **quant-1bit is the outlier**: HT gives *no benefit* (0.95x). The dual-accumulator
  fused FP reduction loop saturates the physical core's FP units; a second HT thread
  on the same core competes for the same execution ports rather than hiding latency.
- **dq-4f / dq-float** benefit most from HT (1.55--1.57x). These are memory-bound
  (loading 1024-byte codes from DRAM); while one HT thread stalls on a cache miss the
  other can execute, effectively hiding memory latency.
- **quant-4bit / dq-bw / d-lut** see moderate HT benefit (1.22--1.38x), reflecting a
  mix of compute and memory work.

**Why dq-bw appears slower than dq-4f / dq-float:** These are cold-query benchmarks
(1 query per code). dq-bw and d-lut include per-query QuantizedQuery / BatchQueryLuts
build cost (~568 ns / ~8 us respectively) that dq-4f and dq-float do not pay. In
production scans (1 query, many codes), this build cost amortizes away and dq-bw is
~23x faster than dq-4f per code (18 ns vs ~1 us hot-scan). Compare dq-float
(1-bit code, same f32 query as dq-4f, no query quantization) to dq-4f to isolate
the code-size advantage of 1-bit vs 4-bit without the query build overhead.

---

# Recall at 1M Vectors

Benchmark data from `cargo bench -p chroma-index --bench quantization_recall -- --dataset <dataset> --size 1000000` (K=10) and `--k 100` (K=100).
Full output in `recall_1M_results.txt` and `recall_1M_results_k100.txt`.

**4-bit** (4bit-code-full-query: 4-bit data, f32 query)

| rerank | cohere_wiki@10 | msmarco@10 | beir@10 | cohere_wiki@100 | msmarco@100 | beir@100 |
|--------|----------------|------------|---------|----------------|-------------|----------|
| 1x | 0.923 | 0.944 | 0.937 | 0.940 | 0.956 | 0.955 |
| 2x | 1.000 | 0.998 | 1.000 | 1.000 | 1.000 | 1.000 |
| 4x | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| 8x | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| 16x | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |

**1-bit** (1bit-code-4bit-query: 1-bit data, 4-bit quantized query, QuantizedQuery)

| rerank | cohere_wiki@10 | msmarco@10 | beir@10 | cohere_wiki@100 | msmarco@100 | beir@100 |
|--------|----------------|------------|---------|----------------|-------------|----------|
| 1x | 0.643 | 0.675 | 0.745 | 0.679 | 0.756 | 0.772 |
| 2x | 0.830 | 0.889 | 0.928 | 0.874 | 0.935 | 0.942 |
| 4x | 0.945 | 0.969 | 0.982 | 0.966 | 0.989 | 0.991 |
| 8x | 0.987 | 0.996 | 0.998 | 0.993 | 0.999 | 0.999 |
| 16x | 0.997 | 0.999 | 1.000 | 0.999 | 1.000 | 1.000 |

4-bit reaches recall_mean 1.0 at rerank 2x–4x on all datasets. 1-bit (1bit-code-4bit-query) needs rerank 8x–16x for recall_mean > 0.99; at rerank 4x, 1-bit recall_mean is 0.95–0.99.

These results measure within-cluster recall (single centroid, flat scan). The next section
addresses centroid-level recall in a multi-cluster IVF setting.

---

# Centroid Recall (IVF)

tl;dr: Recall is primarily determined by the number of probes used. Centroid reranking plays in tiny role: 2x seems to always get us to full recall.

Benchmark data from `cargo bench -p chroma-index --bench recall_ivf -- --size 100000`
(cohere_wiki, N=100K, 316 clusters via KMeans, K=10, 1-bit data, 1-bit centroids).

This measures centroid selection recall: what fraction of the true top-K neighbors
reside in the probed clusters. Centroids are quantized with 1-bit RaBitQ relative to a
global centroid (centroid-of-centroids), matching the production quantized HNSW pipeline.
Centroid search is brute-force over quantized codes (isolating quantization error from
HNSW graph approximation).

**centroid_recall** = fraction of true top-K in the nprobe clusters selected by the
quantized centroid pipeline (quantized search for `nprobe * centroid_rerank` candidates,
then exact-distance rerank to nprobe). **centroid_recall_ceiling** = same metric using
exact centroid distance (no quantization). Namely, this is the maximum recall that can be achieved with the given nprobe.

| nprobe | centroid_rerank | centroid_recall | centroid_recall_ceiling |
|--------|-----------------|-----------------|------------------------|
| 16 | 1x | 0.916 | 0.926 |
| 16 | 2x | 0.926 | 0.926 |
| 16 | 4x | 0.926 | 0.926 |
| 32 | 1x | 0.960 | 0.963 |
| 32 | 2x | 0.963 | 0.963 |
| 32 | 4x | 0.963 | 0.963 |
| 64 | 1x | 0.983 | 0.983 |
| 64 | 2x | 0.983 | 0.983 |
| 64 | 4x | 0.983 | 0.983 |
| 128 | 1x | 0.997 | 0.998 |
| 128 | 2x | 0.998 | 0.998 |
| 128 | 4x | 0.998 | 0.998 |

**Findings:** Centroid quantization error is small. At every nprobe, `centroid_rerank=2x`
is sufficient to close the gap between quantized and exact centroid recall completely.
The gap without reranking (`centroid_rerank=1x`) is at most 1% (0.916 vs 0.926 at
nprobe=16) and shrinks with larger nprobe. At nprobe >= 64 (the production default),
quantized centroid recall matches the exact ceiling even without reranking.

This means `centroid_rerank_factor=2` is a safe default that eliminates centroid
quantization loss at negligible cost (one extra exact-distance pass over nprobe
centroids). For nprobe >= 64, `centroid_rerank_factor=1` (no reranking) also works --
the bottleneck is within-cluster vector recall, not centroid selection.

Or we could not rerank, saving us the space of the raw centroids, and increase the nprobe count a bit instead.
