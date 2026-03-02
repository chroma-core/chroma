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

Benchmark data from `cargo bench -p chroma-index --bench quantization -- thread_scaling` (N=1024, dim=1024).

| Operation | What it does | 1 thread | 8 threads | Speedup |
|-----------|--------------|----------|-----------|---------|
| quant-4bit | 4-bit data encode (ray-walk) | 54 ms, 74 MiB/s | 8.0 ms, 500 MiB/s | ~6.7x |
| quant-1bit | 1-bit data encode (sign-bit, dual accum) | 760 µs, 5.1 GiB/s | 173 µs, 22 GiB/s | ~4.4x |
| dq-4f (cold) | 4-bit code vs f32 query (grid unpack + dot) | 2.3 ms, 1.9 GiB/s | 0.37 ms, 12 GiB/s | ~6.3x |
| dq-float (cold) | 1-bit code vs f32 query (signed_dot) | 2.1 ms, 2.0 GiB/s | 0.32 ms, 13 GiB/s | ~6.5x |
| dq-bw (cold) | 1-bit code vs QuantizedQuery (AND+popcount) | 2.9 ms, 1.4 GiB/s | 0.49 ms, 8.3 GiB/s | ~6.0x |
| d-lut (cold) | 1-bit code vs BatchQueryLuts (nibble LUT) | 10.2 ms, 405 MiB/s | 1.6 ms, 2.5 GiB/s | ~6.2x |

4-bit quantization scales near-linearly (~6.7x with 8 threads). 1-bit quantize scales ~4.4x; the fused loop is compute-bound (dual-accumulator FP reductions dominate) rather than memory-bound at this thread count. Distance-query methods scale ~6x; dq-bw and d-lut benefit from parallel QuantizedQuery/LUT build amortized across threads. Full raw output in `benchmark_results.txt`.

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
