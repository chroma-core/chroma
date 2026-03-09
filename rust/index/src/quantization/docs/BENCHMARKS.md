- [RaBitQ](#rabitq)
  - [1-Bit (vs 4-Bit) Performance Comparison](#1-bit-vs-4-bit-performance-comparison)
    - [r6i.8xlarge](#r6i8xlarge)
    - [MacBook Pro M1](#macbook-pro-m1)
  - [vs Exact f32 Distance Query](#vs-exact-f32-distance-query)
  - [Thread Scaling](#thread-scaling)
  - [Cross-Machine Comparison](#cross-machine-comparison)
- [Error Bound](#error-bound)
- [Recall](#recall)
  - [USearch](#usearch)
    - [1-bit centroids](#1-bit-centroids)
    - [4-bit centroids](#4-bit-centroids)
    - [full precision centroids](#full-precision-centroids)
  - [Real SPANN index + USearch.](#real-spann-index--usearch)
  - [Single Centroid Recall](#single-centroid-recall)
    - [1-bit, 4-bit query (1bit-code-4bit-query)](#1-bit-4-bit-query-1bit-code-4bit-query)
    - [1-bit, 1-bit query (1bit-code-1bit-query)](#1-bit-1-bit-query-1bit-code-1bit-query)
    - [4-bit (4bit-code-full-query)](#4-bit-4bit-code-full-query)
    - [1-bit, f32 query (1bit-code-full-query)](#1-bit-f32-query-1bit-code-full-query)
  - [Synthetic SPANN / Centroid Recall](#synthetic-spann--centroid-recall)
  - [Quantized KMeans Clustering Recall](#quantized-kmeans-clustering-recall)
  - [Synthetic Index - Reranking with both 1-bit and 4-bit centroids](#synthetic-index---reranking-with-both-1-bit-and-4-bit-centroids)
- [SPANN](#spann)
  - [Performance](#performance)
    - [1bit vs 4bit](#1bit-vs-4bit)
- [USearch](#usearch-1)
  - [Parallelism](#parallelism)
    - [Our global lock](#our-global-lock)
    - [Usearch global lock](#usearch-global-lock)
  - [Performance](#performance-1)
    - [Note: USearch ef/k coupling](#note-usearch-efk-coupling)
    - [1-bit vs 4-bit](#1-bit-vs-4-bit)
    - [Navigate() using 1 bit quantized centroids](#navigate-using-1-bit-quantized-centroids)
    - [Reranking](#reranking)
  - [Thread scaling](#thread-scaling-1)
- [Appendix](#appendix)
  - [Sources of performance differences between r6i.8xlarge and MacBook Pro M1](#sources-of-performance-differences-between-r6i8xlarge-and-macbook-pro-m1)
    - [Decode width and instruction window](#decode-width-and-instruction-window)
    - [FP/SIMD execution ports](#fpsimd-execution-ports)
    - [Why M1 wins on signed\_dot despite losing on simsimd\_dot](#why-m1-wins-on-signed_dot-despite-losing-on-simsimd_dot)
    - [Memory bandwidth per core](#memory-bandwidth-per-core)
    - [Why dq-bw/scan ties](#why-dq-bwscan-ties)
    - [Thread scaling](#thread-scaling-2)
- [Central Index Options](#central-index-options)
  - [Usearch 1 bit](#usearch-1-bit)
  - [Usearch 1 bit - Improved Concurrency](#usearch-1-bit---improved-concurrency)
    - [USearch only benchmark](#usearch-only-benchmark)
    - [Quantized SPANN benchmark](#quantized-spann-benchmark)
      - [Specifics](#specifics)
  - [Flat / Brute Force](#flat--brute-force)
  - [Hierarchical SPANN](#hierarchical-spann)

# RaBitQ

## 1-Bit (vs 4-Bit) Performance Comparison

[../../../benches/vector/quantization.rs](../../../benches/vector/quantization.rs)

### r6i.8xlarge


| Function       | Benchmark                                                | 4-bit               | 1-bit               | Speedup |
| -------------- | -------------------------------------------------------- | ------------------- | ------------------- | ------- |
| quantize data  | quantize/quant-4bit/1024 vs quantize/quant-1bit/1024     | 43.2 ms, 92.5 MiB/s | 576 µs, 6.78 GiB/s  | ~75x    |
| quantize query | primitives/quant-query/full/1024                         | N/A                 | 2.21 µs, 1.73 GiB/s | --      |
| distance_code  | distance_code/dc-4bit/1024 vs distance_code/dc-1bit/1024 | 166 µs, 1.50 GiB/s  | 3.99 µs, 17.2 GiB/s | ~42x    |
| distance_query | distance_query/dq-4f/scan vs distance_query/dq-bw/scan   | 762 µs, 1.31 GiB/s  | 40 µs, 6.86 GiB/s   | ~19x    |


[performance_r6i.8xlarge.txt](performance_r6i.8xlarge.txt)

Benchmark data from `cargo bench -p chroma-index --bench quantization` (dim=1024,
BATCH=512 for quantize/distance_code, SCAN_N=2048 for scan).
Throughput for quantize benchmarks counts both input arrays (embedding + centroid =
`2 * dim * 4` bytes per call).

The batch `quant-query` includes residual allocation, `c_dot_q`, `q_norm`, and cache-cold
effects from cycling 512 distinct queries (~2.55 us/query). `quant-query/full` isolates
`QuantizedQuery::new` with a single hot-cache vector (568 ns). The 4.5x per-query gap
is the cost of the preparation pipeline and cache pressure, not the quantization itself.

### MacBook Pro M1


| Function       | Benchmark                                                | 4-bit                   | 1-bit                  | Speedup |
| -------------- | -------------------------------------------------------- | ----------------------- | ---------------------- | ------- |
| quantize data  | quantize/quant-4bit/1024 vs quantize/quant-1bit/1024     | 28 ms, 144 MiB/s        | 365–390 µs, 10.1 GiB/s | ~71x    |
| quantize query | primitives/quant-query/full/1024                         | N/A                     | 568 ns, 6.73 GiB/s     | —       |
| distance_code  | distance_code/dc-4bit/1024 vs distance_code/dc-1bit/1024 | 174 µs, 1.43 GiB/s      | 2.45 µs, 28 GiB/s      | ~71x    |
| distance_query | distance_query/dq-4f/scan vs distance_query/dq-bw/scan   | 1.01 ms, 965–1012 MiB/s | 39 µs, 6.9 GiB/s       | ~25x    |


[performance_mb_pro_m1.txt](performance_mb_pro_m1.txt)

---

## vs Exact f32 Distance Query

[../../../benches/vector/quantization.rs](../../../benches/vector/quantization.rs)

Hot-scan benchmark: 1 query vs 2048 vectors, dim=1024, query in L1.


| Benchmark | Function                     | Time   | Per vector | vs exact        |
| --------- | ---------------------------- | ------ | ---------- | --------------- |
| dq-exact  | f32 x f32, no quantization   | 290 us | 141 ns     | 1.0x            |
| dq-4f     | 4-bit code, unpack + f32 dot | 762 us | 372 ns     | 2.6x slower     |
| dq-bw     | 1-bit code, AND+popcount     | 40 us  | 19.5 ns    | **7.2x faster** |


Benchmark data from `cargo bench -p chroma-index --bench quantization_performance -- dq-`
on r6i.8xlarge.

---

## Thread Scaling

[thread_scaling_r6i.8xlarge.txt](thread_scaling_r6i.8xlarge.txt)


| Operation  | What it does                   | 1 thread            | 16 threads         | 32 threads         | 1->16 | 16->32 (HT) |
| ---------- | ------------------------------ | ------------------- | ------------------ | ------------------ | ----- | ----------- |
| quant-4bit | 4-bit data encode (ray-walk)   | 86.9 ms, 46 MiB/s   | 6.09 ms, 656 MiB/s | 4.54 ms, 880 MiB/s | 14.3x | 1.34x       |
| quant-1bit | 1-bit data encode (dual accum) | 1.17 ms, 3.35 GiB/s | 108 us, 36.1 GiB/s | 114 us, 34.2 GiB/s | 10.8x | **0.95x**   |
| dq-4f      | 4-bit code vs f32 query        | 3.48 ms, 1.27 GiB/s | 261 us, 16.9 GiB/s | 168 us, 26.2 GiB/s | 13.3x | 1.55x       |
| dq-float   | 1-bit code vs f32 query        | 2.94 ms, 1.38 GiB/s | 224 us, 18.1 GiB/s | 143 us, 28.3 GiB/s | 13.1x | 1.57x       |
| dq-bw      | 1-bit code vs QuantizedQuery   | 4.84 ms, 855 MiB/s  | 345 us, 11.7 GiB/s | 250 us, 16.1 GiB/s | 14.0x | 1.38x       |
| d-lut      | 1-bit code vs BatchQueryLuts   | 7.02 ms, 589 MiB/s  | 490 us, 8.24 GiB/s | 401 us, 10.1 GiB/s | 14.3x | 1.22x       |


Benchmark data from `cargo bench -p chroma-index --bench quantization -- thread_scaling`
(N=1024, dim=1024) on r6i.8xlarge (16 physical cores / 32 vCPUs, Intel Ice Lake).
Full raw output in `saved_benchmarks/thread_scaling_r6i.8xlarge.txt`.

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

## Cross-Machine Comparison

Full raw output in `saved_benchmarks/performance_r6i.8xlarge.txt` (Intel) and
`saved_benchmarks/performance_mb_pro_m1.txt` (Apple Silicon).

- **r6i.8xlarge**: Intel Ice Lake, 16 physical cores / 32 vCPUs, AVX-512
- **MacBook Pro M1**: Apple Silicon, 8 performance cores, NEON

**Batch benchmarks** (512 embeddings/queries, dim=1024):


| Benchmark                    | M1                  | r6i.8xlarge         | Ratio (r6i/M1) |
| ---------------------------- | ------------------- | ------------------- | -------------- |
| quant-4bit (data encode)     | 27.6 ms, 145 MiB/s  | 43.2 ms, 93 MiB/s   | 1.6x slower    |
| quant-1bit (data encode)     | 361 us, 10.8 GiB/s  | 576 us, 6.8 GiB/s   | 1.6x slower    |
| dc-1bit (256 pairs)          | 2.69 us, 25.5 GiB/s | 3.99 us, 17.2 GiB/s | 1.5x slower    |
| dc-4bit (256 pairs)          | 182 us, 1.37 GiB/s  | 166 us, 1.50 GiB/s  | 1.1x faster    |
| dq-exact (f32 ground truth)  | 53.6 us, 72.9 GiB/s | 120 us, 32.7 GiB/s  | 2.2x slower    |
| dq-4f (4-bit, cold query)    | 1.17 ms, 1.88 GiB/s | 1.71 ms, 1.29 GiB/s | 1.5x slower    |
| dq-float (1-bit, cold query) | 1.06 ms, 1.91 GiB/s | 1.46 ms, 1.39 GiB/s | 1.4x slower    |
| dq-bw (AND+popcount, cold)   | 1.26 ms, 1.60 GiB/s | 2.38 ms, 870 MiB/s  | 1.9x slower    |
| d-lut (nibble LUT, cold)     | 5.28 ms, 393 MiB/s  | 3.48 ms, 594 MiB/s  | 1.5x faster    |


**Hot-scan benchmarks** (1 query, 2048 codes, dim=1024):


| Benchmark     | M1                  | r6i.8xlarge         | Ratio (r6i/M1) |
| ------------- | ------------------- | ------------------- | -------------- |
| dq-exact/scan | 172 us, 45.3 GiB/s  | 290 us, 27.0 GiB/s  | 1.7x slower    |
| dq-4f/scan    | 1.03 ms, 995 MiB/s  | 762 us, 1.31 GiB/s  | 1.4x faster    |
| dq-float/scan | 456 us, 616 MiB/s   | 933 us, 301 MiB/s   | 2.0x slower    |
| dq-bw/scan    | 40.8 us, 6.74 GiB/s | 40.0 us, 6.86 GiB/s | ~same          |
| d-lut/scan    | 371 us, 759 MiB/s   | 265 us, 1.03 GiB/s  | 1.4x faster    |


**Key primitive differences** (single-vector, hot cache):


| Primitive           | M1                  | r6i.8xlarge         | Ratio (r6i/M1)  |
| ------------------- | ------------------- | ------------------- | --------------- |
| simsimd_dot         | 260 ns, 29.3 GiB/s  | 63 ns, 121 GiB/s    | **4.1x faster** |
| signed_dot          | 198 ns, 19.9 GiB/s  | 443 ns, 8.9 GiB/s   | 2.2x slower     |
| sign_pack           | 143 ns, 26.7 GiB/s  | 320 ns, 11.9 GiB/s  | 2.2x slower     |
| fused_reductions    | 547 ns, 14.0 GiB/s  | 695 ns, 11.0 GiB/s  | 1.3x slower     |
| hamming/simsimd     | 6.54 ns, 36.2 GiB/s | 4.77 ns, 50.0 GiB/s | 1.4x faster     |
| QuantizedQuery::new | 566 ns, 6.74 GiB/s  | 2.21 us, 1.73 GiB/s | **3.9x slower** |


**Observations:**

- **M1 wins on scalar/FP-intensive work.** Data quantization (both 1-bit and 4-bit),
fused reductions, sign packing, and signed_dot are all 1.3-2.2x faster on M1.
Apple Silicon's wide FP pipeline and memory bandwidth advantage over Ice Lake show
clearly in these single-threaded benchmarks.
- **r6i wins on simsimd operations.** AVX-512 gives a 4.1x advantage on raw f32 dot
products (simsimd_dot) and 1.4x on hamming distance. This matters for dq-4f/scan
and d-lut/scan which are both ~1.4x slower on M1.
- **AND+popcount (dq-bw/scan) is identical** on both (~40 us). NEON CNT and AVX-512
VPOPCNTDQ are equally effective for this workload. This is the production-path kernel.
- **QuantizedQuery::new is 3.9x faster on M1** (566 ns vs 2.21 us). The fused
min/max + quantize + bit-plane scatter path benefits from M1's stronger single-core
throughput. This is a per-query cost that amortizes across codes.
- **d-lut is consistently slower on M1** (1.4-1.5x). The nibble LUT approach likely
benefits from AVX-512 gather/scatter instructions not available on NEON.
- **Thread scaling data is only meaningful on r6i** (16 physical cores + HT). The M1
has 8 performance cores with no hyperthreading; the 16t/32t results on M1 are just
contention noise. See the Thread Scaling section for r6i scaling analysis.

Full details below.

---

# Error Bound

[error.txt](saved_benchmarks/error.txt)

dim=1024, 131k samples. Relative error = (d_est - d_true) / d_true.


| method               | mean     | std     | RMSE    | p5       | p95      |
| -------------------- | -------- | ------- | ------- | -------- | -------- |
| 1bit_data_full_query | +0.00033 | 0.02275 | 0.02275 | -0.03703 | +0.03778 |
| 1bit_data_4bit_query | +0.00033 | 0.02275 | 0.02275 | -0.03703 | +0.03778 |
| 1bit_data_1bit_query | -0.00093 | 0.03560 | 0.03561 | -0.05942 | +0.05779 |
| 4bit_data_4bit_query | +0.00053 | 0.00579 | 0.00581 | -0.00898 | +0.01003 |
| 4bit_data_full_query | -0.00000 | 0.00413 | 0.00413 | -0.00681 | +0.00677 |


RSME: "Root Mean Square Error".

- 4-bit codes are ~5x tighter than 1-bit (RMSE 0.004 vs 0.023).
- All methods are near-zero mean (unbiased).
- Quantizing the query side adds negligible error for1-bit (identical rows) and only ~40% more variance for 4-bit.
- The fully quantized 1-bit-vs-1-bit path is the noisiest (RMSE 0.036) but 90% of samples still fall within +/-6% relative error.

# Recall

## USearch

usearch_spann_profile benchmark.

### 1-bit centroids

[usearch_1bit.txt](saved_benchmarks/usearch_1bit.txt)


| # Centroids | Queries | k   | Recall@10 | Recall@100 | Avg latency |
| ----------- | ------- | --- | --------- | ---------- | ----------- |
| 1.01M       | 200     | 100 | 92.80%    | 59.69%     | 552.5µs     |
| 5.8K        | 200     | 100 | 98.05%    | 70.08%     | 109.4µs     |


=== Rerank (k=100) ===


| Rerank                                                              | Fetch | Recall@10 | Recall@100 | Avg lat |
| ------------------------------------------------------------------- | ----- | --------- | ---------- | ------- |
| 1x                                                                  | 100   | 84.75%    | 50.88%     | 378.4µs |
| 2x                                                                  | 200   | 90.50%    | 68.18%     | 695.3µs |
| 4x                                                                  | 400   | 95.70%    | 81.98%     | 1.34ms  |
| 8x                                                                  | 800   | 98.20%    | 91.25%     | 2.61ms  |
| 16x                                                                 | 1600  | 99.20%    | 96.07%     | 5.12ms  |
| [usearch_rerank_1bit.txt](saved_benchmarks/usearch_rerank_1bit.txt) |       |           |            |         |


### 4-bit centroids

[usearch_4bit.txt](saved_benchmarks/usearch_4bit.txt)


| # Centroids | Queries | k   | Recall@10 | Recall@100 | Avg latency |
| ----------- | ------- | --- | --------- | ---------- | ----------- |
| 1.01M       | 200     | 100 | 96.85%    | 86.07%     | 2.75ms      |
| 5.8K        | 200     | 100 | 99.15%    | 93.19%     | 1.23ms      |


=== Rerank (k=100) ===


| Rerank                                                              | Fetch | Recall@10 | Recall@100 | Avg lat |
| ------------------------------------------------------------------- | ----- | --------- | ---------- | ------- |
| 1x                                                                  | 100   | 94.25%    | 81.56%     | 2.75ms  |
| 2x                                                                  | 200   | 96.15%    | 89.45%     | 4.08ms  |
| 4x                                                                  | 400   | 98.60%    | 94.98%     | 7.24ms  |
| 8x                                                                  | 800   | 99.65%    | 97.80%     | 13.32ms |
| 16x                                                                 | 1600  | 99.85%    | 99.17%     | 24.51ms |
| [usearch_rerank_4bit.txt](saved_benchmarks/usearch_rerank_4bit.txt) |       |           |            |         |


### full precision centroids

[usearch_full_precision.txt](saved_benchmarks/usearch_full_precision.txt)


| # Centroids | Queries | k   | Recall@10 | Recall@100 | Avg latency |
| ----------- | ------- | --- | --------- | ---------- | ----------- |
| 1.01M       | 200     | 100 | 97.20%    | 88.64%     | 1.36ms      |
| 5.8K        | 200     | 100 | 99.80%    | 98.21%     | 323.2µs     |


=== Rerank Sweep (k=100) ===


| Rerank | Fetch | Recall@10 | Recall@100 | Avg lat |
| ------ | ----- | --------- | ---------- | ------- |
| 1x     | 100   | 94.25%    | 81.56%     | 2.71ms  |
| 2x     | 200   | 96.15%    | 89.45%     | 4.01ms  |
| 4x     | 400   | 98.60%    | 94.98%     | 7.15ms  |
| 8x     | 800   | 99.65%    | 97.80%     | 13.27ms |
| 16x    | 1600  | 99.85%    | 99.17%     | 24.53ms |


---

## Real SPANN index + USearch.

[quantized_spann benchmark](../../../benches/quantized_spann.rs)
~7k centroids, 5M data vectors, 100 queries

Results: -1-4% (decreasing with nprobe)

=== Recall Summary ===


| navigate()     | CP  | Vectors | Index | Commit | Queries | nprobe=16 R@10 R@100 ms/query | nprobe=32 R@10 R@100 ms/query | nprobe=64 R@10 R@100 ms/query | nprobe=128 R@10 R@100 ms/query | nprobe=256 R@10 R@100 ms/query |
| -------------- | --- | ------- | ----- | ------ | ------- | ----------------------------- | ----------------------------- | ----------------------------- | ------------------------------ | ------------------------------ |
| 1bit qunatized | 5   | 5.00M   | 1.4m  | 7.46s  | 100     | 72% 62% 12ms                  | 80% 69% 12ms                  | 85% 73% 20ms                  | 88% 75% 34ms                   | 92% 77% 64ms                   |
| full precision | 5   | 5.00M   | 2.3m  | 7.47s  | 100     | 75% 62% 12ms                  | 81% 68% 12ms                  | 87% 72% 20ms                  | 91% 75% 36ms                   | 93% 76% 67ms                   |


Sources:

- `1bit quantized`: [quant_spann_1bit.txt](saved_benchmarks/quant_spann_1bit.txt)
  - `cargo bench -p chroma-index --bench quantized_spann -- --dataset wikipedia-en --checkpoint 1 --threads 16 --data-bits 1 --centroid-bits 1`
- `full precision`: [quant_spann_full_precision.txt](saved_benchmarks/quant_spann_full_precision.txt)
  - `cargo bench -p chroma-index --bench quantized_spann -- --dataset wikipedia-en --checkpoint 1 --threads 16`

---

## Single Centroid Recall

Benchmark data from `cargo bench -p chroma-index --bench quantization_recall -- --dataset <dataset> --size 1000000` (K=10) and `--k 100` (K=100).
Full output in `recall_1M_results.txt` and `recall_1M_results_k100.txt`.
Run on r6i.8xlarge (16 physical cores, Intel Ice Lake).

### 1-bit, 4-bit query (1bit-code-4bit-query)


| rerank | cohere_wiki@10 | msmarco@10 | beir@10 | cohere_wiki@100 | msmarco@100 | beir@100 |
| ------ | -------------- | ---------- | ------- | --------------- | ----------- | -------- |
| 1x     | 0.640          | 0.701      | 0.750   | 0.686           | 0.758       | 0.772    |
| 2x     | 0.845          | 0.900      | 0.933   | 0.876           | 0.938       | 0.945    |
| 4x     | 0.962          | 0.967      | 0.986   | 0.967           | 0.990       | 0.991    |
| 8x     | 0.988          | 0.992      | 0.996   | 0.995           | 0.999       | 0.999    |
| 16x    | 0.997          | 0.999      | 1.000   | 0.999           | 1.000       | 1.000    |


### 1-bit, 1-bit query (1bit-code-1bit-query)


| rerank | cohere_wiki@10 | msmarco@10 | beir@10 | cohere_wiki@100 | msmarco@100 | beir@100 |
| ------ | -------------- | ---------- | ------- | --------------- | ----------- | -------- |
| 1x     | 0.497          | 0.577      | 0.661   | 0.550           | 0.654       | 0.667    |
| 2x     | 0.693          | 0.776      | 0.837   | 0.725           | 0.840       | 0.854    |
| 4x     | 0.814          | 0.883      | 0.922   | 0.856           | 0.941       | 0.947    |
| 8x     | 0.910          | 0.939      | 0.973   | 0.939           | 0.981       | 0.982    |
| 16x    | 0.964          | 0.974      | 0.988   | 0.980           | 0.996       | 0.995    |


### 4-bit (4bit-code-full-query)


| rerank | cohere_wiki@10 | msmarco@10 | beir@10 | cohere_wiki@100 | msmarco@100 | beir@100 |
| ------ | -------------- | ---------- | ------- | --------------- | ----------- | -------- |
| 1x     | 0.913          | 0.933      | 0.938   | 0.942           | 0.954       | 0.954    |
| 2x     | 1.000          | 0.999      | 1.000   | 1.000           | 1.000       | 1.000    |
| 4x     | 1.000          | 1.000      | 1.000   | 1.000           | 1.000       | 1.000    |


### 1-bit, f32 query (1bit-code-full-query)


| rerank | cohere_wiki@10 | msmarco@10 | beir@10 | cohere_wiki@100 | msmarco@100 | beir@100 |
| ------ | -------------- | ---------- | ------- | --------------- | ----------- | -------- |
| 1x     | 0.648          | 0.712      | 0.750   | 0.689           | 0.763       | 0.776    |
| 2x     | 0.861          | 0.899      | 0.930   | 0.884           | 0.944       | 0.949    |
| 4x     | 0.964          | 0.972      | 0.986   | 0.971           | 0.991       | 0.993    |
| 8x     | 0.991          | 0.988      | 0.997   | 0.996           | 0.999       | 0.999    |
| 16x    | 0.998          | 1.000      | 1.000   | 1.000           | 1.000       | 1.000    |


---

## Synthetic SPANN / Centroid Recall

This measures centroid selection recall: what fraction of the true top-K neighbors reside in the probed clusters. Centroids are quantized with 1-bit RaBitQ relative to a global centroid (centroid-of-centroids), matching the production quantized HNSW pipeline. Centroid search is brute-force over quantized codes (isolating quantization error from HNSW graph approximation).

The gap without reranking (`centroid_rerank=1x`) is at most 1.4% (0.895 vs 0.909 at
nprobe=64) and is consistently closed by 2x reranking.


| nprobe | centroid_rerank | centroid_recall | centroid_recall_ceiling |
| ------ | --------------- | --------------- | ----------------------- |
| 16     | 1x              | 0.743           | 0.754                   |
| 16     | 2x              | 0.755           | 0.754                   |
| 16     | 4x              | 0.754           | 0.754                   |
| 32     | 1x              | 0.826           | 0.830                   |
| 32     | 2x              | 0.833           | 0.830                   |
| 32     | 4x              | 0.830           | 0.830                   |
| 64     | 1x              | 0.895           | 0.909                   |
| 64     | 2x              | 0.904           | 0.909                   |
| 64     | 4x              | 0.909           | 0.909                   |
| 128    | 1x              | 0.944           | 0.953                   |
| 128    | 2x              | 0.950           | 0.953                   |
| 128    | 4x              | 0.953           | 0.953                   |


Benchmark data from `cargo bench -p chroma-index --bench quantization_recall_ivf -- --size 1000000`
(cohere_wiki, N=1M, 1000 clusters via KMeans, K=10, 1-bit data, 1-bit centroids,
r6i.8xlarge). Full raw output in `saved_benchmarks/recall_ivf_r6i.8xlarge.txt`.

**centroid_recall** = fraction of true top-K in the nprobe clusters selected by the
quantized centroid pipeline (quantized search for `nprobe * centroid_rerank` candidates,
then exact-distance rerank to nprobe). **centroid_recall_ceiling** = same metric using
exact centroid distance (no quantization) -- the maximum recall achievable at this nprobe.

**Findings:** Centroid quantization error is small. At every nprobe, `centroid_rerank=2x`
is sufficient to close the gap between quantized and exact centroid recall completely

---

## Quantized KMeans Clustering Recall

This measures how much end-to-end recall degrades when KMeans uses quantized
code-vs-code distances instead of exact f32 distances for cluster assignment.
Centroid computation still uses raw f32 vectors; only the vector assignment step is approximate.

**Findings:** At 1M vectors, quantized KMeans produces clusters of comparable quality
to exact KMeans. 1-bit KMeans shows a modest degradation of up to 0.9% end-to-end
recall (0.931 vs 0.922 at nprobe=128), with smaller differences at lower nprobes.
4-bit KMeans slightly outperforms exact in this run (+0.6--1.6%), likely due to KMeans
converging to a different (better) local optimum rather than a systematic advantage.

**End-to-end recall** (centroid_rerank=2x, vector_rerank=4x):


| nprobe | exact KMeans | 4-bit KMeans | 1-bit KMeans |
| ------ | ------------ | ------------ | ------------ |
| 16     | 0.751        | 0.757        | 0.741        |
| 32     | 0.820        | 0.826        | 0.816        |
| 64     | 0.902        | 0.908        | 0.891        |
| 128    | 0.931        | 0.947        | 0.922        |


**Centroid recall ceiling** (exact centroid search at nprobe -- reflects clustering quality):


| nprobe | exact KMeans | 4-bit KMeans | 1-bit KMeans |
| ------ | ------------ | ------------ | ------------ |
| 16     | 0.755        | 0.768        | 0.750        |
| 32     | 0.829        | 0.840        | 0.830        |
| 64     | 0.913        | 0.927        | 0.912        |
| 128    | 0.950        | 0.972        | 0.948        |


Benchmark data from `cargo bench -p chroma-index --bench quantization_recall_ivf -- --size 1000000`
with `--cluster-bits 1`, `--cluster-bits 4`, and no flag (exact).
(cohere_wiki, N=1M, 1000 clusters, K=10, 1-bit data, 1-bit centroids, r6i.8xlarge).
Full raw output in `saved_benchmarks/recall_ivf_1M_quantized_clustering_k10.txt`.

## Synthetic Index - Reranking with both 1-bit and 4-bit centroids

[two_stage_rerank.txt](saved_benchmarks/two_stage_rerank.txt)

100k data vectors, 100 queries, 316 clusters, no USearch, cohere_wiki dataset.

Summary: minimum fp_fetched to reach target recall (reranked)


| target recall | 4bit | 1bit | 1bit->4bit(x4) | 1bit->4bit(x8) | 1bit->4bit(x16) | 1bit->4bit(x32) | 1bit->4bit(x64) |
| ------------- | ---- | ---- | -------------- | -------------- | --------------- | --------------- | --------------- |
| 0.90          | 10   | 20   | 10             | 10             | 10              | 10              | 10              |
| 0.92          | 10   | 40   | 10             | 10             | 10              | 10              | 10              |
| 0.95          | 10   | 40   | 20             | 10             | 10              | 10              | 10              |
| 0.97          | 20   | 40   | 20             | 20             | 20              | 20              | 20              |
| 0.99          | -    | -    | -              | -              | -               | -               | -               |


Pipeline descriptions:

- `4bit`: Score all vectors with 4-bit codes -> top R -> exact rerank
- `1bit`: Score all vectors with 1-bit codes -> top R -> exact rerank
- `1bit->4bit(xM)`: Score all with 1-bit -> top k*M -> rescore with 4-bit -> top R -> exact rerank

Sources:

- [two_stage_rerank.txt](saved_benchmarks/two_stage_rerank.txt)

# SPANN

## Performance

### 1bit vs 4bit

Dataset: wikipedia-en (1024 dims)
4bit: cargo bench -p chroma-index --bench quantized_spann -- --dataset wikipedia-en --checkpoint 5 --threads 16 --data-bits 4 --centroid-bits 4
1bit: cargo bench -p chroma-index --bench quantized_spann -- --dataset wikipedia-en --checkpoint 10 --threads 16 --data-bits 1 --centroid-bits 1

=== Cluster Statistics ===


| Quant | CP  | Centroids | Min | Max | Median | P90 | P99 | Avg   | Std   |
| ----- | --- | --------- | --- | --- | ------ | --- | --- | ----- | ----- |
| 4bit  | 5   | 27.6K     | 0   | 512 | 390    | 489 | 511 | 372.3 | 102.1 |
| 1bit  | 5   | 27.7K     | 0   | 512 | 387    | 489 | 511 | 370.6 | 101.8 |


=== Task Counts ===


| Quant | CP  | add   | navigate | register | spawn | scrub  | split | merge | reassign | drop | load   | load_raw | quantize | search |
| ----- | --- | ----- | -------- | -------- | ----- | ------ | ----- | ----- | -------- | ---- | ------ | -------- | -------- | ------ |
| 4bit  | 5   | 1.00M | 2.94M    | 2.11M    | 11.0K | 190.8K | 5.5K  | 28    | 1.94M    | 5.6K | 190.8K | 11.0K    | 7.01M    | 0      |
| 1bit  | 5   | 1.00M | 3.05M    | 2.08M    | 11.0K | 190.3K | 5.6K  | 28    | 2.05M    | 5.6K | 190.3K | 11.1K    | 6.94M    | 0      |


=== Task Total Time ===


| Quant | CP  | add      | navigate | register | spawn  | scrub  | split    | merge | reassign | drop    | load  | load_raw | quantize | search | raw_pts | raw/pt  |
| ----- | --- | -------- | -------- | -------- | ------ | ------ | -------- | ----- | -------- | ------- | ----- | -------- | -------- | ------ | ------- | ------- |
| 4bit  | 5   | 2583.79s | 1309.61s | 304.06s  | 63.41s | 21.22s | 2218.35s | 3.61s | 1442.55s | 269.02s | 4.43s | 381.57s  | 472.92s  | 0ns    | 2.28M   | 167.2µs |
| 1bit  | 5   | 2046.80s | 1270.90s | 14.06s   | 22.49s | 17.65s | 1779.57s | 1.89s | 1214.73s | 272.04s | 1.77s | 396.20s  | 15.68s   | 0ns    | 2.31M   | 171.2µs |


=== Task Avg Time ===


| Quant | CP  | add    | navigate | register | spawn  | scrub   | split    | merge    | reassign | drop    | load   | load_raw | quantize | search |
| ----- | --- | ------ | -------- | -------- | ------ | ------- | -------- | -------- | -------- | ------- | ------ | -------- | -------- | ------ |
| 4bit  | 5   | 2.58ms | 444.7µs  | 143.9µs  | 5.77ms | 111.2µs | 401.29ms | 128.89ms | 742.0µs  | 48.42ms | 23.2µs | 34.62ms  | 67.4µs   | -      |
| 1bit  | 5   | 2.05ms | 416.2µs  | 6.8µs    | 2.04ms | 92.8µs  | 320.12ms | 67.53ms  | 591.5µs  | 48.69ms | 9.3µs  | 35.84ms  | 2.3µs    | -      |


=== Indexing Summary ===
Total vectors: 5.00M
Total time: 14.3m
Overall throughput: 5814 vec/s

=== Recall Summary ===


| Quantization | CP  | Vectors | Index | Commit | Queries | nprobe=16 R@10 R@100 ms/query | nprobe=32 R@10 R@100 ms/query | nprobe=64 R@10 R@100 ms/query | nprobe=128 R@10 R@100 ms/query | nprobe=256 R@10 R@100 ms/query |
| ------------ | --- | ------- | ----- | ------ | ------- | ----------------------------- | ----------------------------- | ----------------------------- | ------------------------------ | ------------------------------ |
| 4bit         | 5   | 5.00M   | 3.0m  | 14.76s | 100     | 0.77 0.72 20ms                | 0.84 0.80 21ms                | 0.88 0.86 33ms                | 0.90 0.89 51ms                 | 0.93 0.91 85ms                 |
| 1bit         | 5   | 5.00M   | 2.3m  | 7.47s  | 100     | 0.75 0.62 12ms                | 0.81 0.68 12ms                | 0.87 0.72 20ms                | 0.91 0.75 36ms                 | 0.93 0.76 67ms                 |


# USearch

## Parallelism

### Our global lock

We have a global read/write lock on the USearch index. Without it we get a 1.3x speedup in throughput.


| lock? | CP  | add    | navigate | register | spawn  | scrub  | split    | merge   | reassign | drop    | load  | load_raw | quantize | search | raw_add | raw_rm | q_add   | q_rm  |
| ----- | --- | ------ | -------- | -------- | ------ | ------ | -------- | ------- | -------- | ------- | ----- | -------- | -------- | ------ | ------- | ------ | ------- | ----- |
| yes   | 5   | 2.05ms | 416.2µs  | 6.8µs    | 2.04ms | 92.8µs | 320.12ms | 67.53ms | 591.5µs  | 48.69ms | 9.3µs | 35.84ms  | 2.3µs    | -      |         |        |         |       |
| no    | 5   | 1.85ms | 370.2µs  | 6.6µs    | 1.55ms | 95.3µs | 292.97ms | 52.95ms | 528.5µs  | 46.76ms | 9.5µs | 34.75ms  | 2.2µs    | -      | 980.2µs | 7.8µs  | 563.5µs | 7.5µs |


overall throughput: 7476 vec/s vs 5800 vec/s (1.3x speedup) mostly due to faster split (-30ms) and merge (-15ms)

cargo bench -p chroma-index --bench quantized_spann -- --dataset wikipedia-en --checkpoint 1 --threads 16 --data-bits 1 --centroid-bits 1

### Usearch global lock

Usearch also has a global lock internally. If we chose to fork Usearch and make this lock more granular, we could see an additional speedup.


---

## Performance

### Note: USearch ef/k coupling

USearch increases the beam width when `k > ef_search`:

```cpp
std::size_t expansion = (std::max)(config.expansion, wanted);
```

The original HNSW paper (Malkov & Yashunin, Algorithm 5) treats `ef` and `K` as independent parameters: `ef` controls beam width (search effort), `K` controls how many results to extract from the `ef` candidates. USearch conflates them, so requesting k=200 with ef_search=128 silently widens the beam to 200.

This means rerank sweep rows (2x, 4x, etc.) show inflated latency -- each row runs a progressively wider search, not just extracts more from the same candidate set. The 1x row (k <= ef_search) is the true ef_search performance.

Tested decoupling (using static `expansion = config.expansion`): recall plateaus at the 2x row since all rerank factors return the same ef=128 candidates. Latency becomes flat as expected. However, the coupling is the right default -- for SPANN we control both k and ef, so we can set ef appropriately rather than relying on the automatic widening.

### 1-bit vs 4-bit

=== USearch SPANN Profile Benchmark ===
Dim: 1024 | Metric: L2 | Centroid bits: 4 | ef_search: 128 | Threads: 32
Initial centroids: 1.00M | Data vectors: 1.00M | Queries: 200
Load profile per data vector: 3.05 navigates, 0.0114 spawns, 0.0057 drops

Task Counts (identical for both):


| navigate | spawn | drop |
| -------- | ----- | ---- |
| 3.05M    | 11.5K | 5.6K |


Task Total Time:


| bits | navigate | spawn  | drop   | wall   |
| ---- | -------- | ------ | ------ | ------ |
| 1    | 28.9m    | 13.51s | 22.82s | 56.35s |
| 4    | 166.8m   | 1.4m   | 41.01s | 5.4m   |


Task Avg Time:


| bits | navigate | spawn  | drop   |
| ---- | -------- | ------ | ------ |
| 1    | 568.0µs  | 1.17ms | 4.05ms |
| 4    | 3.28ms   | 7.26ms | 7.33ms |


### Navigate() using 1 bit quantized centroids

Overall 1.8x speedup: 10597 vec/s vs 5814 vec/s

- Top items:
  - 41% faster adds (1.22ms vs 2.05ms),
  - 41% faster split (219.72ms vs 320.12ms),
  - 22% faster merge (55.50ms vs 67.53ms)

=== Task Avg Time ===


| nav   | CP  | add    | navigate | register | spawn  | scrub  | split    | merge   | reassign | drop    | load  | load_raw | quantize | search | raw_add | raw_rm  | q_add   | q_rm    |
| ----- | --- | ------ | -------- | -------- | ------ | ------ | -------- | ------- | -------- | ------- | ----- | -------- | -------- | ------ | ------- | ------- | ------- | ------- |
| quant | 5   | 1.23ms | 122.1µs  | 5.2µs    | 1.79ms | 77.3µs | 219.72ms | 55.50ms | 221.0µs  | 42.59ms | 8.3µs | 37.08ms  | 1.9µs    | -      | 1.22ms  | 114.4µs | 572.5µs | 379.6µs |
| full  | 5   | 2.05ms | 416.2µs  | 6.8µs    | 2.04ms | 92.8µs | 320.12ms | 67.53ms | 591.5µs  | 48.69ms | 9.3µs | 35.84ms  | 2.3µs    | -      |         |         |         |         |


### Reranking

Centroids: 1.00M | Queries: 100 | Dim: 1024 | Metric: L2 | Centroid bits: 1 | ef_search: 128

=== Rerank Sweep ===


| -0.060,-0.050) 1317 | ▉Rerank | Fetch (k) | Recall@10 | Recall@100 | Avg lat | search  | fetch   | rerank |
| ------------------- | ------- | --------- | --------- | ---------- | ------- | ------- | ------- |
| 1x                  | 100     | 84.75%    | 50.88%    | 383.3µs    | 383.3µs | 0ns     | 0ns     |
| 2x                  | 200     | 90.50%    | 68.18%    | 647.6µs    | 519.8µs | 20.2µs  | 107.6µs |
| 4x                  | 400     | 95.70%    | 81.98%    | 1.25ms     | 1.00ms  | 39.1µs  | 209.8µs |
| 8x                  | 800     | 98.20%    | 91.25%    | 2.39ms     | 1.90ms  | 71.3µs  | 418.1µs |
| 16x                 | 1600    | 99.20%    | 96.07%    | 4.82ms     | 3.80ms  | 139.1µs | 880.5µs |


`cargo bench -p chroma-index --bench usearch_rerank -- --dataset wikipedia-en --centroid-bits 1 --initial-centroids 1000000`
[usearch_rerank_1bit.txt](saved_benchmarks/usearch_rerank_1bit.txt)

## Thread scaling

Using usearch only benchmark. (usearch_spann_profile)
`cargo bench -p chroma-index --bench usearch_spann_profile -- --dataset wikipedia-en --centroid-bits 1 --initial-centroids 1000000 --threads <threads> --data-vectors 1000000`


| threads | navigate | spawn   | drop   |
| ------- | -------- | ------- | ------ |
| 1       | 218.2µs  | 6.59ms  | 3.39ms |
| 4       | 221.8µs  | 682.6µs | 3.56ms |
| 8       | 264.4µs  | 788.3µs | 3.57ms |
| 16      | 356.7µs  | 933.1µs | 3.74ms |
| 32      | 568.0µs  | 1.17ms  | 4.05ms |


Scaling is poor: 4x threads yields 2.6x navigate latency, 1.7x spawn latency. Four layers of lock contention stack up:

1. **Rust `RwLock**` -- `Arc<RwLock<usearch::Index>>` wraps all operations. Spawns/drops take exclusive locks and block all concurrent navigates. Removing this lock gave 10-18% faster navigate and 24-33% faster spawn with no recall impact (see `usearch_concurrency_findings.md`).
2. `**global_mutex_**` (USearch `index.hpp`) -- A `std::mutex` protecting `max_level`_ and `entry_slot_`. Every `add()` holds it exclusively, blocking all concurrent `add()` and `search()` calls. This is the single biggest bottleneck because navigate dominates runtime and every spawn serializes against all navigates.
3. `**slot_lookup_mutex_**` (USearch `index_dense.hpp`) -- A `std::shared_mutex` protecting the key-to-slot hash map. `add()`/`remove()` take exclusive locks; `search()` takes shared. Creates write-side contention that blocks searches during spawns/drops.
4. `**available_threads_mutex_**` (USearch `index_dense.hpp`) -- A `std::mutex` guarding a fixed-size thread-slot pool (size = `hardware_concurrency()`). Acquired on every `search()` and `add()` call, creating a serialization point even for pure-read workloads.

Improvement options (by estimated impact):

1. **Atomic entry point** -- Replace `global_mutex_` with `std::atomic` for `max_level_`/`entry_slot_`. Searches do acquire loads (zero contention); adds use CAS (contention only on the rare new-max-level event). Eliminates the biggest serialization point.
2. **Remove Rust `RwLock`** -- USearch already has internal thread safety; the outer lock is redundant.
3. **Thread-local context pool** -- Cache thread slot IDs in TLS instead of acquiring `available_threads_mutex_` on every call.
4. **Disable key lookups** -- SPANN tracks membership externally; disabling `enable_key_lookups` eliminates `slot_lookup_mutex_`.
5. **Double-buffered indices** -- Frozen read index for navigate (zero locking), separate write index for spawns/drops, merge during commit.

---

# Appendix

## Sources of performance differences between r6i.8xlarge and MacBook Pro M1

### Decode width and instruction window

The M1's Firestorm performance cores are 8-wide decode with a reorder buffer of ~630 entries. Ice Lake Sunny Cove cores are 5-wide decode with ~352 ROB entries. The wider decode means M1 can dispatch more instructions per cycle, and the larger ROB means it can look further ahead to find independent work. This shows up most in operations with mixed integer/FP work and complex data dependencies -- like QuantizedQuery::new (min/max reduction + float-to-int quantization + bit-plane scatter), where M1 is 3.9x faster. Ice Lake simply cannot keep as many operations in flight to hide latencies in these dependency chains.

### FP/SIMD execution ports

M1 has 4 NEON pipes, each 128 bits wide (4 x 128 = 512 bits of FP throughput per cycle). Ice Lake has 2 AVX-512 FMA units, each 512 bits wide (2 x 512 = 1024 bits per cycle). For a pure dot product (simsimd_dot), AVX-512 processes 16 f32s per FMA instruction and does a fused multiply-add (2 FLOPs per element), while each NEON pipe handles 4 f32s. Even with 4 pipes, M1 tops out at 16 f32s/cycle without fused multiply-add, versus Ice Lake's 32 f32s/cycle with FMA. That is the 4.1x gap.
But AVX-512 has a cost: Ice Lake Xeon throttles its clock frequency under sustained AVX-512 workloads (the "AVX-512 downclocking" penalty). This partially erodes the theoretical 2x FLOP advantage, and it means mixed workloads that alternate between AVX-512 and scalar code pay a frequency transition penalty. M1's NEON runs at full clock speed always.

### Why M1 wins on signed_dot despite losing on simsimd_dot

signed_dot is not a pure FP dot product. It first does a table lookup to expand sign bits into +/-1.0 f32 values, then dots the result. The expansion step is a sequence of byte loads and stores with irregular access patterns -- it benefits from M1's wider issue width and much larger L1 data cache (128 KB vs 48 KB on Ice Lake). By the time the dot product starts, the expanded data is hot in L1 and the dot itself is over a small vector. AVX-512's raw throughput advantage does not have enough data to amortize over.

### Memory bandwidth per core

M1 uses unified memory (on-package LPDDR, 68 GB/s) shared across 4 performance cores. A single thread can consume a large fraction of total bandwidth. Ice Lake Xeon in r6i.8xlarge uses DDR4 across multiple channels (200 GB/s aggregate), but shared across 16 physical cores. Per-core available bandwidth is roughly 68/4 = 17 GB/s on M1 vs 200/16 = 12.5 GB/s on Ice Lake. M1 also has much lower memory latency because the DRAM is on-package rather than on DIMMs through a memory controller. This per-core bandwidth advantage explains why dq-exact (pure f32 distance, bandwidth-bound) is 2.2x faster on M1: it is limited by how fast one core can stream vectors from memory, not by compute.

### Why dq-bw/scan ties

AND+popcount over packed bit vectors is both compute-light and memory-light (128 bytes per 1024-dim code). The working set fits in L1 on both architectures, and the popcount instruction is single-cycle on both (NEON CNT, AVX-512 VPOPCNTDQ). Neither core is bottlenecked on decode width, ROB depth, or memory bandwidth -- the operation is just too simple and small to differentiate the architectures.

### Thread scaling

M1 has 4 performance + 4 efficiency cores, no hyperthreading. Ice Lake has 16 physical cores with 2-way SMT (32 vCPUs). For single-threaded benchmarks M1 wins on per-core performance, but r6i has 4x the physical core count for parallel workloads, which is what matters in production. That is why the thread scaling section only uses r6i data.

# Central Index Options
KD-Tree (bad for high dimensional vectors?)
IVF
IVF-HNSW
Hierarchical SPANN
ScaNN — Google's production ANN with tree + asymmetric quantization scoring

## Usearch 1 bit

navigate latency = 568.0µs (@32 threads)

[usearch_1bit.txt](saved_benchmarks/usearch_1bit.txt)

Potential improvements:
- Linear Thread Scaling - 2.6x speedup: 218µs (@1 thread)
  - No global lock - 1.3x speedup

## Usearch 1 bit - Improved Concurrency

Forked USearch (`@USearch/include/usearch/index.hpp`) with two changes:
1. Replaced `global_mutex_` with `std::atomic<level_t> max_level_` and `std::atomic<size_t> entry_slot_`. Searches and adds read atomically (no lock). The rare new-max-level update uses a mutex with double-checked locking.
2. Changed Rust `RwLock` usage: `add()` and `remove()` now take shared (read) locks instead of exclusive (write) locks. Only `reserve()` takes the exclusive lock.

Full details in [usearch_concurrency.md](usearch_concurrency.md)


### USearch only benchmark
Navigate latency at 32 threads dropped from 568us to 211.8us (2.68x improvement), recovering the single-thread baseline. Recall improved slightly (+2.3pp @10, +4.9pp @100).

`cargo bench -p chroma-index --bench usearch_spann_profile -- --dataset wikipedia-en --centroid-bits 1 --initial-centroids 1000000 --threads 32 --data-vectors 1000000`


| Metric             | Before (upstream) | After (forked) | Change       |
| ------------------ | ----------------- | -------------- | ------------ |
| Navigate avg (32t) | 568.0us           | 211.8us        | 2.68x faster |
| Phase 2 wall       | 56.35s            | 25.58s         | 2.2x faster  |
| Recall@10          | 92.80%            | 95.10%         | +2.3pp       |
| Recall@100         | 59.69%            | 64.58%         | +4.9pp       |
| Spawn avg          | 1.17ms            | 7.82ms         | 6.7x slower  |
| Drop avg           | 4.05ms            | 11.31ms        | 2.8x slower  |


Spawn/drop slowdown is expected: before, `add()`/`remove()` held an exclusive RwLock that blocked all concurrent navigates, effectively getting exclusive access. Now they run under shared locks competing with 32 threads of concurrent navigates for per-node locks and `available_threads_mutex_`. Wall time still dropped 2.2x because navigates dominate (3.05M navigates vs 17K spawns+drops).

[usearch_forked_1bit.txt](saved_benchmarks/usearch_forked_1bit.txt)

### Quantized SPANN benchmark

[quant_spann_1bit_forked_usearch.txt](saved_benchmarks/quant_spann_1bit_forked_usearch.txt)
vs
[quant_spann_1bit.txt](saved_benchmarks/quant_spann_1bit.txt)

At CP 5 (5M vectors, 16 threads, 1-bit data, 1-bit centroids):

| Metric       | Original USearch | Forked USearch | Delta       |
| ------------ | ---------------- | -------------- | ----------- |
| navigate avg | 416.2us          | 90.7us         | 4.6x faster |
| spawn avg    | 2.04ms           | 1.23ms         | 1.7x faster |
| drop avg     | 48.69ms          | 40.20ms        | 1.2x faster |

TODO also compare wall clock index build time and vectors/second

#### Specifics
Original needs to be updated - task counts don't match so Total Time is not comparable

=== Task Counts ===
| scenario | CP  | add   | navigate | register | spawn | scrub  | split | merge | reassign | drop | load   | load_raw | quantize | search | raw_add | raw_rm | q_add | q_rm |
| -------- | --- | ----- | -------- | -------- | ----- | ------ | ----- | ----- | -------- | ---- | ------ | -------- | -------- | ------ | ------- | ------ | ----- | ---- |
| original | 5   | 1.00M | 3.05M    | 2.08M    | 11.0K | 190.3K | 5.6K  | 28    | 2.05M    | 5.6K | 190.3K | 11.1K    | 6.94M    | 0      |
| forked   | 5   | 1.00M | 4.01M    | 0        | 11.0K | 191.8K | 5.6K  | 12    | 3.01M    | 5.6K | 191.8K | 11.0K    | 7.33M    | 0      | 11.0K   | 5.6K   | 11.0K | 5.6K |


=== Task Total Time ===
| scenario | CP  | add      | navigate | register | spawn  | scrub  | split    | merge | reassign | drop    | load  | load_raw | quantize | search | raw_pts | raw/pt  |
| -------- | --- | -------- | -------- | -------- | ------ | ------ | -------- | ----- | -------- | ------- | ----- | -------- | -------- | ------ | ------- | ------- |
| original | 5   | 2046.80s | 1270.90s | 14.06s   | 22.49s | 17.65s | 1779.57s | 1.89s | 1214.73s | 272.04s | 1.77s | 396.20s  | 15.68s   | 0ns    | 2.31M   | 171.2µs |
| forked   | 5   | 1806.45s | 363.32s  | 0ns      | 13.56s | 13.83s | 2044.78s | 2.01s | 773.86s  | 223.88s | 1.55s | 388.69s  | 13.22s   | 0ns    | 10.92s  | 46.72ms | 2.62s | 43.77ms | 2.52M | 154.3µs | 153.38s |

=== Task Avg Time ===
| scenario | CP  | add    | navigate | register | spawn  | scrub  | split    | merge    | reassign | drop    | load  | load_raw | quantize | search | raw_add | raw_rm | q_add   | q_rm  |
| -------- | --- | ------ | -------- | -------- | ------ | ------ | -------- | -------- | -------- | ------- | ----- | -------- | -------- | ------ | ------- | ------ | ------- | ----- |
| original | 5   | 2.05ms | 416.2µs  | 6.8µs    | 2.04ms | 92.8µs | 320.12ms | 67.53ms  | 591.5µs  | 48.69ms | 9.3µs | 35.84ms  | 2.3µs    | -      |
| forked   | 5   | 1.81ms | 90.7µs   | -        | 1.23ms | 72.1µs | 367.96ms | 167.16ms | 257.3µs  | 40.20ms | 8.1µs | 35.25ms  | 1.8µs    | -      | 991.4µs | 8.4µs  | 238.0µs | 7.9µs |


## Flat / Brute Force

navigate latency = 1M * distance code latency
                 = 1M * 13.745 µs
                 = 13.745 ms

```
distance_query/dc-1bit/scan
time:   [13.745 µs 13.773 µs 13.805 µs]
thrpt:  [19.896 GiB/s 19.942 GiB/s 19.982 GiB/s]
```
[performance_r6i.8xlarge.txt](performance_r6i.8xlarge.txt)

Potential improvements:
- Batching/SIMD
- Sharding (across 32 threads)

Why Flat Scan Breaks at High Dimensions
The bottleneck shifts from compute to memory bandwidth.
r6id-32xlarge memory bandwidth: ~380 GB/s (Ice Lake, 8-channel DDR4)

1M centroids @ 1024 dims @ float32 = 4 GB → 4GB / 380 GB/s = ~10ms
1M centroids @ 4096 dims @ float32 = 16 GB → 16GB / 380 GB/s = ~42ms

int8 quantized:
1M @ 1024 dims = 1 GB → ~2.6ms
1M @ 4096 dims = 4 GB → ~10ms
Memory bandwidth is not parallelizable across cores — it's a shared bus. Sharding across 64 cores doesn't help; you're scanning the same physical memory. Flat scan is ruled out at your dimension range for the centroid index.

## Hierarchical SPANN
