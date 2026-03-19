- [RaBitQ Implementation](#rabitq-implementation)
  - [Core Functions](#core-functions)
    - [Hot cache](#hot-cache)
    - [Cold Cache](#cold-cache)
    - [Thread Scaling](#thread-scaling)
  - [Primitives](#primitives)
  - [Rerank / Error (+ latency)](#rerank--error--latency)
- [Comparing Different Central Indices](#comparing-different-central-indices)
  - [Summary](#summary)
    - [Usearch Thread Scaling](#usearch-thread-scaling)
  - [Legend:](#legend)
  - [Notes](#notes)
- [E2E](#e2e)

# RaBitQ Implementation

- **distance_code** -- estimate distance between two stored codes (code vs code). Uses Hamming distance on packed sign bits.
- **distance_query** -- estimate distance from a stored code to a query vector. 4-bit (`dq-4f`) uses an f32 query vector; 1-bit (`dq-bw`) uses a 4-bit `QuantizedQuery`
- **quantize** -- encode a data vector into a quantized code (4-bit ray-walk or 1-bit sign extraction).
- **quantize_query** -- build a `QuantizedQuery` from a query residual (min/max, quantize elements, decompose into bit planes).

## Core Functions

[../../../benches/vector/quantization.rs](../../../benches/vector/quantization.rs)

Tested on r6i.8xlarge

### Hot cache
Same pair/vector every call. Function latency should be comparable to the total latency of its constituent primitives (below).

| Function       | Full Precision                               | 4-bit                                         | 1-bit                                          | Speedup (4b vs 1b) |
| -------------- | -------------------------------------------- | --------------------------------------------- | ---------------------------------------------- | ------------------ |
| distance_code  | --                                           | 575 ns, 1.70 GiB/s (distance/dc-4bit/1024)    | 38.4 ns, 6.98 GiB/s (distance/dc-1bit/1024)    | 15x                |
| distance_query | 73.67 ns, 103.56 GiB/s (distance/dq-fp/1024) | 330 ns, 13.0 GiB/s (distance/dq-4f/1024)      | 18.1 ns, 218 GiB/s (distance/dq-bw/1024)       | 18x                |
| quantize data  | --                                           | 51.8 µs, 151 MiB/s (quantize/quant-4bit/1024) | 1.07 µs, 7.15 GiB/s (quantize/quant-1bit/1024) | ~49x               |
| quantize query | --                                           | N/A                                           | 4.77 µs, 819 MiB/s (quantize/quant-query/1024) | --                 |

### Cold Cache
Scan of 100k codes/queries to simulate a cluster scan workload. Per-call average.
Because this is a scan, codes are not cached and we expect the latency to be higher than the hot cache variant.

| Function       | Full Precision                                     | 4-bit                                               | 1-bit                                               | Speedup (4b vs 1b) |
| -------------- | -------------------------------------------------- | --------------------------------------------------- | --------------------------------------------------- | ------------------ |
| distance_code  | --                                                 | 620 ns, 1.57 GiB/s (distance_scan/dc-4bit/scan)     | 20.1 ns, 13.3 GiB/s (distance_scan/dc-1bit/scan)    | 31x                |
| distance_query | 319.29 ns, 11.947 GiB/s (distance_scan/dq-fp/scan) | 365 ns, 1.33 GiB/s (distance_scan/dq-4f/scan)       | 25.5 ns, 5.26 GiB/s (distance_scan/dq-bw/scan)      | 14x                |
| quantize data  | --                                                 | 82.6 µs, 94.6 MiB/s (quantize_scan/quant-4bit/1024) | 1.04 µs, 7.33 GiB/s (quantize_scan/quant-1bit/1024) | ~79x               |
| quantize query | --                                                 | N/A                                                 | 4.80 µs, 815 MiB/s (quantize_scan/quant-query/1024) | --                 |

Note. 4-bit is similar to FP because the 4-bit distance query function compares to a full precision query vector.

[performance_r6i.8xlarge.txt](performance_r6i.8xlarge.txt)

### Thread Scaling

[thread_scaling_r6i.8xlarge.txt](thread_scaling_r6i.8xlarge.txt)

## Primitives

| Function                 | Primitive                 | Benchmark Name                                              | What is tested           | Latency   | Throughput   |
| ------------------------ | ------------------------- | ----------------------------------------------------------- | ------------------------ | --------- | ------------ |
| distance_code            | simsimd hamming           | distance_code/dc-1bit/simsimd_hamming/1024                  | Each call. Scan 1M codes | 11.984    | 22.382 GiB/s |
| distance_quantized_query | AND+popcount [B1+B2]      | primitives/dq-bw/and_popcount/interleaved_chunks/1024       |                          | 13.418 ns | 44.422 GiB/s |
| quantize                 | fused reductions          | primitives/quant-1bit/fused_reductions/1024                 |                          | 675.80 ns | 11.289 GiB/s |
| quantize_query           | bit_plane_decompose [P4+] | primitives/quant-query/bit_plane_decompose/byte_chunks/1024 |                          | 1.2790 µs | 2.9827 GiB/s |


| Function             | Benchmark                 | 1 thread            | 16 threads         | 32 threads          | 1->16 | 16->32 (HT) |
| -------------------- | ------------------------- | ------------------- | ------------------ | ------------------- | ----- | ----------- |
| quantize 4-bit       | thread_scaling/quant-4bit | 84.1 ms, 47.6 MiB/s | 6.94 ms, 576 MiB/s | 5.06 ms, 790 MiB/s  | 12.1x | 1.37x       |
| quantize 1-bit       | thread_scaling/quant-1bit | 1.19 ms, 3.28 GiB/s | 342 µs, 11.4 GiB/s | 84.6 µs, 46.2 GiB/s | 3.5x  | 4.04x       |
| distance_query 4-bit | thread_scaling/dq-4f      | 2.93 ms, 1.50 GiB/s | 232 µs, 19.0 GiB/s | 157 µs, 28.0 GiB/s  | 12.6x | 1.48x       |
| distance_query 1-bit | thread_scaling/dq-bw      | 4.83 ms, 934 MiB/s  | 719 µs, 6.13 GiB/s | 194 µs, 22.7 GiB/s  | 6.7x  | 3.70x       |

## Rerank / Error (+ latency)

1 Bit vs 4 Bit vs FP (distance query)

k=100, vectors=1M, 1 thread, 1k queries/samples

| Metric           | [FP](saved_benchmarks/flat_full_precision.txt) | [4 Bit](saved_benchmarks/flat_4bit.txt) | [1 Bit](saved_benchmarks/flat_1bit.txt) |
| ---------------- | ---------------------------------------------- | --------------------------------------- | --------------------------------------- |
| Navigate latency | 300.34ms                                       | 775.79ms                                | 23.38ms                                 |
| Recall@100       | 100%                                           | 89.94%                                  | 95.56% (Rerank=16x)                     |

4 bit is slower than FP because 4bit distance query is slower than fp distance query (see below), which is because 4bit distance query uses a full precision query vector and it's probably not as optimized as the library dot product for full precision vectors.


# Comparing Different Central Indices

What central index will give us the fastest index build times?

## Summary

**Benchmarks**


| Index                                   | Navigate @1M Centroids                                         | Navigate During Build @1M Centroids                                                | Thread Scaling | SPANN Navigate @27k Centroids              |
| --------------------------------------- | -------------------------------------------------------------- | ---------------------------------------------------------------------------------- | -------------- | ------------------------------------------ |
| Flat - 1 bit, global lock               | [26.49ms (R=95.56%, RR=16x)](saved_benchmarks/flat_1bit.txt)   | [43.12ms (R=95.56%, RR=16x)](saved_benchmarks/flat_1bit.txt)                       | Linear         | ?                                          |
| USearch - 1 bit                         | [1.70ms (R=94.09%, RR=16x)](saved_benchmarks/usearch_1bit.txt) | [2.44ms (R=94.34%, RR=16x)](saved_benchmarks/usearch_1bit.txt)                     | O(linear)      | [?](saved_benchmarks/quant_spann_1bit.txt) |
| Usearch - 4 bit                         | [5.03ms (R=95.30% RR=8x)](saved_benchmarks/usearch_4bit.txt)   | [6.12ms (R=95.45%, RR=8x)](saved_benchmarks/usearch_4bit.txt)                      | ?              | -                                          |
| Hierarchical SPANN - 1 bit, global lock | [?](saved_benchmarks/hierarchical_centroid_profile_1bit.txt)   | [8.27ms (R=61.5%, RR=2x)](saved_benchmarks/hierarchical_centroid_profile_1bit.txt) | Linear         | ?                                          |

### Usearch Thread Scaling

Navigate latency with synthetic workload (nprobe=32, rerank=2x):

| Threads | Navigate | Search  | Rerank | Wall clock time |
| ------- | -------- | ------- | ------ | --------------- |
| 1       | 282.0µs  | 246.2µs | 35.7µs | 8.85s           |
| 16      | 305.0µs  | 268.9µs | 36.0µs | 637.24ms        |
| 32      | 351.8µs  | 313.5µs | 38.1µs | 472.43ms        |


Search-only navigate latency (nprobe=32, rerank=2x):

| Threads | Navigate | Search  | Rerank  |
| ------- | -------- | ------- | ------- |
| 1       | 373.8µs  | 313.4µs | 60.3µs  |
| 16      | 374.2µs  | 313.8µs | 60.3µs  |
| 32      | 374.6µs  | 314.1µs | 60.3µs  |

Navigate latency is flat across threads (~374µs) -- no contention since there are no concurrent writes. Recall is identical at all thread counts.



## Legend:

- Navigate @1M Centroids
  - Query latency over the index alone
  - k=100, 32 threads, >90% Recall@100, 10k data vectors/samples
  - 1M centroids
- Savigate During Build @1M Centroids
  - Uses synthetic SPANN workload (of inserting 1M data vectors) on the index alone, so intentionally produces contention between threads
  - 1M centroids
- SPANN: Navigate latency when inserting 1M data vectors
  - Uses full SPANN index.
  - ~27k centroids, 4M existing data vectors, insert 1M new data vectors
  - Uses the [quantized_spann.rs](../../../benches/quantized_spann.rs) benchmark.

## Notes

- Flat 1M, rerank 16x, rerank_avg is 1.25ms, which seems close enough to full precision distance latency (1600 comparisons * 300ns (fp dist) ~= 500us)

# E2E
