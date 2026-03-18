- [RaBitQ Implementation](#rabitq-implementation)
  - [Primitives](#primitives)
  - [Core Functions](#core-functions)
    - [r6i.8xlarge](#r6i8xlarge)
    - [1 Bit vs 4 Bit vs FP (distance query)](#1-bit-vs-4-bit-vs-fp-distance-query)
    - [Thread Scaling](#thread-scaling)
- [Comparing Different Central Indices](#comparing-different-central-indices)
  - [Summary](#summary)
  - [Legend:](#legend)
  - [Notes](#notes)
- [E2E](#e2e)

# RaBitQ Implementation

## Primitives

| Function                 | Primitive                    | Benchmark Name                                         | What is tested | Latency    | Throughput   |
| ------------------------ | ---------------------------- | ------------------------------------------------------ | -------------- | ---------- | ------------ |
| distance_code            | simsimd hamming              | distance_code/dc-1bit/simsimd_hamming/1024            | Each call. Scan 1M codes | 11.984  | 22.382 GiB/s |
| *distance_quantized_query | AND+popcount [B1+B2]         | primitives/dq-bw/and_popcount/interleaved_chunks/1024  |                | 13.310 ns  | 44.777 GiB/s |
| *quantize                 | fused reductions             | primitives/quant-1bit/fused_reductions/1024            |                | 669.43 ns  | 11.396 GiB/s |
| *quantize_query           | bit_plane_decompose [P4+]    | primitives/quant-query/bit_plane_decompose/byte_chunks/1024 |           | 1.2742 us  | 2.9934 GiB/s |

* OUTDATED: Need to inspect the test setup. Ensure it is measuring the function and not the whole scan.

## Core Functions

[../../../benches/vector/quantization.rs](../../../benches/vector/quantization.rs)

### r6i.8xlarge

Scan of 1M codes (and 1 query for distance_query functions). Take the average latency of each call.

| Function       | Benchmark                                                  | 4-bit               | 1-bit               | Speedup |
| -------------- | ---------------------------------------------------------- | ------------------- | ------------------- | ------- |
| distance_code  | distance_query/dc-4bit/scan vs distance_query/dc-1bit/scan | 549ns, 1.77 GiB/s   | 20.059 ns, 13.371 GiB/s | 27x     |
| distance_query | distance_query/dq-4f/scan vs distance_query/dq-bw/scan     | 381 ns, 1.28 GiB/s  | 29 ns, 4.6 GiB/s    | 13x     |
| quantize data  | quantize/quant-4bit/1024 vs quantize/quant-1bit/1024       | 43.2 ms, 92.5 MiB/s | 576 µs, 6.78 GiB/s  | ~75x    |
| quantize query | primitives/quant-query/full/1024                           | N/A                 | 2.21 µs, 1.73 GiB/s | --      |


[performance_r6i.8xlarge.txt](performance_r6i.8xlarge.txt)

### 1 Bit vs 4 Bit vs FP (distance query)

k=100, vectors=1000000, single thread, 1000 queries/samples

| Metric             | [FP](saved_benchmarks/flat_full_precision.txt) | [4 Bit](saved_benchmarks/flat_4bit.txt) | [1 Bit](saved_benchmarks/flat_1bit.txt) |
| ------------------ | ---------------------------------------------- | --------------------------------------- | --------------------------------------- |
| Navigate latency   | 236ms                                          | 822ms                                   | 298ms                                   |
| Recall@100         | 100%                                           | 90%                                     | 89% (Rerank=8x)                         |


### Thread Scaling

[thread_scaling_r6i.8xlarge.txt](thread_scaling_r6i.8xlarge.txt)


| Operation  | What it does                   | 1 thread            | 16 threads         | 32 threads         | 1->16 | 16->32 (HT) |
| ---------- | ------------------------------ | ------------------- | ------------------ | ------------------ | ----- | ----------- |
| quant-4bit | 4-bit data encode (ray-walk)   | 86.9 ms, 46 MiB/s   | 6.09 ms, 656 MiB/s | 4.54 ms, 880 MiB/s | 14.3x | 1.34x       |
| quant-1bit | 1-bit data encode (dual accum) | 1.17 ms, 3.35 GiB/s | 108 us, 36.1 GiB/s | 114 us, 34.2 GiB/s | 10.8x | **0.95x**   |
| dq-4f      | 4-bit code vs f32 query        | 3.48 ms, 1.27 GiB/s | 261 us, 16.9 GiB/s | 168 us, 26.2 GiB/s | 13.3x | 1.55x       |
| dq-float   | 1-bit code vs f32 query        | 2.94 ms, 1.38 GiB/s | 224 us, 18.1 GiB/s | 143 us, 28.3 GiB/s | 13.1x | 1.57x       |
| dq-bw      | 1-bit code vs QuantizedQuery   | 4.84 ms, 855 MiB/s  | 345 us, 11.7 GiB/s | 250 us, 16.1 GiB/s | 14.0x | 1.38x       |
| d-lut      | 1-bit code vs BatchQueryLuts   | 7.02 ms, 589 MiB/s  | 490 us, 8.24 GiB/s | 401 us, 10.1 GiB/s | 14.3x | 1.22x       |


# Comparing Different Central Indices

What central index will give us the fastest index build times?

## Summary

**Benchmarks**


| Index                                   | Navigate @1M Centroids                                                                  | Navigate During Build @1M Centroids                                                | Thread Scaling | SPANN Navigate @27k Centroids                                   |
| ---------------------------------------- | --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | -------------- | --------------------------------------------------------------- |
| Flat - 1 bit, global lock                | [26.49ms (R=95.56%, RR=16x)](saved_benchmarks/flat_1bit.txt)                           | [43.12ms (R=95.56%, RR=16x)](saved_benchmarks/flat_1bit.txt)                      | Linear         | ?                                                               |
| USearch - 1 bit                          | [1.68ms (R=94.09%, RR=16x)](saved_benchmarks/usearch_1bit.txt)                          | [1.99ms (R=94.34%, RR=16x)](saved_benchmarks/usearch_1bit.txt)                     | O(linear)      | [?](saved_benchmarks/quant_spann_1bit.txt)                      |
| Hierarchical SPANN - 1 bit, global lock  | [?](saved_benchmarks/hierarchical_centroid_profile_1bit.txt)                            | [8.27ms (R=61.5%, RR=2x)](saved_benchmarks/hierarchical_centroid_profile_1bit.txt) | Linear         | ?                                                               |
| Usearch - 4 bit|[5.08ms (R=95.30% RR=8x)](saved_benchmarks/usearch_4bit.txt)|[4.98ms (R=95.44%, RR=8x)](saved_benchmarks/usearch_4bit.txt)|?|-|


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
