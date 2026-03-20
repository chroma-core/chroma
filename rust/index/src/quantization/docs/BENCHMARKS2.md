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
  - [Usearch Raw Results](#usearch-raw-results)
- [SPANN Benchmarks 10, 50, 100M](#spann-benchmarks-10-50-100m)
  - [10M - wikipedia](#10m---wikipedia)
  - [50M - msmarco](#50m---msmarco)

# RaBitQ Implementation

- **distance_code** -- estimate distance between two stored codes (code vs code). Uses Hamming distance on packed sign bits.
- **distance_query** -- estimate distance from a stored code to a query vector. 4-bit (`dq-4f`) uses an f32 query vector; 1-bit (`dq-bw`) uses a 4-bit `QuantizedQuery`
- **quantize** -- encode a data vector into a quantized code (4-bit ray-walk or 1-bit sign extraction).
- **quantize_query** -- build a `QuantizedQuery` from a query residual (min/max, quantize elements, decompose into bit planes).

## Core Functions

[../../../benches/vector/quantization.rs](../../../benches/vector/quantization.rs)

Tested on r6id.8xlarge

### Hot cache
Same pair/vector every call. Function latency should be comparable to the total latency of its constituent primitives (below).

| Function       | Full Precision                               | 4-bit                                         | 1-bit                                          | Speedup (4b vs 1b) |
| -------------- | -------------------------------------------- | --------------------------------------------- | ---------------------------------------------- | ------------------ |
| distance_code  | --                                           | 575 ns, 1.70 GiB/s (distance/dc-4bit/1024)    | 14.0 ns, 19.090 GiB/s (distance/dc-1bit/1024)    | 15x                |
| distance_query | 73.67 ns, 103.56 GiB/s (distance/dq-fp/1024) | 295 ns, 14.60 GiB/s (distance/dq-4f/1024)     | 21.7 ns, 28.063 GiB/s (distance/dq-bw/1024)    | 10x                |
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

Latency and throughput is per batch of 1024 vectors.

| Function             | Benchmark                 | 1 thread            | 16 threads         | 32 threads          | 1->16 | 16->32 (HT) |
| -------------------- | ------------------------- | ------------------- | ------------------ | ------------------- | ----- | ----------- |
| quantize 4-bit       | thread_scaling/quant-4bit | 84.1 ms, 47.6 MiB/s | 6.94 ms, 576 MiB/s | 5.06 ms, 790 MiB/s  | 12.1x | 1.37x       |
| quantize 1-bit       | thread_scaling/quant-1bit | 1.19 ms, 3.28 GiB/s | 342 µs, 11.4 GiB/s | 84.6 µs, 46.2 GiB/s | 3.5x  | 4.04x       |
| distance_query 4-bit | thread_scaling/dq-4f      | 2.93 ms, 1.50 GiB/s | 232 µs, 19.0 GiB/s | 157 µs, 28.0 GiB/s  | 12.6x | 1.48x       |
| distance_query 1-bit | thread_scaling/dq-bw      | 4.83 ms, 934 MiB/s  | 719 µs, 6.13 GiB/s | 194 µs, 22.7 GiB/s  | 6.7x  | 3.70x       |

## Primitives

| Function                 | Primitive                 | Benchmark Name                                              | Latency   | Throughput   |
| ------------------------ | ------------------------- | ----------------------------------------------------------- | --------- | ------------ |
| distance_code            | simsimd hamming           | distance/dc-1bit/simsimd_hamming/1024                       | 5.6685 ns | 47.317 GiB/s |
| distance_quantized_query | AND+popcount [B1+B2]      | primitives/dq-bw/and_popcount/interleaved_chunks/1024       | 13.418 ns | 44.422 GiB/s |
| quantize                 | fused reductions          | primitives/quant-1bit/fused_reductions/1024                 | 675.80 ns | 11.289 GiB/s |
| quantize_query           | bit_plane_decompose [P4+] | primitives/quant-query/bit_plane_decompose/byte_chunks/1024 | 1.2790 µs | 2.9827 GiB/s |

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

| Threads | Navigate | Search  | Rerank |
| ------- | -------- | ------- | ------ |
| 1       | 373.8µs  | 313.4µs | 60.3µs |
| 16      | 374.2µs  | 313.8µs | 60.3µs |
| 32      | 374.6µs  | 314.1µs | 60.3µs |

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


## Usearch Raw Results

Uses canonical usearch (locks and all)

Usearch - 1M centroids, 10k data vectors, canonical usearch (locks and all)
cargo bench -p chroma-index --bench usearch_spann_profile --features chroma-index/usearch -- --dataset wikipedia-en --centroid-bits 1 --initial-centroids 1000000 --data-vectors 10000 --phase-2 --phase-3
Loading Wikipedia EN from HuggingFace Hub...
Loading 1.00M vectors from wikipedia-en (dim=1024)...

=== USearch SPANN Profile Benchmark ===
Dim: 1024 | Metric: L2 | Centroid bits: 1 | ef_search: 128 | Threads: [32]
Initial centroids: 1.00M | Data vectors: 10.0K | Queries: 200
Load profile per data vector: 3.05 navigates, 0.0114 spawns, 0.0057 drops

--- Phase 1: Using cached bootstrap from target/usearch_cache/bootstrap_WikipediaEn_1000000_L2_1.bin ---
  Computing ground truth (1000000 corpus, 200 queries, k=256, 32 threads)...

=== Phase 2: SPANN Workload (10.0K data vectors) ===
| Threads | nprobe | Rerank | R@10   | R@100  | navigate | search  | rerank  | rr_dist | rr_sort | rr_scored | rr_bytes | spawn   | drop    | wall     |
| ------- | ------ | ------ | ------ | ------ | -------- | ------- | ------- | ------- | ------- | --------- | -------- | ------- | ------- | -------- |
| 32      | 32     | 1x     | 71.10% | 49.20% | 626.0µs  | 625.9µs | 0ns     | 0ns     | 0ns     | 0.0       | 0B       | 1.66ms  | 4.42ms  | 625.98ms |
| 32      | 32     | 4x     | 86.25% | 78.08% | 794.9µs  | 705.6µs | 89.1µs  | 87.1µs  | 1.9µs   | 127.4     | 509.5KB  | 1.88ms  | 4.52ms  | 796.87ms |
| 32      | 32     | 8x     | 93.70% | 88.27% | 1.35ms   | 1.10ms  | 253.3µs | 249.1µs | 4.1µs   | 255.3     | 1021.1KB | 2.26ms  | 5.39ms  | 1.37s    |
| 32      | 32     | 16x    | 97.05% | 94.34% | 2.44ms   | 1.83ms  | 611.8µs | 600.8µs | 10.9µs  | 511.2     | 2.0MB    | 3.11ms  | 6.02ms  | 2.46s    |
| 32      | 64     | 1x     | 81.15% | 50.23% | 628.1µs  | 628.0µs | 0ns     | 0ns     | 0ns     | 0.0       | 0B       | 1.55ms  | 4.30ms  | 643.44ms |
| 32      | 64     | 4x     | 93.70% | 81.02% | 1.35ms   | 1.10ms  | 254.4µs | 250.2µs | 4.1µs   | 255.3     | 1021.2KB | 2.24ms  | 5.11ms  | 1.36s    |
| 32      | 64     | 8x     | 97.05% | 90.50% | 2.45ms   | 1.83ms  | 612.3µs | 601.3µs | 10.9µs  | 511.2     | 2.0MB    | 3.14ms  | 5.91ms  | 2.46s    |
| 32      | 64     | 16x    | 98.65% | 95.56% | 4.61ms   | 3.30ms  | 1.32ms  | 1.28ms  | 35.7µs  | 1023.1    | 4.0MB    | 5.07ms  | 7.76ms  | 4.72s    |
| 32      | 128    | 1x     | 86.25% | 51.21% | 631.3µs  | 631.2µs | 0ns     | 0ns     | 0ns     | 0.0       | 0B       | 1.74ms  | 4.36ms  | 746.41ms |
| 32      | 128    | 4x     | 97.05% | 83.04% | 2.45ms   | 1.84ms  | 612.6µs | 601.6µs | 10.9µs  | 511.2     | 2.0MB    | 3.16ms  | 6.14ms  | 2.51s    |
| 32      | 128    | 8x     | 98.65% | 91.83% | 4.62ms   | 3.30ms  | 1.32ms  | 1.29ms  | 35.6µs  | 1023.1    | 4.0MB    | 4.56ms  | 7.89ms  | 4.66s    |
| 32      | 128    | 16x    | 99.45% | 96.56% | 8.69ms   | 6.01ms  | 2.67ms  | 2.60ms  | 78.0µs  | 2047.0    | 8.0MB    | 7.72ms  | 10.69ms | 8.74s    |
| 32      | 256    | 1x     | 93.70% | 53.10% | 904.0µs  | 904.0µs | 0ns     | 0ns     | 0ns     | 0.0       | 0B       | 1.80ms  | 5.03ms  | 911.66ms |
| 32      | 256    | 4x     | 98.65% | 84.64% | 4.57ms   | 3.25ms  | 1.32ms  | 1.28ms  | 35.8µs  | 1023.1    | 4.0MB    | 4.68ms  | 7.52ms  | 4.56s    |
| 32      | 256    | 8x     | 99.45% | 92.95% | 8.72ms   | 6.04ms  | 2.67ms  | 2.60ms  | 77.9µs  | 2047.0    | 8.0MB    | 8.04ms  | 10.95ms | 8.80s    |
| 32      | 256    | 16x    | 99.85% | 97.33% | 17.57ms  | 12.16ms | 5.42ms  | 5.27ms  | 150.1µs | 4094.8    | 16.0MB   | 14.66ms | 17.36ms | 17.79s   |

=== Phase 3: Search-Only Recall (200 queries) ===
| Threads | nprobe | Rerank | R@10   | R@100  | navigate | search  | rerank  | rr_dist | rr_sort | rr_scored | rr_bytes |
| ------- | ------ | ------ | ------ | ------ | -------- | ------- | ------- | ------- | ------- | --------- | -------- |
| 32      | 32     | 1x     | 71.95% | 50.12% | 280.4µs  | 280.4µs | 0ns     | 0ns     | 0ns     | 0.0       | 0B       |
| 32      | 32     | 4x     | 86.00% | 77.95% | 469.2µs  | 351.9µs | 117.1µs | 115.2µs | 1.9µs   | 128.0     | 512.0KB  |
| 32      | 32     | 8x     | 93.20% | 87.98% | 896.6µs  | 659.9µs | 236.6µs | 232.6µs | 3.9µs   | 256.0     | 1.0MB    |
| 32      | 32     | 16x    | 96.75% | 94.09% | 1.70ms   | 1.23ms  | 467.9µs | 459.5µs | 8.4µs   | 512.0     | 2.0MB    |
| 32      | 64     | 1x     | 81.40% | 50.70% | 291.2µs  | 291.1µs | 0ns     | 0ns     | 0ns     | 0.0       | 0B       |
| 32      | 64     | 4x     | 93.20% | 80.84% | 895.5µs  | 659.6µs | 235.7µs | 231.8µs | 3.8µs   | 256.0     | 1.0MB    |
| 32      | 64     | 8x     | 96.75% | 90.30% | 1.68ms   | 1.22ms  | 468.6µs | 460.4µs | 8.1µs   | 512.0     | 2.0MB    |
| 32      | 64     | 16x    | 98.50% | 95.48% | 3.34ms   | 2.38ms  | 953.9µs | 920.9µs | 33.0µs  | 1024.0    | 4.0MB    |
| 32      | 128    | 1x     | 86.00% | 51.29% | 302.7µs  | 302.6µs | 0ns     | 0ns     | 0ns     | 0.0       | 0B       |
| 32      | 128    | 4x     | 96.75% | 82.95% | 1.68ms   | 1.21ms  | 469.0µs | 460.9µs | 8.1µs   | 512.0     | 2.0MB    |
| 32      | 128    | 8x     | 98.50% | 91.79% | 3.33ms   | 2.38ms  | 952.6µs | 920.3µs | 32.2µs  | 1024.0    | 4.0MB    |
| 32      | 128    | 16x    | 99.40% | 96.52% | 6.66ms   | 4.73ms  | 1.93ms  | 1.87ms  | 58.3µs  | 2048.0    | 8.0MB    |
| 32      | 256    | 1x     | 93.20% | 53.18% | 598.1µs  | 598.0µs | 0ns     | 0ns     | 0ns     | 0.0       | 0B       |
| 32      | 256    | 4x     | 98.50% | 84.62% | 3.31ms   | 2.35ms  | 956.7µs | 924.1µs | 32.5µs  | 1024.0    | 4.0MB    |
| 32      | 256    | 8x     | 99.40% | 92.93% | 6.66ms   | 4.74ms  | 1.92ms  | 1.87ms  | 56.0µs  | 2048.0    | 8.0MB    |
| 32      | 256    | 16x    | 99.85% | 97.33% | 14.04ms  | 10.05ms | 3.99ms  | 3.89ms  | 96.8µs  | 4096.0    | 16.0MB   |

=== Legend ===
nprobe   - number of nearest neighbors to retrieve per search
navigate - total navigate latency: search + rerank
search   - index.search() across the HNSW graph
rerank   - re-score candidates with exact f32 distance and sort
spawn    - add() a new centroid (from cluster split)
drop     - remove() a centroid (from cluster split/merge)
wall     - wall-clock time for the full SPANN simulation phase

--------------------------------

# SPANN Benchmarks 10, 50, 100M

Uses canonical usearch (locks and all)

```rust
SpannIndexConfig {
  // Write path parameters
  write_nprobe: Some(64),
  nreplica_count: Some(2),
  write_rng_epsilon: Some(8.0),
  write_rng_factor: Some(4.0),

  // Cluster maintenance
  split_threshold: Some(2048),
  merge_threshold: Some(512),
  reassign_neighbor_count: Some(32),

  // Commit-time parameters
  center_drift_threshold: Some(0.125),

  // HNSW parameters
  ef_construction: Some(256),
  ef_search: Some(128),
  max_neighbors: Some(24),

  quantize, // --data-bits
  centroid_bits, // --centroid-bits
  centroid_rerank_factor, // --centroid-rerank
  data_rerank_factor, // --data-rerank-factors

  // Other
  ..Default::default()
}
```

## 10M - wikipedia

nohup cargo bench -p chroma-index --bench quantized_spann  --features chroma-index/usearch -- --checkpoint 10  --data-bits 1 --centroid-bits 1 --centroid-rerank 16 --data-rerank-factors 1,2,4 --nprobes 32,64,128  --tmp-dir /mnt/data > quant_spann_1bit_wikipedia.txt 2>&1 &
=== QuantizedSpannIndexWriter Benchmark ===
Dataset: wikipedia-en (41.49M vectors, 1024 dims)
Metric: Euclidean | Checkpoints: 10 (1.00M vec/CP) | Threads: 32 | Data bits: 1 | Centroid bits: 1
Centroid rerank: 16x | Data rerank factors: [1, 2, 4] | nprobes: [32, 64, 128]
Total vectors to index: 10.00M

Checkpoint 1: 1.00M vec | load 11.68s | raw 11.46s | index 1.5m | commit 1.48s | 11351 vec/s
Checkpoint 2: 1.00M vec | load 8.76s | raw 11.50s | index 2.7m | commit 3.17s | 6122 vec/s
Checkpoint 3: 1.00M vec | load 8.64s | raw 11.49s | index 2.4m | commit 4.71s | 6857 vec/s
Checkpoint 4: 1.00M vec | load 8.75s | raw 11.60s | index 3.0m | commit 5.98s | 5582 vec/s
Checkpoint 5: 1.00M vec | load 8.75s | raw 11.64s | index 2.9m | commit 7.11s | 5685 vec/s
Checkpoint 6: 1.00M vec | load 8.89s | raw 11.80s | index 3.2m | commit 8.54s | 5272 vec/s
Checkpoint 7: 1.00M vec | load 8.85s | raw 11.80s | index 3.4m | commit 9.61s | 4867 vec/s
Checkpoint 8: 1.00M vec | load 57.25s | raw 11.61s | index 3.2m | commit 10.06s | 5223 vec/s
Checkpoint 9: 1.00M vec | load 1.0m | raw 11.64s | index 3.4m | commit 9.79s | 4936 vec/s
Checkpoint 10: 1.00M vec | load 1.0m | raw 11.70s | index 3.0m | commit 10.76s | 5486 vec/s

=== Cluster Statistics ===
| CP  | Centroids | Min | Max  | Median | P90  | P99  | Avg    | Std   |
| --- | --------- | --- | ---- | ------ | ---- | ---- | ------ | ----- |
| 1   | 1.4K      | 0   | 2048 | 1529   | 1951 | 2040 | 1415.1 | 494.0 |
| 2   | 2.9K      | 0   | 2048 | 1541   | 1958 | 2037 | 1451.2 | 461.7 |
| 3   | 4.3K      | 0   | 2048 | 1543   | 1963 | 2042 | 1455.4 | 457.3 |
| 4   | 5.7K      | 0   | 2048 | 1549   | 1958 | 2041 | 1453.6 | 456.6 |
| 5   | 7.1K      | 0   | 2048 | 1555   | 1965 | 2041 | 1464.3 | 450.7 |
| 6   | 8.5K      | 0   | 2048 | 1544   | 1958 | 2042 | 1454.4 | 451.1 |
| 7   | 9.8K      | 0   | 2048 | 1546   | 1959 | 2042 | 1453.7 | 452.8 |
| 8   | 11.1K     | 0   | 2048 | 1535   | 1961 | 2043 | 1444.4 | 457.6 |
| 9   | 12.4K     | 0   | 2048 | 1522   | 1953 | 2041 | 1413.5 | 485.4 |
| 10  | 13.3K     | 0   | 2048 | 1443   | 1943 | 2040 | 1311.9 | 551.6 |

=== Task Counts ===
| CP  | add   | navigate | nav_search | nav_fetch | nav_rerank | register | spawn | scrub | split | merge | reassign | drop | load  | load_raw | quantize | search | search_scan | search_load_cluster | search_load_raw | search_rerank | raw_add | raw_rm | q_add | q_rm |
| --- | ----- | -------- | ---------- | --------- | ---------- | -------- | ----- | ----- | ----- | ----- | -------- | ---- | ----- | -------- | -------- | ------ | ----------- | ------------------- | --------------- | ------------- | ------- | ------ | ----- | ---- |
| 1   | 1.00M | 3.08M    | 3.08M      | 3.08M     | 3.08M      | 0        | 2.9K  | 51.6K | 1.5K  | 0     | 2.27M    | 1.5K | 51.6K | 2.9K     | 6.98M    | 900    | 900         | 900                 | 900             | 900           | 2.9K    | 1.5K   | 2.9K  | 1.5K |
| 2   | 1.00M | 3.36M    | 3.36M      | 3.36M     | 3.36M      | 0        | 3.1K  | 52.3K | 1.7K  | 0     | 2.40M    | 1.7K | 52.3K | 3.1K     | 7.31M    | 900    | 900         | 900                 | 900             | 900           | 3.1K    | 1.7K   | 3.1K  | 1.7K |
| 3   | 1.00M | 3.32M    | 3.32M      | 3.32M     | 3.32M      | 0        | 3.2K  | 52.8K | 1.7K  | 2     | 2.34M    | 1.7K | 52.8K | 3.2K     | 7.35M    | 900    | 900         | 900                 | 900             | 900           | 3.2K    | 1.7K   | 3.2K  | 1.7K |
| 4   | 1.00M | 3.29M    | 3.29M      | 3.29M     | 3.29M      | 0        | 3.1K  | 52.0K | 1.7K  | 3     | 2.30M    | 1.7K | 52.0K | 3.1K     | 7.26M    | 900    | 900         | 900                 | 900             | 900           | 3.1K    | 1.7K   | 3.1K  | 1.7K |
| 5   | 1.00M | 3.21M    | 3.21M      | 3.21M     | 3.21M      | 0        | 3.0K  | 50.1K | 1.6K  | 4     | 2.22M    | 1.6K | 50.1K | 3.0K     | 7.13M    | 900    | 900         | 900                 | 900             | 900           | 3.0K    | 1.6K   | 3.0K  | 1.6K |
| 6   | 1.00M | 3.32M    | 3.32M      | 3.32M     | 3.32M      | 0        | 3.0K  | 51.7K | 1.6K  | 12    | 2.33M    | 1.6K | 51.7K | 3.0K     | 7.28M    | 900    | 900         | 900                 | 900             | 900           | 3.0K    | 1.6K   | 3.0K  | 1.6K |
| 7   | 1.00M | 3.22M    | 3.22M      | 3.22M     | 3.22M      | 0        | 2.8K  | 48.8K | 1.5K  | 19    | 2.23M    | 1.5K | 48.8K | 2.9K     | 7.06M    | 900    | 900         | 900                 | 900             | 900           | 2.8K    | 1.5K   | 2.8K  | 1.5K |
| 8   | 1.00M | 3.13M    | 3.13M      | 3.13M     | 3.13M      | 0        | 2.8K  | 48.8K | 1.5K  | 35    | 2.14M    | 1.5K | 48.8K | 2.9K     | 7.00M    | 900    | 900         | 900                 | 900             | 900           | 2.8K    | 1.5K   | 2.8K  | 1.5K |
| 9   | 1.00M | 3.30M    | 3.30M      | 3.30M     | 3.30M      | 0        | 3.0K  | 50.8K | 1.5K  | 71    | 2.35M    | 1.6K | 50.8K | 3.0K     | 7.39M    | 900    | 900         | 900                 | 900             | 900           | 3.0K    | 1.6K   | 3.0K  | 1.6K |
| 10  | 1.00M | 2.84M    | 2.84M      | 2.84M     | 2.84M      | 0        | 2.5K  | 42.2K | 1.3K  | 277   | 1.85M    | 1.6K | 42.2K | 2.7K     | 6.39M    | 900    | 900         | 900                 | 900             | 900           | 2.5K    | 1.6K   | 2.5K  | 1.6K |

=== Task Total Time ===
| CP  | add      | navigate | nav_search | nav_fetch | nav_rerank | register | spawn | scrub  | split    | merge    | reassign | drop     | load     | load_raw | quantize | search  | search_scan | search_load_cluster | search_load_raw | search_rerank | raw_add | raw_rm  | q_add | q_rm     | raw_pts | raw/pt | total   |
| --- | -------- | -------- | ---------- | --------- | ---------- | -------- | ----- | ------ | -------- | -------- | -------- | -------- | -------- | -------- | -------- | ------- | ----------- | ------------------- | --------------- | ------------- | ------- | ------- | ----- | -------- | ------- | ------ | ------- |
| 1   | 1896.95s | 1618.63s | 605.06s    | 399.25s   | 537.70s    | 0ns      | 2.57s | 6.08s  | 1623.83s | 0ns      | 1544.14s | 797.00ms | 1.77ms   | 422.9µs  | 13.09s   | 94.94s  | 21.52s      | 3.49s               | 66.70s          | 116.82ms      | 1.05s   | 16.43ms | 1.52s | 613.77ms | 0       | -      | 112.71s |
| 2   | 3427.67s | 2991.85s | 1395.51s   | 597.26s   | 881.11s    | 0ns      | 4.64s | 9.67s  | 3179.84s | 0ns      | 2934.41s | 110.47s  | 458.86ms | 154.65s  | 13.99s   | 114.50s | 29.42s      | 6.84s               | 73.95s          | 138.34ms      | 2.06s   | 26.09ms | 2.58s | 1.12s    | 0       | -      | 186.77s |
| 3   | 3890.34s | 3328.56s | 1711.38s   | 615.14s   | 885.34s    | 0ns      | 5.78s | 11.82s | 3669.80s | 618.44ms | 3301.18s | 189.72s  | 832.09ms | 269.27s  | 14.53s   | 138.76s | 30.89s      | 9.70s               | 93.44s          | 150.56ms      | 2.73s   | 35.20ms | 3.05s | 1.37s    | 0       | -      | 170.68s |
| 4   | 4233.48s | 3586.62s | 1922.28s   | 622.23s   | 925.27s    | 0ns      | 6.40s | 13.04s | 4044.55s | 751.28ms | 3590.08s | 245.65s  | 1.20s    | 351.39s  | 14.73s   | 165.38s | 36.31s      | 13.44s              | 110.19s         | 180.29ms      | 3.13s   | 24.90ms | 3.26s | 1.43s    | 0       | -      | 205.49s |
| 5   | 4390.28s | 3694.58s | 2042.64s   | 612.03s   | 922.80s    | 0ns      | 6.69s | 13.86s | 4026.40s | 888.58ms | 3522.48s | 276.56s  | 1.60s    | 399.52s  | 14.75s   | 170.54s | 37.36s      | 14.93s              | 112.77s         | 194.34ms      | 3.37s   | 29.79ms | 3.32s | 1.48s    | 0       | -      | 203.39s |
| 6   | 4873.64s | 4105.74s | 2294.66s   | 645.16s   | 1042.03s   | 0ns      | 7.33s | 15.19s | 4545.94s | 10.50s   | 3986.54s | 313.41s  | 1.91s    | 455.15s  | 15.64s   | 176.49s | 37.89s      | 15.60s              | 117.43s         | 148.10ms      | 3.66s   | 32.62ms | 3.67s | 1.59s    | 0       | -      | 218.93s |
| 7   | 4833.09s | 4052.88s | 2300.27s   | 606.77s   | 1023.90s   | 0ns      | 7.27s | 15.27s | 4434.58s | 25.86s   | 3874.35s | 322.49s  | 2.28s    | 475.05s  | 15.35s   | 188.26s | 38.29s      | 17.08s              | 127.06s         | 153.14ms      | 3.74s   | 42.09ms | 3.52s | 1.56s    | 0       | -      | 235.72s |
| 8   | 5044.26s | 4235.36s | 2392.45s   | 616.72s   | 1104.17s   | 0ns      | 7.59s | 16.57s | 4401.56s | 38.63s   | 3828.51s | 339.40s  | 2.64s    | 494.43s  | 15.93s   | 192.41s | 40.48s      | 17.81s              | 128.31s         | 157.46ms      | 3.90s   | 42.36ms | 3.68s | 1.66s    | 0       | -      | 270.37s |
| 9   | 5201.53s | 4529.54s | 2588.12s   | 654.49s   | 1156.62s   | 0ns      | 7.91s | 16.64s | 4993.05s | 95.33s   | 4614.23s | 242.26s  | 2.71s    | 355.59s  | 16.85s   | 210.27s | 36.66s      | 17.44s              | 150.37s         | 176.90ms      | 4.05s   | 41.62ms | 3.85s | 1.77s    | 0       | -      | 284.40s |
| 10  | 4836.26s | 4075.12s | 2305.98s   | 562.72s   | 1090.93s   | 0ns      | 6.95s | 14.21s | 4033.49s | 245.67s  | 3713.25s | 325.59s  | 2.78s    | 464.85s  | 15.56s   | 241.79s | 36.40s      | 18.61s              | 180.97s         | 157.25ms      | 3.64s   | 37.89ms | 3.30s | 1.79s    | 0       | -      | 266.89s |

=== Task Avg Time ===
| CP  | add    | navigate | nav_search | nav_fetch | nav_rerank | register | spawn   | scrub   | split | merge    | reassign | drop     | load   | load_raw | quantize | search   | search_scan | search_load_cluster | search_load_raw | search_rerank | raw_add | raw_rm | q_add   | q_rm    | rr_vecs | rr_data |
| --- | ------ | -------- | ---------- | --------- | ---------- | -------- | ------- | ------- | ----- | -------- | -------- | -------- | ------ | -------- | -------- | -------- | ----------- | ------------------- | --------------- | ------------- | ------- | ------ | ------- | ------- | ------- | ------- |
| 1   | 1.90ms | 525.2µs  | 196.3µs    | 129.5µs   | 174.5µs    | -        | 878.0µs | 117.7µs | 1.10s | -        | 680.4µs  | 539.6µs  | 34ns   | 146ns    | 1.9µs    | 105.49ms | 23.92ms     | 3.88ms              | 74.11ms         | 129.8µs       | 358.2µs | 11.1µs | 518.4µs | 415.6µs | 200     | 800.0KB |
| 2   | 3.43ms | 890.3µs  | 415.3µs    | 177.7µs   | 262.2µs    | -        | 1.49ms  | 185.0µs | 1.87s | -        | 1.22ms   | 65.02ms  | 8.8µs  | 49.68ms  | 1.9µs    | 127.22ms | 32.69ms     | 7.60ms              | 82.16ms         | 153.7µs       | 662.9µs | 15.4µs | 828.0µs | 658.6µs | 200     | 800.0KB |
| 3   | 3.89ms | 1.00ms   | 515.5µs    | 185.3µs   | 266.7µs    | -        | 1.83ms  | 223.9µs | 2.12s | 309.22ms | 1.41ms   | 109.41ms | 15.8µs | 85.24ms  | 2.0µs    | 154.18ms | 34.32ms     | 10.78ms             | 103.82ms        | 167.3µs       | 863.0µs | 20.3µs | 966.1µs | 791.1µs | 200     | 800.0KB |
| 4   | 4.23ms | 1.09ms   | 584.0µs    | 189.1µs   | 281.1µs    | -        | 2.08ms  | 250.9µs | 2.42s | 250.43ms | 1.56ms   | 146.83ms | 23.1µs | 114.09ms | 2.0µs    | 183.76ms | 40.35ms     | 14.93ms             | 122.44ms        | 200.3µs       | 1.02ms  | 14.9µs | 1.06ms  | 857.2µs | 200     | 800.0KB |
| 5   | 4.39ms | 1.15ms   | 636.0µs    | 190.6µs   | 287.3µs    | -        | 2.25ms  | 276.8µs | 2.52s | 222.14ms | 1.59ms   | 172.96ms | 31.9µs | 134.43ms | 2.1µs    | 189.49ms | 41.51ms     | 16.59ms             | 125.30ms        | 215.9µs       | 1.13ms  | 18.6µs | 1.12ms  | 925.1µs | 200     | 800.0KB |
| 6   | 4.87ms | 1.24ms   | 690.7µs    | 194.2µs   | 313.6µs    | -        | 2.44ms  | 293.7µs | 2.86s | 875.23ms | 1.71ms   | 195.63ms | 36.9µs | 151.21ms | 2.1µs    | 196.10ms | 42.10ms     | 17.33ms             | 130.48ms        | 164.6µs       | 1.22ms  | 20.4µs | 1.22ms  | 990.3µs | 200     | 800.0KB |
| 7   | 4.83ms | 1.26ms   | 713.7µs    | 188.3µs   | 317.7µs    | -        | 2.56ms  | 312.9µs | 2.95s | 1.36s    | 1.74ms   | 211.75ms | 46.6µs | 165.99ms | 2.2µs    | 209.18ms | 42.54ms     | 18.97ms             | 141.18ms        | 170.2µs       | 1.32ms  | 27.6µs | 1.24ms  | 1.02ms  | 200     | 800.0KB |
| 8   | 5.04ms | 1.35ms   | 763.3µs    | 196.8µs   | 352.3µs    | -        | 2.67ms  | 339.5µs | 2.93s | 1.10s    | 1.79ms   | 220.96ms | 54.2µs | 171.86ms | 2.3µs    | 213.79ms | 44.97ms     | 19.79ms             | 142.57ms        | 174.9µs       | 1.37ms  | 27.6µs | 1.30ms  | 1.08ms  | 200     | 800.0KB |
| 9   | 5.20ms | 1.37ms   | 783.6µs    | 198.1µs   | 350.2µs    | -        | 2.67ms  | 327.7µs | 3.26s | 1.34s    | 1.96ms   | 151.13ms | 53.4µs | 117.35ms | 2.3µs    | 233.64ms | 40.73ms     | 19.38ms             | 167.08ms        | 196.6µs       | 1.37ms  | 26.0µs | 1.30ms  | 1.10ms  | 200     | 800.0KB |
| 10  | 4.84ms | 1.43ms   | 810.9µs    | 197.9µs   | 383.6µs    | -        | 2.82ms  | 336.3µs | 3.12s | 886.88ms | 2.01ms   | 207.51ms | 65.8µs | 169.90ms | 2.4µs    | 268.66ms | 40.45ms     | 20.68ms             | 201.08ms        | 174.7µs       | 1.48ms  | 24.1µs | 1.34ms  | 1.14ms  | 200     | 800.0KB |

=== Indexing Summary ===
Total vectors: 10.00M
Total time: 39.5m
Overall throughput: 4219 vec/s

=== Recall Summary ===
| CP  | nprobe | DRR | Vectors | Queries | RR Vecs | RR Data | R@10 | R@100 | Lat   |
| --- | ------ | --- | ------- | ------- | ------- | ------- | ---- | ----- | ----- |
| 10  | 32     | 1x  | 10.00M  | 100     | 0       | 0B      | 0.73 | 0.62  | 36ms  |
| 10  | 32     | 2x  | 10.00M  | 100     | 200     | 800.0KB | 0.73 | 0.70  | 229ms |
| 10  | 32     | 4x  | 10.00M  | 100     | 400     | 1.6MB   | 0.73 | 0.72  | 438ms |
| 10  | 64     | 1x  | 10.00M  | 100     | 0       | 0B      | 0.78 | 0.65  | 62ms  |
| 10  | 64     | 2x  | 10.00M  | 100     | 200     | 800.0KB | 0.78 | 0.75  | 218ms |
| 10  | 64     | 4x  | 10.00M  | 100     | 400     | 1.6MB   | 0.78 | 0.77  | 556ms |
| 10  | 128    | 1x  | 10.00M  | 100     | 0       | 0B      | 0.80 | 0.67  | 102ms |
| 10  | 128    | 2x  | 10.00M  | 100     | 200     | 800.0KB | 0.80 | 0.78  | 263ms |
| 10  | 128    | 4x  | 10.00M  | 100     | 400     | 1.6MB   | 0.80 | 0.81  | 514ms |


## 50M - msmarco
50M with data rerank and canonical usearch
nohup cargo bench -p chroma-index --bench quantized_spann  --features chroma-index/usearch -- --dataset ms-marco --checkpoint 50  --data-bits 1 --centroid-bits 1 --centroid-rerank 16 --data-rerank-factors 1,2,4 --nprobes 32,64,128  --tmp-dir /mnt/data > quant_spann_1bit_msmarco50M.txt 2>&1 &
[1] 4388
(venv) ubuntu@ip-172-31-75-166:~/chroma$ tail -f quant_spann_1bit_msmarco50M.txt
     Running benches/quantized_spann.rs (target/release/deps/quantized_spann-f5b26d0d1576127a)
Note: ground truth not found at /home/ubuntu/.cache/msmarco_v2/ground_truth.parquet. Recall evaluation will be skipped.
Loading MS MARCO v2 from HuggingFace Hub...
=== QuantizedSpannIndexWriter Benchmark ===
Dataset: msmarco-v2 (138.36M vectors, 1024 dims)
Metric: Euclidean | Checkpoints: 50 (1.00M vec/CP) | Threads: 32 | Data bits: 1 | Centroid bits: 1
Centroid rerank: 16x | Data rerank factors: [1, 2, 4] | nprobes: [32, 64, 128]
Total vectors to index: 50.00M


Checkpoint 1: 1.00M vec | load 17.11s | raw 11.67s | index 1.4m | commit 1.51s | 11792 vec/s
  (no precomputed ground truth for 1M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 2: 1.00M vec | load 14.69s | raw 11.60s | index 2.0m | commit 3.09s | 8205 vec/s
  (no precomputed ground truth for 2M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 3: 1.00M vec | load 14.75s | raw 11.54s | index 2.2m | commit 4.45s | 7524 vec/s
  (no precomputed ground truth for 3M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 4: 1.00M vec | load 16.11s | raw 11.54s | index 2.4m | commit 5.49s | 7087 vec/s
  (no precomputed ground truth for 4M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 5: 1.00M vec | load 16.50s | raw 11.55s | index 2.7m | commit 6.66s | 6251 vec/s
  (no precomputed ground truth for 5M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 6: 1.00M vec | load 15.17s | raw 11.66s | index 2.8m | commit 7.83s | 5970 vec/s
  (no precomputed ground truth for 6M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 7: 1.00M vec | load 20.49s | raw 11.80s | index 2.8m | commit 8.53s | 5941 vec/s
  (no precomputed ground truth for 7M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 8: 1.00M vec | load 13.88s | raw 11.62s | index 2.8m | commit 9.04s | 6019 vec/s
  (no precomputed ground truth for 8M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 9: 1.00M vec | load 14.98s | raw 11.64s | index 2.8m | commit 9.74s | 5986 vec/s
  (no precomputed ground truth for 9M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 10: 1.00M vec | load 18.81s | raw 11.66s | index 3.0m | commit 10.43s | 5629 vec/s
  (no precomputed ground truth for 10M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 11: 1.00M vec | load 19.37s | raw 12.35s | index 3.0m | commit 10.31s | 5577 vec/s
  (no precomputed ground truth for 11M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 12: 1.00M vec | load 15.10s | raw 12.35s | index 2.9m | commit 10.46s | 5820 vec/s
  (no precomputed ground truth for 12M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 13: 1.00M vec | load 15.38s | raw 12.27s | index 2.6m | commit 10.55s | 6477 vec/s
  (no precomputed ground truth for 13M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 14: 1.00M vec | load 15.55s | raw 12.62s | index 2.5m | commit 10.71s | 6652 vec/s
  (no precomputed ground truth for 14M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 15: 1.00M vec | load 15.61s | raw 12.64s | index 2.5m | commit 10.55s | 6694 vec/s
  (no precomputed ground truth for 15M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 16: 1.00M vec | load 20.89s | raw 12.40s | index 2.2m | commit 10.44s | 7748 vec/s
  (no precomputed ground truth for 16M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 17: 1.00M vec | load 14.59s | raw 12.52s | index 2.1m | commit 10.44s | 7857 vec/s
  (no precomputed ground truth for 17M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 18: 1.00M vec | load 19.47s | raw 12.66s | index 2.1m | commit 10.31s | 8085 vec/s
  (no precomputed ground truth for 18M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 19: 1.00M vec | load 14.39s | raw 12.48s | index 2.2m | commit 10.27s | 7722 vec/s
  (no precomputed ground truth for 19M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 20: 1.00M vec | load 14.94s | raw 12.67s | index 2.3m | commit 10.27s | 7119 vec/s
  (no precomputed ground truth for 20M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 21: 1.00M vec | load 15.72s | raw 12.07s | index 1.8m | commit 10.22s | 9351 vec/s
  (no precomputed ground truth for 21M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 22: 1.00M vec | load 18.50s | raw 12.02s | index 1.8m | commit 10.06s | 9342 vec/s
  (no precomputed ground truth for 22M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 23: 1.00M vec | load 16.63s | raw 12.30s | index 1.7m | commit 10.41s | 9710 vec/s
  (no precomputed ground truth for 23M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 24: 1.00M vec | load 17.54s | raw 12.33s | index 1.7m | commit 10.43s | 9820 vec/s
  (no precomputed ground truth for 24M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 25: 1.00M vec | load 17.26s | raw 12.34s | index 2.6m | commit 10.70s | 6484 vec/s
  (no precomputed ground truth for 25M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 26: 1.00M vec | load 15.36s | raw 12.45s | index 1.9m | commit 11.23s | 8668 vec/s
  (no precomputed ground truth for 26M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 27: 1.00M vec | load 16.26s | raw 12.51s | index 2.4m | commit 11.80s | 6849 vec/s
  (no precomputed ground truth for 27M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 28: 1.00M vec | load 15.66s | raw 12.53s | index 2.1m | commit 12.36s | 7757 vec/s
  (no precomputed ground truth for 28M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 29: 1.00M vec | load 14.71s | raw 12.49s | index 3.2m | commit 12.79s | 5286 vec/s
  (no precomputed ground truth for 29M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 30: 1.00M vec | load 14.65s | raw 12.51s | index 2.3m | commit 12.98s | 7261 vec/s
  (no precomputed ground truth for 30M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 31: 1.00M vec | load 14.76s | raw 12.70s | index 2.4m | commit 13.52s | 6906 vec/s
  (no precomputed ground truth for 31M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 32: 1.00M vec | load 15.55s | raw 12.98s | index 2.5m | commit 14.20s | 6661 vec/s
  (no precomputed ground truth for 32M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 33: 1.00M vec | load 48.51s | raw 13.02s | index 2.3m | commit 14.63s | 7325 vec/s
  (no precomputed ground truth for 33M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 34: 1.00M vec | load 48.51s | raw 12.83s | index 2.7m | commit 14.45s | 6061 vec/s
  (no precomputed ground truth for 34M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 35: 1.00M vec | load 51.75s | raw 12.89s | index 2.5m | commit 15.19s | 6652 vec/s
  (no precomputed ground truth for 35M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 36: 1.00M vec | load 45.90s | raw 13.04s | index 2.4m | commit 14.94s | 6963 vec/s
  (no precomputed ground truth for 36M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 37: 1.00M vec | load 51.61s | raw 13.07s | index 2.7m | commit 15.77s | 6265 vec/s
  (no precomputed ground truth for 37M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 38: 1.00M vec | load 45.78s | raw 12.97s | index 2.9m | commit 16.68s | 5747 vec/s
  (no precomputed ground truth for 38M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 39: 1.00M vec | load 49.16s | raw 13.00s | index 3.3m | commit 17.64s | 4987 vec/s
  (no precomputed ground truth for 39M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 40: 1.00M vec | load 55.76s | raw 12.94s | index 3.1m | commit 18.78s | 5328 vec/s
  (no precomputed ground truth for 40M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 41: 1.00M vec | load 48.33s | raw 12.94s | index 3.3m | commit 19.77s | 5103 vec/s
  (no precomputed ground truth for 41M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 42: 1.00M vec | load 1.4m | raw 13.12s | index 3.3m | commit 20.80s | 5070 vec/s
  (no precomputed ground truth for 42M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 43: 1.00M vec | load 52.97s | raw 13.08s | index 3.5m | commit 21.39s | 4783 vec/s
  (no precomputed ground truth for 43M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 44: 1.00M vec | load 51.08s | raw 13.01s | index 3.2m | commit 21.28s | 5132 vec/s
  (no precomputed ground truth for 44M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 45: 1.00M vec | load 45.97s | raw 13.07s | index 2.3m | commit 19.81s | 7349 vec/s
  (no precomputed ground truth for 45M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 46: 1.00M vec | load 57.09s | raw 12.85s | index 2.2m | commit 19.04s | 7614 vec/s
  (no precomputed ground truth for 46M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 47: 1.00M vec | load 46.64s | raw 13.23s | index 2.6m | commit 21.57s | 6380 vec/s
  (no precomputed ground truth for 47M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 48: 1.00M vec | load 49.07s | raw 13.24s | index 2.4m | commit 21.10s | 7009 vec/s
  (no precomputed ground truth for 48M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 49: 1.00M vec | load 46.74s | raw 13.28s | index 2.5m | commit 21.26s | 6735 vec/s
  (no precomputed ground truth for 49M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

Checkpoint 50: 1.00M vec | load 52.76s | raw 13.25s | index 2.4m | commit 19.78s | 6829 vec/s
  (no precomputed ground truth for 50M boundary, skipping recall)
  nprobe 32: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 64: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms
  nprobe 128: drr1x=0.00/0.00 drr2x=0.00/0.00 drr4x=0.00/0.00 | lat=0.0ms

=== Cluster Statistics ===
| CP  | Centroids | Min | Max  | Median | P90  | P99  | Avg    | Std   |
| --- | --------- | --- | ---- | ------ | ---- | ---- | ------ | ----- |
| 1   | 1.4K      | 7   | 2048 | 1543   | 1969 | 2043 | 1448.9 | 476.3 |
| 2   | 2.8K      | 4   | 2048 | 1556   | 1963 | 2042 | 1480.4 | 432.0 |
| 3   | 4.2K      | 4   | 2048 | 1564   | 1955 | 2039 | 1494.4 | 412.2 |
| 4   | 5.5K      | 4   | 2048 | 1577   | 1963 | 2043 | 1505.2 | 404.5 |
| 5   | 6.8K      | 1   | 2048 | 1557   | 1961 | 2043 | 1496.3 | 403.8 |
| 6   | 8.2K      | 0   | 2048 | 1545   | 1950 | 2041 | 1476.9 | 412.4 |
| 7   | 9.4K      | 0   | 2048 | 1540   | 1952 | 2040 | 1469.7 | 416.3 |
| 8   | 10.6K     | 0   | 2049 | 1533   | 1953 | 2041 | 1451.1 | 441.3 |
| 9   | 11.8K     | 1   | 2048 | 1511   | 1944 | 2041 | 1422.6 | 457.8 |
| 10  | 12.9K     | 3   | 2048 | 1484   | 1937 | 2039 | 1375.9 | 496.1 |
| 11  | 14.0K     | 3   | 2048 | 1455   | 1934 | 2041 | 1337.8 | 521.3 |
| 12  | 14.8K     | 3   | 2048 | 1382   | 1924 | 2039 | 1254.8 | 565.4 |
| 13  | 15.4K     | 2   | 2048 | 1299   | 1897 | 2039 | 1177.8 | 581.9 |
| 14  | 16.0K     | 2   | 2048 | 1243   | 1892 | 2037 | 1150.4 | 578.3 |
| 15  | 16.5K     | 2   | 2048 | 1170   | 1868 | 2035 | 1103.5 | 582.9 |
| 16  | 16.8K     | 2   | 2048 | 1097   | 1850 | 2032 | 1063.1 | 588.5 |
| 17  | 17.1K     | 2   | 2048 | 1025   | 1830 | 2028 | 1032.6 | 582.2 |
| 18  | 17.2K     | 2   | 2048 | 964    | 1818 | 2029 | 999.4  | 580.9 |
| 19  | 17.2K     | 2   | 2048 | 932    | 1814 | 2029 | 981.6  | 577.8 |
| 20  | 17.3K     | 2   | 2049 | 880    | 1793 | 2028 | 952.3  | 576.4 |
| 21  | 17.2K     | 1   | 2048 | 876    | 1776 | 2021 | 945.7  | 567.7 |
| 22  | 16.9K     | 1   | 2048 | 858    | 1764 | 2024 | 937.4  | 560.5 |
| 23  | 16.5K     | 0   | 2048 | 868    | 1750 | 2013 | 934.0  | 555.2 |
| 24  | 16.2K     | 0   | 2048 | 912    | 1775 | 2018 | 969.6  | 548.5 |
| 25  | 16.2K     | 0   | 2048 | 949    | 1789 | 2023 | 991.9  | 552.3 |
| 26  | 16.1K     | 0   | 2048 | 990    | 1800 | 2025 | 1013.0 | 553.4 |
| 27  | 16.1K     | 0   | 2048 | 1016   | 1817 | 2027 | 1031.7 | 558.4 |
| 28  | 16.0K     | 1   | 2048 | 1048   | 1819 | 2030 | 1050.6 | 554.5 |
| 29  | 15.9K     | 2   | 2048 | 1097   | 1836 | 2029 | 1080.1 | 556.4 |
| 30  | 15.8K     | 1   | 2048 | 1111   | 1848 | 2029 | 1089.2 | 561.1 |
| 31  | 15.7K     | 3   | 2048 | 1104   | 1846 | 2029 | 1081.9 | 565.1 |
| 32  | 15.4K     | 1   | 2048 | 1104   | 1853 | 2032 | 1084.2 | 568.5 |
| 33  | 15.4K     | 2   | 2048 | 1108   | 1857 | 2030 | 1086.7 | 570.1 |
| 34  | 16.0K     | 4   | 2048 | 1139   | 1862 | 2031 | 1098.9 | 575.4 |
| 35  | 15.5K     | 0   | 2048 | 1113   | 1859 | 2033 | 1078.2 | 586.6 |
| 36  | 15.3K     | 2   | 2048 | 1102   | 1855 | 2032 | 1071.2 | 584.0 |
| 37  | 15.7K     | 0   | 2048 | 1095   | 1853 | 2030 | 1055.1 | 597.8 |
| 38  | 16.2K     | 0   | 2048 | 1109   | 1860 | 2031 | 1052.6 | 609.6 |
| 39  | 17.0K     | 1   | 2048 | 1141   | 1859 | 2032 | 1068.4 | 611.2 |
| 40  | 18.0K     | 1   | 2048 | 1191   | 1876 | 2034 | 1093.5 | 615.6 |
| 41  | 18.8K     | 1   | 2048 | 1211   | 1885 | 2036 | 1102.3 | 620.6 |
| 42  | 19.7K     | 1   | 2048 | 1231   | 1888 | 2037 | 1114.3 | 618.6 |
| 43  | 20.7K     | 1   | 2048 | 1250   | 1895 | 2036 | 1125.6 | 621.6 |
| 44  | 21.2K     | 1   | 2048 | 1248   | 1892 | 2034 | 1129.4 | 613.3 |
| 45  | 21.0K     | 1   | 2048 | 1239   | 1896 | 2036 | 1136.7 | 602.1 |
| 46  | 20.6K     | 1   | 2048 | 1179   | 1878 | 2035 | 1095.9 | 609.1 |
| 47  | 20.5K     | 1   | 2048 | 1134   | 1866 | 2032 | 1062.0 | 616.6 |
| 48  | 20.4K     | 1   | 2048 | 1114   | 1866 | 2032 | 1046.7 | 623.8 |
| 49  | 20.4K     | 0   | 2048 | 1118   | 1867 | 2032 | 1050.8 | 622.3 |
| 50  | 20.4K     | 1   | 2048 | 1092   | 1860 | 2031 | 1034.9 | 630.4 |

=== Task Counts ===
| CP  | add   | navigate | nav_search | nav_fetch | nav_rerank | register | spawn | scrub | split | merge | reassign | drop | load  | load_raw | quantize | search | search_scan | search_load_cluster | search_load_raw | search_rerank | raw_add | raw_rm | q_add | q_rm |
| --- | ----- | -------- | ---------- | --------- | ---------- | -------- | ----- | ----- | ----- | ----- | -------- | ---- | ----- | -------- | -------- | ------ | ----------- | ------------------- | --------------- | ------------- | ------- | ------ | ----- | ---- |
| 1   | 1.00M | 3.11M    | 3.11M      | 3.11M     | 3.11M      | 0        | 2.9K  | 51.1K | 1.5K  | 0     | 2.31M    | 1.5K | 51.1K | 2.9K     | 7.09M    | 0      | 0           | 0                   | 0               | 0             | 2.9K    | 1.5K   | 2.9K  | 1.5K |
| 2   | 1.00M | 3.03M    | 3.03M      | 3.03M     | 3.03M      | 0        | 3.0K  | 50.2K | 1.6K  | 0     | 2.05M    | 1.6K | 50.2K | 3.0K     | 7.01M    | 0      | 0           | 0                   | 0               | 0             | 3.0K    | 1.6K   | 3.0K  | 1.6K |
| 3   | 1.00M | 2.89M    | 2.89M      | 2.89M     | 2.89M      | 0        | 3.0K  | 49.0K | 1.6K  | 1     | 1.90M    | 1.6K | 49.0K | 3.0K     | 6.84M    | 0      | 0           | 0                   | 0               | 0             | 3.0K    | 1.6K   | 3.0K  | 1.6K |
| 4   | 1.00M | 2.84M    | 2.84M      | 2.84M     | 2.84M      | 0        | 2.9K  | 48.0K | 1.6K  | 6     | 1.85M    | 1.6K | 48.0K | 2.9K     | 6.75M    | 0      | 0           | 0                   | 0               | 0             | 2.9K    | 1.6K   | 2.9K  | 1.6K |
| 5   | 1.00M | 2.85M    | 2.85M      | 2.85M     | 2.85M      | 0        | 2.9K  | 49.0K | 1.6K  | 13    | 1.85M    | 1.6K | 49.0K | 3.0K     | 6.77M    | 0      | 0           | 0                   | 0               | 0             | 2.9K    | 1.6K   | 2.9K  | 1.6K |
| 6   | 1.00M | 2.83M    | 2.83M      | 2.83M     | 2.83M      | 0        | 2.9K  | 48.8K | 1.6K  | 22    | 1.83M    | 1.6K | 48.8K | 2.9K     | 6.77M    | 0      | 0           | 0                   | 0               | 0             | 2.9K    | 1.6K   | 2.9K  | 1.6K |
| 7   | 1.00M | 2.72M    | 2.72M      | 2.72M     | 2.72M      | 0        | 2.8K  | 46.0K | 1.5K  | 38    | 1.72M    | 1.5K | 46.0K | 2.8K     | 6.52M    | 0      | 0           | 0                   | 0               | 0             | 2.8K    | 1.5K   | 2.8K  | 1.5K |
| 8   | 1.00M | 2.70M    | 2.70M      | 2.70M     | 2.70M      | 0        | 2.7K  | 44.8K | 1.4K  | 72    | 1.70M    | 1.5K | 44.8K | 2.7K     | 6.43M    | 0      | 0           | 0                   | 0               | 0             | 2.7K    | 1.5K   | 2.7K  | 1.5K |
| 9   | 1.00M | 2.67M    | 2.67M      | 2.67M     | 2.67M      | 0        | 2.6K  | 44.4K | 1.4K  | 81    | 1.67M    | 1.4K | 44.4K | 2.7K     | 6.40M    | 0      | 0           | 0                   | 0               | 0             | 2.6K    | 1.4K   | 2.6K  | 1.4K |
| 10  | 1.00M | 2.68M    | 2.68M      | 2.68M     | 2.68M      | 0        | 2.6K  | 43.7K | 1.3K  | 137   | 1.69M    | 1.5K | 43.7K | 2.7K     | 6.37M    | 0      | 0           | 0                   | 0               | 0             | 2.6K    | 1.5K   | 2.6K  | 1.5K |
| 11  | 1.00M | 2.60M    | 2.60M      | 2.60M     | 2.60M      | 0        | 2.4K  | 41.4K | 1.3K  | 141   | 1.61M    | 1.4K | 41.4K | 2.6K     | 6.14M    | 0      | 0           | 0                   | 0               | 0             | 2.4K    | 1.4K   | 2.4K  | 1.4K |
| 12  | 1.00M | 2.58M    | 2.58M      | 2.58M     | 2.58M      | 0        | 2.3K  | 38.9K | 1.2K  | 254   | 1.59M    | 1.4K | 38.9K | 2.5K     | 5.94M    | 0      | 0           | 0                   | 0               | 0             | 2.3K    | 1.4K   | 2.3K  | 1.4K |
| 13  | 1.00M | 2.23M    | 2.23M      | 2.23M     | 2.23M      | 0        | 1.9K  | 31.9K | 981   | 352   | 1.23M    | 1.3K | 31.9K | 2.2K     | 5.22M    | 0      | 0           | 0                   | 0               | 0             | 1.9K    | 1.3K   | 1.9K  | 1.3K |
| 14  | 1.00M | 2.07M    | 2.07M      | 2.07M     | 2.07M      | 0        | 1.7K  | 28.2K | 860   | 227   | 1.07M    | 1.1K | 28.2K | 1.9K     | 4.83M    | 0      | 0           | 0                   | 0               | 0             | 1.7K    | 1.1K   | 1.7K  | 1.1K |
| 15  | 1.00M | 2.07M    | 2.07M      | 2.07M     | 2.07M      | 0        | 1.6K  | 26.8K | 812   | 294   | 1.07M    | 1.1K | 26.8K | 1.9K     | 4.74M    | 0      | 0           | 0                   | 0               | 0             | 1.6K    | 1.1K   | 1.6K  | 1.1K |
| 16  | 1.00M | 1.83M    | 1.83M      | 1.83M     | 1.83M      | 0        | 1.3K  | 21.9K | 666   | 282   | 831.7K   | 948  | 21.9K | 1.6K     | 4.21M    | 0      | 0           | 0                   | 0               | 0             | 1.3K    | 948    | 1.3K  | 948  |
| 17  | 1.00M | 1.79M    | 1.79M      | 1.79M     | 1.79M      | 0        | 1.2K  | 20.6K | 628   | 303   | 789.7K   | 931  | 20.6K | 1.5K     | 4.08M    | 0      | 0           | 0                   | 0               | 0             | 1.2K    | 931    | 1.2K  | 931  |
| 18  | 1.00M | 1.65M    | 1.65M      | 1.65M     | 1.65M      | 0        | 983   | 16.5K | 505   | 384   | 650.3K   | 889  | 16.5K | 1.4K     | 3.71M    | 0      | 0           | 0                   | 0               | 0             | 983     | 889    | 983   | 889  |
| 19  | 1.00M | 1.58M    | 1.58M      | 1.58M     | 1.58M      | 0        | 832   | 14.1K | 424   | 377   | 581.6K   | 801  | 14.1K | 1.2K     | 3.50M    | 0      | 0           | 0                   | 0               | 0             | 832     | 801    | 832   | 801  |
| 20  | 1.00M | 1.65M    | 1.65M      | 1.65M     | 1.65M      | 0        | 898   | 15.1K | 458   | 389   | 653.4K   | 847  | 15.1K | 1.3K     | 3.61M    | 0      | 0           | 0                   | 0               | 0             | 898     | 847    | 898   | 847  |
| 21  | 1.00M | 1.57M    | 1.57M      | 1.57M     | 1.57M      | 0        | 730   | 12.7K | 373   | 464   | 576.7K   | 837  | 12.7K | 1.2K     | 3.40M    | 0      | 0           | 0                   | 0               | 0             | 730     | 837    | 730   | 837  |
| 22  | 1.00M | 1.41M    | 1.41M      | 1.41M     | 1.41M      | 0        | 468   | 8.4K  | 238   | 543   | 418.6K   | 779  | 8.4K  | 1.0K     | 3.00M    | 0      | 0           | 0                   | 0               | 0             | 468     | 779    | 468   | 779  |
| 23  | 1.00M | 1.36M    | 1.36M      | 1.36M     | 1.36M      | 0        | 446   | 7.8K  | 230   | 537   | 364.8K   | 767  | 7.8K  | 983      | 2.92M    | 0      | 0           | 0                   | 0               | 0             | 446     | 767    | 446   | 767  |
| 24  | 1.00M | 1.33M    | 1.33M      | 1.33M     | 1.33M      | 0        | 335   | 6.1K  | 173   | 507   | 332.7K   | 680  | 6.1K  | 842      | 2.79M    | 0      | 0           | 0                   | 0               | 0             | 335     | 680    | 335   | 680  |
| 25  | 1.00M | 1.73M    | 1.73M      | 1.73M     | 1.73M      | 0        | 1.0K  | 17.3K | 513   | 440   | 740.2K   | 953  | 17.3K | 1.4K     | 3.85M    | 0      | 0           | 0                   | 0               | 0             | 1.0K    | 953    | 1.0K  | 953  |
| 26  | 1.00M | 1.54M    | 1.54M      | 1.54M     | 1.54M      | 0        | 682   | 12.0K | 348   | 502   | 545.6K   | 850  | 12.0K | 1.2K     | 3.31M    | 0      | 0           | 0                   | 0               | 0             | 682     | 850    | 682   | 850  |
| 27  | 1.00M | 1.83M    | 1.83M      | 1.83M     | 1.83M      | 0        | 1.0K  | 18.2K | 539   | 455   | 841.9K   | 994  | 18.2K | 1.5K     | 3.99M    | 0      | 0           | 0                   | 0               | 0             | 1.0K    | 994    | 1.0K  | 994  |
| 28  | 1.00M | 1.71M    | 1.71M      | 1.71M     | 1.71M      | 0        | 939   | 16.1K | 486   | 542   | 707.9K   | 1.0K | 16.1K | 1.5K     | 3.75M    | 0      | 0           | 0                   | 0               | 0             | 939     | 1.0K   | 939   | 1.0K |
| 29  | 1.00M | 1.77M    | 1.77M      | 1.77M     | 1.77M      | 0        | 894   | 15.7K | 460   | 566   | 773.5K   | 1.0K | 15.7K | 1.5K     | 3.78M    | 0      | 0           | 0                   | 0               | 0             | 894     | 1.0K   | 894   | 1.0K |
| 30  | 1.00M | 1.75M    | 1.75M      | 1.75M     | 1.75M      | 0        | 949   | 16.5K | 488   | 529   | 759.7K   | 1.0K | 16.5K | 1.5K     | 3.83M    | 0      | 0           | 0                   | 0               | 0             | 949     | 1.0K   | 949   | 1.0K |
| 31  | 1.00M | 1.76M    | 1.76M      | 1.76M     | 1.76M      | 0        | 1.0K  | 17.7K | 533   | 622   | 763.4K   | 1.2K | 17.7K | 1.7K     | 3.91M    | 0      | 0           | 0                   | 0               | 0             | 1.0K    | 1.2K   | 1.0K  | 1.2K |
| 32  | 1.00M | 1.77M    | 1.77M      | 1.77M     | 1.77M      | 0        | 919   | 16.1K | 473   | 719   | 773.5K   | 1.2K | 16.1K | 1.6K     | 3.87M    | 0      | 0           | 0                   | 0               | 0             | 919     | 1.2K   | 919   | 1.2K |
| 33  | 1.00M | 1.85M    | 1.85M      | 1.85M     | 1.85M      | 0        | 1.1K  | 18.3K | 559   | 558   | 854.1K   | 1.1K | 18.3K | 1.6K     | 4.02M    | 0      | 0           | 0                   | 0               | 0             | 1.1K    | 1.1K   | 1.1K  | 1.1K |
| 34  | 1.00M | 2.26M    | 2.26M      | 2.26M     | 2.26M      | 0        | 1.8K  | 30.0K | 930   | 302   | 1.27M    | 1.2K | 30.0K | 2.1K     | 5.13M    | 0      | 0           | 0                   | 0               | 0             | 1.8K    | 1.2K   | 1.8K  | 1.2K |
| 35  | 1.00M | 1.67M    | 1.67M      | 1.67M     | 1.67M      | 0        | 747   | 13.4K | 384   | 835   | 674.5K   | 1.2K | 13.4K | 1.6K     | 3.59M    | 0      | 0           | 0                   | 0               | 0             | 747     | 1.2K   | 747   | 1.2K |
| 36  | 1.00M | 1.85M    | 1.85M      | 1.85M     | 1.85M      | 0        | 932   | 16.7K | 474   | 667   | 855.4K   | 1.1K | 16.7K | 1.6K     | 3.97M    | 0      | 0           | 0                   | 0               | 0             | 932     | 1.1K   | 932   | 1.1K |
| 37  | 1.00M | 2.18M    | 2.18M      | 2.18M     | 2.18M      | 0        | 1.5K  | 25.1K | 781   | 314   | 1.19M    | 1.1K | 25.1K | 1.8K     | 4.73M    | 0      | 0           | 0                   | 0               | 0             | 1.5K    | 1.1K   | 1.5K  | 1.1K |
| 38  | 1.00M | 2.39M    | 2.39M      | 2.39M     | 2.39M      | 0        | 1.8K  | 30.4K | 977   | 292   | 1.39M    | 1.3K | 30.4K | 2.1K     | 5.23M    | 0      | 0           | 0                   | 0               | 0             | 1.8K    | 1.3K   | 1.8K  | 1.3K |
| 39  | 1.00M | 2.63M    | 2.63M      | 2.63M     | 2.63M      | 0        | 2.2K  | 36.6K | 1.2K  | 210   | 1.64M    | 1.4K | 36.6K | 2.4K     | 5.83M    | 0      | 0           | 0                   | 0               | 0             | 2.2K    | 1.4K   | 2.2K  | 1.4K |
| 40  | 1.00M | 2.68M    | 2.68M      | 2.68M     | 2.68M      | 0        | 2.3K  | 37.5K | 1.2K  | 137   | 1.68M    | 1.3K | 37.5K | 2.4K     | 5.91M    | 0      | 0           | 0                   | 0               | 0             | 2.3K    | 1.3K   | 2.3K  | 1.3K |
| 41  | 1.00M | 2.62M    | 2.62M      | 2.62M     | 2.62M      | 0        | 2.2K  | 37.1K | 1.2K  | 168   | 1.62M    | 1.3K | 37.1K | 2.4K     | 5.83M    | 0      | 0           | 0                   | 0               | 0             | 2.2K    | 1.3K   | 2.2K  | 1.3K |
| 42  | 1.00M | 2.62M    | 2.62M      | 2.62M     | 2.62M      | 0        | 2.2K  | 37.6K | 1.2K  | 151   | 1.62M    | 1.3K | 37.6K | 2.4K     | 5.85M    | 0      | 0           | 0                   | 0               | 0             | 2.2K    | 1.3K   | 2.2K  | 1.3K |
| 43  | 1.00M | 2.78M    | 2.78M      | 2.78M     | 2.78M      | 0        | 2.3K  | 38.7K | 1.2K  | 127   | 1.79M    | 1.3K | 38.7K | 2.4K     | 6.03M    | 0      | 0           | 0                   | 0               | 0             | 2.3K    | 1.3K   | 2.3K  | 1.3K |
| 44  | 1.00M | 2.28M    | 2.28M      | 2.28M     | 2.28M      | 0        | 1.7K  | 29.6K | 896   | 320   | 1.28M    | 1.2K | 29.6K | 2.0K     | 5.05M    | 0      | 0           | 0                   | 0               | 0             | 1.7K    | 1.2K   | 1.7K  | 1.2K |
| 45  | 1.00M | 1.68M    | 1.68M      | 1.68M     | 1.68M      | 0        | 823   | 14.5K | 420   | 606   | 679.1K   | 1.0K | 14.5K | 1.4K     | 3.62M    | 0      | 0           | 0                   | 0               | 0             | 823     | 1.0K   | 823   | 1.0K |
| 46  | 1.00M | 1.57M    | 1.57M      | 1.57M     | 1.57M      | 0        | 673   | 12.0K | 345   | 719   | 575.0K   | 1.1K | 12.0K | 1.4K     | 3.40M    | 0      | 0           | 0                   | 0               | 0             | 673     | 1.1K   | 673   | 1.1K |
| 47  | 1.00M | 1.77M    | 1.77M      | 1.77M     | 1.77M      | 0        | 944   | 16.1K | 486   | 536   | 783.7K   | 1.0K | 16.1K | 1.5K     | 3.79M    | 0      | 0           | 0                   | 0               | 0             | 944     | 1.0K   | 944   | 1.0K |
| 48  | 1.00M | 1.69M    | 1.69M      | 1.69M     | 1.69M      | 0        | 883   | 15.3K | 454   | 584   | 697.7K   | 1.0K | 15.3K | 1.5K     | 3.67M    | 0      | 0           | 0                   | 0               | 0             | 883     | 1.0K   | 883   | 1.0K |
| 49  | 1.00M | 1.82M    | 1.82M      | 1.82M     | 1.82M      | 0        | 1.1K  | 18.8K | 563   | 494   | 831.3K   | 1.1K | 18.8K | 1.6K     | 4.04M    | 0      | 0           | 0                   | 0               | 0             | 1.1K    | 1.1K   | 1.1K  | 1.1K |
| 50  | 1.00M | 1.73M    | 1.73M      | 1.73M     | 1.73M      | 0        | 1.0K  | 17.5K | 517   | 519   | 740.7K   | 1.0K | 17.5K | 1.5K     | 3.83M    | 0      | 0           | 0                   | 0               | 0             | 1.0K    | 1.0K   | 1.0K  | 1.0K |

=== Task Total Time ===
| CP  | add      | navigate | nav_search | nav_fetch | nav_rerank | register | spawn | scrub  | split    | merge    | reassign | drop     | load     | load_raw | quantize | search | search_scan | search_load_cluster | search_load_raw | search_rerank | raw_add  | raw_rm  | q_add    | q_rm     | raw_pts | raw/pt | total   |
| --- | -------- | -------- | ---------- | --------- | ---------- | -------- | ----- | ------ | -------- | -------- | -------- | -------- | -------- | -------- | -------- | ------ | ----------- | ------------------- | --------------- | ------------- | -------- | ------- | -------- | -------- | ------- | ------ | ------- |
| 1   | 1905.93s | 1624.09s | 586.40s    | 429.94s   | 532.47s    | 0ns      | 2.45s | 6.28s  | 1601.87s | 0ns      | 1521.26s | 793.14ms | 1.81ms   | 329.9µs  | 13.48s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 985.56ms | 17.53ms | 1.47s    | 604.12ms | 0       | -      | 115.10s |
| 2   | 3275.30s | 2851.21s | 1301.81s   | 607.86s   | 832.69s    | 0ns      | 4.73s | 9.35s  | 2678.47s | 0ns      | 2443.13s | 103.53s  | 447.42ms | 144.46s  | 13.78s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 2.09s    | 27.92ms | 2.63s    | 1.17s    | 0       | -      | 151.24s |
| 3   | 3665.53s | 3112.24s | 1569.26s   | 602.67s   | 834.74s    | 0ns      | 5.68s | 11.38s | 2882.31s | 246.35ms | 2520.96s | 191.37s  | 868.48ms | 261.25s  | 14.12s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 2.62s    | 32.19ms | 3.06s    | 1.39s    | 0       | -      | 163.65s |
| 4   | 3876.83s | 3254.42s | 1718.58s   | 611.28s   | 818.95s    | 0ns      | 6.37s | 12.55s | 3104.06s | 4.63s    | 2673.69s | 243.71s  | 1.25s    | 331.59s  | 14.14s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.17s    | 40.77ms | 3.19s    | 1.46s    | 0       | -      | 174.23s |
| 5   | 4233.28s | 3545.73s | 1907.89s   | 621.76s   | 906.63s    | 0ns      | 6.94s | 13.83s | 3404.71s | 14.27s   | 2924.44s | 280.02s  | 1.60s    | 380.22s  | 15.08s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.44s    | 39.40ms | 3.49s    | 1.57s    | 0       | -      | 194.68s |
| 6   | 4450.48s | 3704.10s | 2037.86s   | 623.71s   | 932.51s    | 0ns      | 7.48s | 15.23s | 3530.39s | 41.43s   | 3019.96s | 323.65s  | 1.91s    | 437.66s  | 15.49s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.88s    | 36.45ms | 3.60s    | 1.63s    | 0       | -      | 202.18s |
| 7   | 4386.20s | 3640.50s | 2052.28s   | 604.93s   | 875.41s    | 0ns      | 7.32s | 14.62s | 3342.46s | 41.23s   | 2830.19s | 326.33s  | 2.32s    | 441.35s  | 14.96s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.82s    | 31.21ms | 3.50s    | 1.59s    | 0       | -      | 209.16s |
| 8   | 4613.48s | 3829.00s | 2161.87s   | 593.83s   | 963.93s    | 0ns      | 7.49s | 15.80s | 3607.70s | 95.67s   | 3116.87s | 348.60s  | 2.48s    | 474.32s  | 15.66s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.92s    | 55.25ms | 3.56s    | 1.69s    | 0       | -      | 200.69s |
| 9   | 4707.17s | 3952.77s | 2229.87s   | 605.94s   | 1003.42s   | 0ns      | 7.58s | 15.29s | 3519.61s | 95.46s   | 3057.10s | 331.21s  | 2.67s    | 446.66s  | 16.76s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 4.05s    | 37.49ms | 3.53s    | 1.67s    | 0       | -      | 203.42s |
| 10  | 4875.16s | 4109.63s | 2334.63s   | 600.72s   | 1057.52s   | 0ns      | 7.64s | 14.69s | 3804.76s | 149.92s  | 3388.64s | 338.11s  | 2.70s    | 455.97s  | 17.51s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 4.11s    | 45.47ms | 3.52s    | 1.75s    | 0       | -      | 218.55s |
| 11  | 4628.22s | 3944.57s | 2268.18s   | 584.79s   | 978.33s    | 0ns      | 7.34s | 14.81s | 3515.59s | 115.91s  | 3143.78s | 284.41s  | 3.01s    | 383.83s  | 16.39s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.93s    | 41.72ms | 3.41s    | 1.67s    | 0       | -      | 221.36s |
| 12  | 4563.78s | 3916.49s | 2261.55s   | 574.97s   | 968.40s    | 0ns      | 6.85s | 13.56s | 3314.43s | 251.19s  | 3115.38s | 265.98s  | 2.60s    | 350.73s  | 16.02s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.61s    | 44.48ms | 3.24s    | 1.72s    | 0       | -      | 209.74s |
| 13  | 4318.03s | 3644.30s | 2119.77s   | 507.01s   | 912.53s    | 0ns      | 6.60s | 11.93s | 2820.57s | 295.57s  | 2640.06s | 299.20s  | 2.86s    | 390.76s  | 15.73s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.29s    | 42.32ms | 3.30s    | 2.10s    | 0       | -      | 192.59s |
| 14  | 3920.49s | 3315.22s | 1921.68s   | 458.75s   | 839.22s    | 0ns      | 5.37s | 10.19s | 2332.51s | 194.00s  | 2114.47s | 259.00s  | 2.54s    | 339.23s  | 14.61s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 2.97s    | 41.29ms | 2.40s    | 1.35s    | 0       | -      | 189.19s |
| 15  | 3925.59s | 3330.39s | 1932.49s   | 447.91s   | 853.39s    | 0ns      | 5.03s | 9.89s  | 2356.77s | 191.20s  | 2150.92s | 250.00s  | 2.34s    | 326.94s  | 14.88s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 2.76s    | 31.85ms | 2.27s    | 1.35s    | 0       | -      | 188.20s |
| 16  | 3550.34s | 3007.29s | 1755.27s   | 398.83s   | 765.60s    | 0ns      | 4.19s | 8.24s  | 1856.72s | 205.07s  | 1712.80s | 226.40s  | 2.25s    | 291.29s  | 13.59s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 2.29s    | 11.74ms | 1.90s    | 1.20s    | 0       | -      | 172.80s |
| 17  | 3538.28s | 3023.96s | 1786.14s   | 384.14s   | 766.77s    | 0ns      | 4.05s | 7.56s  | 1768.27s | 188.00s  | 1637.64s | 208.41s  | 1.97s    | 266.10s  | 13.51s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 2.10s    | 22.63ms | 1.95s    | 1.26s    | 0       | -      | 164.84s |
| 18  | 3187.54s | 2719.75s | 1613.71s   | 341.39s   | 686.09s    | 0ns      | 3.13s | 5.94s  | 1331.26s | 246.63s  | 1301.32s | 185.75s  | 1.86s    | 233.96s  | 12.17s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.73s    | 19.71ms | 1.40s    | 1.14s    | 0       | -      | 166.13s |
| 19  | 3036.30s | 2612.30s | 1560.05s   | 322.02s   | 655.84s    | 0ns      | 2.61s | 5.22s  | 1113.98s | 234.54s  | 1112.97s | 158.59s  | 1.59s    | 198.46s  | 11.55s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.41s    | 24.41ms | 1.19s    | 1.04s    | 0       | -      | 166.64s |
| 20  | 3062.71s | 2658.91s | 1590.53s   | 327.55s   | 664.23s    | 0ns      | 2.71s | 4.89s  | 1232.05s | 248.09s  | 1264.91s | 143.94s  | 1.40s    | 179.39s  | 11.60s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.45s    | 16.88ms | 1.26s    | 1.06s    | 0       | -      | 178.35s |
| 21  | 2840.97s | 2480.65s | 1494.59s   | 308.60s   | 606.49s    | 0ns      | 2.14s | 4.13s  | 991.12s  | 243.26s  | 1060.18s | 117.01s  | 1.12s    | 142.01s  | 10.35s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.10s    | 12.76ms | 1.05s    | 1.06s    | 0       | -      | 144.96s |
| 22  | 2630.26s | 2297.10s | 1399.43s   | 267.87s   | 566.06s    | 0ns      | 1.43s | 2.63s  | 707.32s  | 323.10s  | 881.39s  | 109.17s  | 836.56ms | 128.97s  | 9.72s    | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 753.47ms | 6.88ms  | 677.91ms | 999.31ms | 0       | -      | 147.63s |
| 23  | 2596.10s | 2272.74s | 1400.11s   | 257.74s   | 551.95s    | 0ns      | 1.41s | 2.26s  | 564.25s  | 313.30s  | 738.23s  | 103.09s  | 758.19ms | 121.06s  | 9.54s    | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 748.81ms | 11.71ms | 657.52ms | 1.01s    | 0       | -      | 142.32s |
| 24  | 2569.18s | 2282.84s | 1416.68s   | 251.21s   | 553.96s    | 0ns      | 1.10s | 1.82s  | 470.71s  | 300.09s  | 672.25s  | 71.11s   | 557.92ms | 84.05s   | 9.77s    | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 584.45ms | 13.31ms | 517.26ms | 920.71ms | 0       | -      | 142.14s |
| 25  | 3126.57s | 2817.79s | 1786.57s   | 335.84s   | 623.20s    | 0ns      | 2.93s | 5.13s  | 1226.89s | 264.06s  | 1363.11s | 67.22s   | 492.39ms | 88.12s   | 9.99s    | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.46s    | 18.52ms | 1.47s    | 1.26s    | 0       | -      | 194.53s |
| 26  | 2830.96s | 2526.97s | 1596.27s   | 293.90s   | 571.97s    | 0ns      | 2.16s | 3.51s  | 916.96s  | 240.97s  | 1031.78s | 78.85s   | 601.53ms | 99.36s   | 8.97s    | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.13s    | 8.89ms  | 1.02s    | 1.13s    | 0       | -      | 154.41s |
| 27  | 3313.32s | 2973.27s | 1867.42s   | 368.16s   | 660.61s    | 0ns      | 3.13s | 5.62s  | 1480.65s | 252.40s  | 1572.85s | 87.21s   | 695.03ms | 115.72s  | 10.46s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.59s    | 21.50ms | 1.54s    | 1.32s    | 0       | -      | 186.58s |
| 28  | 3201.48s | 2835.70s | 1803.59s   | 335.03s   | 624.84s    | 0ns      | 3.13s | 5.26s  | 1271.71s | 287.34s  | 1373.95s | 109.26s  | 786.60ms | 145.17s  | 9.69s    | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.64s    | 16.01ms | 1.49s    | 1.40s    | 0       | -      | 169.47s |
| 29  | 3228.62s | 2861.63s | 1802.79s   | 336.75s   | 647.89s    | 0ns      | 2.77s | 5.56s  | 1360.01s | 331.23s  | 1501.96s | 115.27s  | 875.17ms | 151.69s  | 10.11s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.51s    | 31.99ms | 1.26s    | 1.32s    | 0       | -      | 229.19s |
| 30  | 3274.39s | 2916.77s | 1834.99s   | 349.14s   | 659.49s    | 0ns      | 2.97s | 5.86s  | 1369.98s | 319.67s  | 1512.89s | 101.18s  | 884.91ms | 135.74s  | 10.27s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.56s    | 14.83ms | 1.41s    | 1.35s    | 0       | -      | 177.87s |
| 31  | 3334.63s | 2930.10s | 1837.63s   | 362.01s   | 654.74s    | 0ns      | 3.37s | 5.92s  | 1456.05s | 351.62s  | 1582.31s | 139.67s  | 964.32ms | 182.75s  | 10.59s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.79s    | 27.41ms | 1.57s    | 1.54s    | 0       | -      | 185.78s |
| 32  | 3373.82s | 2968.55s | 1883.08s   | 350.50s   | 659.11s    | 0ns      | 2.99s | 5.66s  | 1334.80s | 495.02s  | 1605.69s | 141.46s  | 1.03s    | 184.76s  | 10.35s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.62s    | 25.47ms | 1.38s    | 1.59s    | 0       | -      | 192.85s |
| 33  | 3501.93s | 3083.59s | 1925.14s   | 375.78s   | 703.74s    | 0ns      | 3.43s | 6.45s  | 1626.26s | 341.38s  | 1733.57s | 141.13s  | 1.02s    | 188.20s  | 11.02s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.81s    | 26.78ms | 1.62s    | 1.50s    | 0       | -      | 212.68s |
| 34  | 3971.09s | 3545.13s | 2189.02s   | 481.45s   | 778.70s    | 0ns      | 5.05s | 10.94s | 2336.69s | 171.63s  | 2263.77s | 116.85s  | 922.38ms | 166.93s  | 12.55s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 2.38s    | 23.40ms | 2.68s    | 1.59s    | 0       | -      | 240.78s |
| 35  | 3199.40s | 2825.23s | 1780.61s   | 330.48s   | 641.74s    | 0ns      | 2.37s | 4.65s  | 1144.68s | 471.91s  | 1422.45s | 123.19s  | 1.04s    | 161.94s  | 10.28s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.25s    | 12.62ms | 1.12s    | 1.65s    | 0       | -      | 230.18s |
| 36  | 3334.35s | 2982.11s | 1948.95s   | 351.85s   | 605.31s    | 0ns      | 3.00s | 5.46s  | 1495.53s | 426.08s  | 1747.91s | 101.70s  | 883.23ms | 133.41s  | 9.31s    | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.57s    | 19.67ms | 1.43s    | 1.55s    | 0       | -      | 217.48s |
| 37  | 3915.09s | 3488.21s | 2251.43s   | 440.58s   | 706.18s    | 0ns      | 4.79s | 8.33s  | 2460.78s | 195.98s  | 2413.11s | 121.29s  | 894.13ms | 183.62s  | 10.84s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 2.48s    | 29.67ms | 2.31s    | 1.47s    | 0       | -      | 240.07s |
| 38  | 4422.09s | 3879.65s | 2449.65s   | 510.23s   | 817.43s    | 0ns      | 5.94s | 10.73s | 2937.31s | 155.36s  | 2739.25s | 187.06s  | 1.09s    | 277.00s  | 12.77s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.06s    | 39.35ms | 2.88s    | 1.72s    | 0       | -      | 249.44s |
| 39  | 4837.05s | 4200.11s | 2597.50s   | 585.31s   | 903.35s    | 0ns      | 7.01s | 13.76s | 3683.65s | 121.48s  | 3363.19s | 235.35s  | 1.29s    | 349.39s  | 14.27s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.64s    | 39.30ms | 3.37s    | 1.82s    | 0       | -      | 280.33s |
| 40  | 4824.97s | 4156.95s | 2527.47s   | 595.73s   | 917.26s    | 0ns      | 7.36s | 14.35s | 3779.52s | 84.56s   | 3391.12s | 251.81s  | 1.49s    | 376.84s  | 14.42s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.85s    | 34.43ms | 3.50s    | 1.74s    | 0       | -      | 275.18s |
| 41  | 5009.79s | 4280.08s | 2581.37s   | 595.08s   | 985.43s    | 0ns      | 7.43s | 14.81s | 3787.31s | 91.12s   | 3351.01s | 291.75s  | 1.80s    | 430.76s  | 15.88s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.94s    | 50.63ms | 3.49s    | 1.81s    | 0       | -      | 277.01s |
| 42  | 5113.37s | 4356.39s | 2618.88s   | 606.00s   | 1011.37s   | 0ns      | 7.53s | 15.34s | 3864.01s | 117.05s  | 3428.46s | 309.70s  | 2.01s    | 454.19s  | 16.60s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 4.00s    | 46.46ms | 3.52s    | 1.80s    | 0       | -      | 313.42s |
| 43  | 5318.19s | 4530.69s | 2688.40s   | 651.43s   | 1062.91s   | 0ns      | 7.50s | 16.02s | 4247.67s | 104.08s  | 3775.00s | 320.42s  | 2.18s    | 473.62s  | 17.20s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.95s    | 42.32ms | 3.55s    | 1.76s    | 0       | -      | 296.52s |
| 44  | 4502.13s | 3835.51s | 2304.49s   | 494.45s   | 931.43s    | 0ns      | 5.71s | 12.37s | 3081.73s | 180.14s  | 2794.39s | 269.33s  | 2.36s    | 392.20s  | 15.27s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 3.10s    | 31.86ms | 2.61s    | 1.60s    | 0       | -      | 280.20s |
| 45  | 3413.75s | 3002.58s | 1853.83s   | 348.19s   | 718.13s    | 0ns      | 2.83s | 6.14s  | 1385.61s | 379.33s  | 1547.26s | 133.16s  | 1.93s    | 180.44s  | 12.91s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.56s    | 19.46ms | 1.27s    | 1.42s    | 0       | -      | 214.92s |
| 46  | 3211.34s | 2822.11s | 1784.38s   | 306.96s   | 657.42s    | 0ns      | 2.39s | 4.17s  | 1057.59s | 499.01s  | 1353.84s | 138.74s  | 1.23s    | 174.20s  | 11.27s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.33s    | 28.94ms | 1.06s    | 1.50s    | 0       | -      | 220.32s |
| 47  | 3461.49s | 3055.40s | 1933.62s   | 348.44s   | 693.38s    | 0ns      | 3.20s | 7.74s  | 1731.56s | 336.79s  | 1846.65s | 140.23s  | 3.49s    | 179.23s  | 11.45s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.73s    | 25.76ms | 1.46s    | 1.42s    | 0       | -      | 238.19s |
| 48  | 3444.22s | 3021.80s | 1920.56s   | 335.90s   | 687.42s    | 0ns      | 3.13s | 8.05s  | 1428.42s | 334.96s  | 1526.82s | 152.35s  | 3.89s    | 195.59s  | 11.55s   | 0ns    | 0ns         | 0ns                 | 0ns             | 0ns           | 1.73s    | 31.95ms | 1.40s    | 1.47s    | 0       | -      | 226.09s |
| 49  | 3498.38s | 3114.30s | 2024.16s   | 369.19s   | 641.29s    | 0ns      | 3.79s | 8.53s  | 1590.30s | 306.25s  | 1691.50s | 119.38s  | 3.01s    | 157.50s  | 10.70s   | 0ns    | 0ns         |

                                                                                                                0ns |             0ns |           0ns |    2.07s |  33.49ms |    1.72s |    1.48s |       0 |       - |  229.75s |
| 50 | 3333.84s | 2984.47s |   1904.45s |   344.59s |    658.09s |      0ns |    3.31s |    5.59s | 1434.30s |  231.73s | 1495.64s |   95.93s | 665.87ms |  127.04s |   11.26s |      0ns |         0ns |                 0ns |             0ns |           0ns |    1.78s |  36.90ms |    1.52s |    1.44s |       0 |       - |  232.24s |

=== Task Avg Time ===
| CP  | add    | navigate | nav_search | nav_fetch | nav_rerank | register | spawn   | scrub   | split | merge    | reassign | drop     | load    | load_raw | quantize | search | search_scan | search_load_cluster | search_load_raw | search_rerank | raw_add | raw_rm | q_add   | q_rm    | rr_vecs | rr_data |
| --- | ------ | -------- | ---------- | --------- | ---------- | -------- | ------- | ------- | ----- | -------- | -------- | -------- | ------- | -------- | -------- | ------ | ----------- | ------------------- | --------------- | ------------- | ------- | ------ | ------- | ------- | ------- | ------- |
| 1   | 1.91ms | 521.9µs  | 188.4µs    | 138.2µs   | 171.1µs    | -        | 846.4µs | 122.9µs | 1.09s | -        | 659.7µs  | 537.4µs  | 35ns    | 114ns    | 1.9µs    | -      | -           | -                   | -               | -             | 339.8µs | 11.9µs | 505.3µs | 409.3µs | -       | -       |
| 2   | 3.28ms | 940.7µs  | 429.5µs    | 200.6µs   | 274.7µs    | -        | 1.56ms  | 186.1µs | 1.63s | -        | 1.19ms   | 62.94ms  | 8.9µs   | 47.72ms  | 2.0µs    | -      | -           | -                   | -               | -             | 691.6µs | 17.0µs | 868.1µs | 710.2µs | -       | -       |
| 3   | 3.67ms | 1.08ms   | 543.2µs    | 208.6µs   | 288.9µs    | -        | 1.90ms  | 232.2µs | 1.76s | 246.35ms | 1.33ms   | 116.69ms | 17.7µs  | 87.26ms  | 2.1µs    | -      | -           | -                   | -               | -             | 875.0µs | 19.6µs | 1.02ms  | 845.2µs | -       | -       |
| 4   | 3.88ms | 1.14ms   | 604.6µs    | 215.0µs   | 288.1µs    | -        | 2.18ms  | 261.8µs | 1.95s | 772.40ms | 1.45ms   | 152.70ms | 26.1µs  | 113.13ms | 2.1µs    | -      | -           | -                   | -               | -             | 1.08ms  | 25.5µs | 1.09ms  | 916.5µs | -       | -       |
| 5   | 4.23ms | 1.24ms   | 669.9µs    | 218.3µs   | 318.3µs    | -        | 2.36ms  | 282.5µs | 2.16s | 1.10s    | 1.58ms   | 176.44ms | 32.7µs  | 128.76ms | 2.2µs    | -      | -           | -                   | -               | -             | 1.17ms  | 24.8µs | 1.19ms  | 987.5µs | -       | -       |
| 6   | 4.45ms | 1.31ms   | 720.6µs    | 220.5µs   | 329.7µs    | -        | 2.56ms  | 312.3µs | 2.27s | 1.88s    | 1.65ms   | 205.23ms | 39.1µs  | 148.81ms | 2.3µs    | -      | -           | -                   | -               | -             | 1.33ms  | 23.1µs | 1.23ms  | 1.04ms  | -       | -       |
| 7   | 4.39ms | 1.34ms   | 754.9µs    | 222.5µs   | 322.0µs    | -        | 2.66ms  | 318.2µs | 2.29s | 1.08s    | 1.65ms   | 217.99ms | 50.4µs  | 158.02ms | 2.3µs    | -      | -           | -                   | -               | -             | 1.39ms  | 20.8µs | 1.27ms  | 1.06ms  | -       | -       |
| 8   | 4.61ms | 1.42ms   | 800.5µs    | 219.9µs   | 356.9µs    | -        | 2.80ms  | 352.7µs | 2.55s | 1.33s    | 1.83ms   | 234.28ms | 55.3µs  | 172.73ms | 2.4µs    | -      | -           | -                   | -               | -             | 1.47ms  | 37.1µs | 1.33ms  | 1.14ms  | -       | -       |
| 9   | 4.71ms | 1.48ms   | 834.6µs    | 226.8µs   | 375.6µs    | -        | 2.89ms  | 344.0µs | 2.59s | 1.18s    | 1.83ms   | 229.68ms | 60.2µs  | 165.37ms | 2.6µs    | -      | -           | -                   | -               | -             | 1.54ms  | 26.0µs | 1.35ms  | 1.16ms  | -       | -       |
| 10  | 4.88ms | 1.53ms   | 870.5µs    | 224.0µs   | 394.3µs    | -        | 2.96ms  | 336.0µs | 2.84s | 1.09s    | 2.01ms   | 228.76ms | 61.9µs  | 167.70ms | 2.7µs    | -      | -           | -                   | -               | -             | 1.59ms  | 30.8µs | 1.36ms  | 1.19ms  | -       | -       |
| 11  | 4.63ms | 1.52ms   | 872.6µs    | 225.0µs   | 376.4µs    | -        | 3.00ms  | 357.6µs | 2.79s | 822.05ms | 1.96ms   | 203.30ms | 72.7µs  | 148.42ms | 2.7µs    | -      | -           | -                   | -               | -             | 1.61ms  | 29.8µs | 1.39ms  | 1.20ms  | -       | -       |
| 12  | 4.56ms | 1.52ms   | 876.4µs    | 222.8µs   | 375.3µs    | -        | 3.00ms  | 348.7µs | 2.83s | 988.95ms | 1.96ms   | 186.52ms | 66.7µs  | 138.30ms | 2.7µs    | -      | -           | -                   | -               | -             | 1.58ms  | 31.2µs | 1.42ms  | 1.20ms  | -       | -       |
| 13  | 4.32ms | 1.64ms   | 952.2µs    | 227.8µs   | 409.9µs    | -        | 3.48ms  | 374.1µs | 2.88s | 839.67ms | 2.15ms   | 224.45ms | 89.7µs  | 173.67ms | 3.0µs    | -      | -           | -                   | -               | -             | 1.73ms  | 31.8µs | 1.74ms  | 1.57ms  | -       | -       |
| 14  | 3.92ms | 1.60ms   | 927.5µs    | 221.4µs   | 405.0µs    | -        | 3.21ms  | 361.7µs | 2.71s | 854.62ms | 1.97ms   | 238.27ms | 90.3µs  | 178.26ms | 3.0µs    | -      | -           | -                   | -               | -             | 1.77ms  | 38.0µs | 1.43ms  | 1.25ms  | -       | -       |
| 15  | 3.93ms | 1.61ms   | 934.3µs    | 216.5µs   | 412.6µs    | -        | 3.17ms  | 369.0µs | 2.90s | 650.35ms | 2.01ms   | 226.04ms | 87.2µs  | 173.90ms | 3.1µs    | -      | -           | -                   | -               | -             | 1.74ms  | 28.8µs | 1.43ms  | 1.22ms  | -       | -       |
| 16  | 3.55ms | 1.64ms   | 958.4µs    | 217.8µs   | 418.0µs    | -        | 3.21ms  | 375.6µs | 2.79s | 727.19ms | 2.06ms   | 238.82ms | 102.8µs | 183.55ms | 3.2µs    | -      | -           | -                   | -               | -             | 1.75ms  | 12.4µs | 1.46ms  | 1.26ms  | -       | -       |
| 17  | 3.54ms | 1.69ms   | 998.1µs    | 214.7µs   | 428.5µs    | -        | 3.29ms  | 366.6µs | 2.82s | 620.48ms | 2.07ms   | 223.86ms | 95.4µs  | 173.47ms | 3.3µs    | -      | -           | -                   | -               | -             | 1.70ms  | 24.3µs | 1.58ms  | 1.35ms  | -       | -       |
| 18  | 3.19ms | 1.65ms   | 978.6µs    | 207.0µs   | 416.1µs    | -        | 3.19ms  | 360.5µs | 2.64s | 642.27ms | 2.00ms   | 208.95ms | 112.6µs | 171.15ms | 3.3µs    | -      | -           | -                   | -               | -             | 1.76ms  | 22.2µs | 1.43ms  | 1.28ms  | -       | -       |
| 19  | 3.04ms | 1.65ms   | 986.3µs    | 203.6µs   | 414.7µs    | -        | 3.14ms  | 369.9µs | 2.63s | 622.13ms | 1.91ms   | 197.99ms | 113.0µs | 164.15ms | 3.3µs    | -      | -           | -                   | -               | -             | 1.70ms  | 30.5µs | 1.44ms  | 1.30ms  | -       | -       |
| 20  | 3.06ms | 1.61ms   | 965.4µs    | 198.8µs   | 403.2µs    | -        | 3.02ms  | 323.1µs | 2.69s | 637.75ms | 1.94ms   | 169.94ms | 92.6µs  | 139.38ms | 3.2µs    | -      | -           | -                   | -               | -             | 1.61ms  | 19.9µs | 1.40ms  | 1.25ms  | -       | -       |
| 21  | 2.84ms | 1.58ms   | 950.4µs    | 196.2µs   | 385.6µs    | -        | 2.94ms  | 325.7µs | 2.66s | 524.27ms | 1.84ms   | 139.80ms | 88.1µs  | 118.94ms | 3.0µs    | -      | -           | -                   | -               | -             | 1.50ms  | 15.2µs | 1.43ms  | 1.27ms  | -       | -       |
| 22  | 2.63ms | 1.63ms   | 992.2µs    | 189.9µs   | 401.3µs    | -        | 3.06ms  | 312.3µs | 2.97s | 595.02ms | 2.11ms   | 140.14ms | 99.2µs  | 127.82ms | 3.2µs    | -      | -           | -                   | -               | -             | 1.61ms  | 8.8µs  | 1.45ms  | 1.28ms  | -       | -       |
| 23  | 2.60ms | 1.67ms   | 1.03ms     | 189.0µs   | 404.8µs    | -        | 3.15ms  | 288.7µs | 2.45s | 583.42ms | 2.02ms   | 134.41ms | 97.0µs  | 123.16ms | 3.3µs    | -      | -           | -                   | -               | -             | 1.68ms  | 15.3µs | 1.47ms  | 1.32ms  | -       | -       |
| 24  | 2.57ms | 1.71ms   | 1.06ms     | 188.7µs   | 416.1µs    | -        | 3.29ms  | 296.6µs | 2.72s | 591.90ms | 2.02ms   | 104.57ms | 91.0µs  | 99.83ms  | 3.5µs    | -      | -           | -                   | -               | -             | 1.74ms  | 19.6µs | 1.54ms  | 1.35ms  | -       | -       |
| 25  | 3.13ms | 1.62ms   | 1.03ms     | 193.6µs   | 359.3µs    | -        | 2.93ms  | 296.0µs | 2.39s | 600.13ms | 1.84ms   | 70.54ms  | 28.4µs  | 61.15ms  | 2.6µs    | -      | -           | -                   | -               | -             | 1.46ms  | 19.4µs | 1.47ms  | 1.32ms  | -       | -       |
| 26  | 2.83ms | 1.64ms   | 1.04ms     | 190.6µs   | 370.9µs    | -        | 3.16ms  | 293.5µs | 2.63s | 480.03ms | 1.89ms   | 92.76ms  | 50.3µs  | 83.92ms  | 2.7µs    | -      | -           | -                   | -               | -             | 1.66ms  | 10.5µs | 1.50ms  | 1.33ms  | -       | -       |
| 27  | 3.31ms | 1.62ms   | 1.02ms     | 200.8µs   | 360.4µs    | -        | 2.98ms  | 308.9µs | 2.75s | 554.72ms | 1.87ms   | 87.74ms  | 38.2µs  | 76.94ms  | 2.6µs    | -      | -           | -                   | -               | -             | 1.52ms  | 21.6µs | 1.47ms  | 1.33ms  | -       | -       |
| 28  | 3.20ms | 1.66ms   | 1.06ms     | 196.4µs   | 366.3µs    | -        | 3.33ms  | 325.9µs | 2.62s | 530.14ms | 1.94ms   | 106.29ms | 48.8µs  | 98.02ms  | 2.6µs    | -      | -           | -                   | -               | -             | 1.74ms  | 15.6µs | 1.59ms  | 1.36ms  | -       | -       |
| 29  | 3.23ms | 1.62ms   | 1.02ms     | 190.6µs   | 366.6µs    | -        | 3.10ms  | 353.7µs | 2.96s | 585.21ms | 1.94ms   | 112.35ms | 55.6µs  | 103.89ms | 2.7µs    | -      | -           | -                   | -               | -             | 1.69ms  | 31.2µs | 1.41ms  | 1.29ms  | -       | -       |
| 30  | 3.27ms | 1.66ms   | 1.05ms     | 199.2µs   | 376.2µs    | -        | 3.13ms  | 355.2µs | 2.81s | 604.28ms | 1.99ms   | 99.49ms  | 53.7µs  | 91.84ms  | 2.7µs    | -      | -           | -                   | -               | -             | 1.64ms  | 14.6µs | 1.49ms  | 1.33ms  | -       | -       |
| 31  | 3.33ms | 1.66ms   | 1.04ms     | 205.6µs   | 371.8µs    | -        | 3.27ms  | 335.5µs | 2.73s | 565.31ms | 2.07ms   | 121.04ms | 54.6µs  | 110.69ms | 2.7µs    | -      | -           | -                   | -               | -             | 1.74ms  | 23.8µs | 1.53ms  | 1.33ms  | -       | -       |
| 32  | 3.37ms | 1.68ms   | 1.06ms     | 198.0µs   | 372.4µs    | -        | 3.26ms  | 350.8µs | 2.82s | 688.48ms | 2.08ms   | 118.67ms | 64.1µs  | 112.80ms | 2.7µs    | -      | -           | -                   | -               | -             | 1.76ms  | 21.4µs | 1.50ms  | 1.33ms  | -       | -       |
| 33  | 3.50ms | 1.67ms   | 1.04ms     | 203.2µs   | 380.5µs    | -        | 3.20ms  | 352.8µs | 2.91s | 611.80ms | 2.03ms   | 126.34ms | 55.9µs  | 115.39ms | 2.7µs    | -      | -           | -                   | -               | -             | 1.69ms  | 24.0µs | 1.51ms  | 1.34ms  | -       | -       |
| 34  | 3.97ms | 1.57ms   | 968.0µs    | 212.9µs   | 344.3µs    | -        | 2.82ms  | 363.9µs | 2.51s | 568.30ms | 1.78ms   | 94.85ms  | 30.7µs  | 79.75ms  | 2.4µs    | -      | -           | -                   | -               | -             | 1.33ms  | 19.0µs | 1.49ms  | 1.29ms  | -       | -       |
| 35  | 3.20ms | 1.69ms   | 1.07ms     | 197.7µs   | 383.8µs    | -        | 3.18ms  | 347.2µs | 2.98s | 565.17ms | 2.11ms   | 101.23ms | 77.5µs  | 102.49ms | 2.9µs    | -      | -           | -                   | -               | -             | 1.68ms  | 10.4µs | 1.50ms  | 1.36ms  | -       | -       |
| 36  | 3.33ms | 1.61ms   | 1.05ms     | 190.5µs   | 327.6µs    | -        | 3.21ms  | 327.4µs | 3.16s | 638.79ms | 2.04ms   | 89.13ms  | 53.0µs  | 83.43ms  | 2.3µs    | -      | -           | -                   | -               | -             | 1.68ms  | 17.2µs | 1.53ms  | 1.35ms  | -       | -       |
| 37  | 3.92ms | 1.60ms   | 1.03ms     | 202.3µs   | 324.2µs    | -        | 3.23ms  | 332.6µs | 3.15s | 624.16ms | 2.03ms   | 110.76ms | 35.7µs  | 102.24ms | 2.3µs    | -      | -           | -                   | -               | -             | 1.67ms  | 27.1µs | 1.56ms  | 1.35ms  | -       | -       |
| 38  | 4.42ms | 1.63ms   | 1.03ms     | 213.8µs   | 342.5µs    | -        | 3.25ms  | 352.8µs | 3.01s | 532.06ms | 1.96ms   | 147.40ms | 35.7µs  | 130.60ms | 2.4µs    | -      | -           | -                   | -               | -             | 1.67ms  | 31.0µs | 1.57ms  | 1.36ms  | -       | -       |
| 39  | 4.84ms | 1.60ms   | 986.6µs    | 222.3µs   | 343.1µs    | -        | 3.20ms  | 376.3µs | 3.15s | 578.49ms | 2.05ms   | 170.42ms | 35.3µs  | 145.52ms | 2.4µs    | -      | -           | -                   | -               | -             | 1.66ms  | 28.5µs | 1.54ms  | 1.31ms  | -       | -       |
| 40  | 4.82ms | 1.55ms   | 943.7µs    | 222.4µs   | 342.5µs    | -        | 3.25ms  | 382.9µs | 3.12s | 617.26ms | 2.01ms   | 186.94ms | 39.7µs  | 157.02ms | 2.4µs    | -      | -           | -                   | -               | -             | 1.70ms  | 25.6µs | 1.54ms  | 1.29ms  | -       | -       |
| 41  | 5.01ms | 1.63ms   | 985.0µs    | 227.1µs   | 376.0µs    | -        | 3.35ms  | 399.1µs | 3.22s | 542.38ms | 2.06ms   | 216.92ms | 48.4µs  | 180.69ms | 2.7µs    | -      | -           | -                   | -               | -             | 1.78ms  | 37.6µs | 1.57ms  | 1.34ms  | -       | -       |
| 42  | 5.11ms | 1.66ms   | 1.00ms     | 231.5µs   | 386.3µs    | -        | 3.37ms  | 407.8µs | 3.28s | 775.18ms | 2.11ms   | 233.03ms | 53.3µs  | 190.59ms | 2.8µs    | -      | -           | -                   | -               | -             | 1.79ms  | 35.0µs | 1.58ms  | 1.35ms  | -       | -       |
| 43  | 5.32ms | 1.63ms   | 967.2µs    | 234.4µs   | 382.4µs    | -        | 3.27ms  | 413.9µs | 3.53s | 819.51ms | 2.11ms   | 240.74ms | 56.3µs  | 195.79ms | 2.9µs    | -      | -           | -                   | -               | -             | 1.72ms  | 31.8µs | 1.55ms  | 1.32ms  | -       | -       |
| 44  | 4.50ms | 1.68ms   | 1.01ms     | 216.7µs   | 408.1µs    | -        | 3.30ms  | 418.4µs | 3.44s | 562.93ms | 2.18ms   | 221.67ms | 79.9µs  | 191.50ms | 3.0µs    | -      | -           | -                   | -               | -             | 1.79ms  | 26.2µs | 1.51ms  | 1.32ms  | -       | -       |
| 45  | 3.41ms | 1.79ms   | 1.11ms     | 207.6µs   | 428.2µs    | -        | 3.44ms  | 423.5µs | 3.30s | 625.96ms | 2.28ms   | 129.78ms | 133.4µs | 126.27ms | 3.6µs    | -      | -           | -                   | -               | -             | 1.90ms  | 19.0µs | 1.54ms  | 1.39ms  | -       | -       |
| 46  | 3.21ms | 1.79ms   | 1.13ms     | 195.2µs   | 418.0µs    | -        | 3.55ms  | 346.6µs | 3.07s | 694.04ms | 2.35ms   | 130.52ms | 102.1µs | 125.33ms | 3.3µs    | -      | -           | -                   | -               | -             | 1.98ms  | 27.2µs | 1.57ms  | 1.41ms  | -       | -       |
| 47  | 3.46ms | 1.73ms   | 1.09ms     | 197.0µs   | 392.0µs    | -        | 3.38ms  | 480.2µs | 3.56s | 628.34ms | 2.36ms   | 137.34ms | 216.5µs | 121.27ms | 3.0µs    | -      | -           | -                   | -               | -             | 1.83ms  | 25.2µs | 1.55ms  | 1.39ms  | -       | -       |
| 48  | 3.44ms | 1.79ms   | 1.14ms     | 198.7µs   | 406.6µs    | -        | 3.54ms  | 526.5µs | 3.15s | 573.56ms | 2.19ms   | 146.77ms | 254.5µs | 133.33ms | 3.1µs    | -      | -           | -                   | -               | -             | 1.96ms  | 30.8µs | 1.58ms  | 1.41ms  | -       | -       |
| 49  | 3.50ms | 1.71ms   | 1.11ms     | 202.7µs   | 352.0µs    | -        | 3.46ms  | 452.7µs | 2.82s | 619.93ms | 2.03ms   | 112.94ms | 160.1µs | 99.18ms  | 2.6µs    | -      | -           | -                   | -               | -             | 1.89ms  | 31.7µs | 1.57ms  | 1.40ms  | -       | -       |
| 50  | 3.33ms | 1.72ms   | 1.10ms     | 198.7µs   | 379.5µs    | -        | 3.27ms  | 319.2µs | 2.77s | 446.50ms | 2.02ms   | 92.59ms  | 38.0µs  | 83.09ms  | 2.9µs    | -      | -           | -                   | -               | -             | 1.76ms  | 35.6µs | 1.51ms  | 1.39ms  | -       | -       |


=== Indexing Summary ===
Total vectors: 50.00M
Total time: 175.9m
Overall throughput: 4738 vec/s
