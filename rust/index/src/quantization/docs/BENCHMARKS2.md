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
  - [100M - msmarco](#100m---msmarco)

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

## 100M - msmarco
