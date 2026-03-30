- [Intro to Large Index Benchmarking](#intro-to-large-index-benchmarking)
- [Key Results](#key-results)
- [RaBitQ Implementation](#rabitq-implementation)
  - [Primitives](#primitives)
  - [Core Functions](#core-functions)
    - [r6i.8xlarge](#r6i8xlarge)
    - [vs Baseline: Exact f32 Distance Query](#vs-baseline-exact-f32-distance-query)
    - [Thread Scaling](#thread-scaling)
  - [Error Bound](#error-bound)
- [Comparing Different Central Indices](#comparing-different-central-indices)
  - [Summary](#summary)
  - [Usearch - 1 Bit](#usearch---1-bit)
    - [Parallelism (blockers)](#parallelism-blockers)
      - [Our global lock](#our-global-lock)
      - [Usearch global lock](#usearch-global-lock)
    - [Performance](#performance)
      - [1-bit vs 4-bit](#1-bit-vs-4-bit)
      - [Recall: 1 bit centroids + Reranking](#recall-1-bit-centroids--reranking)
      - [Note: USearch ef/k coupling](#note-usearch-efk-coupling)
    - [Thread scaling](#thread-scaling-1)
  - [Usearch - 1 Bit, Improved Concurrency](#usearch---1-bit-improved-concurrency)
    - [USearch only benchmark](#usearch-only-benchmark)
    - [Full Quantized SPANN benchmark](#full-quantized-spann-benchmark)
      - [Specifics](#specifics)
  - [USearch - Reranked, Improved Concurrency, Improved Concurrency](#usearch---reranked-improved-concurrency-improved-concurrency)
    - [1 Bit build + search](#1-bit-build--search)
    - [4 Bit build + search](#4-bit-build--search)
  - [Flat / Brute Force](#flat--brute-force)
    - [Architecture](#architecture)
      - [Thread Safety](#thread-safety)
    - [FP vs 1 Bit vs 4 Bit (Navigate only)](#fp-vs-1-bit-vs-4-bit-navigate-only)
    - [Flat vs USearch HNSW (forked) -- 1-bit code-to-code, wikipedia-en dim=1024 (Navigate only)](#flat-vs-usearch-hnsw-forked----1-bit-code-to-code-wikipedia-en-dim1024-navigate-only)
    - [Sythetic Workload (Thread Scaling + Performance + Recall)](#sythetic-workload-thread-scaling--performance--recall)
  - [Hierarchical SPANN](#hierarchical-spann)
    - [Design](#design)
    - [Hierarchical Tree Config vs SPANN Config](#hierarchical-tree-config-vs-spann-config)
    - [Balanced k-means (100K centroids, wikipedia-en, f32, eps=1.0, r=4)](#balanced-k-means-100k-centroids-wikipedia-en-f32-eps10-r4)
- [Recall](#recall)
  - [USearch Recall](#usearch-recall)
    - [1-bit centroids](#1-bit-centroids)
    - [4-bit centroids](#4-bit-centroids)
    - [full precision centroids](#full-precision-centroids)
  - [Real SPANN index + USearch.](#real-spann-index--usearch)
  - [Single Centroid Recall](#single-centroid-recall)
    - [1-bit, 4-bit query (1bit-code-4bit-query)](#1-bit-4-bit-query-1bit-code-4bit-query)
    - [1-bit, 1-bit query (1bit-code-1bit-query)](#1-bit-1-bit-query-1bit-code-1bit-query)
    - [4-bit (4bit-code-full-query)](#4-bit-4bit-code-full-query)
    - [1-bit, f32 query (1bit-code-full-query)](#1-bit-f32-query-1bit-code-full-query)
  - [Quantized KMeans Clustering Recall (Needs Redo)](#quantized-kmeans-clustering-recall-needs-redo)
  - [Synthetic Index - Reranking with both 1-bit and 4-bit centroids](#synthetic-index---reranking-with-both-1-bit-and-4-bit-centroids)
- [Older Benchmarks](#older-benchmarks)
  - [SPANN](#spann)
  - [Synthetic SPANN / Centroid Recall (Obsolete)](#synthetic-spann--centroid-recall-obsolete)

# Intro to Large Index Benchmarking

We want a vector index that can support 1B+ vectors. This means:
- Read + Write latency
  - Can index 1M vectors in < 2 minutes
  - < ~100ms queries
- Recall
  - /> 90%
- Reasonable Memory, Disk, and Network usage

To do this we need to address these core components/bottle necks:
- Performance of core functions: quantize, distance_code, distance_query, and their variants
- Recall (at both levels: central index and posting lists)
- Central index performance
- Index build time
- Rerank time (of both centroid and data vectors)
- Data vector (S3) load time
- Thread scaling in general

This document contains benchmarks for most of the above components.


# Key Results

- USearch sans global locks: 2X+ speedup
- USearch 1 Bit (add 1M data vectors): 96.07% Recall, 4.74ms latency (vs 4 bit: WIP)
- E2E Quantized SPANN 1M data vectors: X% Recall, Y latency

# RaBitQ Implementation

## Primitives

| Function      | Primitive | Benchmark Name                          | Latency   | Throughput   |
| ------------- | --------- | --------------------------------------- | --------- | ------------ |
| distance_code | simsimd   | primitives/dc-1bit/hamming/simsimd/1024 | 4.7657 ns | 50.025 GiB/s |


## Core Functions

[../../../benches/vector/quantization.rs](../../../benches/vector/quantization.rs)

### r6i.8xlarge


| Function       | Benchmark                                                  | 4-bit               | 1-bit               | Speedup |
| -------------- | ---------------------------------------------------------- | ------------------- | ------------------- | ------- |
| quantize data  | quantize/quant-4bit/1024 vs quantize/quant-1bit/1024       | 43.2 ms, 92.5 MiB/s | 576 µs, 6.78 GiB/s  | ~75x    |
| quantize query | primitives/quant-query/full/1024                           | N/A                 | 2.21 µs, 1.73 GiB/s | --      |
| distance_code  | distance_query/dc-4bit/scan vs distance_query/dc-1bit/scan | 549ns, 1.77 GiB/s   | 34 ns, 8 GiB/s      | 16x     |
| distance_query | distance_query/dq-4f/scan vs distance_query/dq-bw/scan     | 381 ns, 1.28 GiB/s  | 29 ns, 4.6 GiB/s    | 13x     |



[performance_r6i.8xlarge.txt](performance_r6i.8xlarge.txt)

Benchmark data from `cargo bench -p chroma-index --bench quantization` (dim=1024,
BATCH=512 for quantize/distance_code, SCAN_N=2048 for scan).
Throughput for quantize benchmarks counts both input arrays (embedding + centroid =
`2 * dim * 4` bytes per call).

The batch `quant-query` includes residual allocation, `c_dot_q`, `q_norm`, and cache-cold
effects from cycling 512 distinct queries (~2.55 us/query). `quant-query/full` isolates
`QuantizedQuery::new` with a single hot-cache vector (568 ns). The 4.5x per-query gap
is the cost of the preparation pipeline and cache pressure, not the quantization itself.


### vs Baseline: Exact f32 Distance Query

[../../../benches/vector/quantization.rs](../../../benches/vector/quantization.rs)

Hot-scan benchmark: 1 query vs 2048 vectors, dim=1024, query in L1.

| Benchmark | Function                   | Time   | Per vector | vs exact        |
| --------- | -------------------------- | ------ | ---------- | --------------- |
| dq-exact  | f32 x f32, no quantization | 290 us | 141 ns     | 1.0x            |
| dq-4f     | 4-bit code x f32 query     | 762 us | 372 ns     | 2.6x slower     |
| dq-bw     | 1-bit code x 4 bit query   | 40 us  | 19.5 ns    | **7.2x faster** |


Benchmark data from `cargo bench -p chroma-index --bench quantization_performance -- dq-`
on r6i.8xlarge.

---

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

## Error Bound

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


# Comparing Different Central Indices

What central index will give us the fastest index build times?

## Summary

**Benchmarks**

| Index                                 | Navigate @1M Centroids                                                                  | Navigate During Build @1M Centroids                                           | SPANN Navigate @27k Centroids                                   | Thread Scaling |
| ------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------- |---|
| Flat - 1 bit, global lock             | [131.77ms (R=95.56%, RR=16x)](saved_benchmarks/flat_1bit_rerank.txt)                    | [164.45ms (R=95.56%, RR=16x)](saved_benchmarks/flat_1bit_rerank.txt)          | ?                                                               | Linear |
| USearch - 1 bit                       | [6.09ms (R=96.24%, RR=16x)](saved_benchmarks/usearch_1bit.txt)                          | [3.72ms (R=96.25%, RR=16x)](saved_benchmarks/usearch_1bit.txt)                | [?](saved_benchmarks/quant_spann_1bit.txt)                | O(linear) |
| Hierarchical SPANN - 1 bit, global lock| [?](saved_benchmarks/hierarchical_centroid_profile_1bit.txt)                           | [8.27ms (R=61.5%, RR=2x)](saved_benchmarks/hierarchical_centroid_profile_1bit.txt) | ?                                                          | Linear |
|                                       |                                                                                         |                                                                               |                                                                 | ? |
| USearch - 4 bit (baseline)            | [2.75ms (R=86.07%, RR=1x)](saved_benchmarks/usearch_4bit.txt)                                  | [3.28ms](saved_benchmarks/usearch_4bit.txt)                                   | [444.7µs](saved_benchmarks/quant_spann_4bit.txt)                | ? |
| Flat - Full Precision                 | [529.73ms (R=100%, RR=1x)](saved_benchmarks/flat_full_precision.txt)                           | ?                                                                             | -                                                               | ? |
| Hierarchical SPANN - FullP (100k)     | [58.98ms (R=98.47%)](saved_benchmarks/hierarchical_centroid_profile_full_precision.txt) | [?](saved_benchmarks/hierarchical_centroid_profile_full_precision.txt)        | -                                                               | ? |
| USearch - 1 bit, Threadsafe           | [2.58ms (R=94.95%)](saved_benchmarks/usearch_forked_1bit.txt)                           | ?                                                                             | [90.7µs](quant_spann_1bit_forked_usearch.txt)                   | ? |
| USearch - 1 bit, Threadsafe, Reranked | [2.39ms (R=91.25%)](saved_benchmarks/usearch_rerank_1bit.txt)                           | ?                                                                             | [100.9µs](saved_benchmarks/quant_spann_1bit_forked_usearch.txt) | ? |
| USearch - 1 bit, Reranked             | [2.39ms (R=91.25%)](saved_benchmarks/usearch_rerank_1bit.txt)                           | ?                                                                             | ?                                                               | ? |


Legend:

- Standalone Query:
  - Query latency over the index alone
  - k=100, 32 threads, >90% Recall@100, 10k data vectors/samples
  - 1M centroids
- Synthetic: Navigate latency when synthetically inserting 1M data vectors
  - Uses synthetic SPANN workload on the index alone, so intentionally produces contention between threads
  - 1M centroids
- SPANN: Navigate latency when inserting 1M data vectors
  - Uses full SPANN index.
  - ~27k centroids, 4M existing data vectors, 1M new data vectors
  - Uses the [quantized_spann.rs](../../../benches/quantized_spann.rs) benchmark.

## Usearch - 1 Bit

navigate latency = 568.0µs (@32 threads)

[usearch_1bit.txt](saved_benchmarks/usearch_1bit.txt)


### Parallelism (blockers)

#### Our global lock

We have a global read/write lock on the USearch index. Without it we get a 1.3x speedup in throughput.


| lock? | CP  | add    | navigate | register | spawn  | scrub  | split    | merge   | reassign | drop    | load  | load_raw | quantize | search | raw_add | raw_rm | q_add   | q_rm  |
| ----- | --- | ------ | -------- | -------- | ------ | ------ | -------- | ------- | -------- | ------- | ----- | -------- | -------- | ------ | ------- | ------ | ------- | ----- |
| yes   | 5   | 2.05ms | 416.2µs  | 6.8µs    | 2.04ms | 92.8µs | 320.12ms | 67.53ms | 591.5µs  | 48.69ms | 9.3µs | 35.84ms  | 2.3µs    | -      |         |        |         |       |
| no    | 5   | 1.85ms | 370.2µs  | 6.6µs    | 1.55ms | 95.3µs | 292.97ms | 52.95ms | 528.5µs  | 46.76ms | 9.5µs | 34.75ms  | 2.2µs    | -      | 980.2µs | 7.8µs  | 563.5µs | 7.5µs |


overall throughput: 7476 vec/s vs 5800 vec/s (1.3x speedup) mostly due to faster split (-30ms) and merge (-15ms)

cargo bench -p chroma-index --bench quantized_spann -- --dataset wikipedia-en --checkpoint 1 --threads 16 --data-bits 1 --centroid-bits 1

#### Usearch global lock

Usearch also has a global lock internally. If we chose to fork Usearch and make this lock more granular, we could see an additional speedup.

---

### Performance

#### 1-bit vs 4-bit

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


#### Recall: 1 bit centroids + Reranking

Dim: 1024 | Metric: L2 | Centroid bits: 1 | ef_search: 128 | Centroids: 1.00M | Queries: 200

=== Rerank Sweep (k=100) ===


| Rerank | Fetch | Recall@10 | Recall@100 | Avg lat | search  | fetch   | rerank  |
| ------ | ----- | --------- | ---------- | ------- | ------- | ------- | ------- |
| 1x     | 100   | 84.75%    | 50.88%     | 383.3µs | 383.3µs | 0ns     | 0ns     |
| 2x     | 200   | 90.50%    | 68.18%     | 647.6µs | 519.8µs | 20.2µs  | 107.6µs |
| 4x     | 400   | 95.70%    | 81.98%     | 1.25ms  | 1.00ms  | 39.1µs  | 209.8µs |
| 8x     | 800   | 98.20%    | 91.25%     | 2.39ms  | 1.90ms  | 71.3µs  | 418.1µs |
| 16x    | 1600  | 99.20%    | 96.07%     | 4.82ms  | 3.80ms  | 139.1µs | 880.5µs |


`cargo bench -p chroma-index --bench usearch_rerank -- --dataset wikipedia-en --centroid-bits 1 --initial-centroids 1000000`
[usearch_rerank_1bit.txt](saved_benchmarks/usearch_rerank_1bit.txt)

#### Note: USearch ef/k coupling

USearch increases the beam width when `k > ef_search`:

```cpp
std::size_t expansion = (std::max)(config.expansion, wanted);
```

The original HNSW paper (Malkov & Yashunin, Algorithm 5) treats `ef` and `K` as independent parameters: `ef` controls beam width (search effort), `K` controls how many results to extract from the `ef` candidates. USearch conflates them, so requesting k=200 with ef_search=128 silently widens the beam to 200.

This means rerank sweep rows (2x, 4x, etc.) show inflated latency -- each row runs a progressively wider search, not just extracts more from the same candidate set. The 1x row (k <= ef_search) is the true ef_search performance.

Tested decoupling (using static `expansion = config.expansion`): recall plateaus at the 2x row since all rerank factors return the same ef=128 candidates. Latency becomes flat as expected. However, the coupling is the right default -- for SPANN we control both k and ef, so we can set ef appropriately rather than relying on the automatic widening.

### Thread scaling

Using usearch only benchmark. (usearch_spann_profile)
`cargo bench -p chroma-index --bench usearch_spann_profile -- --dataset wikipedia-en --centroid-bits 1 --initial-centroids 1000000 --threads <threads> --data-vectors 1000000`


| threads | navigate | spawn   | drop   |
| ------- | -------- | ------- | ------ |
| 1       | 218.2µs  | 6.59ms  | 3.39ms |
| 4       | 221.8µs  | 682.6µs | 3.56ms |
| 8       | 264.4µs  | 788.3µs | 3.57ms |
| 16      | 356.7µs  | 933.1µs | 3.74ms |
| 32      | 568.0µs  | 1.17ms  | 4.05ms |


Four layers of lock contention stack up:

1. **Rust `RwLock`** -- `Arc<RwLock<usearch::Index>>` wraps all operations. Spawns/drops take exclusive locks and block all concurrent navigates. Removing this lock gave 10-18% faster navigate and 24-33% faster spawn with no recall impact (see `usearch_concurrency_findings.md`).
2. `**global_mutex_**` (USearch `index.hpp`) -- A `std::mutex` protecting `max_level`_ and `entry_slot`_. Every `add()` holds it exclusively, blocking all concurrent `add()` and `search()` calls. This is the single biggest bottleneck because navigate dominates runtime and every spawn serializes against all navigates.
3. `**slot_lookup_mutex_**` (USearch `index_dense.hpp`) -- A `std::shared_mutex` protecting the key-to-slot hash map. `add()`/`remove()` take exclusive locks; `search()` takes shared. Creates write-side contention that blocks searches during spawns/drops.
4. `**available_threads_mutex_**` (USearch `index_dense.hpp`) -- A `std::mutex` guarding a fixed-size thread-slot pool (size = `hardware_concurrency()`). Acquired on every `search()` and `add()` call, creating a serialization point even for pure-read workloads.

Improvement options (by estimated impact):

1. **Atomic entry point** -- Replace `global_mutex`_ with `std::atomic` for `max_level`*/`entry_slot*`. Searches do acquire loads (zero contention); adds use CAS (contention only on the rare new-max-level event). Eliminates the biggest serialization point.
2. **Remove Rust `RwLock`** -- USearch already has internal thread safety; the outer lock is redundant.
3. **Thread-local context pool** -- Cache thread slot IDs in TLS instead of acquiring `available_threads_mutex`_ on every call.
4. **Disable key lookups** -- SPANN tracks membership externally; disabling `enable_key_lookups` eliminates `slot_lookup_mutex`_.
5. **Double-buffered indices** -- Frozen read index for navigate (zero locking), separate write index for spawns/drops, merge during commit.


## Usearch - 1 Bit, Improved Concurrency

Forked USearch (`@USearch/include/usearch/index.hpp`) with two changes:

1. Replaced `global_mutex`_ with `std::atomic<level_t> max_level`_ and `std::atomic<size_t> entry_slot`_. Searches and adds read atomically (no lock). The rare new-max-level update uses a mutex with double-checked locking.
2. Changed Rust `RwLock` usage: `add()` and `remove()` now take shared (read) locks instead of exclusive (write) locks. Only `reserve()` takes the exclusive lock.

Full details in [usearch_concurrency.md](usearch_concurrency.md)

### USearch only benchmark

Simulate adding 1M data vectors. Operates only on the USearch index.

Navigate latency at 32 threads dropped from 568us to 211.8us (2.68x improvement), recovering the single-thread baseline. Recall improved slightly (+2.3pp @10, +4.9pp @100).

`cargo bench -p chroma-index --bench usearch_spann_profile -- --dataset wikipedia-en --centroid-bits 1 --initial-centroids 1000000 --threads 32 --data-vectors 1000000`


| Metric             | Before (upstream) | After (forked) | Change       |
| ------------------ | ----------------- | -------------- | ------------ |
| Navigate avg (32t) | 568.0us           | 211.8us        | 2.68x faster |
| Phase 2 wall clock | 56.35s            | 25.58s         | 2.2x faster  |
| Spawn avg          | 1.17ms            | 7.82ms         | 6.7x slower  |
| Drop avg           | 4.05ms            | 11.31ms        | 2.8x slower  |
| Recall@10          | 92.80%            | 95.10%         | +2.3pp       |
| Recall@100         | 59.69%            | 64.58%         | +4.9pp       |


Spawn/drop slowdown is expected: before, `add()`/`remove()` held an exclusive RwLock that blocked all concurrent navigates, effectively getting exclusive access. Now they run under shared locks competing with 32 threads of concurrent navigates for per-node locks and `available_threads_mutex`_. Wall time still dropped 2.2x because navigates dominate (3.05M navigates vs 17K spawns+drops).

[usearch_forked_1bit.txt](saved_benchmarks/usearch_forked_1bit.txt)

### Full Quantized SPANN benchmark

Creates a real SPANN index with 5M data vectors

[quant_spann_1bit_forked_usearch.txt](saved_benchmarks/quant_spann_1bit_forked_usearch.txt)
vs
[quant_spann_1bit.txt](saved_benchmarks/quant_spann_1bit.txt)

At CP 5 (5M vectors, 16 threads, 1-bit data, 1-bit centroids):


| Metric       | Original USearch | Forked USearch | Delta       |
| ------------ | ---------------- | -------------- | ----------- |
| navigate avg | 416.2us          | 90.7us         | 4.6x faster |
| spawn avg    | 2.04ms           | 1.23ms         | 1.7x faster |
| drop avg     | 48.69ms          | 40.20ms        | 1.2x faster |

#### Specifics

TODO Original needs to be updated - task counts don't match so Total Time is not comparable

=== Task Counts ===


| scenario | CP  | add   | navigate | register | spawn | scrub  | split | merge | reassign | drop | load   | load_raw | quantize | search | raw_add | raw_rm | q_add | q_rm |
| -------- | --- | ----- | -------- | -------- | ----- | ------ | ----- | ----- | -------- | ---- | ------ | -------- | -------- | ------ | ------- | ------ | ----- | ---- |
| original | 5   | 1.00M | 3.05M    | 2.08M    | 11.0K | 190.3K | 5.6K  | 28    | 2.05M    | 5.6K | 190.3K | 11.1K    | 6.94M    | 0      |         |        |       |      |
| forked   | 5   | 1.00M | 4.01M    | 0        | 11.0K | 191.8K | 5.6K  | 12    | 3.01M    | 5.6K | 191.8K | 11.0K    | 7.33M    | 0      | 11.0K   | 5.6K   | 11.0K | 5.6K |


=== Task Total Time ===


| scenario | CP  | add      | navigate | register | spawn  | scrub  | split    | merge | reassign | drop    | load  | load_raw | quantize | search | raw_pts | raw/pt  |
| -------- | --- | -------- | -------- | -------- | ------ | ------ | -------- | ----- | -------- | ------- | ----- | -------- | -------- | ------ | ------- | ------- |
| original | 5   | 2046.80s | 1270.90s | 14.06s   | 22.49s | 17.65s | 1779.57s | 1.89s | 1214.73s | 272.04s | 1.77s | 396.20s  | 15.68s   | 0ns    | 2.31M   | 171.2µs |
| forked   | 5   | 1806.45s | 363.32s  | 0ns      | 13.56s | 13.83s | 2044.78s | 2.01s | 773.86s  | 223.88s | 1.55s | 388.69s  | 13.22s   | 0ns    | 10.92s  | 46.72ms |


=== Task Avg Time ===


| scenario | CP  | add    | navigate | register | spawn  | scrub  | split    | merge    | reassign | drop    | load  | load_raw | quantize | search | raw_add | raw_rm | q_add   | q_rm  |
| -------- | --- | ------ | -------- | -------- | ------ | ------ | -------- | -------- | -------- | ------- | ----- | -------- | -------- | ------ | ------- | ------ | ------- | ----- |
| original | 5   | 2.05ms | 416.2µs  | 6.8µs    | 2.04ms | 92.8µs | 320.12ms | 67.53ms  | 591.5µs  | 48.69ms | 9.3µs | 35.84ms  | 2.3µs    | -      |         |        |         |       |
| forked   | 5   | 1.81ms | 90.7µs   | -        | 1.23ms | 72.1µs | 367.96ms | 167.16ms | 257.3µs  | 40.20ms | 8.1µs | 35.25ms  | 1.8µs    | -      | 991.4µs | 8.4µs  | 238.0µs | 7.9µs |

## USearch - Reranked, Improved Concurrency, Improved Concurrency

### 1 Bit build + search
Dim: 1024 | Metric: L2 | Centroid bits: 1 | ef_search: 128 | Centroids: 1.00M | Queries: 200 | k=100

| Rerank | Fetch | Recall@10 | Recall@100 | Avg lat | search  | fetch   | rerank  |
| ------ | ----- | --------- | ---------- | ------- | ------- | ------- | ------- |
| 1x     | 100   | 84.75%    | 50.88%     | 354.5µs | 354.5µs | 0ns     | 0ns     |
| 2x     | 200   | 90.50%    | 68.18%     | 638.8µs | 510.5µs | 20.4µs  | 107.9µs |
| 4x     | 400   | 95.70%    | 81.98%     | 1.23ms  | 982.1µs | 39.4µs  | 209.9µs |
| 8x     | 800   | 98.20%    | 91.25%     | 2.37ms  | 1.88ms  | 73.3µs  | 417.7µs |
| 16x    | 1600  | 99.20%    | 96.07%     | 4.74ms  | 3.73ms  | 138.9µs | 876.0µs |

### 4 Bit build + search

Dim: 1024 | Metric: L2 | Centroid bits: 4 | ef_search: 128 | Centroids: 1.00M | Queries: 200 | k=100


| Rerank | Fetch | Recall@10 | Recall@100 | Avg lat | search  | fetch   | rerank  |
| ------ | ----- | --------- | ---------- | ------- | ------- | ------- | ------- |
| 1x     | 100   | 94.25%    | 81.56%     | 2.67ms  | 2.67ms  | 0ns     | 0ns     |
| 2x     | 200   | 96.15%    | 89.45%     | 3.95ms  | 3.82ms  | 22.5µs  | 107.9µs |
| 4x     | 400   | 98.60%    | 94.98%     | 7.25ms  | 7.00ms  | 42.5µs  | 213.3µs |
| 8x     | 800   | 99.65%    | 97.80%     | 13.00ms | 12.49ms | 77.6µs  | 427.0µs |
| 16x    | 1600  | 99.85%    | 99.17%     | 24.50ms | 23.45ms | 153.4µs | 899.7µs |

## Flat / Brute Force

### Architecture

benchmark code: [../../../benches/flat_centroid_profile.rs](../../../benches/flat_centroid_profile.rs)

#### Thread Safety

The flat index uses two mechanisms:
- parking_lot::RwLock on the mutable collections -- keys, vectors, codes, and tombstones are each wrapped in an RwLock. Search methods (search_f32, search_quantized, search_code_to_code) acquire read locks, so many searches can run concurrently without blocking each other. Write methods (add, remove) acquire write locks, which block readers and other writers.
- Tombstone-based soft deletes -- remove() doesn't actually modify the keys/vectors/codes arrays. It just inserts the key into the tombstones: RwLock<HashSet<u32>> set. Searches skip tombstoned keys during the scan. This avoids the need to compact or shift array elements under a write lock, keeping the write critical section very short.

### FP vs 1 Bit vs 4 Bit (Navigate only)
k=100, vectors=1000000, single thread, 1000 queries/samples

| Metric             | [FP](saved_benchmarks/flat_full_precision.txt) | [4 Bit](saved_benchmarks/flat_4bit.txt) | [1 Bit](saved_benchmarks/flat_1bit.txt) |
| ------------------ | ---------------------------------------------- | --------------------------------------- | --------------------------------------- |
| Navigate latency   | 236ms                                          | 822ms                                   | 298ms                                   |
| Latency per vector | 236ns                                          | 822ns                                   | 298ns                                   |
| Recall@100         | 100%                                           | 90%                                     | 89% (Rerank=8x)                         |


### Flat vs USearch HNSW (forked) -- 1-bit code-to-code, wikipedia-en dim=1024 (Navigate only)

| Metric          | Flat (1M, 32t) | HNSW (1M, 32t) | Flat (10K, 32t) | HNSW (5.7K, 8t) |
| --------------- | -------------- | -------------- | --------------- | --------------- |
| Navigate avg    | 94.06ms        | 210.5us        | 593.5us         | 81.6us          |
| Recall@10 (1x)  | 89.60%         | 98.50%         | 93.50%          | 99.80%          |
| Recall@100 (1x) | 49.53%         | 64.96%         | 55.60%          | 78.78%          |
| Recall@10 (4x)  | 97.50%         | 99.10%         | 99.20%          | 100.00%         |
| Recall@100 (4x) | 80.01%         | 89.25%         | 88.44%          | 99.60%          |
| Total lat (4x)  | 236.3ms        | 1.23ms         | 3.88ms          | 496.0us         |

### Sythetic Workload (Thread Scaling + Performance + Recall)

Index uses global lock

[source](saved_benchmarks/flat_1bit.txt)

=== Phase 2: Task Total Time ===
| threads | navigate | spawn    | drop     | wall   |
| ------- | -------- | -------- | -------- | ------ |
| 1       | 1.6m     | 12.18ms  | 3.8µs    | 1.6m   |
| 2       | 1.6m     | 111.41ms | 2.2µs    | 46.89s |
| 4       | 1.4m     | 100.41ms | 22.91ms  | 21.66s |
| 8       | 1.6m     | 294.70ms | 91.08ms  | 11.98s |
| 16      | 1.7m     | 223.42ms | 90.71ms  | 6.68s  |
| 32      | 1.8m     | 386.31ms | 212.38ms | 3.98s  |

=== Phase 2: Task Avg Time ===
| threads | navigate | spawn   | drop    |
| ------- | -------- | ------- | ------- |
| 1       | 30.86ms  | 1.52ms  | 549ns   |
| 2       | 30.62ms  | 12.38ms | 743ns   |
| 4       | 28.06ms  | 12.55ms | 7.64ms  |
| 8       | 30.72ms  | 32.74ms | 30.36ms |
| 16      | 32.91ms  | 31.92ms | 30.24ms |
| 32      | 35.64ms  | 35.12ms | 30.34ms |


## Hierarchical SPANN

### Design

Indexing

- hierarchical spann
- branching factor 100 (3 levels for 100k centroids, 4 levels for 1M centroids)
- balanced k-means clustering
- Boundary vector Replication
Querying
- Dynamic beam width

### Hierarchical Tree Config vs SPANN Config


| Hierarchical Tree       | Value   | SPANN (quantized_spann.rs) | Value   | Notes                                                                                                                                                              |
| ----------------------- | ------- | -------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `bf` (branching_factor) | 100     | N/A                        | N/A     | SPANN uses HNSW for centroid lookup, not a tree. No branching factor.                                                                                              |
| `beam_width`            | 10      | `ef_search`                | **128** | Both control search quality. beam_width = candidates kept per tree level. ef_search = HNSW beam width.                                                             |
| `distance_fn`           | L2      | L2                         | L2      | Same                                                                                                                                                               |
| `centroid_bits`         | None/1  | `centroid_bits`            | None/1  | Same -- controls whether centroid index uses 1-bit RaBitQ                                                                                                          |
| `expansion_factor`      | 0.0-1.0 | `write_rng_epsilon`        | **8.0** | Both control boundary replication radius. Conceptually analogous but different mechanisms -- tree expansion uses distance ratio, SPANN uses RNG rule with epsilon. |
| `max_replicas`          | 1-4     | `nreplica_count`           | **2**   | Max clusters a vector can be assigned to                                                                                                                           |
| `kmeans_iters`          | 10      | N/A                        | N/A     | SPANN doesn't use k-means for the centroid index; it uses HNSW. K-means is used internally for cluster splits.                                                     |
| N/A                     |         | `ef_construction`          | **256** | HNSW build-time parameter. No tree analog.                                                                                                                         |
| N/A                     |         | `max_neighbors` (M)        | **24**  | HNSW graph connectivity. No tree analog.                                                                                                                           |
| N/A                     |         | `write_nprobe`             | **64**  | How many centroids to probe when assigning a new vector to clusters. Analogous to how the tree's `search()` is called with k=NPROBE=64 during the workload sim.    |
| N/A                     |         | `split_threshold`          | **512** | Max posting list size before splitting. No direct tree analog.                                                                                                     |
| N/A                     |         | `merge_threshold`          | **128** | Min posting list size before merging. No direct tree analog.                                                                                                       |
| N/A                     |         | `write_rng_factor`         | **4.0** | RNG rule factor for selecting replica clusters. No tree analog.                                                                                                    |


### Balanced k-means (100K centroids, wikipedia-en, f32, eps=1.0, r=4)

See `--balanced` flag in [hierarchical_centroid_profile.rs](../../../benches/hierarchical_centroid_profile.rs)


| Metric             | Unbalanced | Balanced (lambda=100) |
| ------------------ | ---------- | --------------------- |
| R@10 (beam=10)     | 78.7%      | 66.5%                 |
| R@100 (beam=10)    | 62.1%      | 48.7%                 |
| Nav latency        | 2.37ms     | 1.09ms                |
| Leaf max size      | 851        | 520                   |
| Leaf p50 size      | 22         | 27                    |
| Avg replication    | 3.94x      | 3.67x                 |
| Phase 2 wall clock | 338ms      | 172ms                 |


Balanced clustering produces more uniform leaf sizes (max 520 vs 851) and ~2x faster
navigate, but loses ~12% recall at beam=10. The gap narrows at high beam widths
(beam=1000: both ~98% R@10). At iso-recall, balanced does not win on latency because
the wider beam needed to recover recall cancels out the per-step savings.


# Recall

Our question: What recall and performance can we get with what latency and quantization level?

Many approaches are used to
- estimate what recall @1B would be
- isolate recall at various sub-stages (e.g. in central index)

## USearch Recall

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

## Quantized KMeans Clustering Recall (Needs Redo)

EDIT: A more useful version of this benchmark would be one that uses our SPANN code

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

Our question: how effective is reranking with 4 bit centroids instead of full precision centroids?
Answer: Very!

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



# Older Benchmarks

But still valid! Just less relevant to the current decisions


## SPANN

Early benchmark showing that
- quantization does not help much with indexing as many steps use full precision embeddings anyway
- recall suffers without reranking


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

## Synthetic SPANN / Centroid Recall (Obsolete)

EDIT: Instead of this benchmark, we now use quantized_spann.rs to measure the real SPANN implementation.

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
