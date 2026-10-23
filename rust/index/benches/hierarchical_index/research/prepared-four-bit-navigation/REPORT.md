# Prepared four-bit centroid scoring experiment

The authors’ optimized packed-code kernel makes four-bit centroids affordable for navigation. At approximately 94.5% recall, it lowers warm mean latency by 11.4%, halves reranking, and reduces measured filesystem read bytes by 39.1% compared with the current one-bit-centroid reader. At another setting it reduces the leaf beam by 25%, scans 24.8% fewer posting codes, and lowers warm latency by 14.8% at essentially the same recall.

The packed kernel is the preferred candidate. Prepared bit planes preserve the same results and remove most scoring overhead, but the packed kernel is faster in complete queries. With the optimized kernel, four-bit and FP32 queries are effectively tied for latency and have small accuracy differences across the tested beams. These measurements establish no consistent advantage for quantizing the navigation query.

## Complete query measurements

Each row averages three runs of the same 100 exact L2 queries on the saved 40M Wikipedia index, with 1024 dimensions and 32 query workers. All root children are retained. The table’s internal beam is 256. The original tree and posting codes are shared across implementations; only navigation scoring changes.

| Implementation | Leaves | Posting codes scanned | FP32 vectors reranked | Leaf reachable recall | Final recall@100 | Warm mean ms | Navigation ms | Cold mean ms | Cold filesystem MiB/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Existing reader | 256 | 380,460 | 800 | 94.57% | 94.42% | 28.54 | 4.42 | 950.69 | 724.70 |
| Prepared bit planes / 4-bit query | 240 | 357,253 | 400 | 95.39% | 94.51% | 26.13 | 5.96 | 518.55 | 441.43 |
| Authors’ packed kernel / 4-bit query | 240 | 357,253 | 400 | 95.39% | 94.51% | 25.30 | 5.02 | 526.66 | 441.42 |
| Authors’ packed kernel / FP32 query | 240 | 357,395 | 400 | 95.40% | 94.51% | 25.28 | 4.85 | 528.38 | 441.24 |
| Authors’ packed kernel / 4-bit query, fewer leaves | 192 | 285,934 | 800 | 94.61% | 94.45% | 24.32 | 4.90 | 935.95 | 723.37 |

The reduced-reranking setting cuts cold mean latency from 950.69 to 526.66 ms. The smaller-leaf-beam setting retains 800 reranked vectors, so its filesystem reads stay close to the baseline even though it scans fewer posting codes. This distinguishes posting-list savings from original-vector fetch savings.

Near 90% recall, the optimized four-bit-query path uses 112 leaves and reranks 400 vectors, reaching 90.33% recall in 12.67 ms warm. The existing path uses 128 leaves and 800 reranked vectors, reaching 90.05% in 16.28 ms. Codes scanned fall from 190,524 to 166,885, and filesystem reads fall from 722.47 to 437.06 MiB/query.

## Scoring alone

The microbenchmark uses 4,096 real saved centroids for each of 100 real queries. The table reports the median of three runs; scoring time includes code lookup, distance arithmetic, and checksum accumulation. Query preparation and centroid-ID selection occur outside that timer. Its overall wall time also includes those excluded steps, so the scoring numbers describe a component rather than complete-query throughput.

| Implementation | Selected kernel | One worker: ns/centroid | 32 workers: ns/centroid |
|---|---|---:|---:|
| 1bit-4bit | existing | 97.1 | 166.5 |
| 4bit-4bit | existing | 2700.0 | 2715.1 |
| 4bit-planes-4bit | popcnt | 234.1 | 235.7 |
| 4bit-packed-4bit | avx512 | 161.2 | 261.8 |
| 4bit-fp | existing | 487.5 | 569.9 |
| 4bit-packed-fp | avx512 | 161.7 | 250.0 |

Prepared bit planes reduce scoring cost by about 11.5×. The packed kernel reduces it by 16.7× with one worker and 10.4× with 32 workers. The existing four-bit-query implementation rebuilds centroid bit planes during every comparison. The prepared implementations move that layout work to reader setup.

## Implementation and precision

The bit-plane candidate stores four coordinate bit planes and the coordinate sum once per centroid. Query scoring computes sixteen weighted bitwise intersections per 64 dimensions. Hardware population counts are selected once when the reader is created.

The packed candidate prepares the authors’ compact nibble layout once: each eight-byte block stores sixteen centroid integers. The scoring kernel unpacks that block with SIMD instructions and accumulates its dot product. AVX-512 is selected on the benchmark node, with an AVX2/FMA implementation and a portable scalar fallback also available. Instruction selection occurs once per reader.

The four-bit query retains the same quantization as the existing experiment. Its integer codes, each between 0 and 15, are represented as floats once per query for the packed kernel. This buffer contains exactly four-bit precision. The kernel computes an integer-valued dot product that is exactly representable in FP32 for 1024 dimensions. Both prepared candidates then apply the existing query scale, offset, original norm, and RaBitQ correction factors. The FP32-query control feeds the original query values through the same packed kernel.

## Preparation, memory, and compatibility

The derived scoring-cache allocation is approximately 16.32 MiB per prepared mode, including estimated map entries and heap buffer capacities. The numeric payload is approximately 14.34 MiB. The experimental reader also holds the checkpoint’s original one-bit tree codes. Hash-table control bytes and allocator metadata are excluded from the cache estimate; these figures do not represent total reader RSS. Empty map capacity can remain when switching modes, and that retained capacity is included in the allocation estimate.

Preparing codes from the saved original centroids takes roughly 2–3 seconds per mode in the complete-query runs. The microbenchmark, with different cache contents, measures approximately 0.11–0.12 seconds for prepared modes. Preparation is excluded from query latency and includes original-centroid reads, four-bit quantization, and layout conversion. A production design should persist four-bit centroid codes so opening a reader can avoid reading original FP32 centroids and quantizing them again. This experiment derives codes in memory and performs no persisted format migration.

## Verification and reproducibility

Both local portable tests and EC2 SIMD tests pass. Tests cover all nibble values, constant queries, padding and incomplete SIMD blocks, several dimensions through 1024, the supported distance metrics, and equivalence to the existing four-bit-query scorer. AVX2 and AVX-512 are exercised on EC2. The reader integration test confirms identical navigation choices and final results for the prepared four-bit-query modes on a saved test tree. All 27 integration test executions pass. Strict Clippy passes locally across index targets and features, with additional x86 library Clippy checks on EC2.

The scoring microbenchmark has identical checksums for the existing four-bit-query scorer and both prepared implementations. In all 120 full-query rows, the two prepared four-bit-query implementations have identical reachable recall, leaf counts, posting-code scan counts, rerank counts, and final recall. Each configuration has three cold/warm pairs, with implementation order reversed in the middle run.

Cold clears reader posting data and block pins; the 1 GiB provider cache and OS file cache remain. Filesystem byte counts come from process read counters over the concurrent query batch. They include full block reads served by the OS cache and are not production network bytes or physical disk bytes. The 100-query set is fixed and small. Further accuracy validation should use a larger query set, and production I/O conclusions require network-backed storage.

The original checkpoint file manifest remains unchanged after every run. The primary repository checkout is unchanged. The benchmark node is the existing Intel Xeon Platinum 8375C r6id.16xlarge instance with 64 vCPUs. All heavy work holds `/mnt/data/chroma-benchmark.lock`.

Branch: https://github.com/chroma-core/chroma/tree/codex/prepared-four-bit-navigation

Kernel source and microbenchmark commit: `7767a4c1b4fc8441e7fa8cfed1405b9d891be8a4`. Full-query benchmark commit: `b635eee6f83e56ecf6c8e0c1eada25b7b66b4bd3`; it adds targeted configuration selection without changing the kernels. The authors’ pinned source is `VectorDB-NTU/RaBitQ-Library` commit `2f6b57e4907659a79fd039cc826164e7d320f83d`, functions `ip16_fxu4_avx2` and `ip16_fxu4_avx512` in `src/simd/space_excode_avx2.cpp` and `src/simd/space_excode_avx512.cpp`. The Apache-2.0 license and attribution accompany the Rust port.

The input index is `/mnt/data/wikipedia-40m-baseline-20261009`. Exact ground truth is `/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet`, SHA-256 `d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf`.

Local artifacts include `micro.jsonl`, `QUERY_RESULTS.md`, `query-summary.json`, `queries.csv`, and `queries/run-*-output.txt`. The runner scripts record the commands; `queries/command-*.txt` record the exact executed query commands. Remote results are `/home/ubuntu/benchmark-data/prepared-four-bit-navigation-7767a4c1b` and `/home/ubuntu/benchmark-data/prepared-four-bit-navigation-b635eee6f`.
