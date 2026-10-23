# 40M Wikipedia benchmark

The benchmark builds a fresh index from the first 40 million English Wikipedia embeddings and records every checkpoint and the complete command output. Data preparation, exact nearest-neighbor calculation, and compilation run before the timed benchmark.

## Results

The current run completes all 40 million vectors, but its reported build time is 59% longer than the historical run. Insertion is about 27 times faster in aggregate; balancing grows enough to erase that gain.

| Measurement | Historical 40M | Current 40M |
|---|---:|---:|
| CLI build time | 347.1 min | 552.3 min |
| CLI throughput | 1,920 vectors/s | 1,207 vectors/s |
| Sum of insertion phases | 174.56 min | 6.45 min |
| Sum of balancing phases | 163.07 min | 472.23 min |
| Sum of dataset loading phases | 8.66 min | 10.89 min |
| Sum of commit phases | Not reported | 9.03 min |
| Sum of reopen phases | Not reported | 6.36 s |

The checkpoint totals sum to approximately 499.0 minutes. About 53.3 minutes lie outside those totals. The benchmark materializes all posting lists and computes tree statistics before it records its reported build time, so this diagnostic work is included in the 552.3-minute result. Those steps lack separate timers; the entire 53.3-minute difference cannot be assigned to one step from this run.

The final checkpoint shows the scaling problem clearly. Adding its one million vectors takes 13.55 seconds, while balancing takes about 55 minutes. The historical final checkpoint reports roughly nine minutes adding vectors and 9.5 minutes balancing. Small-index insertion gains therefore do not predict the latency of the complete large-index build.

| Current checkpoint | Add | Balance | Load | Commit | Printed checkpoint total |
|---|---:|---:|---:|---:|---:|
| 1M | 3.04 s | 26.43 s | 14.53 s | 5.79 s | 49.79 s |
| 10M | 7.70 s | 25.04 s | 15.45 s | 9.07 s | 57.32 s |
| 20M | 9.48 s | 2.0 min | 16.69 s | 13.78 s | 2.6 min |
| 30M | 11.48 s | 17.7 min | 17.06 s | 17.69 s | 18.5 min |
| 40M | 13.55 s | 55.0 min | 16.93 s | 20.73 s | 55.8 min |

The reassignment timers identify destination registration as the next operation to investigate. Reassignment moves a vector to a better leaf; registration inserts its replacement posting into that destination. At the last checkpoint, the operation count is only about 5.4% higher than the historical count, so more reassignments alone do not explain the latency increase.

| Final checkpoint reassignment measurement | Historical | Current |
|---|---:|---:|
| Reassignment count | 558,000 | 588,400 |
| Mean time per reassignment | 52.2 ms | 275.4 ms |
| Navigation within reassignment | 24.7 ms | 246.6 µs |
| Destination registration within reassignment | 3.6 µs | 105.8 ms |
| Recursive destination balancing within reassignment | 27.5 ms | 169.4 ms |

The current registration timer includes resolving the vector version and loading a lazy destination posting. Its measured leaf-lock wait and quantization costs are tiny relative to the total, so the expensive work appears outside those measured substeps. Registration, lazy posting loading, and cold version prefetch should be separated with timers before choosing a fix. The historical registration timer has a different scope, so the table does not imply that the append primitive itself becomes 29,000 times slower.

The version prefetch implementation is a concrete hypothesis to test. It fetches scattered version keys in small batches, clears pinned scalar blocks after each 64 reads, and shares the 1 GiB block cache with posting and embedding blocks. The final scalar metadata is 0.722 GiB serialized; it competes with 6.246 GiB of postings and 158.375 GiB of vector data. Whether decoded cache weights and competing reads cause repeated decoding must be measured. No cause is proven by this benchmark alone.

## Read results

The read results use different navigation and cache behavior from the historical run, so they do not establish an equivalent-recall speedup. At tolerance 2, the current four-bit path reaches 74.40% recall without vector reranking and 93.55% with an eightfold reranking candidate count. The historical page reports roughly 75.2–75.5% and 95.2–96.0% for its one-bit/full-precision paths at that tolerance, using undocumented query identities.

| Current read tolerance | Vector reranking | Recall | Reader cold mean latency | Reader warm mean latency |
|---|---:|---:|---:|---:|
| 0.7 | 1× | 57.55% | 216.1 ms | 1.6 ms |
| 0.7 | 8× | 63.69% | 825.0 ms | 9.5 ms |
| 1.0 | 1× | 57.55% | 11.1 ms | 1.7 ms |
| 1.0 | 8× | 63.69% | 722.6 ms | 8.8 ms |
| 2.0 | 1× | 74.40% | 78.3 ms | 21.0 ms |
| 2.0 | 8× | 93.55% | 856.5 ms | 31.3 ms |

The entire recall section takes about 1.4 minutes. Its timing is not comparable to the historical 41.5-minute recall section because this run disables the expensive cluster-coverage diagnostic and uses lazy cold/warm execution. Reader cold measurements clear retained reader data; the operating system can still cache files.

## Resources and checks

The process completes with exit status zero and commits exactly 40 million vectors at checkpoint 40. Its tree statistics report 40 million unique valid vectors, zero orphaned vectors, 27,575 leaves, and 439 internal nodes. Leaf sizes range from 349 to 2,048; the historical page reports 579 orphaned vectors.

The timed process lasts 9:13:40, including recall. Peak resident memory is 220.26 GiB. Its average CPU use is 174%, equivalent to about 1.74 CPU cores, despite the configured 32 balancing workers. It records 4,795,260,859 minor page faults and ten major page faults; these counters warrant investigating memory allocation and repeated decoding but do not identify a cause. The final index uses approximately 165.35 GiB; the NVMe volume retains about 75 GiB free.

The test node has 64 virtual CPUs, 512 GiB of RAM, and an Intel Xeon Platinum 8375C. No competing benchmark runs while this job holds the shared benchmark lock. The instance remains running after completion, and the lock is released.

## Source and settings

The source is the current feature branch at commit `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`. The adaptive startup balancing and deferred destination balancing pull requests are unmerged and are excluded.

The writer adds one million vectors per checkpoint for 40 checkpoints. It uses 32 insertion workers and 32 balancing workers, full-precision navigation and reassignment, a branching factor of 100, split and merge thresholds of 2,048 and 512, one replica, a write beam between 10 and 16, a write tolerance of 0.5 with level tolerances `_,_,0.30`, and a 1 GiB block cache. The write-level minimum percentages are omitted as Lyon requested in the benchmark settings.

The read measurements use the current four-bit navigation path, tolerances 0.7, 1, and 2, vector reranking factors 1 and 8, and a read beam between 10 and 256. Read-level minimum percentages are 50, 5, and 0. The tolerances and vector reranking factors match the historical sweep, but the current benchmark does not expose the historical full-precision and one-bit navigation choices or the centroid reranking sweep. The benchmark reports reader cache cold and warm passes separately. A cold pass clears the reader's retained data; the operating system can still cache files.

## Historical comparison

The [historical 40M benchmark](https://app.notion.com/p/34458a6d8191808b8f9ed1330132c1b0) reports a 347.1-minute build and 1,920 vectors per second. Summing its checkpoint output gives approximately 174.6 minutes adding vectors, 163.1 minutes balancing, and 8.7 minutes loading vectors. These sums are approximate because many checkpoint durations are rounded to tenths of a minute.

The historical page includes write-level minimum percentages of 50, 5, and 0; this run omits them. The page does not identify the historical hardware, source commit, exact query IDs, or block-cache capacity. This run explicitly commits, saves, and reopens the index at each checkpoint; equivalent historical persistence behavior is not documented. Changes in code, hardware, and settings can all contribute to a difference, so this comparison does not isolate the speedup from individual improvements.

The historical run computes a per-query cluster coverage diagnostic and retains input vectors for exhaustive neighbor calculations. This run generates exact neighbors before the timed build and disables that expensive cluster diagnostic. Its preparation and recall timings must be compared separately from index build time.

## Data preparation

The dataset consists of 1,024-dimensional embeddings in shard order from `CohereLabs/wikipedia-2023-11-embed-multilingual-v3`. Downloaded sources extend the existing 10M cache to the required 40M prefix. The final index occupies its own directory; existing checkpoint fixtures are preserved.

The download completes in 1,030.3 seconds. The Wikipedia source cache occupies 86,900,953,088 bytes (80.9 GiB) on instance-local NVMe. Only the original 10M prefix, 21,678,321,664 bytes (20.2 GiB), is present on persistent EBS; the remaining sources are lost when the instance stops. No additional paid storage is created.

Exact ground truth means the nearest 100 vectors are selected by scanning every vector in the relevant prefix. The calculation uses the same 100 queries as the recent 1M and 10M benchmarks, selected from the first million vectors using NumPy `RandomState(42)`. Distances use float64 accumulation. Separate neighbor sets are calculated at each one-million-vector boundary through 40M. The exhaustive calculation completes in 693.9 seconds. All 40 boundaries pass checks for query counts, dimensions, unique neighbor IDs, and prefix membership; overlapping results match the existing exact ground truth.

The exact ground-truth file, source manifest, and preparation scripts are saved locally and under `/mnt/benchmark-source/wikipedia-40m-current` on persistent EBS. The ground-truth SHA256 is `d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf`. This ground truth is reusable for the same ordered dataset prefix, fixed queries, and distance metric.

## Execution and artifacts

The detached job holds the shared benchmark lock through preparation, compilation, and execution. Its process ID and final exit status are recorded remotely, so closing the laptop does not terminate the job. The node remains running after completion.

The remote results directory is `/home/ubuntu/benchmark-data/wikipedia-40m-current`. The new index is `/mnt/data/wikipedia-40m-current`. The complete output is `full-cli-output.txt`; source and executable hashes, exact arguments, preparation logs, resource measurements, and final checkpoint metadata accompany it.

An initial trial is excluded because two unsupported historical read flags cause the benchmark parser to ignore subsequent arguments. The first checkpoint exposes the wrong cache capacity and missing save directory, so the trial is stopped. Its output is preserved under `excluded-cli-mismatch/`. The valid run uses `run-benchmark.sh`, checks every command flag against the executable help, and verifies the printed cache capacity and persisted checkpoint metadata.

The completed output is [full-cli-output.completed-22eb704.txt](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/wikipedia-40m-current/full-cli-output.completed-22eb704.txt>). It contains all 1,300 lines, begins with `Loading Wikipedia EN from HuggingFace Hub...`, and matches the remote file byte for byte. Its SHA256 is `7bc4f1d5594cc40ac49a13083856835b95c49aa1b502e239aab081854d56b7dc`. The source executable SHA256 is `f8ffbabb6b03c3236e73cc0d2f3d19258ef915de6721cbf7817402dc51b54252`.
