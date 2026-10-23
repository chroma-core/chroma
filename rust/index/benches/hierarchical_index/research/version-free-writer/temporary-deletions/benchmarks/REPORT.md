# Temporary deletion benchmark results

Temporary per-leaf deletion bits do not show a convincing speed improvement over the finer balancing scheduler. The candidate is 4.2% slower in a fresh 1M build, 3.3% faster in a fresh 3M build, and 1.6% slower when adding 1M to the preserved 8M index. These are single matched pairs, and the differences are small enough that they do not establish a repeatable gain. Keep this candidate isolated rather than adding its complexity to the version-free writer.

A moved vector is appended to its destination and its old physical source position is marked deleted. Posting scans skip that position, live counts guide balancing, and touched surviving leaves compact after workers join. Final cleanup is included in all balance timings below. The experiment retains existing transfer claims and uses the finer scheduler in both variants. It excludes the separate batched-transfer experiment.

## Indexing results

Core indexing time sums insertion including production input checks, balance including final cleanup, commit preparation, commit flush, and writer reopen. It excludes input loading, benchmark diagnostics, printing, reader-pin cleanup, destruction, and benchmark garbage collection. It does not represent all production latency. Benchmark posting validation is disabled for both variants; saved-index correctness checks run separately.

| Workload | Finer scheduler reference | Temporary deletions | Change |
|---|---:|---:|---:|
| Fresh 1M, ten 100k checkpoints: core indexing | 27.641s | 28.815s | +4.2% |
| Fresh 1M: balance | 18.214s | 19.391s | +6.5% |
| Fresh 3M, three 1M checkpoints: core indexing | 66.526s | 64.298s | −3.3% |
| Fresh 3M: balance | 33.275s | 32.039s | −3.7% |
| Saved 8M → 9M, one 1M checkpoint: core indexing | 31.874s | 32.396s | +1.6% |
| Saved 8M → 9M: balance | 16.236s | 16.715s | +3.0% |

The first pair runs reference before candidate; the 3M pair reverses that order. The replay pair runs reference before candidate, with both starting from independent full copies of the exact same retained 8M snapshot. The 3M benefit alone does not justify a speedup claim: approximately 0.91s of its 2.23s core difference comes from insertion, whose code is unchanged by this experiment. No additional datasets or larger fresh builds run. Given the inconsistent small differences, no reverse-order repeat of a promising result is warranted.

## Transfers and cleanup

| Measure | 1M reference | 1M candidate | 3M reference | 3M candidate | 8M → 9M reference | 8M → 9M candidate |
|---|---:|---:|---:|---:|---:|---:|
| Joined retry phase | 1.142s | 1.218s | 3.270s | 2.645s | 1.356s | 1.277s |
| Structural worker wall time | 17.067s | 18.102s | 29.994s | 29.297s | 14.869s | 15.391s |
| Summed claim-holding durations | 129.616s | 127.019s | 253.281s | 244.391s | 137.076s | 132.575s |
| Claim conflicts | 4,834 | 5,055 | 8,422 | 8,263 | 1,405 | 1,218 |
| Retry counter | 9,776 | 10,299 | 16,814 | 15,873 | 2,562 | 2,578 |
| Source physical ID slots scanned | 11.506M | 12.194M | 38.783M | 40.736M | 12.542M | 13.046M |
| Vectors transferred | 332,704 | 347,234 | 1,169,691 | 1,164,482 | 379,829 | 383,326 |
| Final cleanup wall time, already inside balance | — | 0.065653s | — | 0.087044s | — | 0.033234s |
| Final cleanup physical entries scanned | — | 4.376M | — | 9.713M | — | 4.981M |
| Final cleanup survivor payload retained/written | — | 607.4MB | — | 1,316.3MB | — | 696.6MB |

The candidate removes the whole-source code snapshot and survivor-copy work from immediate transfer application. It still scans source IDs to locate proposed rows. The scan counter therefore remains meaningful as ID slots examined, but it cannot quantify bytes copied or memory bandwidth saved. Dead physical rows remain until cleanup or a split, which can increase physical ID scans; the measured counter rises by about 4–6% in these pairs. Final cleanup is cheap, but neither claim duration nor retry time falls enough to create a consistent wall-time improvement.

Claim-holding durations overlap across workers and are not CPU consumption or elapsed time saved. The conflict counter counts failed claims, whereas the retry counter covers queued transfer work; they need not move together. Placement, moved-vector counts, and tree sizes vary with concurrent worker ordering, so these are end-to-end comparisons rather than isolated measurements of one copy operation.

Cleanup survivor bytes count retained ID-plus-code payload, not physical memory traffic. Cleanup performed when a leaf splits is included in total balance time but excluded from the final-cleanup counters. No instrumented peak count of temporarily dead physical slots is available; final saved-index audits prove that physical and live counts are equal after cleanup.

The replay resource samples show essentially unchanged parallelism: mean sampled use is approximately 17.59 cores for the reference and 17.49 for the candidate, with about 265.8 and 264.3 sampled CPU-seconds respectively. Neither sampled balance interval has a below-two-core tail. Resource windows are inferred from timestamped output and include sampling/output-latency uncertainty; sampled CPU work is not a projected recoverable latency.

## Memory, recall, and leaf sizes

| Measure | 1M reference | 1M candidate | 3M reference | 3M candidate | 8M → 9M reference | 8M → 9M candidate |
|---|---:|---:|---:|---:|---:|---:|
| Peak process RSS | 9.222GiB | 8.460GiB | 23.257GiB | 23.393GiB | 58.990GiB | 58.866GiB |
| Recall@100, tau 1.5, rerank 1 | 78.05% | 78.26% | 77.09% | 76.33% | 76.28% | 76.32% |
| Recall@100, tau 1.5, rerank 8 | 99.32% | 99.24% | 98.34% | 98.37% | 96.96% | 96.93% |
| Recall@100, tau 2, rerank 8 | 99.64% | 99.63% | 98.53% | 98.60% | 97.61% | 97.58% |
| Final undersized leaves | 0 | 0 | 0 | 0 | 3 | 3 |
| Final oversized leaves | 0 | 0 | 0 | 1 | 3 | 2 |
| Largest final leaf | 2048 | 2046 | 2043 | 2117 | 2340 | 2216 |

Reranked recall remains comparable in these runs. The fresh 3M candidate's recall without reranking is 0.76 percentage points lower at tau 1.5, which is a limitation if that query setting matters; no repeated run establishes whether it is systematic. Memory has no consistent improvement across sizes. Both implementations permit residual size violations after the shared eight-round limit once all mandatory transfers finish; these residuals are reported rather than counted as missing vectors.

## Correctness

All six saved indexes pass independent reopen audits. Every vector ID from zero through the expected corpus size minus one is reachable and appears exactly once; total physical posting counts and live counts equal 1M, 3M, and 9M respectively. All topology/count checks and sampled embedding/code checks pass. The initial audits sample 100 known source vectors; extended 3M and 9M audits add 100 stratified vectors from the newest million, giving 200 bit-exact embedding and centroid-relative code checks per larger index.

The checker reuses the finer-scheduler audit binary and its saved audit-only patch. This intentionally provides an independent implementation for opening the candidate's unchanged persisted format. The exact candidate binary also reopens committed checkpoints during the timed build and executes final matching-ground-truth recall. The implementation handoff records 193 local test executions plus independent concurrency and storage reviews, benchmark compilation, and formatting checks.

All runs use the same query vectors and ground truth keyed to their exact final corpus size. The 1M/3M ground truth retains the earlier independently verified provenance. The existing 9M ground truth comes from the recorded source cache; query identity, row counts, neighbor counts, and ID range were checked, but its nearest neighbors are not independently recomputed in this experiment. The 9M result therefore supports relative comparison using the shared saved answers, with that provenance limitation.

## Revisions, environment, and artifacts

The reference is `14dc9698eb6e400ba6f677575eff7a5659deeed6`; the candidate is `1d659e171b8456d4e0bbd6c77dbf6cbb40674470`. The candidate is isolated on `codex/balance-slot-tombstones`, with local checkout `/private/tmp/chroma-balance-slot-tombstones`. This experiment makes no GitHub writes, merges, or changes to PR #7927.

Wikipedia uses 1024 dimensions, L2, 32 insertion threads, 32 balancing threads, full-precision writer navigation and neighbor reassignment, writer tau 0.5, level taus _,_,0.30, branching factor 100, split threshold 2048, merge threshold 512, a 1GiB cache, and fixed 100 queries. Read tau values are 1.5 and 2 with rerank factors 1 and 8. No replication flags or writer level minimum percentages are present. Exact argument arrays, environment, SHA files, candidate archive checksum, binary checksums, per-process resource samples, raw CLI logs, and audit commands/results are in `artifacts/`.

Builds use Rust 1.92.0 and `-C target-cpu=native` on the existing 64-vCPU/32-core Intel Xeon 8375C machine with approximately 495GiB RAM. The exact reference executable and compatible audit executable are reused from their prior recorded builds. The candidate compiles in 57.40s. Builds, runs, audits, and cleanup acquire `/mnt/data/chroma-benchmark.lock`; no competing benchmark runs alongside them. Both members of each pair write to `/mnt/data` NVMe. Root disk is unused.

At recovery there are no active benchmark/build processes and no remote experiment directory; only the candidate archive exists. The recovery owner starts the build and records actual progress. This distinguishes the measured runs from the preceding status message that said benchmarks were starting.

The replay source is `/mnt/msmarco/version-free-followup-20261020/wikipedia-diagnostic-9m-1/snapshots/8000000`. Each run copies its data and metadata with full independent files, and its source metadata remains unchanged. Original 40M Wikipedia checkpoints, MS MARCO sources, the 8M snapshot, and prior artifacts are preserved. After successful full and extended audits, only the two completed isolated 9M replay output directories are removed under the user's existing cleanup authorization; their checkpoint metadata, hashes, commands, logs and audits remain. A guarded deletion manifest records the exact paths. The fresh 1M/3M outputs remain. `/mnt/data` has approximately 109GiB free and the benchmark lock is available at completion.

## Recommendation

Keep the finer scheduler and retain temporary deletions as an isolated experiment. The candidate makes final deletion cheap, but the measured copy savings do not translate into a consistent overall latency reduction. A further attempt should first isolate the cost of scanning source IDs and preparing moves; carrying stable source positions could avoid that scan, but position lifetime and structural mutations require a separate correctness design. The present evidence does not justify another implementation or larger benchmark automatically.
