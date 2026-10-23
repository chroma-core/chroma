# Reassignment profile after PR #7842

Navigation is the largest recurring balancing cost. Across checkpoints 2–10 of both 1M-vector builds, finding destination leaves accounts for about 66% of exclusive balance-worker elapsed time. Loading destination posting lists accounts for another 14–15%. Accessing the vector being reassigned and updating destination leaves after loading each account for less than 0.4%.

## Measurements

The table describes the sum of elapsed time across concurrent balance workers, excluding nested stages from their parents. It does not describe CPU utilization or a fraction of whole-build wall time. Percentages are approximate because the benchmark formats some large counters in minutes rounded to one decimal place.

| Stage | Wikipedia, checkpoints 2–10 | MS MARCO, checkpoints 2–10 | MS MARCO, controlled 800k→1M replays |
| --- | ---: | ---: | ---: |
| Navigation and destination selection | 65.8% | 65.9% | 59–61% |
| Loading destination posting lists | 14.5% | 15.3% | 20–21% |
| Remaining destination registration | 0.36% | 0.35% | 0.21–0.26% |
| Accessing the reassigned embedding | 0.05% | 0.03% | 0.02% |
| Checking the vector version | 0.09% | 0.07% | 0.06% |

Embedding access is small specifically inside reassignment. Splitting and neighbor scanning can load embeddings before reassignment starts; this result does not imply that all embedding I/O is negligible.

| Run | Sum of balancing wall time | Source-vector loading | Sum of checkpoint wall time |
| --- | ---: | ---: | ---: |
| MS MARCO replay 1 | 8.51 s | 27.96 s | 39.07 s |
| MS MARCO replay 2 | 8.89 s | 27.99 s | 39.48 s |
| Wikipedia full 1M | 46.02 s | 16.00 s | 71.46 s |
| MS MARCO full 1M | 44.03 s | 83.73 s | 136.89 s |

The first checkpoint remains a separate latency opportunity. Its balance phase takes 15.99 seconds on Wikipedia and 10.89 seconds on MS MARCO, with limited parallel work. These full runs vary from the previous report because clustering and scheduling vary; the instrumentation is not an optimization and these differences are not measured speedups.

The largest cost in the complete MS MARCO harness is still source-vector loading. That loader reads and decodes the input dataset, including earlier rows when reaching later offsets. Its 83.73 seconds are separate from production index operations and from destination posting-list loading.

## Why navigation is the next target

The navigation loop retrieves child nodes and computes their distances from a query vector. Its existing timer accounts for 94–95% of navigation elapsed time in the controlled replays. These existing navigation counters also include calls outside reassignment, so they corroborate the hot region without providing an exact reassignment-only substage breakdown. They combine node-map access, allocation, lock waiting, memory access, and distance arithmetic; they do not prove that arithmetic alone is the bottleneck.

Balancing disables the contiguous child-centroid representation used during additions. In the fallback path, each visited internal node copies its child IDs, then looks up each child in the concurrent node map and calculates a distance while holding that child's read guard. This creates a concrete opportunity to improve memory locality and shorten lock hold times. The relevant code is `writer/writer.rs:1124` for disabling packed navigation and `writer/writer.rs:765–803` for looking up and scoring children.

The next experiment should make coherent navigation payloads available during balancing. A parent could expose its child IDs and contiguous centroid data through an immutable shared payload; a search could take a reference under a short guard, release the guard, and score the children. Splits, merges, and parent changes must publish consistent replacements and preserve existing handling of disappearing destinations. Reusing the additions snapshot unchanged throughout balancing would not satisfy those requirements. A controlled comparison should measure this region and verify posting reachability and recall before claiming a speedup.

Destination loading is the second target. The first replacement registration explicitly materializes the destination posting list before adding one vector (`writer/writer.rs:671–719`). Its measured stage includes the synchronous bridge, blockfile read, version prefetch, copying, and node updates; no timer currently separates those costs. Investigate preserving the existing lazy posting delta here while retaining the rule that the replacement posting must exist before the new vector version becomes visible. The remaining registration work is already small, so optimizing quantization or appends alone would miss most of this cost.

## Instrumentation and validation

The instrumented source is commit `85d41e1de71d1166214a6ed8b46d9126d81f8772` on `codex/reassignment-profile-7842`, based on the report's exact revision `2b547c4f289d960c0042eca9996b0dbe79bfcb08`. The local checkout is `/private/tmp/chroma-reassignment-profile-7842`. The EC2 checkout contains that base plus the saved `instrumentation.patch`; its compiled binary is preserved alongside the remote results.

The new stages distinguish version checks, embedding access, navigation with policy and destination selection, destination registration, and destination posting-list loading. Scope guards include early returns and subtract nested recursive balancing. Counters accumulate per worker and flush when the worker finishes. The legacy `reassign() Avg Breakdown` remains unsuitable for percentages because it counts attempts and completions differently; conclusions here use the exclusive-worker table.

All four processes exit with status zero. The source checkpoint metadata checksum matches before and after the controlled replays. Both full builds report 1M unique vectors and zero orphaned vectors. Cold R@100 is 77.86% for Wikipedia and 80.83% for MS MARCO. These observations do not establish statistical recall equivalence or replace the posting-integrity tests.

The two relevant test harnesses pass all 35 test executions, including shared tests executed in both harnesses. A new timer test covers early returns and nested reassignment and verifies that summed exclusive time does not exceed elapsed time. Formatting, diff checks, and commit hooks pass. No index algorithm changes are included.

## Reproduction and artifacts

All runs execute sequentially on Lyon's EC2 node, an `r6id.16xlarge` with 64 vCPUs and 512 GiB RAM. Settings match the input report: 1024 dimensions, L2, 100k checkpoints, full-precision navigation and neighbor reassignment, parallel balancing, a 1 GiB block cache, 100 precomputed recall queries, tau 1.5, and rerank factor 1. Wikipedia uses 32 workers, write tau 0.5, per-level taus `_,_,0.30`, and RNG factor 0. MS MARCO uses 64 workers, write tau 1.5, and RNG factor 4. No `--write-level-min-pcts` option is used.

Exact commands, logs, process resource measurements, the patch, the runner, checksum records, and `summarize.py` are beside this report. Remote artifacts are at `/home/ubuntu/benchmark-data/reassignment-profile-7842`. The benchmark processes have completed; the EC2 instance remains running.
