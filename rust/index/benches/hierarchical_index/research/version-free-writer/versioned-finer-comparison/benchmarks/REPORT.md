# Versioned versus version-free writers with finer scheduling

The version-free writer is faster in both reversed-order pairs on both fresh Wikipedia workloads. Its mean insertion + balance + commit/reopen time is 29.4% lower at 1M and 10.4% lower at 3M. Recall remains comparable and all eight saved-index audits pass. Peak process memory is higher: about 8.6% at 1M and 6.4% at 3M.

| Fresh workload | Versioned + finer scheduler | Version-free + finer scheduler | Elapsed reduction |
|---|---:|---:|---:|
| 1M | 37.247s | 26.290s | 29.4% |
| 3M | 73.939s | 66.284s | 10.4% |

The primary elapsed measure includes all production insertion checks, insertion, balancing, commit preparation, flush, and reopen. It excludes input loading, benchmark diagnostics, benchmark validation, reader-pin clearing, old-writer/input destruction, benchmark GC, output and snapshot management. Some excluded cleanup can occur in production; this table is a defined set of writer phases, not every part of production wall time. Full instrumented driver build times appear below. Benchmark posting validation is disabled during every timing run and performed separately.

## Workloads and remaining algorithm differences

The 1M workload uses ten 100k checkpoints; the 3M workload uses three 1M checkpoints. The percentage difference cannot be attributed solely to index size because checkpoint size also changes. Each workload runs versioned, version-free, version-free, versioned in that order, sequentially on the same EC2 host. Two runs per implementation demonstrate repeated direction, not a precise statistical confidence interval. The 3M versioned balance time varies substantially between runs.

Both implementations use a largest-first leaf job queue and a joined round barrier. Active versioned rounds visit all leaves to preserve stale-entry cleanup; the version-free queue visits eligible leaves because every physical posting is live. The versioned port reserves a leaf for its whole balance call and retains recursive destination balancing, version validation, caches and persistence. The version-free writer requires physical source/destination transfers, defers destination balancing to outer rounds, and includes serial immutable-input checks. This is an end-to-end comparison of safe implementations, not an isolated version-table ablation. Temporary deletion bitmaps and batched round-boundary transfers are excluded.

## Phase breakdown

Values are arithmetic means of two runs. Insertion includes serial preflight where present.

| Workload | Phase | Versioned | Version-free | Difference (version-free − versioned) |
|---|---|---:|---:|---:|
| 1M | Insertion including checks | 2.671s | 3.279s | +0.607s |
| 1M | Balance | 27.416s | 16.868s | -10.548s |
| 1M | Commit preparation + flush + reopen | 7.159s | 6.143s | -1.016s |
| 3M | Insertion including checks | 13.533s | 16.242s | +2.710s |
| 3M | Balance | 40.916s | 33.071s | -7.846s |
| 3M | Commit preparation + flush + reopen | 19.490s | 16.971s | -2.519s |

At 1M, the version-free implementation saves 10.548 seconds in balance and 1.016 seconds in commit/reopen, while insertion costs 0.607 seconds more. At 3M, it saves 7.846 seconds in balance and 2.519 seconds in commit/reopen, while insertion costs 2.710 seconds more. Serial preflight costs 0.715 seconds at 1M and 2.180 seconds at 3M. These are observed phase differences across complete algorithms; they do not isolate individual version operations.

## Individual runs, recall and memory

| Workload | Variant / repeat | Core writer phases | Balance | Full driver build | Peak process RSS | R@100, tau 2 / rerank 8 |
|---|---|---|---|---|---|
| 1M | versioned / 1 | 37.293s | 27.360s | 56.462s | 8.282 GiB | 99.21% |
| 1M | versioned / 2 | 37.201s | 27.472s | 56.154s | 8.167 GiB | 99.48% |
| 1M | versionfree / 1 | 25.452s | 16.044s | 43.949s | 8.930 GiB | 99.58% |
| 1M | versionfree / 2 | 27.127s | 17.692s | 45.526s | 8.928 GiB | 99.60% |
| 3M | versioned / 1 | 69.179s | 37.456s | 89.792s | 22.041 GiB | 98.26% |
| 3M | versioned / 2 | 78.700s | 44.377s | 98.812s | 22.001 GiB | 98.43% |
| 3M | versionfree / 1 | 65.255s | 32.197s | 84.007s | 23.293 GiB | 98.50% |
| 3M | versionfree / 2 | 67.313s | 33.945s | 86.142s | 23.548 GiB | 98.46% |

Full driver build ends before recall evaluation but includes cached dataset loading and benchmark diagnostics/cleanup. Peak RSS covers the entire process, including the subsequent reader evaluation. Memory is not reduced in these tests despite removing version structures. Cold/warm query rows in raw logs describe application cache state; the OS file cache is not cleared.

| Workload | Recall setting | Versioned range | Version-free range |
|---|---|---|---|
| 1M | tau1.50-rerank1x | 78.01–78.21% | 78.08–78.17% |
| 1M | tau1.50-rerank8x | 98.97–99.23% | 99.30–99.30% |
| 1M | tau2.00-rerank1x | 78.06–78.34% | 78.14–78.27% |
| 1M | tau2.00-rerank8x | 99.21–99.48% | 99.58–99.60% |
| 3M | tau1.50-rerank1x | 76.18–76.48% | 76.44–77.20% |
| 3M | tau1.50-rerank8x | 98.10–98.13% | 98.20–98.21% |
| 3M | tau2.00-rerank1x | 76.22–76.57% | 76.53–77.34% |
| 3M | tau2.00-rerank8x | 98.26–98.43% | 98.46–98.50% |

## Saved-index correctness and residual balance

All eight audits pass after flush and lazy reopen, before any repair or additional balancing. Each audit checks complete root-reachable ID coverage, reachable child existence and acyclicity, globally exact-once authoritative live IDs, and 200 bit-exact source embeddings plus regenerated codes relative to their stored leaf centroids. Samples include the 100 fixed old query IDs and 100 IDs spread through the newest million vectors. Versioned audit filtering uses authoritative current versions and the deleted bit; stale physical rows are reported rather than counted as corruption. Version-free audits require physical exact-once coverage. The audited live population equals exactly 1M or 3M.

| Run | Physical postings | Stale postings | Leaves | Live undersized | Live oversized | Largest live leaf |
|---|---:|---:|---:|---:|---:|---:|
| wikipedia-versioned-1m-1 | 1,004,505 | 4,505 | 698 | 0 | 0 | 2,041 |
| wikipedia-versioned-1m-2 | 1,003,799 | 3,799 | 707 | 1 | 0 | 2,048 |
| wikipedia-versioned-3m-1 | 3,005,260 | 5,260 | 2,264 | 3 | 0 | 2,048 |
| wikipedia-versioned-3m-2 | 3,004,840 | 4,840 | 2,224 | 0 | 0 | 2,048 |
| wikipedia-versionfree-1m-1 | 1,000,000 | 0 | 704 | 0 | 0 | 2,036 |
| wikipedia-versionfree-1m-2 | 1,000,000 | 0 | 698 | 1 | 2 | 2,075 |
| wikipedia-versionfree-3m-1 | 3,000,000 | 0 | 2,125 | 1 | 0 | 2,048 |
| wikipedia-versionfree-3m-2 | 3,000,000 | 0 | 2,120 | 0 | 1 | 2,214 |

The thresholds are 512 and 2,048. These implementations can stop after eight rounds with size residuals. In particular, the second version-free 3M run retains one 2,214-entry leaf, whereas neither versioned 3M run retains an oversized live leaf. The observed speedup does not guarantee identical completed balancing work or identical resulting trees; recall and residual sizes must accompany it. Versioned physical sizes can hide live undersize.

## Reproduction and evidence

- Versioned scheduler revision: `97fa32d84be264c42db836c72f32512ff811f0fc`, based on `1f433be7969b3d28c78e463fe33a299fa0c5fee9`. Its implementation handoff reports 151 passing test executions, benchmark compilation, formatting and commit hooks; independent safety/comparison reviews clear the port.
- Version-free scheduler revision: `14dc9698eb6e400ba6f677575eff7a5659deeed6`. The timed binary is the exact artifact used in the prior finer-scheduler experiments, copied with its SHA-256 recorded. Both use Rust 1.92.0 and `RUSTFLAGS=-C target-cpu=native`.
- Host: Lyon EC2, Intel Xeon Platinum 8375C, 32 physical cores / 64 vCPUs, about 495 GiB RAM. Runs use 32 writer and 32 balancing threads, local NVMe, a 1 GiB configured block cache, full-precision write navigation and neighbor reassignment, L2, branching factor 100, split 2,048 / merge 512, writer tau 0.5, writer level taus `_,_,0.30`, and no writer level minimum percentages or replication flags.
- Both use the same 100 fixed queries, exact final 1M/3M checkpoint ground truth, reader taus 1.5 and 2, rerank factors 1 and 8, beam maximum 256 and read level minimum percentages 50,5,0. Ground-truth verification records exhaustive float64 calculations and input shard hashes at the pinned HuggingFace revision.
- The used ground-truth parquet also contains 9M rows and therefore has a different whole-file hash from the original 1M/3M verification manifest. `gt-provenance-verification.json` verifies that every 1M/3M query and neighbor list matches the original hash-verified container exactly and records both hashes.
- `artifacts/comparison-with-audits.json` contains machine-readable timings, counts, recall, memory and per-round metrics where available. `artifacts/comparison.json` is the raw parser output. Worker task timers overlap and are not CPU time or additive wall time. Versioned code does not expose the version-free leaf-transfer retry counters; absence is not zero.
- Every `artifacts/wikipedia-*.command` and `.command.json` records the exact CLI. `.log`, `.lines.jsonl`, `.resources.jsonl`, `.time`, `.started.json`, `.result.json`, `.checkpoint.json` and `.audit-extended*` preserve raw output, timestamps, resource samples, hashes and audits. `environment.txt`, `binaries.sha256`, source archive hashes and `gt-verification.json` preserve provenance.
- `run.py`, `initial.sh`, `extended.sh`, `build.sh`, `build-audits.sh`, `audit-overlay.py` and `audit.py` preserve orchestration and the untimed audit-only source patch. All builds/runs hold `/mnt/data/chroma-benchmark.lock`; no concurrent benchmark ran. No timed production-code overlay is applied.
- Remote output remains `/mnt/data/versioned-finer-20261020`. All outputs are fresh independent builds; incompatible saved formats are opened only with their matching reader. Original 40M/MS MARCO/8M checkpoint sources remain untouched. About 41 GiB remains free on data NVMe at completion. No GitHub write or merge occurs.

The useful next comparison for production scale is a larger matched fresh-build workload with the intended checkpoint size. These bounded runs support retaining the version-free finer-scheduler implementation for further evaluation; they do not establish a universal speedup or a memory saving.
