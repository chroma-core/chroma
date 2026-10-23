# Version-cache fill comparison

Both fixes reduce balancing time for a real 9M→10M Wikipedia increment by about 37–39%. They reduce a separate 40M metadata-loading reproduction from about 25 seconds to 3 seconds, including the bulk approach's startup cost. The complete 21M→22M comparison shows about 87–89% lower balancing time and about 75–76% lower opening-plus-checkpoint latency.

## What changes

Balancing checks the current version of each vector before moving it. Persisted metadata stores many versions together in each storage block. The ordinary writer copies only the requested version into its compact cache, then releases decoded reader blocks after 64 requests. Scattered vector IDs repeatedly fetch the same blocks after those blocks leave the shared cache.

The metadata-block approach copies every version from the block containing a requested vector into the compact cache. Later requests for any vector from that block use its compact copy. The reader returns owned integers and releases its temporary ownership of the decoded block before returning. The existing shared block cache can retain its own bounded copies; this path adds no per-reader pins. It preserves lazy opening and performs this work only when a version is needed.

The bulk approach fills the compact cache during writer opening. It scans the version prefix sequentially, copying one storage block's owned integers at a time. It releases each decoded block before fetching the next one. The cache is ready before balancing workers start, at the cost of doing all version work for every opened writer.

Both approaches preserve the mutable version overlay. Updates and deletions in the opened writer take precedence over immutable checkpoint values. Bulk filling finishes before the writer becomes available to callers; block filling can run alongside updates because current-version lookup checks the overlay again after loading. Missing IDs, sparse IDs, and checkpoints without maximum-ID metadata retain their existing behavior. Neither approach retains an unbounded decoded-block pin set.

## Independent branches

Both branches start from `22eb704e71490d95cf715bdb8901f7d6c8a3cf34` and contain separate implementations.

| Approach | Branch | Commit | Worktree |
|---|---|---|---|
| Metadata-block fill | `codex/metadata-page-version-fill` | `bc0faf9ecd8cff6287c95605a7501d73b4371d14` | `/private/tmp/chroma-metadata-page-version-fill` |
| Bounded bulk scan | `codex/bulk-version-cache-fill` | `b5c566540` | `/private/tmp/chroma-bulk-version-cache-fill` |

The owned scalar reader methods live in `rust/blockstore/src/arrow/blockfile.rs` and `rust/blockstore/src/types/reader.rs`. Writer integration lives in `rust/index/benches/hierarchical_index/writer/persistence.rs`. Tests live in `rust/index/tests/hierarchical_posting_delta.rs`. [PR #7874](https://github.com/chroma-core/chroma/pull/7874) contains the recommended metadata-block fill. [PR #7875](https://github.com/chroma-core/chroma/pull/7875) preserves the bulk scan as a draft alternative. Both target `hierarchical-spann`.

## Actual 9M→10M balancing

The first screen performs two alternating runs per implementation and control against the same independent copy of the immutable 9M checkpoint. Execution order is baseline, block fill, bulk fill, bulk fill, block fill, baseline. Persistence is skipped so every writer starts from the same checkpoint.

| Measurement | Baseline | Metadata-block fill | Bounded bulk scan |
|---|---:|---:|---:|
| Balance samples | 22.80 / 23.54 s | 14.70 / 14.16 s | 15.07 / 13.64 s |
| Mean balance | 23.17 s | 14.43 s | 14.36 s |
| Balance reduction | — | 37.7% | 38.0% |
| Initial writer open | 65 / 62 ms | 62 / 61 ms | 592 / 581 ms |
| Checkpoint plus initial open | 44.94 / 45.65 s | 36.64 / 35.89 s | 37.40 / 36.06 s |
| Full process | 61.61 / 62.41 s | 52.90 / 52.22 s | 54.22 / 52.98 s |

The no-commit harness prints recall against the persisted 9M reader while comparing to 10M ground truth. Those rows do not evaluate the new 10M index and are excluded. Every writer reports 10M valid unique vectors and zero orphans. The printed resumed-run throughput divides by the total 10M vectors, although only 1M are added; it is also excluded.

## Full persistence and valid final recall

A second comparison performs one complete increment per implementation and control. Before each run, the independent temporary copy is restored from the immutable 9M source, with distinct file inodes verified. Each run commits and reopens the resulting 10M index, then reads 100 queries against precomputed exact ground truth.

| Measurement | Baseline | Metadata-block fill | Bounded bulk scan |
|---|---:|---:|---:|
| Initial open | 63 ms | 59 ms | 577 ms |
| Dataset loading | 14.06 s | 14.25 s | 13.98 s |
| Add | 7.64 s | 7.62 s | 7.69 s |
| Balance | 23.75 s | 15.03 s | 14.49 s |
| Commit | 9.44 s | 9.56 s | 10.12 s |
| Reopen | 48 ms | 47 ms | 572 ms |
| Initial open plus checkpoint | 54.99 s | 46.55 s | 47.43 s |
| Reduction including open | — | 15.4% | 13.8% |
| Full process including final diagnostics and recall | 90.46 s | 65.44 s | 66.98 s |
| Peak RSS | 63.06 GiB | 62.67 GiB | 63.00 GiB |
| Recall at tau 1.5, without full-precision reranking | 74.55% | 74.54% | 74.72% |
| Reported valid unique vectors | 10M | 10M | 10M |
| Reported orphans | 0 | 0 | 0 |

The full-process timer includes post-checkpoint diagnostics and recall. Those diagnostics open fresh metadata readers and also benefit from the cache fills, so process improvement is larger than write-checkpoint improvement. One committed sample per implementation does not establish a recall or commit-time change. Parallel balancing changes work order and tree shape; measured iteration counts range from one to two.

## Full 21M→22M comparison

The larger test confirms the same bottleneck in a complete committed increment. Each implementation and control runs twice, in the order control, block fill, bulk scan, bulk scan, block fill, control. Every run starts from an independent copy of the same preserved 21M tree. The fixture extends the validated 10M output to 21M with the block-fill implementation; it does not reuse or overwrite the original 40M tree. Each increment commits, reopens, and evaluates 100 exact queries against saved 22M ground truth.

| Measurement | Control | Metadata-block fill | Bounded bulk scan |
|---|---:|---:|---:|
| Balance samples | ~240 / ~216 s | 28.22 / 29.22 s | 27.50 / 22.20 s |
| Mean balance | ~228 s | 28.72 s | 24.85 s |
| Balance reduction | — | **~87%** | **~89%** |
| Initial-open samples | 146 / 140 ms | 145 / 145 ms | 1.42 / 1.38 s |
| Checkpoint samples as printed | 4.7 / 4.2 min | 1.1 / 1.1 min | 1.1 / 1.0 min |
| Mean initial open + checkpoint | ~267 s | ~66 s | ~64 s |
| Reduction including open | — | **~75%** | **~76%** |
| Full-process samples | 586.98 / 423.62 s | 110.19 / 109.67 s | 111.10 / 106.12 s |
| Mean full process | 505.30 s | 109.93 s | 108.61 s |
| Peak RSS range | 125.98–126.41 GiB | 121.55–121.84 GiB | 121.64–121.88 GiB |
| Recall range, tau 1.5 without full-precision reranking | 72.99–73.30% | 73.15–73.22% | 73.12–73.23% |
| Valid unique vectors / orphans, every output | 22M / 0 | 22M / 0 | 22M / 0 |
| Balance rounds | 3 / 2 | 8 / 8 | 6 / 3 |

The CLI rounds durations longer than 60 seconds to 0.1 minutes. Control balancing and all checkpoint totals therefore have roughly ±3 seconds of display rounding; their averages and percentage reductions are approximate. Full-process times come from the precise external process timer. The full-process comparison includes expensive post-reopen diagnostics and recall, so it does not replace the separately reported write-checkpoint comparison.

The amount of reassignment work remains similar while destination loading gets much faster. In the first sample, the control performs 454.7k reassignments and 772 splits; block fill performs 451.2k reassignments and 767 splits. Average destination registration within reassignment falls from 10.5ms to 31.5µs, and the average entire reassignment falls from 33.9ms to 2.7ms. These timers include waiting and nested work and cannot be added to obtain wall time.

The two fixes have similar overall performance in this sample count. Bulk scan has a lower mean balancing time, while block fill preserves faster opening. Parallel work ordering changes the number of balance rounds and tree shape; two samples do not establish a small performance difference between the fixes or recall equivalence. Candidate recall ranges fall within the control's observed range. The peak-memory difference also includes changes in parallel work and reader pinning, so it is not attributed solely to cache filling.

The shared 21M source remains at `/mnt/data/bulk-version-cache-fixes-source-21m` for future comparisons. All 57,510 files match its before/after manifest, totaling 93,212,784,474 bytes. Its checkpoint SHA256 is `bb1b21158c5f070c3a108fe7f2b9233b5e9477f827b423a016ee44e3e201095d`. The original 40M manifest and checkpoint hash also match. Existing global ground truth is restored after the comparison, the lock is released, and no benchmark is running.

## Larger metadata working set

The separate 40M reproduction opens an independent copy of scalar metadata, postings, tree metadata, and centroids. It loads the same deterministic 200 leaves through the actual writer loading method. It performs no embedding loading, balancing, persistence, or source checkpoint writes.

| Measurement | Baseline | Metadata-block fill | Bounded bulk scan |
|---|---:|---:|---:|
| Full process samples, including open | 24.77 / 24.88 s | 3.06 / 3.05 s | 3.06 / 3.05 s |
| Mean process reduction | — | 87.7% | 87.7% |
| Instrumented initial open | Not separately instrumented | 0.290 / 0.289 s | 2.607 / 2.594 s |
| Instrumented leaf loading | 24.311 / 24.441 s | 2.626 / 2.626 s | 0.319 / 0.315 s |
| Approximate process read traffic | 40.1 / 40.8 GiB | 1.21 GiB | 1.21 GiB |
| Cached checkpoint versions after loading | Not counted in this probe | 40M | 40M |
| Compact cache allocation | Not counted in this probe | 80,315,584 bytes | 80,315,584 bytes |
| Peak RSS range | 1.36–1.39 GiB | 1.33–1.35 GiB | 1.36–1.37 GiB |

The selected leaves scatter across enough metadata blocks to populate every checkpoint version even with demand-driven block fills. Both fixes store about 80 MB of compact versions instead of repeatedly copying roughly 40 GiB of metadata bytes. Read traffic uses `/proc/self/io`'s `rchar` counter. Every case has zero physical filesystem input and zero major page faults; the byte reduction measures repeated operating-system file-cache copies, hashing, and decoding.

The local storage backend hashes fetched file contents. Production object storage can supply a fingerprint without that local hashing, so these percentages do not estimate production speedup. They demonstrate removal of repeated block work under the same backend. This reproduction is not a full 40M balancing checkpoint.

## Commands and settings

The node is the existing 64-vCPU, 512-GiB Intel Xeon EC2 machine. Every timed process runs alone under `/mnt/data/chroma-benchmark.lock`; compilations use the same lock. Settings use 1024-dimensional Wikipedia vectors, Euclidean distance, 32 add workers, 32 balancing workers, full-precision writer navigation and neighbor reassignment, and a 1 GiB shared block cache. Write-level minimum percentages are omitted.

The commit-inclusive command follows. `BINARY` selects the control or candidate executable. `COPY` is an independent temporary copy.

```sh
BINARY --dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --compute-gt-clusters false --resume --save --save-dir COPY --max-cache-bytes 1073741824
```

The short screen substitutes `--no-commit` for `--save`. Exact commands are saved beside each log. The component probes set `CHROMA_BALANCE_COMPONENT_FIXTURE=/mnt/data/large-index-investigation-components-40m`, `CHROMA_COMPONENT_CACHE_BYTES=1073741824`, and `CHROMA_COMPONENT_LEAF_COUNT=200`. Their instrumentation is isolated in saved diffs and binaries.

## Correctness and provenance

Each implementation passes 60 test executions across the posting/version and policy suites. Tests cover concurrent loads and updates, deletions, sparse IDs, legacy metadata, stale postings, reopening, and committing. The added test preserves sparse scalar values and tombstone bytes while verifying zero reader pins. All nine writer increments report 10M valid unique vectors and zero orphans; the three committed runs provide valid final recall.

Local commits include final formatting and tests. Saved production binaries use the same behavior from exact patches applied to pinned base `22eb704`; those patches and hashes are retained. Probe binaries add only saved instrumentation. The baseline is the completed 40M run's executable, with SHA256 `f8ffbabb6b03c3236e73cc0d2f3d19258ef915de6721cbf7817402dc51b54252`.

| Executable | SHA256 |
|---|---|
| Metadata-block production benchmark | `4cce0c907c925c6727c72426c942fa48c750afd4a0db6c152a308fd5368f2fc2` |
| Bulk production benchmark | `2ef3719d18f8e3db62cef4d0ef0c3d617e23d5eb54c583427dde7d0c71c7ccb4` |
| Metadata-block component probe | `d0f2162cc8bb73308a9988a199ce50d552520241b0c4249b0ed7c9b5f418e0b8` |
| Bulk component probe | `00a21342096f930a4a5551756f2bf24b91e801aa42bfdb30685f2303004751a8` |

## Preservation and storage

The original 40M index remains unchanged. Its file names, sizes, modification times, and inodes match the before manifest: all 109,539 files match, totaling 177,546,237,564 bytes. The checkpoint SHA256 remains `6e19351646b024612bcbca9d307c46ceca6e386ccfb2d761abd1db372d854935`. The original 9M checkpoint is read only. The investigator's explicitly released independent temporary copy now contains the bulk candidate's committed 10M result.

Lyon explicitly authorizes deleting completed benchmark copies while retaining the 40M checkpoints. Five independent continuous-scheduler outputs are recycled after their checkpoints, file counts, and byte totals are saved. Their logs, commands, hashes, audits, and original source checkpoints remain available. No original fixture, source cache, or saved ground truth is removed; the unknown second NVMe remains untouched.

The shared 21M fixture and every original checkpoint remain preserved. Its before/after file manifest matches, and the original 40M's 109,539 files still match. Additional completed, independently copied scheduler and deferred-balancing outputs are recycled under Lyon's explicit authorization to make room for the larger comparison. Their checkpoint/count/byte records are saved in the `version-cache-39m40m/authorized-cleanup.json` record; reports, commands, logs, audits, and binary hashes remain available. The preserved 21M, 25M, and original 40M fixtures remain available.

## Recommendation

The metadata-block fill is the stronger default candidate. It delivers the same large-working-set benefit as the bulk scan, preserves fast writer opening, and avoids scanning all versions for writers that do little work. The bulk scan adds about half a second to opening and reopening 9M versions and about 2.3 seconds to opening 40M versions in this reproduction. The full higher-checkpoint increment confirms the effect. A complete 40M whole-build rerun is not measured, so the larger checkpoint and component results do not establish a whole-build 40M speedup.

Raw evidence, exact commands, metrics, test logs, patches, probe diffs, and preservation proof are saved beside this report and at `/home/ubuntu/benchmark-data/bulk-version-cache-fixes`. The EC2 lock is released and no cache-comparison benchmark is running.

## Additional committed comparison: 25M to 26M

Both published PR commits complete the larger increment in about 69 seconds including opening, adding, balancing, committing, and reopening. The whole process takes about two minutes when final index validation and recall are included. The metadata-block fill keeps opening fast; the bulk scan moves a little balancing work into opening and reopening. These single clean samples do not establish a meaningful overall advantage for the bulk scan.

The common starting tree contains 25M Wikipedia vectors. It is generated by extending an independent copy of the preserved 21M fixture with the metadata-block implementation. Both timed runs begin with independently copied files from that same frozen 25M source. The source retains its exact file manifest after both runs. The original 40M index retains all 109,539 original files and the same checkpoint hash. The preserved 21M source is read only throughout preparation. No 39M build runs.

| Measurement | Metadata-block fill, #7874 | Bulk scan, #7875 |
|---|---:|---:|
| Initial writer open | 0.170 s | 1.660 s |
| Load next 1M source vectors | 14.00 s | 13.94 s |
| Add 1M vectors | 10.26 s | 10.27 s |
| Balance | 27.07 s, 2 rounds | 25.93 s, 1 round |
| Commit | 16.10 s | 16.09 s |
| Reopen | 0.155 s | 1.500 s |
| Sum of initial open and checkpoint phases | 67.76 s | 69.39 s |
| Full process including diagnostics and recall | 120.09 s | 121.29 s |
| Peak process RSS | 142.91 GiB | 143.83 GiB |
| Final recall at tau 1.5 without full-precision reranking | 72.81% | 72.70% |
| Final valid unique vectors | 26M | 26M |
| Orphan vectors | 0 | 0 |
| Sampled process read traffic, including OS file cache | 117.85 GiB | 117.86 GiB |
| Physical filesystem reads | 0.34 MiB | 23.72 MiB |

The phase sum uses the CLI's individual second-resolution timings instead of its rounded total in minutes. Process read traffic is sampled every 50 ms from the benchmark process and includes the final whole-index diagnostics and recall. Most reads are served by the operating system's file cache; these figures do not measure a production network workload. Single-sample recall differences and different balancing round counts are not evidence of a correctness or performance regression.

The first metadata-block run also completes writing and reports 26M valid unique vectors, zero orphans, and 72.82% recall. Its balancing takes 28.74 seconds. Its observer raises a permission error while reading the exiting process, so that attempt lacks a verified exit/resource summary and is excluded from the clean comparison. The observer now handles that exit race, and the repeated metadata-block run and the bulk run both exit successfully. The benchmark code remains unchanged.

The benchmark executables are built directly from the published commits: `bc0faf9ecd8cff6287c95605a7501d73b4371d14` for #7874 and `b5c566540bb29309054e4ff69fbf858444de9e0d` for #7875. Their SHA256 hashes are `112429cfddb816dc5c5f2c507d9d982cdc175535f1385723b805207175385ceb` and `17f94a6cbe636c63794ee2db91a22c5e94c584eccc606e0d93e60a325db75e2e`. The exact command uses checkpoint 26, checkpoint size 1M, 32 add and balance threads, full-precision write navigation and reassignment, a 1 GiB cache, 100 precomputed exact queries, and the approved beam settings. No minimum-percentage level settings are supplied.

The frozen source is `/mnt/data/version-cache-source-25m`. The clean saved outputs are `/mnt/data/version-cache-pr7874-26m-verified` and `/mnt/data/version-cache-pr7875-26m-verified`. Full CLI output, commands, hashes, resource summaries, sampled counters, checkpoint metadata, and preservation proof are saved in the local `25m26m` directory and remotely at `/home/ubuntu/benchmark-data/version-cache-25m26m`. There is no new unfixed 25M control run, so this comparison does not measure a percentage improvement against an unfixed tree of the same size.
