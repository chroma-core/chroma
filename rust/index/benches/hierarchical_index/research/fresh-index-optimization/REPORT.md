# Fresh-index startup benchmark comparison

Balancing small batches while the tree is small is the strongest improvement in this comparison. The adaptive schedule reduces median full build time by 15.7% on Wikipedia and 22.1% on MS MARCO. Parallel distance calculations in the first root split provide no median gain on Wikipedia and a modest 3.3% median gain on MS MARCO.

All 26 fresh builds complete. Separate read-only audits of all 26 final persisted indexes confirm exactly the valid IDs `[0, 1_000_000)` and that every child link resolves. Each build preserves ten 100k persisted checkpoints. The node lock is released, and the saved indexes and executable artifacts remain available.

## Repeated comparisons

The table reports medians of three independent fresh builds per variant. Build time includes source batch loading and all add, balance, commit, reopen, garbage collection, and summary work. Process wall time also includes initialization and final recall. Index phases sum add, balance, commit, and reopen. RSS is the process maximum, including recall. Recall uses the same 100 precomputed L2 queries at the final checkpoint.

| Dataset | Variant | Full build (s) | Process wall (s) | Index phases (s) | Build change | Peak RSS (GiB) | Recall@100 |
|---|---|---:|---:|---:|---:|---:|---:|
| Wikipedia | baseline | 60.61 | 61.17 | 40.39 | +0.0% | 7.72 | 77.75% |
| Wikipedia | parallel | 61.09 | 61.68 | 40.94 | +0.8% | 7.68 | 77.61% |
| Wikipedia | adaptive | 51.09 | 51.66 | 30.78 | -15.7% | 7.53 | 77.66% |
| MS MARCO | baseline | 68.66 | 69.41 | 48.72 | +0.0% | 7.13 | 81.32% |
| MS MARCO | parallel | 66.38 | 67.00 | 46.51 | -3.3% | 7.39 | 81.18% |
| MS MARCO | adaptive | 53.49 | 53.98 | 33.37 | -22.1% | 7.11 | 81.17% |

The adaptive timing gain repeats in every sample and exceeds the observed baseline timing range. Wikipedia takes 50.07–51.74s with the adaptive schedule versus 60.28–67.60s for the baseline. MS MARCO takes 52.91–53.74s versus 68.05–71.45s. The index phase medians fall by 23.8% and 31.5%, respectively, so the improvement survives removing source-loading time from the comparison.

Recall medians are slightly lower with the adaptive schedule: 0.09 percentage points on Wikipedia and 0.15 percentage points on MS MARCO. Wikipedia baseline recall spans 77.48–78.16%, and adaptive spans 77.34–77.83%. MS MARCO baseline spans 80.90–81.56%, and adaptive spans 81.06–81.47%. The ranges overlap, but three random clustering runs and 100 queries do not establish recall equivalence. Earlier clustering trains centers on different data prefixes; that tradeoff remains relevant when choosing the policy.

The parallel-root result depends on the dataset and ordinary clustering variation. Wikipedia takes 59.84–62.23s, overlapping the baseline, with a 0.8% slower median. MS MARCO takes 61.99–66.68s, producing a 3.3% faster median. The first-root worker caps are explicitly enabled at 32 and 64; the experiment is not accidentally measured with its serial default. These measurements favor the adaptive schedule over adding parallel-root complexity as the next performance improvement.

## Where the time changes

Each cell is the median of that phase across the same three runs. Since each column takes its own median, summing a row can differ slightly from the median of the complete run.

| Dataset | Variant | Add (s) | Balance (s) | Commit (s) | Reopen (s) | Load (s) | First checkpoint balance (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| Wikipedia | baseline | 3.030 | 30.410 | 6.955 | 0.025 | 15.830 | 11.700 |
| Wikipedia | parallel | 2.942 | 30.980 | 6.995 | 0.024 | 15.670 | 13.080 |
| Wikipedia | adaptive | 3.167 | 20.530 | 7.063 | 0.024 | 15.970 | 1.730 |
| MS MARCO | baseline | 2.606 | 38.910 | 7.168 | 0.026 | 16.090 | 17.280 |
| MS MARCO | parallel | 2.558 | 36.890 | 7.008 | 0.025 | 15.940 | 16.240 |
| MS MARCO | adaptive | 2.653 | 23.680 | 7.016 | 0.023 | 16.150 | 2.320 |

The adaptive schedule reduces balancing latency while add and commit work remain similar. Its first checkpoint balance median falls from 11.70s to 1.73s on Wikipedia and from 17.28s to 2.32s on MS MARCO. The schedule begins with 5k additions, then chooses `clamp(leaf_count × 2048, 5000, 100000)` after each completed balance. This limits the first large cluster and grows the interval as the tree exposes independent leaf jobs. Every add batch finishes before balancing, and each persisted boundary remains at another 100k vectors.

Parallel-root distance calculations accelerate only part of the startup work. The existing first Wikipedia baseline split breakdown attributes 26% of summed split task time to clustering, 2% to quantization, 67% to reassignment through newly created clusters, and 5% to neighboring leaves. These are overlapping worker/task measurements, not additive wall-clock phases. Only distance calculations in the first root split become parallel; later clustering, serial accumulation, and reassignment retain their existing behavior. The measurements do not isolate a deterministic root-only speedup because normal builds retain random seed selection.

Commit preparation and block writing remain roughly 4.3s and 2.6–2.8s over the complete 1M build. Reopening totals only about 24–26ms. Loading the source batches totals about 16s for every repeated variant. These phases do not explain the adaptive gain.

## Fixed-cadence screening

Each non-baseline cell below is a single screening run; the baseline column is the three-run median. The default fixed 100k candidate uses the same boundaries as the baseline and is an additional compatibility/control sample. Differences between those samples include the existing random clustering variation.

| Dataset | Baseline median (s) | Fixed 5k (s) | Fixed 10k (s) | Fixed 50k (s) | Fixed 100k (s) | Adaptive first screen (s) |
|---|---:|---:|---:|---:|---:|---:|
| Wikipedia | 60.61 | 123.67 | 90.62 | 58.92 | 64.74 | 51.09 |
| MS MARCO | 68.66 | 105.62 | 85.65 | 64.78 | 68.62 | 53.49 |

Frequent balancing throughout startup can increase total work. Fixed 5k and 10k intervals shorten the initial cluster but repeat navigation setup, cleanup, and reassignment through later checkpoints. They are slower on both datasets. Fixed 50k is the strongest tested fixed interval, but the adaptive schedule is faster on both datasets and repeats reliably. It is the best tested schedule for these settings, not a universally optimal frequency. No exhaustive interval sweep is needed to distinguish these candidates.

A combined variant is not measured. The earlier schedule reduces the amount of data in the first root split from 100k to 5k, removing much of the work the parallel-root option targets. The independent parallel result is small or absent, so these measurements do not justify spending additional runs on their combination.

## Revisions and environment

All variants share feature baseline `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`. The parallel-root variant is [PR #7867](https://github.com/chroma-core/chroma/pull/7867), commit `20788c3fee5b66745ea4af68246086cd588a301a`. The startup-cadence variant is [PR #7868](https://github.com/chroma-core/chroma/pull/7868), commit `ef8fb22f74f39c9cf1051d080be7398850f1f23b`. Both are independent; scheduler and destination-queue changes are excluded equally. Release builds clean the index crate before compilation and copy each executable separately. Exact commits and SHA256 executable identities are in the raw manifests.

The existing lyon machine is an EC2 r6id.16xlarge with 64 logical CPUs, 32 physical cores, 512 GiB advertised memory, and local NVMe storage. The CPU is an Intel Xeon Platinum 8375C. Release builds and runs take the shared machine lock and run sequentially. These are warm local-cache measurements of existing 1024-dimensional source prefixes. No source vectors are downloaded or converted. Copying source data from persistent storage to NVMe and cold storage/network behavior are outside the comparison.

Every run adds 1M vectors and persists ten 100k checkpoints. Writer navigation and neighbor reassignment use full precision. Wikipedia uses 32 add and balance workers, beam 0.5, level thresholds `_,_,0.30`, and neighbor-selection factor 0. MS MARCO uses 64 workers, beam 1.5, and neighbor-selection factor 4. Both use L2, a 1 GiB block cache, read threshold 1.5, one reranked vector, 100 queries, and no ground-truth cluster diagnostics. The minimum-percent level option is absent. The actual cached ground truth is checked to include the exact 1M prefix, and its hashes are preserved.

## Validation and measurement limits

Every run ends at checkpoint index 9 with exactly 1M inserted vectors. Separate read-only audits open each saved final checkpoint, load posting lists, check that every reachable child resolves, and verify the set of current-version IDs is exactly `[0, 1_000_000)`. All 26 audits pass. They run after the timed comparisons, so validation does not change cache behavior during a build. Intermediate persisted checkpoint copies are not audited in this comparison; focused implementation tests cover add/balance and persistence behavior, while final saved-index audits cover every measured variant and seed outcome.

The build timer includes loading each source batch, adding, balancing, committing, reopening, garbage collection, and the build summary. It excludes query setup before the timer and subsequent recall. Long printed durations round to 0.1 minute, so the parser recovers a close estimate from printed vector throughput, rounded to one vector per second. Full process wall time is the independent higher-precision measurement and shows the same result. Per-phase values are sums of the existing checkpoint timers and retain their printed rounding.

The add timer includes navigation setup, buffered registration/flush, and adaptive leaf-count inspection. Every startup balance call is included. Commit includes preparing and writing changed blocks. Peak RSS includes indexing and recall; the adaptive median is 7.53GiB versus 7.72GiB baseline on Wikipedia, and 7.12GiB versus 7.13GiB on MS MARCO. The comparison does not claim improvements for larger indexes, reopened trees, cold source storage, different thread budgets, or the subsequently merged scheduler changes.

## Commands and raw artifacts

Exact shell-quoted commands, revisions, executable checksums, host information, raw logs, process timing files, checkpoint metadata, and all audit logs are stored under `benchmarks/raw`. Runner scripts and the parser are stored under `benchmarks`. Remote outputs and binaries remain in `/home/ubuntu/benchmark-data/fresh-index-20261008`, and each saved index has its own `/mnt/data/fresh-index-20261008-*` directory. No source fixtures are removed or changed.

The common command settings are:

```sh
--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir <unique-output-directory>
```

Wikipedia adds:

```sh
--dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0
```

MS MARCO adds:

```sh
--dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4
```

The parallel candidate adds `--initial-split-threads 32` for Wikipedia and `--initial-split-threads 64` for MS MARCO. The fixed cadence screens add `--startup-balance-policy fixed --startup-balance-size N`, for N in 5000, 10000, 50000, and 100000. The adaptive candidate adds `--startup-balance-policy adaptive --startup-balance-min-size 5000 --startup-balance-size 100000`. Baseline runs use neither experimental option.

The screen runs baseline → parallel → fixed50k → fixed10k → adaptive → fixed5k → fixed100k → baseline for each dataset. The targeted repetition order is adaptive → parallel → baseline → parallel → adaptive for each dataset. All runs are sequential, and the machine lock is released after the final audits.
