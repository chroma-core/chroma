## Summary

A fresh index benefits from creating independent leaves before its first posting list becomes very large. Balancing more often can expose parallel work earlier, but it also repeats balancing overhead and makes navigation use a growing tree sooner.

The existing benchmark already balances 100k-vector startup add batches through the first 1M vectors. This experiment adds smaller fixed intervals and an adaptive interval. The adaptive interval estimates how many new vectors would add roughly one split-threshold of rows to each existing leaf, then bounds that estimate between 5k and 100k by default. It reevaluates the leaf count after every completed balance.

Each subbatch finishes routing, joins the add workers, and flushes its postings before balancing begins. The next subbatch starts with the refreshed navigation state. Persisted checkpoints, commits, dataset loading, and ground-truth boundaries retain their existing frequency.

## Comparison settings

The default fixed 100k schedule preserves the existing subbatch boundaries, including small checkpoints and the transition past the first 1M vectors.

- Use `--startup-balance-policy fixed --startup-balance-size 5000`, `10000`, `50000`, or `100000` to compare fixed frequencies.
- Use `--startup-balance-policy adaptive` for the leaf-capacity heuristic, bounded by `--startup-balance-min-size 5000` and `--startup-balance-size 100000` by default.

Adaptive scheduling applies to fresh build sessions. Resume uses the selected fixed interval during any remaining startup prefix. After the first 1M indexed vectors, additions use the existing whole-checkpoint batch behavior. The CLI reports the effective policy.

## Rationale and tradeoffs

The heuristic lets the initial interval stay small while only one leaf exists, then increases it as leaf splits expose more independent work. Under uniform routing, `leaf_count × split_threshold` gives an approximate growth budget per balancing pass. The explicit minimum limits repeated setup overhead; the maximum retains the existing 100k ceiling during startup.

This is an experimental heuristic rather than a measured optimal schedule. Skewed routing, neighbor reassignment, and dataset geometry can favor a different interval. Earlier clustering can change tree shape and recall because its centers are trained on smaller prefixes. More frequent balancing can also increase total work. Full build time and recall must decide which interval is preferable.

## Validation

The focused harness passes 32 test executions, including six new cases. They cover exact default scheduling boundaries, the four proposed fixed intervals, adaptive growth and bounds, overflow/empty-input handling, resume/established-index behavior, rejection of invalid intervals, and complete reachable-vector preservation through joined adaptive add/balance phases. The actual benchmark target compiles. Changed-file formatting and diff checks pass.

## Performance results

The adaptive schedule is the strongest tested startup policy on both datasets. It reduces median full build time by 15.7% on Wikipedia and 22.1% on MS MARCO. Each repeated value below is the median of three fresh builds with the same ten persisted 100k checkpoints. The candidate uses `--startup-balance-policy adaptive --startup-balance-min-size 5000 --startup-balance-size 100000`.

| Dataset | Baseline build | Adaptive build | Change | Baseline process wall | Adaptive process wall | Baseline index phases | Adaptive index phases | Baseline peak RSS | Adaptive peak RSS | Baseline recall@100 | Adaptive recall@100 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Wikipedia | 60.61s | 51.09s | −15.7% | 61.17s | 51.66s | 40.39s | 30.78s | 7.72GiB | 7.53GiB | 77.75% | 77.66% |
| MS MARCO | 68.66s | 53.49s | −22.1% | 69.41s | 53.98s | 48.72s | 33.37s | 7.13GiB | 7.12GiB | 81.32% | 81.17% |

The adaptive build ranges are 50.07–51.74s on Wikipedia and 52.91–53.74s on MS MARCO. They remain below every baseline sample, whose ranges are 60.28–67.60s and 68.05–71.45s. Index phases sum all add, balance, commit, and reopen work; their medians fall by 23.8% and 31.5%. Source loading remains about 16s. The gain therefore survives removing dataset loading and includes all early balancing work.

Balancing explains the improvement. Whole-build balance medians fall 30.41→20.53s on Wikipedia and 38.91→23.68s on MS MARCO. First-checkpoint balance falls 11.70→1.73s and 17.28→2.32s. Add, commit, and reopen phases remain similar. Navigation initialization and adaptive leaf-count inspection are included in add time. The smaller initial batch limits the first large cluster, then the interval grows as independent leaves become available.

### Fixed-cadence screen

Each fixed setting is screened once; baseline is the three-run median and adaptive is its first screen. The fixed 100k candidate preserves baseline boundaries and supplies an extra compatibility/control sample. Random clustering accounts for some differences between runs with the same boundaries.

| Dataset | Baseline median | Fixed 5k | Fixed 10k | Fixed 50k | Fixed 100k | Adaptive screen |
|---|---:|---:|---:|---:|---:|---:|
| Wikipedia | 60.61s | 123.67s | 90.62s | 58.92s | 64.74s | 51.09s |
| MS MARCO | 68.66s | 105.62s | 85.65s | 64.78s | 68.62s | 53.49s |

Fixed 5k/10k settings shorten initial clustering but repeat enough cleanup/navigation/reassignment to increase total build time. Fixed 50k is the strongest tested fixed interval. The adaptive rule avoids sustaining tiny intervals as the tree grows and wins on both datasets. This is the best tested schedule for these settings; it does not establish a universally optimal interval or justify changing the default without reviewing the tradeoff.

Recall medians differ by −0.09 percentage points on Wikipedia and −0.15 percentage points on MS MARCO. Ranges overlap: Wikipedia baseline 77.48–78.16%, adaptive 77.34–77.83%; MS MARCO baseline 80.90–81.56%, adaptive 81.06–81.47%. Three random clustering runs and 100 queries do not establish recall equivalence. Earlier clustering trains centers on different prefixes, so recall remains a decision criterion.

The independent [parallel first-root experiment (#7867)](https://github.com/chroma-core/chroma/pull/7867) measures +0.8% build time on Wikipedia and −3.3% on MS MARCO. A combined variant is not measured: this schedule shrinks the first root split from 100k to 5k, removing much of the work parallel distance passes target. Cold source storage, larger indexes, resumed trees, and later scheduler changes are outside these measurements.

### Reproduction settings

The three independent revisions are baseline `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`, parallel-root `20788c3fee5b66745ea4af68246086cd588a301a`, and startup-cadence `ef8fb22f74f39c9cf1051d080be7398850f1f23b`. They exclude scheduler/destination-queue changes equally. Executables are rebuilt separately, copied, and recorded with SHA256 manifests. Builds and runs serialize on the existing lyon EC2 r6id.16xlarge using a shared machine lock. The machine has 64 logical CPUs, 32 physical cores, an Intel Xeon Platinum 8375C, 512GiB advertised RAM, and local NVMe. Existing source/ground-truth caches are reused with warm OS caching; no data conversion or new infrastructure is involved.

Every fresh run uses the same 1M-vector prefix, ten persisted 100k checkpoints, 1024 dimensions, L2, full-precision write navigation and neighbor reassignment, 100 precomputed exact-prefix queries, read threshold 1.5, one reranked vector, and a 1GiB cache. The minimum-percent level option is absent. The commands use:

```sh
--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir <unique-output-directory>
```

Wikipedia adds `--dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0`. MS MARCO adds `--dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4`.

The screen order is baseline → parallel → fixed50k → fixed10k → adaptive → fixed5k → fixed100k → baseline for each dataset. Repetitions use adaptive → parallel → baseline → parallel → adaptive for each dataset. This produces three runs for baseline, parallel, and adaptive, plus one screen of each fixed cadence. All 26 builds complete; separate read-only audits of every final saved index confirm exactly current-version IDs `[0, 1_000_000)` and that every child resolves. Audits run after timing. Intermediate persisted checkpoints are not separately audited in this comparison.

Build time includes source loading, add, balance, commit, reopen, garbage collection, and summary work. It excludes query initialization and final recall. Process wall time includes those phases and agrees with the build comparison. Long printed build durations round to 0.1min, so values below use the close estimate recovered from rounded vector throughput. Exact process wall times, phase sums, commands, revisions, binary manifests, and audit logs are preserved in `/home/ubuntu/benchmark-data/fresh-index-20261008`; each index has its own `/mnt/data/fresh-index-20261008-*` directory. Local report: `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/fresh-index-optimization/REPORT.md`.

## Code reference

The experiment is confined to the profiling harness and a small schedule helper (`rust/index/benches/startup_balancing.rs`), with a focused integration test. It is independent of the parallel first-root clustering PR and can be combined with that variant after comparing them separately.
