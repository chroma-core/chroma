## Summary

A new index begins with one posting list. Splitting it chooses two group centers, called centroids, repeatedly scores every vector against those centers, and recomputes their means. The tree exposes independent balancing jobs only after this first split creates child leaves.

This experiment evaluates the first root split’s independent point distances concurrently. Distance totals and centroid accumulation keep the input order and serial floating-point arithmetic. Later splits use the existing serial routine while ordinary balance workers process independent leaves.

The worker pool is created only for that first split and drops before child splitting and neighbor reassignment. Its size is the smaller of the requested initial-split cap and the balancing phase’s worker budget. Reopened writers never claim this startup operation. If creating the pool fails, the operation reports the failure and uses the serial routine.

## Comparison setting

The default remains serial. Enable the experiment with `--initial-split-threads 32` or `--initial-split-threads 64`; the existing `--balance-threads` value still bounds the actual worker count. Dataset, metric, posting order, versions, and checkpoint boundaries retain their existing behavior.

This PR is independent of the earlier-startup-balancing experiment. Its existing startup cadence remains 100k-vector add batches through the first 1M vectors.

## Validation

A seeded core comparison checks exact centroid bits, labels, versions, and group order for L2, inner product, and cosine distances using 1, 2, and 4 workers. It covers empty, singleton, small, coincident, and mixed inputs, verifies finite centers and complete vector coverage, and checks the existing minimum partition size.

Both hierarchical writer/navigation/persistence harnesses pass: 65 test executions. New tests cover the worker cap, one-time root claim, full reachable-vector preservation through a fresh parallel build, and disabling the claim after reopening. The actual benchmark target compiles. Changed-file formatting and diff checks pass.

## Performance results

Parallel first-root distance assignment provides no median full-build improvement on Wikipedia and a modest improvement on MS MARCO at the approved settings. Each value is the median of three independent fresh builds. The option is explicitly enabled with `--initial-split-threads 32` for Wikipedia and `--initial-split-threads 64` for MS MARCO.

| Dataset | Baseline build | Parallel build | Change | Baseline process wall | Parallel process wall | Baseline peak RSS | Parallel peak RSS | Baseline recall@100 | Parallel recall@100 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Wikipedia | 60.61s | 61.09s | +0.8% | 61.17s | 61.68s | 7.72GiB | 7.67GiB | 77.75% | 77.61% |
| MS MARCO | 68.66s | 66.38s | −3.3% | 69.41s | 67.00s | 7.13GiB | 7.39GiB | 81.32% | 81.18% |

Wikipedia baseline build times span 60.28–67.60s and parallel spans 59.84–62.23s. MS MARCO baseline spans 68.05–71.45s and parallel spans 61.99–66.68s. The first-checkpoint balance medians are 11.70→13.08s and 17.28→16.24s. Whole-build balance medians are 30.41→30.98s and 38.91→36.89s. These samples support a modest MS MARCO improvement, not a general startup speedup.

Only part of the first split is accelerated. Existing first-checkpoint Wikipedia counters attribute 26% of summed split task time to clustering, 67% to reassignment through new clusters, and the remainder to quantization/neighboring leaves. Task times overlap across workers and are not additive wall times. The pool accelerates distance passes inside one root split; later splits, centroid accumulation, and reassignment retain their existing behavior. Random seed selection also changes tree shape and clustering duration between builds. Seeded tests verify exact serial/parallel arithmetic and output order, but ordinary benchmark builds remain random.

Recall ranges overlap: Wikipedia baseline 77.48–78.16%, parallel 77.22–77.99%; MS MARCO baseline 80.90–81.56%, parallel 80.76–81.34%. Three runs and 100 queries do not prove recall equivalence. Peak RSS is the process-wide maximum, including final recall.

The independently measured [adaptive startup schedule (#7868)](https://github.com/chroma-core/chroma/pull/7868) is the stronger result: median build 51.09s on Wikipedia and 53.49s on MS MARCO, improvements of 15.7% and 22.1%. A combined variant is not measured because that schedule shrinks the first root split to 5k vectors, removing much of the work this pool targets.

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

The shared split utility gains a caller-owned pool entry point while retaining its serial entry point (`rust/index/src/spann/utils.rs`). Rayon moves from a development dependency to a regular dependency for that helper. Startup claiming and worker budgeting live in the prototype writer; the cap is exposed in its benchmark configuration and CLI.
