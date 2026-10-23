# Worker-local instrumentation at 10M Wikipedia vectors

## Result

The change retains every instrumentation timer, exact operation count, per-level navigation count, and split-size sample. Three alternating pairs resume independent copies of the same saved 9M checkpoint to 10M. Median add time is 7.620 seconds for the baseline and 7.520 seconds for the candidate (-1.31%). These runs show no substantial add-time improvement from removing shared statistics updates. Median balancing time changes from 23.90 to 24.19 seconds (+1.21%); balancing produces different trees and sometimes requires a different number of rounds, so that change does not establish an instrumentation-driven speedup.

| Median across three runs | Baseline | Candidate | Candidate change |
| --- | ---: | ---: | ---: |
| Routing and append | 7.620 s | 7.520 s | -1.31% |
| Worker collection | 7.399 s | 7.299 s | -1.35% |
| Regroup and posting flush | 0.212 s | 0.212 s | +0.00% |
| Balancing | 23.900 s | 24.190 s | +1.21% |
| Dataset loading | 14.420 s | 14.160 s | -1.80% |
| Commit preparation | 5.880 s | 5.910 s | +0.51% |
| Storage flush | 3.590 s | 3.570 s | -0.56% |
| Checkpoint phases combined | 55.780 s | 55.350 s | -0.77% |
| Process peak memory | 62.810 GiB | 63.008 GiB | +0.32% |

The previously recorded merged-code resume takes 7.578 seconds for add, 23.12 seconds for balance, and 54.09 seconds for the checkpoint phases. The candidate's median add differs from that previous result by -0.77%. The fresh paired measurements provide the stronger comparison because both revisions execute in this session on the same machine.

## Individual runs

Pair one executes baseline then candidate, pair two executes candidate then baseline, and pair three executes baseline then candidate. Runs execute sequentially. Each run starts with a separate writable copy of the original 9M checkpoint.

| Run | Add | Balance | Rounds | Checkpoint phases | Recall at 100 |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline 1 | 7.620 s | 23.82 s | 1 | 55.20 s | 74.51% |
| candidate 1 | 7.624 s | 24.48 s | 2 | 55.78 s | 74.56% |
| baseline 2 | 7.602 s | 23.90 s | 2 | 55.78 s | 74.60% |
| candidate 2 | 7.520 s | 23.75 s | 1 | 54.93 s | 74.64% |
| baseline 3 | 7.745 s | 23.97 s | 1 | 55.99 s | 74.59% |
| candidate 3 | 7.468 s | 24.19 s | 1 | 55.35 s | 74.73% |

All six runs finish successfully and commit exactly 10,000,000 vectors. Their final diagnostics report 10M unique vectors and zero orphaned IDs. Recall varies with the resulting tree; its range is 74.51%–74.73%. These are build comparisons, so different parallel split and merge schedules prevent an exact same-index recall or query-latency comparison. No balancing round-cap warning occurs.

The combined checkpoint metric sums dataset loading, add, balance, commit, and reopen. The profiler's overall throughput uses all 10M vectors as its numerator even though this process adds only 1M, so this report excludes it. Each individual process also performs final materialization, diagnostics, and recall evaluation outside the printed checkpoint phases. Timings are three-run medians, not a statistically established bound on smaller differences.

## Instrumentation retained

Workers collect statistics in ordinary per-worker integer arrays and publish their totals when the worker exits. All detailed clock reads remain enabled. Direct calls outside worker scopes publish immediately. A global snapshot includes completed workers and receives an active worker's counts at that worker's phase boundary. Nested calls for the same writer share an accumulator; a distinct writer receives its own accumulator. Unwinding publishes the completed measurements and restores the enclosing accumulator.

Split-size samples collect locally and join the shared vector once per worker. Progress reports exact processed counts in chunks of at most 1,024 vectors, including skipped vectors and each worker's remainder. For this one-million-vector, 32-worker batch, progress reporting performs 992 shared progress increments instead of 1,000,000. Registration captures the lock-wait and quantization timing boundaries while holding the leaf guard and publishes the measurements after releasing the relevant guards.

## Validation

The `hierarchical_policy_cache` and `hierarchical_posting_delta` suites pass on macOS and in an optimized EC2 build: 29 and 36 test executions respectively. New checks cover exact parallel count aggregation, per-level totals, split-size samples, nested writer isolation, panic cleanup, and chunk/remainder progress accounting with a deleted vector. Existing checks exercise concurrent split/merge preservation and checkpoint persistence. The optimized benchmark compiles successfully.

Every copied file has a different inode from its original fixture. Before/after hashes of the original 9M and 10M checkpoint metadata match. The default ground-truth cache has the same first 100 selected 10M query vectors, their exact float32 bits, and their L2 neighbor IDs as the previous comparison. No datasets or ground truth are recomputed.

## Method and provenance

The machine is Lyon's existing EC2 r6id.16xlarge with 64 virtual CPUs and 512 GiB RAM. Files reside on local NVMe. The baseline source is `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`, and its preserved executable SHA-256 is `d8388df10f7e90b1115589617c3e853b0d837a670bca03729fa5f0fbf007b3d6`. The candidate source is `37a831afe`, with executable SHA-256 `a4c90883d1fa9f6bbc887dfb1480c1b8407d23415a09003cc0a8da4670d022d6`. Every changed source file on EC2 is verified against the committed PR. Rust 1.92.0 and the usual bench profile are used. The measured executables are copied before execution, so subsequent builds cannot replace a running comparison binary.

The exact shared settings are `--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --resume --save-dir <independent working copy>`. There are no per-level minimum percentages.

PR: [Reduce shared instrumentation updates during hierarchical writes](https://github.com/chroma-core/chroma/pull/7863).

## Artifacts

Raw logs, exact commands, input/output metadata, resource measurements, source and binary hashes, test output, the patch, a source archive, and `summary.json` reside beside this report. Remote results are `/home/ubuntu/benchmark-data/worker-statistics-performance`. Independent working indexes are `/mnt/data/worker-statistics-<variant>-<repeat>`. The original fixtures remain under `/mnt/data/wikipedia-10m-scaling-snapshots-20261008`.
