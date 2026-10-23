# Reassignment experiments at 40M vectors

## Findings

Deferring destination maintenance reduces balancing time by 36.5% in the 39M→40M Wikipedia replay. Grouped destination writes reduce it by 37.3%, which adds only 0.35 seconds beyond deferral. The evidence favors evaluating deferred maintenance independently before adding grouped publication complexity.

All values below are arithmetic means across three interleaved repetitions per variant. The full build wall time includes posting validation, memory cleanup, and tree diagnostics, and excludes recall and the separate persisted-index audit. The add, balance, and commit sum is also shown to separate writer work from diagnostics.

| Metric | Current writer with instrumentation | Deferred maintenance | Deferred maintenance and grouped writes |
| --- | ---: | ---: | ---: |
| Add | 13.087s | 13.416s | 13.268s |
| Balance | 41.935s | 26.630s | 26.277s |
| Commit, fork and flush | 21.456s | 21.760s | 21.741s |
| Add, balance, and commit | 76.478s | 61.806s | 61.286s |
| Full build wall time | 186.184s | 169.716s | 168.823s |
| Recall@100, tau 2 and rerank 8 | 94.363% | 94.373% | 94.360% |
| Warm query latency, range across runs | 30.0–31.0ms | 30.0–32.4ms | 30.0–32.3ms |

Deferral reduces repeated maintenance work. The current writer runs eight outer balancing iterations in all three trials; both candidates finish in one. Posting cleanup calls fall from approximately 554K to 75K with deferral, a reduction of about 86.5%. Reassignment attempts fall from approximately 564K to 510K. These counters support the interpretation that deferral avoids repeated work, but they do not establish that subsequent writes have identical maintenance costs.

The uninstrumented control averages 41.940s balancing and 185.147s full build wall time. Its balancing time matches the instrumented control within the observed variation. The control indicates that the added timers do not explain the measured balancing improvement.

## Validation and limits

All twelve persisted-index audits find exactly 40,000,000 reachable current-version vector IDs covering the expected prefix. The source 39M snapshot's complete before-and-after file manifest matches. The saved 39M and final 40M checkpoints are preserved.

Recall uses the same 100 exact-prefix queries from the existing 1M-through-40M Wikipedia ground-truth file. This is a regression screen, not sufficient evidence of recall equivalence within a small margin. A larger paired recall check and a subsequent write checkpoint remain necessary before recommending a merge. Root navigation remains enabled in both candidates; direct destination assignment is not tested here.

The first attempted raw trial finishes its build but skips recall because the default shared ground-truth file lacks the 40M boundary. Its results remain in `cp-40` and are excluded from the comparison. The twelve successful trials use an experiment-local binary ground-truth cache exported from the existing exact 40M source. Shared ground-truth files are unchanged.

## Reproduction and evidence

The run uses the same four pinned binaries as the 9M→10M comparison. `variants.json` records their commits, and `source-archives.sha256` records the source archives. The experiment compares an uninstrumented control, an instrumented current writer, deferred destination balancing, and deferral with bounded grouped destination writes. The initial feature base is `ba8a8c764643582eac295cd5df004a15ee46b65d`.

The machine is the lyon `r6id.16xlarge` instance with 64 vCPUs and 512 GiB nominal memory. Every trial uses 32 add threads, 32 balancing threads, full-precision write navigation and neighbor reassignment, one replica, a 1 GiB configured cache, and a 1M checkpoint size. Exact command lines, full CLI logs, external process time, checkpoint metadata, and audits are saved under `cp-40-exact-gt`. Each variant starts from its own copy of the saved 39M snapshot. Validated temporary copies are deleted after each trial.

Remote evidence is under `/home/ubuntu/benchmark-data/reassignment-experiments-20261012/cp-40-exact-gt`. A local copy is beside this report. The run finishes successfully with exit status zero and all twelve progress entries recorded.
