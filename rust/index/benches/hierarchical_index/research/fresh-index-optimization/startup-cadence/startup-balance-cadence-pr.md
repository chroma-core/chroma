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

## Performance status

Benchmarks are pending. No EC2 work or timing runs are performed for this experiment. Compare all add, balance, commit, and build time together, with identical dataset prefixes, thread counts, checkpoints, query settings, and repeated runs. Recall and memory remain required comparisons; moving work between timing categories is not an improvement.

## Code reference

The experiment is confined to the profiling harness and a small schedule helper (`rust/index/benches/startup_balancing.rs`), with a focused integration test. It is independent of the parallel first-root clustering PR and can be combined with that variant after comparing them separately.
