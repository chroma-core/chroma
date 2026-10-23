# Fresh-index implementation experiments

Two independent experiments target the first large posting-list split and the amount of data accumulated before startup balancing. Both preserve the existing serial/100k defaults so later comparisons can isolate each change. Benchmarking is now authorized. The comparison uses the pinned common base and both independent PR heads; results are recorded in REPORT.md after measurements finish.

## Parallel first-root distance assignment

[PR #7867](https://github.com/chroma-core/chroma/pull/7867) evaluates independent point distances in the first root split on a bounded worker pool. A split chooses two group centers, called centroids, assigns each vector to one, and recomputes the centroids repeatedly. That first posting list exposes no independent subtree jobs yet, so its point-distance work can use otherwise idle balance workers.

Distance totals and centroid accumulation retain serial input order. The seeded comparison verifies exact centroid bits and group membership/order against the serial algorithm. Seed selection retains the existing random behavior in normal runs. With identical seeds, the experiment preserves the arithmetic; ordinary unseeded build runs remain subject to the existing seed variability.

The pool’s size is the smaller of the experimental cap and the requested balancing worker count. Only one fresh-root split can claim it. The pool drops before child splitting and neighbor reassignment, and reopened writers never claim it. Subsequent leaf splits retain serial clustering within ordinary parallel balancing jobs. Failed pool creation reports the error and falls back to serial distances.

Enable it with `--initial-split-threads 32` for the Wikipedia settings or `--initial-split-threads 64` for MS MARCO, retaining the existing balance-thread budgets. The default cap is one. The existing shared serial split entry point remains available. A new caller-owned pool entry point is added to the shared split utility; Rayon becomes a regular dependency for that helper.

Validation passes one core matrix test over L2/IP/cosine, 1/2/4 workers, four RNG seeds, empty/singleton/small/coincident/mixed inputs, exact output bits, finite centroids, complete ID/version coverage, and the existing minimum partition constraint. The two hierarchical writer/persistence/navigation harnesses pass 65 executions, including worker-budget, one-time claim, fresh-build reachability, and reopen cases. The benchmark target compiles; formatting and diff checks pass.

The independent branch is `codex/parallel-initial-clustering`, commit `20788c3fee5b66745ea4af68246086cd588a301a`, in `/private/tmp/chroma-parallel-initial-clustering`.

## Earlier and adaptive startup balancing

[PR #7868](https://github.com/chroma-core/chroma/pull/7868) changes only the profiling harness’s startup add intervals. The existing behavior already balances every 100k additions through the first 1M indexed vectors when checkpoint batches are larger. The default fixed schedule retains those exact boundaries, including small checkpoints and the transition past the startup prefix.

Fixed comparison intervals are 5k, 10k, 50k, and 100k using `--startup-balance-policy fixed --startup-balance-size N`. Adaptive mode uses `--startup-balance-policy adaptive`. It chooses `clamp(leaf_count × split_threshold, minimum, maximum)` after every completed balance, with a 5k minimum and 100k maximum by default. The minimum and maximum are available as `--startup-balance-min-size` and `--startup-balance-size`.

A small initial interval limits the amount of data handled by the first serial root split. As the tree gains leaves, a larger interval reduces repeated setup work. Under uniform routing, adding roughly one split-threshold of rows per current leaf tends to expose independent leaf splitting work. The explicit bounds limit both repeated tiny passes and a very large initial cluster. This is a testable heuristic rather than a measured optimal schedule; skewed routing and neighbor reassignment can favor other intervals.

Every subbatch finishes buffered routing, joins workers, and flushes postings before balancing. Its next add receives refreshed navigation/policy state. Persisted checkpoint/commit boundaries remain fixed. Adaptive scheduling applies to fresh sessions; resume uses the selected fixed interval during any remaining startup prefix. Beyond the first 1M indexed vectors, the harness retains its whole-checkpoint add behavior.

The focused harness passes 32 executions, including six new cases covering exact default boundaries, all four fixed intervals, adaptive growth/bounds/overflow/empty input, resume and established-tree behavior, invalid configuration rejection, and full ID reachability through real joined add/balance operations. The benchmark target compiles; formatting and diff checks pass.

The independent branch is `codex/startup-balance-cadence`, commit `ef8fb22f74f39c9cf1051d080be7398850f1f23b`, in `/private/tmp/chroma-startup-balance-cadence`.

## Comparison protocol

The authorized comparison uses common feature baseline `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`. Scheduler/reassignment changes are excluded equally from all three variants so they cannot confound this comparison. Use fresh isolated output directories, identical source prefixes, cache state, dataset/metric/query settings, thread budgets, and persisted checkpoint sizes. Retain Wikipedia’s 32-worker settings and MS MARCO’s 64-worker settings unless the comparison explicitly varies CPU budget. The leaf/read ground-truth files continue to use the same source IDs and checkpoint boundaries.

The initial matrix should compare baseline serial/100k, parallel first-root clustering at the existing cadence, fixed 5k/10k/50k/100k startup intervals, and the adaptive schedule. Report several sequential runs per dataset and compare the distributions, not only a single favorable build. Include total add, total balance, total commit/flush, total build time, peak memory, recall, and first-checkpoint details. Pool creation/destruction is included in clustering time, and adaptive leaf-count inspection is included in add/build time. Moving work between phase timers is not a performance gain.

Select the smallest total-build interval that also meets recall and memory requirements, then narrow neighboring interval values if the first measurements show a clear minimum. The adaptive rule provides one candidate schedule without asserting an optimal frequency. Increasing a scheduling interval changes which data prefixes train centroids, so recall and tree-shape comparisons remain necessary.

Combining the two experiments may help, but their benefits overlap: earlier balancing shrinks the first split and leaves less work for its parallel distance passes. Measure the combination after the separate comparisons. A third option is training initial leaf centers on a representative sample and assigning the full first batch to them in parallel; it could avoid repeated large-data clustering passes, but changes the clustering method and recall behavior substantially. That option is assessed here rather than implemented without evidence.

Both PRs target `hierarchical-spann`, are authored by `lyon-chroma`, and are attached to the parent task. Their common base is `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`. Raw test/build logs and PR descriptions are saved in the adjacent subdirectories. GitHub CI is pending at publication.

## Completed benchmark comparison

The authorized 26-run comparison and all 26 separate final posting audits complete. The full results, commands, hashes, timing distributions, memory, recall, and limitations are in [REPORT.md](REPORT.md). Both PR descriptions include the results. The adaptive schedule reduces median full build time by 15.7% on Wikipedia and 22.1% on MS MARCO. Parallel first-root distances produce no median gain on Wikipedia and a 3.3% median gain on MS MARCO. The shared EC2 lock is released; saved indexes, source fixtures, and binaries remain available.
