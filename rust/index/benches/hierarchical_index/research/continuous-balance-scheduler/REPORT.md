# Continuous balancing scheduler

The implementation replaces fixed worker assignments and round barriers with a shared queue of leaf tasks. Workers claim the leaves that a destructive operation can remove, publish follow-up work, and continue until no queued task or active producer remains. The completed Wikipedia comparison shows no material latency improvement. The scheduler keeps workers occupied, but changes task order and increases neighbor reassignment work. PR #7864 remains a draft pending a useful latency improvement.

## Scheduling and ownership

A split owns its source and claims both replacement child IDs before publishing them. Those children remain available for posting appends, but another worker cannot split or merge them until the original split finishes neighbor reassignment. A merge claims its source and destination. If another task owns the destination, the merge releases its claims and retries after the conflict ends; its next dispatch claims both leaves together. Parent structure changes retain the existing tree lock. Posting appends retain the existing leaf/version lock ordering, including publishing the first replacement posting before making its version visible.

Successful reassignment queues its source and destinations. Splits queue their new children. Stale-row scrubbing queues a leaf that becomes undersized. Configurations with replicas perform a final stale-row discovery scan once producers become idle because a shared version change can invalidate a replica outside the explicit source leaf. This is a convergence check rather than a per-round scheduling barrier.

Each leaf receives at most eight structural attempts. A separate finite structural-work budget also covers oscillations that create fresh IDs: eight times the initial leaf count plus twice the number of split-threshold-sized groups in the current postings, plus one. Healthy visits and claim conflicts do not consume that budget. Residual overfilled and underfilled leaves, claim conflicts, capped leaves, and total-budget exhaustion are reported explicitly. A worker panic releases claims and wakes other workers so they can stop.

Queued follow-up tasks retain their causal reassignment depth. The existing depth limit bounds neighbor reassignment inside a cascade. Work beyond that limit restarts at depth zero as a new queued task, since there is no later outer round to rediscover it. Initial discovery and split-child cleanup do not overwrite the depth of an already queued causal follow-up. Scheduling nevertheless changes the distribution of depths: the queue exposes more shallow work than recursive execution, so it can perform more reassignment work even with the same depth policy.

Percentage-based navigation widths use a snapshot taken before workers start. It stays stable for that balancing call and is cleared afterward. A large rebalance can make the snapshot stale and affect recall. The measurements use no percentage-based minimum widths.

## Experiment

The current feature baseline is `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`. Its preserved executable is the merged reader benchmark executable, SHA-256 `d8388df10f7e90b1115589617c3e853b0d837a670bca03729fa5f0fbf007b3d6`. Both variants resume independent writable copies of the same 9M Wikipedia checkpoint and add exactly one million vectors. Each copy has different file inodes from the preserved fixture. The host is Lyon's EC2 r6id.16xlarge, with 64 virtual CPUs, 512 GiB RAM, and local NVMe. Measured runs execute sequentially; other agents hold off on builds and benchmarks during the reservation.

The settings are `--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --resume`. The exact commands and source/executable hashes are preserved with each run. Existing exact ground truth and source vectors are reused. The harness's overall throughput incorrectly divides all 10M vectors by a resumed run's duration; this report excludes that value.

## Final Wikipedia results

The scheduler and baseline have overlapping balance times. Three current-feature baselines average 24.62 seconds; two final scheduler runs average 24.57 seconds. This difference is smaller than the variation between repeats and does not demonstrate a speedup.

| Run | Add (s) | Balance (s) | Commit (s) | Peak memory (GiB) | Recall |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline 2 | 7.64 | 24.41 | 9.03 | 62.70 | 74.52% |
| Baseline 3 | 7.64 | 24.50 | 9.31 | 63.06 | 74.56% |
| Baseline 4 | 7.61 | 24.94 | 9.76 | 63.16 | 74.54% |
| Scheduler 1 | 7.57 | 24.13 | 9.33 | 65.29 | 74.73% |
| Scheduler 2 | 7.70 | 25.01 | 9.35 | 66.17 | 74.69% |

The scheduler changes how much work balancing performs. It requests 420.7k and 451.8k reassignments, compared with 331.2k–347.6k for baseline. It reduces scrubs to 76.2k and 81.2k, compared with 204.3k–213.2k. The final task ordering advances recently affected leaves before older work, but still exposes more shallow reassignment work than recursive execution. Reduced scheduling idle time therefore does not directly translate into lower elapsed time.

Workers spend 96.1% of available worker time inside tasks in both scheduler runs. This measurement includes waits inside tasks and is not CPU utilization. There is no matching current-baseline occupancy measurement. Each scheduler run reports one claim conflict, zero capped leaves, no total-budget exhaustion, and zero remaining oversized or undersized leaves.

Peak memory increases by approximately 2–3 GiB. Recall is slightly higher in these samples, but 100 corpus-derived queries do not establish a quality improvement. Both variants reuse the same queries and exact ground truth.

The final scheduler source is `355f85a9fecc14735d22972fa93b38c97d671b21`. Its preserved executable SHA-256 is `9b999d5ad1b80451dd40afeedbc4e70b7c1fbf66fb3e09d44a5fd108a957f9e2`. The package was cleaned and rebuilt in release mode to avoid reuse of a stale executable from the shared Cargo target. Final logs are `raw/final-1.log` and `raw/final-2.log`; baseline logs are `raw/baseline-2.log` through `raw/baseline-4.log`. Commands, process resource measurements, and hashes are adjacent to the logs.

## Exploratory variants

An exploratory implementation resets every task to depth zero. It balances in 27.87 seconds versus 24.33 seconds for baseline, requests 567.9k versus 341.0k reassignments, and reaches 74.74% versus 74.68% recall. It performs full neighbor reassignment on every split. This version is excluded from the final comparison. Its logs and executable remain preserved as `candidate-1` and `exploratory-full-npa-binary`.

A FIFO variant that retains causal depth but finishes the initial scan before queued follow-ups balances in 24.75, 26.64, and 25.00 seconds. It requests 564.3k–574.3k reassignments and peaks near 69 GiB. Those logs are `candidate-2` through `candidate-4` and are excluded from the final comparison. They help explain why task ordering matters independently of worker occupancy.

## Validation

The final code passes 85 test executions across the `hierarchical_policy_cache` and `hierarchical_posting_delta` integration targets, with 39 and 46 passing executions respectively. Shared module tests execute in both targets. The local output is preserved in `local-tests.log`.

Focused tests cover queue wakeups when an active producer publishes work, atomic source/destination claims, replacement-child claims, claim release during unwinding, deduplication, healthy-visit budgeting, bounded structural attempts, replica discovery, causal-depth retention, and retry-depth retention. Writer integration tests exercise parallel split/merge cascades with 1, 4, and 16 workers, two replicas, and exact root-reachable vector IDs. Existing posting persistence and parent-change tests also run.

Both final persisted 10M Wikipedia outputs pass an independent audit that checks every root-reachable child link and proves the valid vector IDs equal exactly `[0, 10000000)`. The audit loads the saved index with an existing separately built audit executable. Logs are `raw/audit-final-1.log` and `raw/audit-final-2.log`.

A fresh MS MARCO build inserts 1M vectors in ten 100k checkpoints with 64 add/balance workers, full-precision navigation and neighbor reassignment, beam tau 1.5, and RNG factor 4. Exact posting validation passes before commit and after reopen at every checkpoint. All checkpoints finish with zero oversized/undersized leaves, no capped leaves, and no exhausted total budget. Final observed recall is 80.78%. This is a correctness run without a paired baseline, so it supports no performance comparison. Its command and output are `raw/msmarco-validation.command` and `raw/msmarco-validation.log`.

Fresh-tree task occupancy is much lower than the mature Wikipedia replay: approximately 6% at the first MS MARCO checkpoint and 49% at the last. A fresh tree initially exposes few independently claimable leaves. The continuous queue therefore does not establish that startup parallelism is solved.

## Related work

The separate deferred-destination experiment ([PR #7866](https://github.com/chroma-core/chroma/pull/7866)) keeps the round scheduler and drains a local follow-up queue after a posting-list scan. This continuous scheduler necessarily includes destination deferral, so the two PRs overlap and should not be merged independently without reconciling that code. Their separate comparisons help distinguish deferral from changes to global scheduling and task depth.
