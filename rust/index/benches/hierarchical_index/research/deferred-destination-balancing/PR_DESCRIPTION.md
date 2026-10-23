When a split or merge moves vectors into another leaf, that destination may also need splitting or merging. Entering that work immediately interrupts the current posting-list scan and repeatedly cleans destinations between individual appends.

This change records destination work on a worker-owned stack. The worker finishes its current split or merge, then drains pending destinations. Newly generated cascades run before older pending work. Each task retains its original depth because depth determines whether a split performs neighbor reassignment. Duplicate requests share a pending task only when both leaf and depth match. Posting registration and version publication retain their existing order; destination existence and size are checked when its task runs. The existing round scheduler and eight-round convergence warning remain in place.

On the existing r6id.16xlarge, resuming the same Wikipedia checkpoint from 9M to 10M vectors reduces median balancing time by **9.5%**. Every final run completes in one balancing round.

| Measurement | Baseline median, 4 runs | Deferred median, 3 runs |
| --- | ---: | ---: |
| Balance | 24.46 s | 22.13 s |
| Add | 7.64 s | 7.68 s |
| Commit preparation + flush | 9.31 s | 9.54 s |
| All printed checkpoint phases | 55.61 s | 53.58 s |
| Scrub calls | 212,800 | 34,500 |
| Reassignment calls | 343,000 | 345,800 |
| Peak resident memory | 63.09 GiB | 63.76 GiB |
| Recall@100 | 74.545% | 74.66% |

Baseline balancing samples are 24.17, 24.41, 24.50, and 24.94 seconds; final samples are 21.89, 22.26, and 22.13 seconds. Fresh baselines are shared with #7864 and use the unmodified current feature branch (`22eb704e71`). All builds and measured runs are serialized on the host. Every replay starts from an independent copy of the immutable 9M checkpoint. The final candidate is `4747e1207`; source and executable hashes are recorded. These measurements cover one saved checkpoint. Parallel balancing produces different valid trees, so the small recall differences do not establish a recall improvement.

The comparison uses full-precision write navigation and neighbor reassignment, 32 add and balance threads, a 1M checkpoint, write tau 0.5 with level taus `_,_,0.30`, RNG factor 0, a 1 GiB cache, and the same 100 queries with exact precomputed ground truth. Read evaluation uses tau 1.5 and rerank factor 1. There are no per-level minimum percentages.

Validation passes all 63 test executions in `hierarchical_policy_cache` and `hierarchical_posting_delta`. New tests cover deferred publication, deduplication without losing causal depth, the depth-four reassignment cutoff, tasks being re-enqueued, vanished destinations, and complete draining. Existing tests cover parallel splits, overlapping merges, and persistence. Separate audits pass on all three final saved outputs: exactly 10,000,000 current-version IDs remain reachable from the root, and all child links resolve.

This is the focused deferral experiment within the existing scheduler. The continuous scheduler in #7864 also defers destination work; the two implementations should be reconciled if both move forward. A FIFO deferral alternative increases reassignment work substantially, so this implementation uses a depth-first pending stack.

Raw evidence and the full report are in Lyon's shared workspace at `deferred-destination-balancing/REPORT.md`; EC2 artifacts are under `/home/ubuntu/benchmark-data/deferred-destination-balancing`. The timed baseline and candidate executable SHA-256 values are `d8388df10f7e90b1115589617c3e853b0d837a670bca03729fa5f0fbf007b3d6` and `5e20c3e529b273f5b18b65ac0402693368ff4e06d9a4c344ec618791cd4accc2`.
