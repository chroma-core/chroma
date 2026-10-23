## Description of changes

Balancing keeps the vector lists at the bottom of the index tree within their size limits by splitting large lists and merging small ones. Workers continuously claim eligible lists and publish follow-up work as the tree changes. Newly affected lists take priority over the initial scan so a cascade of related changes can advance promptly.

The nodes storing these lists are leaves. A task claims each leaf it can remove. Splits claim their replacement children until neighbor reassignment finishes; merges claim both their source and destination. Conflicting merges release their claims and retry after the owner finishes. Ordinary posting appends retain their existing leaf/version locks, and parent restructuring retains its tree lock. Completion requires an empty queue and no active producer. Replica configurations also discover indirect stale postings before finishing.

The scheduler retains the neighbor-reassignment depth policy in queued tasks and bounds oscillation with eight structural attempts per leaf plus a finite total structural-work budget. It reports residual imbalance and budget exhaustion explicitly. Percentage-based navigation widths use one snapshot per balance call; a large rebalance can make those widths stale and affect recall.

## Test plan

- [x] 85 local test executions pass across two integration targets, covering policy, scheduling, posting persistence, and the writer.
- [x] Tests cover wakeups, deduplication, opposing merge claims, replacement-child claims, unwinding, work budgets, replica discovery, causal depth, and retry depth.
- [x] Split/merge cascades preserve exact root-reachable IDs with 1, 4, and 16 workers and two replicas.
- [x] Two final committed scheduler replays on the EC2 Wikipedia 9M checkpoint, adding the final 1M vectors each.
- [x] Both persisted 10M Wikipedia outputs pass an independent audit proving exactly IDs `[0, 10000000)` are reachable and all child links resolve.
- [x] A fresh 1M MS MARCO build passes exact posting validation before commit and after reopen at every 100k checkpoint; no residual imbalance or work caps remain. This run has no paired baseline.

The PR remains a draft because the current measurements show no material latency improvement. Three fresh current-feature baselines balance in 24.41, 24.50, and 24.94 seconds. The final scheduler balances in 24.13 and 25.01 seconds. Mean balance time is 24.62 seconds for baseline and 24.57 seconds for the scheduler; the ranges overlap. All runs start from independent writable copies of the same 9M checkpoint on the same EC2 r6id.16xlarge, with sequential execution.

The scheduler keeps workers inside tasks for 96.1% of available worker time, with one claim conflict and no capped or residual oversized/undersized leaves in either run. It performs approximately 421–452k reassignments versus 331–348k for baseline, while reducing scrubs from 204–213k to 76–81k. Peak memory rises from 62.70–63.16 GiB to 65.29–66.17 GiB. Observed recall is 74.69–74.73% versus 74.52–74.56%; 100 queries do not establish a statistically meaningful quality gain. Task order changes the amount of neighbor reassignment, so this is not a comparison with identical computational work.

The benchmark uses Wikipedia, 1024 dimensions, full-precision write navigation and neighbor reassignment, 32 add/balance workers, L2 distance, a 1 GiB block cache, and the existing exact ground truth. The baseline is `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`; the candidate is `355f85a9fecc14735d22972fa93b38c97d671b21`. The final executable SHA-256 is `9b999d5ad1b80451dd40afeedbc4e70b7c1fbf66fb3e09d44a5fd108a957f9e2`.

## Migration plan

This changes the prototype hierarchical writer's in-memory scheduler. Persisted index formats and benchmark arguments are unchanged. The profiling harness reports completed leaf tasks instead of outer-round iterations.

## Observability plan

Each balance call reports completed tasks, remaining oversized/undersized leaves, worker task time versus available worker time, claim conflicts, per-leaf caps, and total-budget exhaustion. Task occupancy includes time spent waiting inside a task and is not CPU utilization. The fresh MS MARCO correctness run has approximately 6% occupancy at its first checkpoint and 49% at its last; a small tree exposes few independently claimable leaves, so startup parallelism remains a limitation.

## Related work

The separately developed deferred-destination experiment ([PR #7866](https://github.com/chroma-core/chroma/pull/7866)) retains the round scheduler and isolates the effect of replacing recursive destination balancing with a local queue. This PR necessarily includes destination deferral, so the overlapping code must be reconciled before both changes land.
