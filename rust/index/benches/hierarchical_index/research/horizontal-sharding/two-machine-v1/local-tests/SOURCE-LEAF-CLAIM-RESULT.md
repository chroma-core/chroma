# Source-leaf claims eliminate discarded preparation

Keep the source-leaf claim change. The fixed 100K/32-worker run commits all 70 prepared splits and discards none, compared with 252 discarded preparations in the retained baseline. Balance CPU falls from 51.85 to 30.70 CPU-seconds, a 40.8% reduction. Elapsed time rises from 14.055 to 15.449 seconds, so this run establishes work reduction, not an elapsed-time speedup. Correctness validation passes, and both task machines are API-stopped.

The earlier standard reference 100K run records 4.962 seconds of balancing and 17.0 CPU-seconds in the [campaign report](../REPORT.md), compared with the instrumented/profiled reference below at 12.965 seconds and 26.07 CPU-seconds. This discrepancy remains unexplained. The latter result must not be treated as the reference’s representative speed. The [separate 500K follow-up](SOURCE-LEAF-CLAIM-500K-RESULT.md) reuses the current binaries and preserves these 100K measurements.

## What changed and why

A split claims its source membership before taking the snapshot used for vector access, clustering and code generation. The claim is a token attached to the resident node, held through installation. It holds neither a node write lock nor a subtree reservation during preparation. Navigation can continue reading coherent routing images, and independent leaves can prepare concurrently. Competing membership mutations defer; installation obtains structural protection afterward and accepts only its own claim token.

Installation refreshes parent information after an ancestor edit while preserving the prepared membership and centroid. It retries installation with the prepared payload within existing bounds instead of reclustering solely because the parent changes. Cleanup releases only the operation's own token on early return, error or unwind. Replacement installation takes over protection before unlinking the old node, and reassignment begins after the source claim is released.

Retaining the claim exposes a progress problem in synchronous reassignment. A controlled test produces 1,024 rejected attempts in 1.72 seconds against a legitimately claimed destination. The correction retains those IDs as bounded coordinator jobs and frees the worker. The scheduler waits for the active dependency while allowing independent jobs to proceed; queued work participates in drain and save checks. No retry budget is increased. Migration export rejection also releases its unmutated region and phase admission, avoiding stranded protection when it encounters a claimed leaf.

## Matched workload and results

The candidate uses the same retained 100,000 FineWeb vectors, order, 768 dimensions, squared L2, 32 workers, 100K insertion interval, branching factor 100, split threshold 2048, merge threshold 512 and construction search settings as the baseline. Settings are compared exactly in the machine-readable summary. Both use Rust 1.92.0 with the same release, native-CPU, frame-pointer and debug flags on the same 32-physical-core r6i.16xlarge worker. The reference and previous candidate measurements are retained epoch12 runs; they are not rerun in this window. Random initialization, scheduler order and operating-system cache state are uncontrolled.

| Measured balance quantity | Unsharded reference, retained | Distributed baseline, retained | Source-leaf claims |
|---|---:|---:|---:|
| Elapsed seconds | 12.965 | 14.055 | 15.449 |
| Process CPU-seconds | 26.070 | 51.850 | 30.700 |
| Clustering CPU-seconds | 21.928 | 37.626 | 18.880 |
| CPU-seconds outside clustering | 4.142 | 14.224 | 11.820 |
| Clustering calls | 69 | 324 | 70 |
| Committed leaf splits | 69 | 72 | 70 |
| Discarded prepared splits | Not separately counted; 69 calls match 69 splits | 252 | 0 |
| Input rows processed by clustering | 617,525 | 2,099,736 | 626,546 |
| Clustering distance calculations | 63,459,422 | 125,380,052 | 61,274,655 |
| Clustering iterations | 1,844 | 7,298 | 1,845 |
| Final leaves | 70 | 72 | 71 |

The common clustering kernel consumes 18.746 fewer CPU-seconds, accounting arithmetically for 88.6% of the observed 21.15 CPU-second reduction. Work outside clustering falls by 2.404 CPU-seconds. Different construction decisions prevent assigning the entire before/after difference causally to one mechanism, but zero discarded preparations directly confirms that the targeted waste is removed in this run. The candidate's useful clustering work is close to the reference, while its non-clustering CPU remains 7.678 seconds higher.

Elapsed time remains adverse evidence. It is 9.9% higher than the previous candidate and 19.2% above the retained reference. The longest observed candidate job lasts 7.988 seconds, versus 2.496 before; the reference's first single-worker round lasts 7.847 seconds. The aggregate job counter does not identify which candidate job is longest, so it is not claimed to be the initial split. A longer serial operation and greater contention are plausible contributors; these measurements do not isolate their contributions to elapsed time. Peak active jobs rises from 21 to 32, but blocked jobs count as active and do not establish effective CPU concurrency.

## Remaining costs and next decision

The claim change shifts the remaining problem toward transfer coordination and reservation contention. The following are phase counter differences, not whole-run totals.

| Quantity | Previous candidate | Source-leaf claims |
|---|---:|---:|
| Posting rows transferred | 119,846 | 23,976 |
| Source compactions | 3,639 | 4,159 |
| Source entries scanned | 19,981,894 | 6,261,322 |
| Explicit ID/code copy bytes | 2,397,827,280 | 751,358,640 |
| Rejected reassignment groups | 395 | 3,209 |
| Reservation requests | 7,692 | 11,291 |
| Active-conflict records examined | 74,322 | 441,616 |
| Ancestry steps | 2,178,314 | 14,896,293 |
| Sum of reservation-mutex acquisition waits, seconds | 2.568 | 40.810 |
| Sum of queued reservation waits, seconds | 33.403 | 78.461 |
| Transfer-driver inclusive CPU-seconds | 9.229 | 11.952 |
| Source-compaction CPU-seconds | 0.657 | 0.182 |
| Navigation inclusive CPU-seconds | 2.970 | 3.184 |

Waiting sums overlap across workers and must not be added to balance elapsed time. Operation CPU scopes also overlap; the transfer driver includes nested work and must not be added to its endpoint scopes. Reservation history scanning remains fixed: these are examinations of active conflicts, not a return of completed-history scanning. Both owners end balance with zero active grants and zero queued reservations.

Fewer rows move, yet more source compactions and reservation requests occur. This makes a reference-style same-owner bulk-transfer path the leading next investigation: combine destination groups, compact once and avoid unnecessary local protocol/reservation churn while preserving claims, retries and unique membership. Separate cheap rejection of a known claimed dependency from structural reservation work, treating any early check as advisory until protected revalidation. Use the existing counters to distinguish reduced coordination from reduced useful transfers. Source compaction alone is only 0.182 CPU-seconds here, so optimizing its copy loop alone is unlikely to address the main remaining cost.

Direct local navigation remains second. It consumes 3.184 CPU-seconds, copies 0.722 GB of query coordinates into requests and clones 0.572 GB of reply payloads; reassignment separately copies 0.722 GB of queries. Reassess synchronization and critical-path variability after a focused transfer change. No additional implementation or benchmark starts as part of this completed request.

## Correctness, scope and limitations

Four focused tests pass, including a real split paused during vector access, readable navigation, independent leaf splitting, ancestor reparenting, transfer/merge/append conflicts, migration rejection cleanup, claim release on failure/unwind and deferred cross-owner dependencies. Holding a claim for more than 5.1 seconds causes one deferred reassignment attempt instead of a retry storm; the retained work then drains with exact membership and valid codes. The applicable suite passes 172 tests. The known separate local-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership` remains explicitly excluded, so the unfiltered wrapper is not claimed green. All three native executables and both required Linux executables build successfully.

The benchmark's independent fresh reader validates exactly 100,000 memberships, every full vector's bits, all one-bit posting codes and all saved four-bit centroid codes. All construction takes place on the initial owner; redistribution afterward leaves 54,990 and 45,010 vectors on the two physical machines. Balance sends only 149 framed RPC bytes. This is an initial-owner construction measurement followed by physical handoff/save validation, not distributed construction performance. The resulting tree has one internal node and 71 leaves. It does not test deep-tree pruning or cache-capacity crossing. The 16 training-vector queries remain smoke tests, not quality-parity or held-out recall evidence; fewer transferred rows do not establish equivalent clustering quality.

The full timed balance is covered by acknowledged profiler enable/disable gates, with zero lost samples. Instruction-location samples are 41.56% distance, 34.59% clustering and 2.96% one-bit quantization. They measure sampled userspace CPU, not blocked time. Counter/clock overhead remains uncalibrated and nested timers must not be summed. Peak reported resident memory is 1.466 GB versus 1.206 GB before; the counter does not attribute that increase to claims, queued payloads or allocator state. Gross construction totals include profiler-gate/reporting delays and are not used for the latency comparison.

## Closure and retained evidence

The fresh build takes 71.04 seconds. The one candidate run, full-phase profile, fresh-reader validation and process cleanup complete successfully. Both task instances are API-stopped, preserving disks and evidence. The window adds a conservative $0.8633927565002442 compute bound once, with no new storage. Covered task compute totals $35.622979529747426983; identified recurring storage remains $19.52/month. This is not a whole-account bill.

The [summary](../operations/window-epoch13/summary.json), [candidate result](../operations/window-epoch13/100k-distributed/result.json), [reader validation](../operations/window-epoch13/100k-distributed/durable-validation.json), [profile](../operations/window-epoch13/100k-distributed/perf-report.txt), [profile coverage](../operations/window-epoch13/100k-distributed/profile-result.json), [source comparison](epoch13-source-closure-comparison.json), [native validation and retained red evidence](SOURCE-LEAF-CLAIM-VALIDATION.json), [independent review](SOURCE-LEAF-CLAIM-REVIEW.md), [Linux build](../operations/window-epoch13/linux-build.json), [API closure](../operations/window-epoch13/closed.json) and [previous matched comparison](FULL-BALANCE-OBSERVABILITY-RESULT.md) retain provenance. Failed local test assertions and compilation attempts remain in the validation receipt; no benchmark attempt failed in this window.
