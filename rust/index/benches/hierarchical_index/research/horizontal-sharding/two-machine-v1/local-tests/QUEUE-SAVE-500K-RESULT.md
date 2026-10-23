# Removing the queue lock convoy and duplicate local saving at 500K

Keep both changes. The controlled test and the benchmark agree that holding the operation mutex while waiting for the scheduler queue causes substantial transfer delays. Reversing that lock order nearly eliminates measured admission/completion waiting and lets later balancing use more cores. Reusing verified local saved objects eliminates their duplicate durable writes. Exact membership and all saved codes validate.

## Comparison

| Measurement | Reference epoch14 | Candidate epoch17 | Candidate epoch18 |
| --- | ---: | ---: | ---: |
| Insertion wall seconds | 0.699 | 1.037 | 1.069 |
| Balancing wall seconds | 9.054 | 21.067 | 13.422 |
| Balancing CPU seconds | 52.700 | 57.130 | 66.760 |
| Clustering CPU seconds | 35.296 | 32.360 | 38.188 |
| Other balancing CPU seconds | 17.404 | 24.770 | 28.572 |
| Save wall seconds | 2.840 | 7.413 | 6.470 |
| Construction plus save wall seconds | 12.601 | 30.986 | 22.388 |

The subsequent four intervals after the first 100K vectors take **2.067 seconds**, down from **9.067 seconds**. The reference takes **2.314 seconds**. Candidate tail CPU increases from 34.66 to 41.90 seconds, versus 33.44 reference, so average occupied cores rise from 3.82 to 20.27. This is evidence that removing waiting helps elapsed time, not evidence of equal work or overall construction parity. The first interval remains 11.355 seconds versus 12.000 previously and 6.740 reference. Its clustering CPU is 17.738 versus 16.956 previously and 15.275 reference. Independent random construction and changed concurrency prevent assigning that first-interval gap to one cause.

The candidate additionally spends 1.277 seconds in placement checks and final physical redistribution; the reference has no ownership handoff. Construction plus saving includes another approximately 0.150 seconds of boundary reporting and setup beyond the recorded add, balance and placement phases. Report these separately from the algorithmic comparison.

## Transfer mechanism

The operation mutex protects phase admission and completion bookkeeping. Three enqueue handlers and phase close acquire the scheduler queue first, then that operation mutex, and hold both through their atomic decision. Other operation-mutex holders never acquire the queue. The controlled regression holds the queue and confirms unrelated admission/completion remain possible; both enqueue-winning and close-winning orders preserve accepted work and drain checks. Waiting requests revalidate the run and epoch after acquiring the locks. No construction or transfer ordering rule changes.

Across overlapping worker operations, admission wall time falls from **106.931 to 0.018 seconds**, and terminalization from **81.301 to 0.006 seconds**. Outer local transfer wall time falls from **190.866 to 2.974 seconds**, while its thread CPU is 2.870 seconds. These sums exceed or overlap phase time and must not be added to elapsed balancing. Cleanup includes terminalization. The exact test plus the specific scope reductions strongly support removing this convoy as the source of the later balancing improvement.

More concurrency creates more repeated work in this observation. Reassignment rounds rise from 4,737 to 6,388, rejected destination groups from 7,353 to 17,420, local transfer batches from 4,428 to 5,755, and copied reassignment query bytes from 1.446GB to 1.874GB. Clustering comparisons also increase from 91.46M to 94.10M. All 344 prepared leaf splits commit; none is discarded. The queue records 2,036,372 withheld producer-dependency visits, down from 4,149,664, but these count repeated visits rather than unique jobs or waiting time. The next construction candidate is reference-style combination of compatible pending reassignment work, preserving exclusions, producer readiness, current membership and replay identities.

## Saving mechanism

The local owner writes directly into the combined immutable namespace. The coordinator verifies its exact manifest identity, checksum, referenced payloads and root coverage, then reuses those references. Remote owners keep their staging and bounded copy path. File/directory synchronization, failure behavior, generation validation and publication-last ordering remain intact. Local reuse still reads and checks saved data; it does not remove all local save I/O.

This run reuses **182 objects / 33,809,427 bytes** in **0.045 seconds** and records **zero local copied objects or bytes**. Coordinator file and directory synchronization counts each fall from 359 to 172; coordinator immutable supplied bytes fall from 64.16MB to 30.30MB. The two-leaf fixture isolates the work reduction exactly: the local owner still performs five immutable writes, while the coordinator performs three instead of eight. Replay, missing/corrupt objects, a mixed local/remote save and previous-generation readability all pass.

Save elapsed falls **12.7%**, from 7.413 to 6.470 seconds. Owner materialization increases from 2.582 to 3.157 seconds, reflecting changed objects and storage timing; remote copying remains about 3.060 seconds. Thus the full 1.582-second removed copy interval is not an observed end-to-end saving reduction. Reference saving remains 2.840 seconds under a different persistence contract: ordinary writes without matching fsync barriers, while writing full vectors rather than referencing retained input. Remote representation overhead, sequential owner staging and per-leaf durable objects remain potential future costs.

## Workload and validation

The fixed workload uses the same retained 500K vectors and order, 768-dimensional L2 distance, 32 workers, five 100K intervals, split2048/merge512/branch100, full-precision construction, four-bit query centroids and one-bit posting codes. Two machines each have 32 physical cores. The 1.536GB vector working set fits the 12GiB cache; cache-capacity crossing is not tested. All timed construction occurs on the initial owner, followed by final physical redistribution to 263,893/236,107 vectors. The final root/inner/leaf shape is 1/6/344. This does not measure distributed construction throughput.

The fresh reader verifies every membership exactly once, every posting and centroid code and the full-vector digest without writer memory. The sixteen training queries are smoke checks, not held-out recall evidence. Peak process RSS is 2,869,874,688 bytes. The [native validation receipt](QUEUE-SAVE-VALIDATION.json) records 197 passing applicable tests and three passing CLI builds; one known unrelated reference-writer test remains excluded. The expected red convoy test is retained. [Independent review](EPOCH18-QUEUE-SAVE-REVIEW.md) binds the seven changed source files and checks lock ordering, publication guarantees and measurement meaning.

## Closure and next decision

Both approved instances are API-confirmed stopped. Worker supervisor cleanup is confirmed. The peer exit receipt was not collected, and a bounded late read timed out after shutdown; peer process cleanup is therefore unconfirmed. The known launcher PermissionError remains recorded. Confirmed AWS shutdown is separate from a clean launcher/process exit; no restart or retry was used to erase this uncertainty.

This closed window adds **$0.7944031401062012** compute upper bound and no new storage. Raw results, runtime/source identities, coarse profiles and closure are in `../operations/window-epoch18/`. The frozen source SHA256 is `b4ce293a8729fcd8334a0729037717fa61864cf6e3f38b96281699d639a32409`. Keep the two fixes, retain the increased CPU/retry counts and inspect compatible pending-work combination next. Use fixed-queue correctness evidence and work counts before another bounded benchmark. Do not infer linear scaling or overall reference parity from the faster tail.
