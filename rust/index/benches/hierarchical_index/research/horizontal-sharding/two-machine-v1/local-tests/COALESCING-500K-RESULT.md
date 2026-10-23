# Combining compatible pending reassignments at 500K

Keep the bounded combination of compatible pending work. A fixed-tree test proves that it eliminates repeated navigation without changing membership or posting codes, and the 500K run validates every saved membership and code. The measured workload performs fewer reassignment rounds and copies fewer query bytes. The full elapsed reduction cannot be attributed to this change: most of it occurs in the variable first interval, while CPU outside clustering increases.

## Comparison

| Measurement | Reference epoch14 | Candidate epoch18 | Candidate epoch19 |
| --- | ---: | ---: | ---: |
| Insertion wall seconds | 0.699 | 1.069 | 1.251 |
| Balancing wall seconds | 9.054 | 13.422 | 9.174 |
| Balancing CPU seconds | 52.700 | 66.760 | 63.520 |
| Clustering CPU seconds | 35.296 | 38.188 | 33.575 |
| Other balancing CPU seconds | 17.404 | 28.572 | 29.945 |
| Save wall seconds | 2.840 | 6.470 | 6.477 |
| Construction plus save wall seconds | 12.601 | 22.388 | 18.317 |

Construction plus saving is 18.2% lower in this observation. Balancing is 31.7% lower, but the first interval alone changes from 11.355 to 7.053 seconds; the reference takes 6.740 seconds. First-interval clustering CPU changes from 17.738 to 14.638 seconds. Across all intervals, clustering comparisons change from 94.10M to 79.77M, near the reference's 79.82M. Independent tree construction and uncontrolled schedules materially change useful work, so this is not an isolated 31.7% speedup from combining requests.

The subsequent four balancing intervals take **2.121 seconds**, versus **2.067 previously** and **2.314 reference**. Their CPU increases from 41.90 to 42.64 seconds, and CPU outside clustering increases from 21.45 to 23.70 seconds. Average occupied cores remain about 20.1. Insertion also increases from 1.069 to 1.251 seconds. These adverse measurements remain part of the result. No repeat or size sweep was used to select a more favorable outcome.

The candidate spends another 1.270 seconds on placement checks and final physical redistribution. Reference construction has no ownership handoff. Saving remains 6.477 seconds versus 6.470 previously; no saving behavior changes in this epoch. The retained reference's 2.840-second save has a different durability and full-vector persistence contract.

## Mechanism and work counts

The scheduler combines only currently ready, undispatched reassignment requests that have the same complete source identity, run, epoch and exclusion policy. It never crosses a same-source balancing or neighbor decision, absorbs blocked or active work, or exceeds 32,768 unique vector IDs. Execution revalidates current membership and routing. Original request hashes remain immutable for duplicate-request checks; a private execution identity represents the combined request. Newly arriving requests remain distinct from active execution. This follows the reference's grouping intent while preserving distributed request semantics.

The controlled fixture executes three compatible requests separately and as one combined request. Actual query rows fall from five to three and reassignment rounds from three to one, with identical final memberships and posting codes. This establishes the mechanism on fixed work, not its end-to-end speedup.

| Work count | Epoch18 | Epoch19 |
| --- | ---: | ---: |
| Reassignment rounds | 6,388 | 5,838 |
| Reassignment query rows | 610,481 | 595,083 |
| Copied reassignment query bytes | 1,874,334,720 | 1,826,989,056 |
| Rejected destination groups | 17,420 | 18,098 |
| Local transfer batches | 5,755 | 5,510 |
| Local destination groups | 44,291 | 46,715 |
| Source membership probes | 7,286,632 | 6,741,908 |
| Source compactions | 4,368 | 4,173 |
| Prepared / committed splits | 344 / 344 | 358 / 358 |
| Discarded preparations | 0 | 0 |

The queue combines 713 extra requests into 2,054 dispatched batches and removes 37,295 duplicate pending IDs between requests. Most duplicate IDs, 35,666, occur in the first interval. These queue counts are not guaranteed execution rows saved: later membership checks, retry schedules and tree differences affect actual work. Actual rounds fall 8.6% and copied query bytes fall 2.5%. Rejections and destination groups increase. Combining requests performs bounded union work while holding the queue mutex; its residual CPU tradeoff is unresolved.

Transfer admission and completion remain short after the epoch18 lock correction: aggregate worker wall times are 0.0177 and 0.0055 seconds. Outer local transfer time is 3.020 worker-wall seconds and 2.917 thread-CPU seconds. These overlapping scopes cannot be added to phase elapsed time. The lock convoy has not returned in this observation.

## Workload and correctness

The workload uses the retained 500K vectors in the same order, 768-dimensional L2 distance, 32 workers, five 100K insertion intervals, split2048/merge512/branch100, full-precision construction, four-bit query centroids and one-bit posting codes. The 1.536GB full-vector input fits the 12GiB cache. All timed construction occurs on the initial owner; final physical redistribution leaves **261,946 / 238,054 vectors**, or at least 47.61% on either machine. This is not a distributed construction-throughput measurement. The final tree has one root, five inner nodes and 358 leaves, with root depth zero and maximum depth two. Peak RSS is 2,913,574,912 bytes.

A fresh reader verifies all memberships exactly once, every posting and centroid code, and the full-vector digest without writer objects. Its sixteen reused training queries remain smoke checks rather than held-out quality evidence. [Native validation](COALESCING-VALIDATION.json) records 203 passing applicable tests, the same known unrelated exclusion and three successful CLI builds. [Independent review](EPOCH19-COALESCING-REVIEW.md) binds all five changed source files and checks request identity, dependencies, barriers, bounds and counter interpretation.

## Closure and next experiment

Both approved instances are API-confirmed stopped. Both matching supervisor receipts confirm process cleanup: the worker exits successfully and the peer stops on the operator's request. The launcher still reports the known peer-stop permission error; that failure is retained separately from the successful independent cleanup and AWS shutdown. This window adds **$1.0451570373535155** compute upper bound and no storage commitment.

The next candidate reduces saved-object transport expansion. Remote copying takes 2.936 seconds for 30,556,667 raw bytes in this run; local reuse takes 0.045 seconds. Prepare bounded base64 chunk encoding inside the existing JSON messages, preserving exact object bytes, checksums, chunk bounds, durability and reader formats. Require fixed-byte TCP evidence before measuring the same 500K workload. Leave first-interval variability and excess CPU as open questions rather than claiming construction parity.

Raw results, source/runtime bindings and closure are in `../operations/window-epoch19/`. The frozen source archive SHA256 is `6e27f2d8e490aed29f9e3c479769be441ab07318d5a3a3fb525b480dce7dbc6e`. The comparison uses retained reference epoch14 and previous candidate epoch18; no reference rerun or external profiler is included.
