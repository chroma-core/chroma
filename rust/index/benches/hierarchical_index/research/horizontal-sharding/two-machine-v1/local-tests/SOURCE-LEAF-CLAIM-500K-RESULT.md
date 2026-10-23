# The 500K comparison exposes substantial owner-local overhead

The same retained binaries complete and validate both 500K runs. Sharded balancing takes **54.346 seconds versus 9.054 seconds for the reference, a 6.00× elapsed gap**. It uses 117.80 versus 52.70 CPU-seconds. Source claims still eliminate discarded preparation, but they do not establish comparable construction performance. The next priority is same-owner transfer coordination and reservation waiting, followed by direct local navigation. Both machines are API-stopped; no further benchmark or implementation is started.

This is a second result document. The [100K source-claim result](SOURCE-LEAF-CLAIM-RESULT.md) retains its measurements, including the adverse elapsed result and the earlier reference's 4.962-second balance. There is no unclaimed 500K arm here, so this experiment compares the current implementations rather than measuring the claim change's isolated speedup.

## What we ran

Both arms ingest the same 500,000 FineWeb vectors in the same order, using five batches of 100K additions and 32 workers on the same 32-physical-core r6i.16xlarge machine. The metric is squared L2 over 768-dimensional full-precision vectors. Branching is 100, split threshold 2048, merge threshold 512, construction beam 10–16 with tau 1.5, and reassignment neighbor count 32. Posting codes use one bit and saved query centroids use four bits. No implementation or executable is rebuilt.

All five sharded construction phases run on the initial owner. A 500K vector-capacity setting triggers redistribution only above 400K, with a target of at most 350K. The observed first four placements move nothing. After the last balance, two branches containing 122,707 and 111,018 vectors move to the second machine, leaving 266,275/233,725 memberships. This measures owner-local construction followed by physical redistribution; it does not measure distributed construction throughput.

The larger input reaches the intended extra tree level. The reference ends with levels [1, 5, 354], or six internal nodes and 354 leaves. The candidate ends with [1, 6, 347], or seven internal nodes and 347 leaves. The raw input is 1.536 GB, below the candidate's 12 GiB per-owner vector-cache budget; this experiment does not investigate cache-capacity crossing.

Both binaries retain their built-in counters, with the external profiler and profiling gate disabled. Random clustering initialization, worker scheduling and operating-system page caches remain uncontrolled. Each process starts with fresh application state; input verification and loading occur before the timed construction phases. One run per arm is sufficient to expose the large gap, but does not establish its variance or a causal breakdown of every second.

## Timings and CPU work

Balance CPU is process CPU measured around each timed balance on the construction owner. CPU-seconds can exceed elapsed seconds when multiple cores work concurrently.

| Quantity | Reference | Sharded candidate |
|---|---:|---:|
| Insertion, seconds | 0.699 | 5.487 |
| Balancing, seconds | 9.054 | 54.346 |
| Balancing, CPU-seconds | 52.700 | 117.800 |
| Clustering, CPU-seconds | 35.296 | 30.918 |
| Balance CPU outside clustering | 17.404 | 86.882 |
| Average busy CPU cores during balance | 5.82 | 2.17 |
| Redistribution, seconds | — | 1.194 |
| Save, seconds | 2.840 | 7.411 |
| Gross construction plus save, seconds | 12.601 | 68.566 |

The phase boundaries are the primary comparison. Gross construction totals include boundary reporting and bookkeeping; the candidate also includes placement. Save is not an identical persistence workload: the reference persists its full-vector embedding store, while the candidate references the retained immutable corpus. These differences do not explain the timed balance gap. Validation runs separately and is not included in construction plus save.

| Vectors indexed after batch | Reference balance, seconds | Sharded balance, seconds | Reference balance CPU | Sharded balance CPU |
|---|---:|---:|---:|---:|
| 100K | 6.740 | 14.859 | 19.26 | 27.15 |
| 200K | 0.574 | 10.587 | 8.54 | 20.19 |
| 300K | 0.570 | 10.289 | 8.57 | 22.08 |
| 400K | 0.551 | 9.395 | 7.26 | 22.54 |
| 500K | 0.620 | 9.215 | 9.07 | 25.84 |

The gap persists after initial construction. The subsequent four intervals take 39.487 seconds sharded versus 2.314 seconds reference, a 17.06× elapsed gap. Their CPU totals are 90.65 versus 33.44 seconds. Sharded insertion also grows from 0.180 to 2.069 seconds per interval, while the reference grows from 0.098 to 0.180. Insertion therefore belongs in the investigation queue as well as balancing; its cause is not isolated by the balance counters below.

## What the counters establish

The candidate performs no discarded preparation. Its 349 counted preparations all commit, and the shared clustering kernel executes 356 calls, including work outside those preparation counters. The reference kernel executes 358 calls. Clustering processes 1,309,033 candidate rows versus 1,314,627 reference rows; candidate distance calculations are 91.259 million versus 79.818 million. Useful work is not identical, but the candidate uses **4.378 fewer clustering CPU-seconds**, while work outside clustering costs **69.478 additional CPU-seconds**. Extra clustering cannot explain the observed total CPU gap.

Transfer and coordination work remain the leading measured overhead candidates. The following quantities cover only balance phases, summed across all five intervals.

| Quantity | Reference | Sharded candidate |
|---|---:|---:|
| Posting rows transferred | 183,585 | 161,484 |
| Source compaction operations / reference transfer batches | 3,269 | 29,038 |
| Source entries scanned during transfer | 5,999,685 | 41,309,426 |
| Explicit source-compaction copy bytes | 0.675 GB | 4.957 GB |
| Counted construction-navigation distances | 71,268,544 | 62,362,906 |

The operation counts describe each implementation's recorded grouping, not identical transaction types. Nevertheless, fewer candidate rows move while it performs substantially more source-list work. Candidate source compaction itself consumes only **1.252 CPU-seconds**, so merely accelerating its copy loop would address little of the gap. The stronger target is grouping destinations and avoiding unnecessary same-owner transfer protocol and reservation work.

The candidate makes 60,617 reservation requests, examines 3,203,721 active conflicts, walks 110,367,457 ancestry steps and executes 321,814 transfer-protocol calls. Reservation-mutex waits sum to 191.710 worker-seconds, and queued reservation waits sum to 518.926 worker-seconds. These overlap across workers and cannot be added to elapsed time or treated as an exclusive latency attribution. The completed-history scan fix remains intact; these counters concern active conflicts.

The inclusive transfer-driver scope records 117.258 CPU-seconds, navigation records 23.123, and reservation-mutex scopes record 11.306. These scopes overlap and can nest; they are diagnostic clues, not additive slices of the 117.80 process CPU-seconds. Navigation copies 2.070 GB of query coordinates and 5.095 GB of reply payloads despite doing fewer counted distances than the reference. A direct local navigation path remains a worthwhile second target. Low average busy-core counts are consistent with waiting or serial execution; they do not by themselves identify which dependency determines the elapsed critical path.

## Insertion attribution from existing phase snapshots

Insertion overhead is concentrated in navigation. In the final 100K addition interval, candidate elapsed time is 2.069 seconds versus 0.180 reference. Candidate owner CPU snapshots increase by 60.92 seconds, and the inclusive navigation scope records 60.311 seconds. Its inner owned-routing scoring scope records 55.691 CPU-seconds. Candidate navigation scores 28.5 million children versus 28.3 million reference distances, so extra search work does not explain the large CPU difference. The snapshot interval includes boundary observations, and scoring includes result construction and counting as well as distance arithmetic.

The implementation comparison identifies a particularly direct difference inside that hot loop: the candidate updates one shared atomic distance counter for every child scored, while the reference counts locally and adds the total at the end of navigation. Thirty-two workers therefore repeatedly write the same candidate counter. Both call the same distance API, but the reference also packs child centroids contiguously whereas candidate child records each carry a separate vector. Atomic contention and layout are leading hypotheses inside the measured scoring scope; neither contribution is isolated yet. Request/reply construction, cloning and validation add further local overhead, but should not be used as a catch-all explanation for the entire scoring cost.

The broad insertion structure matches: workers navigate a stable tree, combine contributions across the entire input batch, and append once per destination leaf. The details differ. The reference generates posting codes in the navigation workers using the supplied vectors; the candidate groups IDs first, resolves vectors again and generates codes in destination-leaf jobs. Candidate appends also retain replay bookkeeping, scan existing posting IDs for duplicates while holding the leaf and operation-table locks, and publish a revised routing image. The reference prechecks new IDs and directly appends the prepared IDs/codes. These are concrete differences, but the existing timings put the final-interval bottleneck in navigation, not posting append. See [per-interval insertion attribution](../operations/window-epoch14/insertion-attribution.json). No implementation or benchmark is changed by this analysis.

## What this says about the reference's 12.96-second run

The retained instrumented reference balances all 500K in 9.054 seconds here, including 6.740 seconds for the first 100K. The separate instrumented/profiled 100K run takes 12.965 seconds, and an earlier reference run takes 4.962 seconds. The 12.965-second observation is not a representative baseline to assume for future comparisons.

Measured work varies substantially. The first interval here performs 43,958,701 clustering distances versus 63,459,422 in that separate 100K run. Its initial single-worker balancing round takes 2.815 seconds versus 7.847 seconds. External profiling is also disabled here, and its overhead is not isolated. These observations establish variability in clustering work and the serial beginning of construction; they do not prove which factor causes the entire historical timing difference. The same-window 500K pair makes the current sharded overhead clear without needing to resolve that attribution first.

## Decision and next work

Keep source-leaf claims because preparation remains useful and correctness passes, but reject any conclusion that the current sharded construction is close to reference speed. This workload gives enough signal to prioritize an implementation change; another larger benchmark is not needed to select it.

1. Compare the same-owner transfer path with the reference's bulk operation. Combine destination groups and avoid repeated reservation/protocol bookkeeping while preserving source claims, unique membership and safe remote ordering. Use the existing counters to distinguish cheaper coordination from fewer transfers.
2. Compare direct local navigation with the current request/reply path. Preserve global selection while reducing avoidable query/reply copying and validation work. Include insertion, whose per-interval latency grows more quickly than the reference's.
3. Reassess the scheduler and reservation critical path after those changes. The present waiting and CPU totals warrant investigation, but do not justify attributing every excess elapsed second to one mutex or increasing worker counts without evidence.

Implementation remains unchanged at the end of this experiment. No follow-up optimization or benchmark runs automatically.

## Validation, operational failure and evidence

Both indexes validate all 500,000 memberships, full vectors, one-bit posting codes and four-bit saved centroid codes. Candidate validation uses a fresh reader without writer objects. Its second owner receives 46.745% of memberships after construction. Sixteen training-vector smoke queries are not held-out recall evidence or a claim of clustering-quality parity.

Both workload supervisors exit successfully and confirm process cleanup. The local candidate launcher exits with an error when its peer-stop helper encounters a permission error reading an unrelated process. The peer's independent exit receipt confirms operator stop and cleanup; both instances are subsequently API-confirmed stopped. The [cleanup reconciliation](../operations/window-epoch14/cleanup-reconciled.json) preserves the wrapper failure alongside the valid benchmark result. A separate admission-review rejection over adding a missing window label is resolved through normal review before execution; the absolute shutdown deadline is unchanged.

The window adds a conservative **$1.2061185787963866** compute bound once, with no additional storage commitment. Covered task compute totals $36.829098108543813583. Identified retained storage remains $19.52/month, leaving $943.650901891456186417 after the covered first-month reservation under the $1,000 allowance. These amounts are task receipts and commitments, not a whole-account invoice.

The input SHA256 is `165f6baa847dacb65c1e9c718c1f6edd976f52ec3907136c447de9ebf5972f48`. The unchanged reference executable is `8566a052e2fe0908188862c5ede2b98215664236198f6ab40a43193248016fb6`; the claimed writer is `1cc7228294dd3418f06969549d94cbbd4d216cea1f262d8e2340dbcbfd31e902`; the reader is `fb09190fc2adf6720bd6c13f8e1ae639351e66adce2a9885364a12724a4da14c`.

Retained evidence includes the [combined summary](../operations/window-epoch14/summary.json), [reference result](../operations/window-epoch14/500k-reference/result.json), [candidate result](../operations/window-epoch14/500k-distributed/result.json), [fresh-reader validation](../operations/window-epoch14/500k-distributed/durable-validation.json), [configuration](../operations/config-epoch14.json), [adapter review](../operations/review-epoch14.json), [input verification](../operations/window-epoch14/prefix-worker.json), [raw reference receipts](../operations/window-epoch14/500k-reference/run-receipts.json), [raw candidate receipts](../operations/window-epoch14/500k-distributed/run-receipts.json) and [API shutdown receipt](../operations/window-epoch14/closed.json).
