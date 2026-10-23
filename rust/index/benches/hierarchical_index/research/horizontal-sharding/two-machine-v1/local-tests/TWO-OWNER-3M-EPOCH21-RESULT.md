# Three million vectors: navigation fails after physical redistribution

The candidate completes construction through 2M vectors, redistributes existing branches, and adds the last million across both physical owners. The following balance fails after 0.510 seconds because a navigation attempt exhausts its retry budget. This is a failed construction run, with no complete candidate generation or final validation. Repair the navigation failure before another performance comparison. In parallel, investigate the 58.846 GB of application traffic in the final insertion interval.

## Workload and interpretation

Both arms use the same retained 3M vectors and order, 768 dimensions, L2, 32 workers, split 2048, merge 512, branching 100, and 100K add/balance intervals through 1M followed by 1M intervals. The candidate runs one owner on each of two 32-core machines; the reference uses one machine. Source and binaries are unchanged from epoch20. The current instrumented reference runs once because the older 3M reference uses different instrumentation. See the [readiness assessment](TWO-OWNER-3M-READINESS.md), [admission review](EPOCH21-THREE-MILLION-REVIEW.md) and [runtime identities](../operations/runtime-binaries-epoch21.json).

Capacity is 2.16M vectors per owner, with an 80% trigger and a 70% receiving target. The first placement occurs after the 2M balance. This structural condition is necessary to exercise subsequent construction on both machines. Both processes preload the immutable corpus; this experiment does not cross the configured vector-cache capacity. Current posting membership is owned once; full-vector cache bytes are duplicated.

## Measurements

| Phase | Current reference | Candidate |
| --- | ---: | ---: |
| Add through 2M | 6.933 s | 9.192 s |
| Balance through 2M | 25.028 s | 24.066 s |
| Placement through 2M | Not applicable | 2.671 s |
| Final 1M add | 7.375 s | 31.920 s |
| Final 1M add process CPU | 206.18 s | 945.00 s across both owners |
| Final balance | 6.846 s, completes | Fails after 0.510 s |
| Total saving | 17.089 s | Not reached |
| Construction plus saving | 63.295 s | Incomplete |

Before redistribution, all timed construction runs on owner1. Candidate balance through 2M uses 415.83 process CPU-seconds, including 113.111 clustering CPU-seconds. Different constructed trees and clustering work prevent treating its slightly lower elapsed balance as a causal speedup or parity result. Process CPU snapshots include boundary/control work and are slightly wider than the coordinator wall timers. Inclusive worker timings overlap.

After 2M redistribution, the owners contain 1,491,238 and 508,762 vectors. The final insertion adds 746,974 and 253,026 respectively, reaching 2,238,212 and 761,788 before balancing. Both machines therefore perform real insertion work. The smaller owner contains 25.39% at this intermediate boundary; the final 30% placement criterion is not reached or validated because balancing fails before the next redistribution. Capacity is a placement threshold at phase boundaries, not a hard per-add memory ceiling.

Final insertion sends 9.835 GB and receives 49.011 GB of framed application messages, totaling 58.846 GB across 16,378 request/reply pairs. These are coordinator endpoint deltas counted once per connection; they exclude TCP/IP headers and retransmissions. The measured traffic warrants inspecting response contents and batching. It does not by itself establish exclusive network latency or serialization CPU. Owner CPU is 684.75 and 260.25 seconds; reference CPU is 206.18 seconds. The [source-level insertion analysis](EPOCH21-DISTRIBUTED-INSERTION-ANALYSIS.md) identifies repeated scored-child metadata in JSON and extra candidate ordering work. Candidate comparison counts are 4.5% lower than reference. Replies contain no centroid vectors; that alternative explanation is rejected. Inclusive candidate assembly consumes 312.972 CPU-seconds and remote owned scoring only 29.172 of its 260.25 process CPU-seconds. These scopes select the next mechanism tests without uniquely attributing every CPU-second.

The failed balance reports 18 and 12 splits before aborting. These partial counts demonstrate both owners enter balance work, but cannot establish complete membership correctness or successful distributed balancing. The first error is `split leaf 18446744073712431530: split self reassignment from 18446744073714361113: construction navigation exceeded its whole-attempt retry budget`. Subsequent errors report an unresolved owner mutation. Preserve the original error and all subsequent failures; do not interpret them as independent root causes. A controlled reproduction must distinguish stale navigation state from legitimate concurrent changes before changing retry behavior.

The reference validates all 3M memberships, full-vector bits, one-bit posting codes and saved four-bit centroids. Its tree has levels `[1, 35, 2123]`. Reference build plus save excludes input load and final validation; total supervisor time is 89.934 seconds. Candidate supervisor time is 85.730 seconds before error and cleanup. Comparing those supervisor times would reward an incomplete run and is invalid. The small reused-query smoke check remains separate from recall quality.

## Decision and next experiment

Keep the validated owner-local improvements from epochs15–20. The next correctness question is navigation liveness during simultaneous mutation on both owners. Reproduce it with a controlled fixture, fix the actual state/validation problem, run relevant split/merge/transfer checks and obtain one focused independent review. Then run one candidate on the same retained 3M workload against this current reference. Do not merely raise the retry limit or rerun the failing binary.

The leading performance question becomes distributed insertion payloads and coordination. Use existing work and byte counters to identify repeated data and compare the actual reference path. A narrow fixed-tree remote/local parity fixture should establish any representation or batching change before another measured run. Saving and initial-interval variance remain lower-priority investigations until physical distributed construction completes.

## Evidence, shutdown and spending

Raw [reference receipts](../operations/window-epoch21/3m-reference/run-receipts.json), [candidate receipts](../operations/window-epoch21/3m-distributed/run-receipts.json), [partial analysis](../operations/window-epoch21/candidate-partial-summary.json) and [reference analysis](../operations/window-epoch21/reference-summary.json) retain phase data. The [partial analyzer](summarize-partial-construction.py) calls the reviewed per-owner subtraction function from `summarize-distributed-construction.py`; it unwraps the failed event's `owners.Ok` response and explicitly marks the run incomplete. Its output exactly reproduces the retained summary. It makes no saved-generation or quality claim.

Both exact instances are API-confirmed stopped. Reference and candidate-coordinator supervisor cleanup receipts pass. The peer stop helper fails and its matching supervisor exit receipt is missing, so peer process cleanup remains unconfirmed separately from verified AWS shutdown. See [cleanup reconciliation](../operations/window-epoch21/cleanup-reconciled.json).

The closed window adds $1.361019497680664 of compute once and no storage. Covered compute is 43.493519757179677683; identified recurring storage is $19.52/month, giving a first-month covered bound of 63.013519757179677683 and 936.986480242820322317 remaining under the $1,000 authorization. This is task receipt accounting, not a whole-account invoice. The window is closed and cannot be reused.
