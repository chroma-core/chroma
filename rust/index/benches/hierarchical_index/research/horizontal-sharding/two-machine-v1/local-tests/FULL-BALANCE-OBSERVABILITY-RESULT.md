# Whole-balance 100K attribution

The strongest measured problem is discarded split preparation. The distributed implementation prepares 324 leaf splits, commits 72 and discards 252 after installation rejects changed state or unavailable protection. Those discarded attempts process 1,721,645 vector rows. Preventing this repeated preparation is the next investigation; posting transfers and navigation remain secondary candidates.

Both 100K builds pass saved-state validation, and both profiles cover the entire timed balance. The distributed arm consumes almost twice the CPU but takes only 8.4% longer in this pair. The earlier roughly threefold elapsed gap does not reproduce here. Preserve both results: uncontrolled clustering initialization and scheduling change the amount of work and the longest serial operation.

## Matched setup and measured costs

Both arms use the same retained 100,000 FineWeb vectors in the same order, 768 dimensions, squared L2, 32 workers, one 100K insertion interval, branching factor 100, split threshold 2048, merge threshold 512 and matching construction search settings. Both use the same instrumented clustering kernel, Rust1.92.0 and release/native-CPU/frame-pointer/debug flags on the same r6i.16xlarge worker with 32 physical cores. The reference runs first. Fresh processes and indexes are used; operating-system cache state and random initialization are not controlled.

The distributed balance occurs entirely on the initial owner. The second physical machine holds zero vectors during that phase; only149 framed application bytes cross the network. Redistribution follows balancing and ends with 55,840/44,160 vectors. This is a local construction-overhead comparison with a real peer, not a distributed-construction speedup test. The reference ends with one internal node and 70 leaves; the distributed balance ends with one internal node and 72 leaves.

| Measurement | Unsharded reference | Distributed implementation |
| --- | ---: | ---: |
| Timed balance | 12.965s | 14.055s |
| Process CPU during balance | 26.070 CPU-s | 51.850 CPU-s |
| Average CPU cores used, CPU divided by wall time | 2.01 | 3.69 |
| Clustering kernel CPU | 21.928 CPU-s | 37.626 CPU-s |
| Remaining process CPU after subtracting kernel CPU | 4.142 CPU-s | 14.224 CPU-s |
| Clustering calls | 69 | 324 |
| Committed leaf splits | 69 | 72 |
| Discarded prepared leaf splits | No extra kernel calls beyond committed splits | 252 |
| Clustering input rows, including repeated work | 617,525 | 2,099,736 |
| Assignment row-iterations | 29,256,845 | 54,280,135 |
| Clustering distance calculations | 63,459,422 | 125,380,052 |
| Navigation distance calculations | 5,118,392 | 7,408,751 |

Clustering accounts for 15.698 of the25.780 additional CPU-seconds, or60.9% of the CPU difference. This is a measured difference between complete phase counters, not an estimated wall-time saving. The remaining10.082 additional CPU-seconds include vector access, code preparation, navigation, transfers, coordination and instrumentation. Nested operation timers cannot be added to produce an exclusive breakdown of that remainder.

## Why split preparation is first

The distributed writer releases its source snapshot lock before clustering and constructing replacement posting lists. It then acquires protection and revalidates installation. A rejected installation discards prepared results and queues another job. In this run 77.8% of prepared attempts and 82.0% of their input rows belong to rejected installations. This directly establishes consequential wasted work. Counters do not yet distinguish each rejection condition or assign kernel CPU separately to accepted and discarded attempts; the percentages are not claimed CPU or elapsed savings.

The reference retains a source claim through preparation. Its69 kernel calls match69 committed splits. The kernel source itself is identical between implementations. The distributed kernel performs 1.976 times as many distance calculations but uses 1.716 times as much kernel CPU, which provides no support here for a generally slower distance kernel. Different input sizes, iteration distributions and memory conditions prevent treating CPU per distance as an isolated kernel-speed comparison.

Elapsed time depends on the longest serial work as well as total CPU. The reference spends7.847 seconds,60.5% of its balance, in the first round with one worker. Its later rounds expose 2,4,8,16 and 29 worker partitions. The distributed scheduler completes 516jobs, reaches 21 simultaneously active jobs and has a longest observed job of2.496 seconds. Active jobs can be blocked; they are not a count of occupied CPU cores. These different critical paths explain why a large CPU difference can coexist with a much smaller elapsed difference. This pair does not establish a stable8.4% sharding overhead or eliminate the earlier adverse timings.

## Other candidate costs

Posting transfers rebuild far more source data in the distributed arm. It performs3,639 source compactions and scans19,981,894 source entries, versus1,741,391entries and 473successful transfer batches in the reference. Explicit survivor/destination ID and code copies total2.398 GB versus0.202 GB; the reference separately copies0.203 GB of source snapshots. Byte definitions and ID widths differ, so these are explicit payload counts rather than total memory traffic. Distributed membership checking makes 488,178linear-scan probes; summed haystack lengths give a 5.171billion comparison upper bound, not an actual comparison count. Reference probes use hash lookup and are not equivalent units.

The distributed transfer-driver scope consumes 9.229 CPU-seconds including nested endpoint work and waiting-loop work. Transferred rows also differ: 119,846 versus 51,719, so extra scans and copies are not an isolated measure of the grouping strategy. Source compaction itself consumes0.657 CPU-seconds; transfer preparation consumes1.143 CPU-seconds and endpoint steps1.334 CPU-seconds. These scopes overlap. Reference bulk-transfer elapsed time sums to0.370 seconds across calls; no corresponding reference thread-CPU timer exists, so it is not a matched9.229-versus0.370CPU comparison. Combining destinations and compacting once remains promising, but the measured split waste is the stronger first target.

Navigation consumes2.970 CPU-seconds in the distributed arm. It copies0.700 GB of query coordinates into expansion requests, clones0.615 GB of reply payloads for validation, and separately copies0.700 GB when preparing reassignment queries. Query-copy timing is0.261 CPU-seconds, reply validation0.545, candidate selection0.591 and owned scoring0.874; these are nested within broader navigation scopes. The reference's navigation elapsed sum is0.667 seconds with fewer distance calculations. Its materialization CPU is not separately measured. A direct local navigation path remains worthwhile after split preparation and transfer grouping.

Reservation waiting is visible but is not an additive wall-time bill. Workers accumulate33.403 seconds queued for reservations and 2.568 seconds acquiring the manager mutex; the mutex is held for 0.242 seconds in aggregate. The manager visits 74,322active conflict records and 2,178,314ancestry nodes. It ends with 4,048 retained replay records, zero active grants and zero queued requests. Completed-history scans are already removed; retained-record population alone is not evidence that this regression returned. The reference claim/tree mutex acquisition sums are0.0020/0.0024 seconds, but its protection model differs. Reassess waits after reducing preparation retries.

## Profile coverage and limitations

The profiler acknowledges enabled recording before each benchmark starts its balance timer. It acknowledges disabled recording after the completed timer. Both receipts confirm full timed-phase coverage and zero lost samples. Reference instruction-location samples are 51.40%distance,40.53%split and 2.64%one-bit quantization; distributed samples are 45.90%,35.94%and 4.81%respectively. Distance samples also include work outside clustering. These exclusive instruction shares must not be added to inclusive caller shares or multiplied by wall time. Sampling covers userspace CPU and includes small boundary activity; it does not measure blocked time or all kernel CPU.

Instrumentation adds clocks and measurement merges, including many small operations. Its perturbation is uncalibrated. The same clustering counters are in both arms, but distributed coordination instrumentation is more extensive. This is an informative instrumented pair, not an uninstrumented performance claim. Gross construction totals include profiler-gate waits/reporting and should not be compared with older construction totals. The requested balance timers and their process-CPU measurements exclude those waits.

The reference verifies 100,000 unique saved memberships, full-vector bits, recomputed one-bit posting codes and saved four-bit centroid codes. The independent distributed reader verifies exact 100Kmembership, vector checksum and all posting/centroid codes without writer objects. Reused training-vector queries are smoke checks, not held-out recall evidence. This shallow tree and resident working set do not test deep navigation or cache-capacity crossing.

## Decision and faster next experiment

First bring source preparation closer to the reference: prevent conflicting source mutations from repeatedly invalidating expensive split work while preserving navigation, unique membership and independent subtree concurrency. Inspect the exact rejection conditions and design the source claim without a blocking leaf-to-parent lock upgrade. A tree-wide lock is not the intended remedy. Validate the affected interleavings, then use the existing100K/32 worker harness to compare committed/discarded work, CPU and elapsed time. Keep or reject the change from that evidence; do not expand this into a larger benchmark campaign.

Second, combine posting-transfer destinations and compact once where semantics allow. Third, remove unnecessary local navigation envelopes/copies. Reassess lock/queue waiting after those work reductions. No further implementation or benchmark is performed in this experiment.

The testing process should prioritize decisions over exhaustive attribution. Reuse this working harness and compiled dependencies, add only counters needed for the next hypothesis, run focused tests, and accept one profile/pair when it is enough to choose the next change. Repeat only if uncertainty could change that choice. Required sandbox identity, fixed shutdown bounds, correctness checks and spending limits remain in force.

## Execution failures, closure and evidence

The upload preflight initially applied compiler headroom to the runtime-only peer and stopped before transferring bytes. The worker had 80.8 GBfree and the peer 14.8 GB; the reviewed workload needs 2 GiB, so a 4 GiB peer preparation bound sufficed without resizing or deletion. The first reference attempt then stopped before balancing because installed perf emits `ack\n\0`, while the collector expected `ack\n`. A bounded compatibility diagnostic established the exact bytes. A five-byte parser with partial-read coverage passed focused tests, and an explicitly selected corrected pair reused the unchanged binaries in a fresh output namespace. Both failures remain retained.

The peer stop helper encountered a permission error inspecting an unrelated process. The coordinator exits successfully with cleanup confirmed; both profilers confirm cleanup. The root then stops both exact task instances through AWS and verifies both stopped states. Disks, failed artifacts and raw profiles remain retained. The closed window adds a conservative $2.2087163510131838compute bound once. Covered task compute totals $34.759586773247182783; identified recurring storage remains $19.52/month. This is not a whole-account bill.

The [machine-readable summary](../operations/window-epoch12/summary.json) contains exact phase deltas and sample summaries. The [reference result](../operations/window-epoch12/100k-reference-ackfix/result.json), [distributed result](../operations/window-epoch12/100k-distributed-ackfix/result.json), [reference profile](../operations/window-epoch12/100k-reference-ackfix/perf-report.txt), [distributed profile](../operations/window-epoch12/100k-distributed-ackfix/perf-report.txt), [Linux build receipts](../operations/window-epoch12/linux-build.json), [source closure comparison](epoch12-source-closure-comparison.json), [validation](FULL-BALANCE-OBSERVABILITY-VALIDATION.json), [source review](FULL-BALANCE-OBSERVABILITY-REVIEW.md), [counter definitions](FULL-BALANCE-OBSERVABILITY.md), [ACK correction](../operations/admission-epoch12-ackfix.json) and [API closure](../operations/window-epoch12/closed.json) preserve reproducibility and limits.
