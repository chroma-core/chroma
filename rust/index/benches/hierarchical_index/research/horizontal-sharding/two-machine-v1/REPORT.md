# Two-machine construction comparison

The latest [epoch27 result](local-tests/METADATA-DICTIONARY-3M-EPOCH27-RESULT.md) validates physical 3M construction in 97.369 seconds including saving, versus 109.605 previous and 63.295 reference. Exact response-local metadata sharing reduces final-million insertion traffic from 34.624 to 7.206 GB and CPU from 448.44 to 342.12 seconds with similar candidate work. Keep the change. Balancing improves with less work, so its full gain is not attributed to the codec. Both machines are API-stopped; peer process cleanup receipt is missing. Safe owner batching is the next local implementation.

The retained [epoch26 result](local-tests/REPLY-VALIDATION-3M-EPOCH26-RESULT.md) validates physical3M construction but takes109.605 seconds including saving, versus104.710 previous and63.295 reference. Reply copies are removed; validation CPU per row improves during final balancing and worsens during final insertion. Preserve the overall slowdown and changed work. Both machines are API-stopped; peer process cleanup receipt is missing. The [fixed-transfer diagnostic](local-tests/FIXED-TRANSFER-DIAGNOSTIC.md) separately confirms repeated small transactions and source compactions. Candidate27 measures metadata sharing; the active plan owns subsequent priorities.

The retained [epoch25 result](local-tests/PARALLEL-OWNER-SAVE-3M-EPOCH25-RESULT.md) validates3M construction and saves in24.258seconds, versus33.305 with sequential owner calls. Complete construction/save is104.710seconds versus63.295reference. Keep bounded concurrent saving; candidate26 reports the subsequent reply-validation result. Both epoch25 cleanup receipts and API stops pass.

The retained [complete physical 3M result](local-tests/COMPACT-EXPANSION-3M-EPOCH24-RESULT.md) validates both-owner construction and the fresh reader. Construction and saving take 117.411 seconds versus 63.295 reference; final ownership is 49.68%/50.32%. Keep bounded compact navigation and prioritize overlapping independent owner materialization plus diagnosis of final-balance overhead. Both instances are stopped. Earlier failed runs remain evidence, with their original limits.

The [epoch23 result](local-tests/CONTINUATION-ASSEMBLY-3M-EPOCH23-RESULT.md) reduces final-million insertion from 31.920 to 25.719 seconds and from 945.00 to 741.78 CPU-seconds relative to epoch21, with slightly more candidate rows. Canonical candidate assembly removes measured ordering work; independent tree construction limits causal attribution. Traffic remains 59.967 GB.

The run fails in final balancing with unexpected connection closure; server logs show oversized replies. There is no complete candidate generation or full-build comparison. Both machines are API-stopped; peer process cleanup remains unconfirmed. Bound expansion replies and finish the separate lossless codec before another reviewed candidate. Reference21 remains valid.

## Retained comparisons

The [epoch22 result](local-tests/NAVIGATION-3M-EPOCH22-RESULT.md) fails during balancing at2M before redistribution. Eight navigation attempts encounter stale local images in0.495514seconds while the shared-view sequence stays2. No complete3M candidate generation is available. The controlled shared-centroid fix remains validated, but local-route liveness needs a separate diagnosis. Both task instances and matching supervisor jobs are stopped with cleanup confirmed.

The current reference21 remains valid at63.295seconds including saving. Epoch21's completed distributed insertion identifies58.846GB of applicationtraffic and945CPU-seconds; candidate ordering and payload representation remain performance priorities after correctness. The current plan governs local repair and a separately prepared canonical-assembly optimization.



The [epoch21 3M result](local-tests/TWO-OWNER-3M-EPOCH21-RESULT.md) fails during the first balance after physical redistribution and subsequent insertion. The candidate completes 2M construction, moves branches, and adds the final million across both machines in 31.920 seconds, then exhausts a navigation retry budget after 0.510 seconds of balancing. No complete candidate generation or final correctness result is claimed. The current reference validates all 3M vectors in 63.295 seconds including saving.

The final insertion generates 58.846 GB of framed application traffic and uses 945 CPU-seconds across both owners, versus 7.375 seconds and 206.18 CPU-seconds reference. Correctness repair and remote payload attribution take priority. Both task instances are API-stopped; the peer process cleanup receipt remains missing. The current plan governs the controlled diagnosis and next bounded comparison.



The [epoch20 result](local-tests/SAVED-CHUNK-500K-RESULT.md) validates all 500K memberships and codes and supports keeping compact saved-object transport. Save traffic falls from 104.8MB to 41.3MB, remote copying from 2.936 to 1.876 seconds, and total saving from 6.477 to 4.683 seconds. Local storage timing also improves, so the full save reduction is not isolated to encoding. Construction plus saving takes 16.962 seconds versus 12.601 for the retained reference.

Later balancing takes 2.094 seconds versus 2.314 reference, but clustering work, excess CPU, physical redistribution and unequal persistence contracts prevent an overall parity claim. All timed construction remains on the initial owner. The next meaningful question is construction on both physical owners, using the retained 3M input and current binaries after readiness checks. Both epoch20 instances are stopped and both supervisor cleanup receipts pass. The [current plan](../CURRENT-PLAN.md) governs authorization and next actions.



The [epoch19 result](local-tests/COALESCING-500K-RESULT.md) keeps bounded pending-work combination for exact fixed-work reduction. Actual rounds fall 8.6% and query copies 2.5%, while non-clustering CPU increases. Build plus save is 18.317 seconds; most elapsed improvement is in the variable first interval. The [epoch18 result](local-tests/QUEUE-SAVE-500K-RESULT.md) keeps corrected queue lock ordering and local saved-object reuse, with build plus save at 22.388 seconds. Its missing peer process receipt remains specific to that closed window; both machines were API-stopped.


The [epoch17 attribution result](local-tests/READINESS-SAVE-500K-RESULT.md) validates exact membership but shows no balancing speedup from producer-aware readiness. It identifies operation admission/completion waiting and1.582seconds of duplicate local save copying. All timed construction is on the initial owner; final redistribution puts roughly half the vectors on each physical machine. Both instances are stopped. See [the plan](../CURRENT-PLAN.md) for the next corrections.

Epoch15 result: [reference-style local transfers and navigation at 500K](local-tests/LOCAL-TRANSFER-PACKED-NAV-500K-RESULT.md) reduces construction plus saving from 68.566 to 30.002 seconds, versus 12.601 for the reference. All current memberships, vectors and codes validate after final physical redistribution to 265,479/234,521 vectors. The native applicable suite passes 185 tests with one known unrelated exclusion. Construction remains on the initial owner; this is not distributed-construction throughput. Keep the three changes. Both machines are stopped and the requested run is complete.

The [epoch14 500K result](local-tests/SOURCE-LEAF-CLAIM-500K-RESULT.md) uses the retained reference and claimed-writer binaries and validates both indexes. Owner-local balancing takes 54.346 seconds sharded versus 9.054 reference. No preparation is discarded; transfer coordination and navigation remain priorities. Both machines are stopped. The measurements below retain their original scope and dates.

The [full-balance 100K attribution](local-tests/FULL-BALANCE-OBSERVABILITY-RESULT.md) identifies discarded split preparation as the leading measured waste: 252 of 324 preparations are rejected. Both indexes validate and both complete-phase profiles succeed. The new instrumented CPU gap is 51.850 versus 26.070 CPU-seconds; elapsed time is 14.055 versus 12.965 seconds, with different serial work. This result selects the next investigation and preserves the earlier measurements below.

**Decision for the stopped epoch8 comparison:** Lyon directs stopping larger comparisons and fixing the owner-local regression first. Epoch8 is interrupted during 2M balancing, with partial evidence preserved and both machines API-confirmed stopped. No further large test is scheduled. The current plan uses minimal reproductions and focused profiles to distinguish reservation-history scanning, repeated source compaction and navigation/allocation work. Static candidates are not measured latency attribution.

The distributed implementation passes physical correctness and both 100K comparisons, including construction after redistribution. The repaired epoch7 source completes all ten construction cycles through one million vectors, then reaches the fixed deadline while balancing two million. The 3M reference passes; a complete distributed 3M result remains outstanding. Both machines are stopped. Owner-local overhead diagnosis takes priority over completing the larger comparison.

## Reservation-history correction and remaining question

The corrected runtime removes completed reservation records from active-conflict scans while retaining replay outcomes. The minimal fixed-topology diagnostic verifies zero conflict-record visits with either zero or 64 completed attempts. Applicable protocol/runtime tests pass. In the separate standard 100K pair, balance takes 16.9493 seconds before and 13.2926 seconds after; process CPU is 56.38 versus 51.84 seconds. Different navigation and job counts prevent attributing the full timing change to this correction. Both saved trees validate, and final smaller-owner shares are 44.648% and 44.512%. See the [full result and limitations](local-tests/RESERVATION-HISTORY-RESULT.md).

The [bounded CPU profile](local-tests/OWNER-LOCAL-PROFILE-RESULT.md) records an early balance slice dominated by full-precision distance calculations (49.60% of samples) and the clustering split routine itself (40.16%). The corrected 100K build validates and both machines are stopped. These are sampled CPU shares, not whole-phase latency attribution. The next question compares clustering work with the exact measured reference. The failed unprivileged preflight remains recorded; the successful observer leaves kernel permissions unchanged. No larger run is next.

## Validated measurements

These are individual executions with independently constructed trees and uncontrolled split seeds and worker scheduling. They establish measured outcomes for their source versions, not general timing variance or a pure sharding-overhead ratio.

| Workload | Implementation | Insertion | Balancing | Placement | Saving | Construction plus save |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 100K, one 100K interval | Reference | 0.0946s | 4.9621s | — | 0.6074s | 5.6659s |
| 100K, one 100K interval | Distributed, epoch6 | 0.1750s | 14.8396s | 0.9138s | 1.6822s | 17.6257s |
| 100K, two 50K intervals | Reference | 0.1114s | 3.3522s | — | 0.5747s | 4.0409s |
| 100K, two 50K intervals | Distributed, epoch6 | 0.1421s | 9.1548s | 0.4251s | 1.7058s | 11.4546s |
| 3M, standard cadence | Reference | 14.6985s | 33.6187s | — | 17.1812s | 65.5200s |
| 3M, standard cadence | Distributed | — | — | — | — | Incomplete; epoch7 reaches its deadline during balancing at 2M. |

Small differences between phase sums and totals include bookkeeping between measured phases. The standard 100K distributed run ends with 54,584 and 45,416 current memberships. Its sole insertion interval precedes redistribution, so it measures initial-owner construction and subsequent placement rather than distributed construction.

The two 50K diagnostic ends with 55,328 and 44,672 memberships. Its second interval adds 28,437 vectors to owner 1 and 21,563 to owner 2; those owners perform 19 and 16 splits during that interval. Both actual machines contribute useful construction work, and both final shares exceed 30%. Complete saved membership, one-bit posting-code and four-bit centroid validation pass.

## What explains the observed difference

Balancing dominates the elapsed difference at 100K. In the standard arm, the distributed runtime uses 53.28 CPU-seconds across both owner processes during balance, versus 17.0 CPU-seconds for the reference. It records 111,604 transferred rows and 6,565,903 counted navigation distances, versus 53,555 and 5,405,317. Its initial-owner balance sends only 149 framed application bytes to the empty peer. Network throughput therefore cannot explain that phase; extra algorithmic work and local implementation costs require investigation.

The two 50K distributed arm records 43.28 balance CPU-seconds and 298,929,730 framed application bytes during balancing. This is actual network participation. These byte counts cover application envelopes and length prefixes in both directions, counted once at the coordinator; they exclude TCP/IP headers and retransmissions. Process CPU snapshots include bookkeeping between samples. They are not exclusive kernel or lock attribution.

Shared-upper-tree navigation bypasses the owner distance counters, so counts after handoff understate total work. Peak active jobs include waiting work; they do not establish CPU parallelism. Approximate average busy cores during balance are 3.59 for the initial-owner arm and 4.73 across both processes for the two50K arm. These interval averages warrant profiling before attributing waiting to a specific lock.

The source comparison identifies serial per-leaf posting-code preparation, repeated duplicate-ID scans, differences in immediate scheduling and retry behavior, and communication representation as investigation candidates. The measurements do not isolate their individual causal contributions. [The implementation comparison](IMPLEMENTATION-DIFFERENCES.md) separates necessary distributed work from possible missing reference optimizations.

## Implemented architecture and correctness

One logical tree begins on one owner. Shared upper-tree navigation selects candidates globally, and each machine owns its subtrees and current posting lists. Actual owner services communicate over the private network. Direct posting transfers, cross-owner merging, shared-root changes and whole-subtree handoff are implemented. Full-vector access uses an immutable corpus with an independent cache on each machine; cached vector bytes are duplicated, while current posting membership is unique.

The physical fixture uses 125 vectors with small structural thresholds to exercise remote insertion and navigation after handoff, root replacement, cross-owner reassignment and merge, and capacity rejection. Epoch6 ends with 62/63 memberships and validates every required mechanism. The epoch1 durable-storage fixture publishes exact-version objects to S3, restores them on the other machine after both writers stop, and validates memberships, posting and centroid codes and full-vector checksum in a fresh reader. Conditional creation rejects conflicting data with HTTP 412 and preserves the existing version.

The epoch6 source passes 156 applicable native tests, all three native CLI builds and independent review. The earlier complete wrapper records 153 passes and one failure in a separate local-writer parent/leaf split test. That writer source is unchanged from epoch4, and the failure reproduces without constructing the distributed runtime. It differs from the retained reference benchmark implementation. The failure remains recorded; the complete wrapper is not claimed to pass.

## Workload and accounting limits

Each machine has 32 physical cores and 512 GiB RAM. The reference uses one machine; the distributed runtime can balance with 32 workers per owner. Insertion navigation uses the coordinator's worker pool. Compare total CPU and allocated hardware as well as elapsed time; this is not an equal-resource efficiency comparison.

Both arms use identical canonical vectors and order, 768 dimensions, squared L2 distance, branching 100, split 2048, merge 512, full-precision construction vectors and centroids, and matched construction search settings. Posting codes use one bit and saved-query centroids use four bits. Standard cadence is 100K additions per balance through 1M, then 1M. The additional 100K diagnostic uses two 50K intervals in both implementations.

Each process starts with empty application caches. OS page caches are not flushed. Input verification and cache warming precede construction and are retained separately. Each distributed owner has a bounded 12 GiB vector preload budget. Vector-count placement budgets do not establish physical memory capacity or cache-capacity crossing.

The reference saves its full-vector embedding store. The distributed input adapter references the existing immutable corpus, so saving excludes that persistence work. The distributed benchmark saves immutable owner files into the coordinator's local generation namespace and validates with a fresh process; the separate 125-vector fixture establishes actual S3 publication and cross-machine restore. Training-vector queries are smoke tests, not held-out recall measurements or evidence of quality parity.

## Retained failures and next decision

Earlier attempts expose unnecessary posting-revision invalidation, concurrent arrivals during merge evacuation, shared-centroid publication races, and full-width node-ID report serialization. Controlled failures, repairs and review receipts are retained in [local tests](local-tests/). The build process also rejects a stale cached executable before execution and now requires newly compiled artifacts from the frozen source. No failed attempt contributes a completed build-time ratio.

Epoch6 fails at 200K with a reservation progress error. Two controlled tests identify initial admission invalidation after unrelated structural edits and a total-runtime retry clock that can fail after successful work. The reviewed epoch7 repair admits current transfer paths atomically and resets a five-second stall clock only after a fully acknowledged transfer, retaining the round cap and absolute workload deadline. It passes 161 applicable tests, native and Linux executable builds, and physical correctness.

Epoch7 completes balancing through 1M, with 249.1780 seconds summed across ten balance phases. Adding the next million takes 40.9312 seconds. The absolute deadline then stops the job during balancing at 2M; no construction error precedes shutdown in the retained coordinator log. This is incomplete timing evidence, not a validated 3M result. Epoch8 uses a separately reviewed longer bound with those same executables, then stops by user direction during 2M balancing. Neither partial run contributes a completed 3M timing ratio.

Raw receipts are in [epoch2](operations/window-epoch2/), [epoch5](operations/window-epoch5/) and [epoch6](operations/window-epoch6/); prior failed windows and source packages remain retained. Epoch7 is API-confirmed stopped and adds a $2.020666703872681 compute upper bound. Covered research compute totals $29.337722439140737183; identified recurring disks remain $19.52/month. These are task-specific receipts and commitments, not a whole-account bill. No new disk capacity is added in this campaign.

Epoch8 closes with both process groups cleaned up and both instances API-confirmed stopped. Its compute upper bound is $1.6277948393249513, counted once. Covered compute totals $30.965517278465688483, and identified recurring disks remain $19.52/month. The current investigation uses minimal diagnostics and defers larger runs.
