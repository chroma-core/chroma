# Reservation-history work removal

The reservation manager no longer scans completed operations when finding active conflicts. A deterministic test proves the work removal and correctness tests pass. The fixed 100K comparison validates both saved trees, but different construction work prevents assigning its entire timing difference to this correction. Substantial owner-local overhead remains.

## Question and minimal reproduction

The same uncontended request runs against one unchanged leaf with zero versus 64 completed/released attempts. No other request is active or queued. The original code visits one versus 65 retained records while finding zero active conflicts. The corrected code visits zero conflict records in both cases. Completed outcomes remain available for duplicate replay. The test takes under 0.01 seconds; its individual timing samples have first-use noise and do not establish latency improvement.

The correction adds an ordered index of active grants under the existing manager mutex. Grant and release update it atomically. Conflict checks read current protected roots from the retained records, preserving root replacement, ancestry checks, conflicting-request FIFO, cancellation and replay. It changes only the reservation manager relative to the frozen epoch7 source. It does not reclaim retained history or remove replay protection. The [diagnostic receipt](RESERVATION-HISTORY-DIAGNOSTIC.md) and [independent review](RESERVATION-ACTIVE-INDEX-REVIEW.md) contain exact commands and hashes.

Twenty-six protocol tests and 164 applicable distributed tests pass. All three native and fresh Linux CLI builds pass. The unchanged local-writer regression remains explicitly filtered; this is not a claim that the complete unfiltered wrapper passes.

## Fixed construction comparison

Each arm uses the same retained FineWeb 100K vectors/order, 768 dimensions, squared L2, 32 workers, branching 100, split threshold 2,048 and merge threshold 512, full-precision construction navigation, one-bit posting codes and four-bit saved centroids. One initial 100K insertion interval is followed by balancing on the initial owner, then ordinary placement and saving. The peer holds zero vectors during balance and exchanges only 149 framed application bytes. Both machines have 32 physical cores and 512 GiB RAM; the idle peer remains required by the current CLI. Application caches start fresh, OS caches are not flushed, and seed/scheduler differences remain uncontrolled.

| Measurement | Before: epoch7 | After: active-grant index |
| --- | ---: | ---: |
| Add seconds | 0.176804 | 0.174616 |
| Balance seconds | 16.949309 | 13.292576 |
| Balance CPU-seconds | 56.38 | 51.84 |
| Placement seconds | 0.884262 | 0.829342 |
| Save seconds | 1.708609 | 1.646144 |
| Construction plus save seconds | 19.733662 | 15.957155 |
| Balance splits | 68 | 66 |
| Completed transferred rows | 112,754 | 108,395 |
| Counted navigation distances | 7,190,176 | 5,879,263 |
| Vector fetches | 7,808,025 | 7,933,784 |
| Completed balance jobs | 688 | 416 |
| Final leaves | 69 | 67 |
| Final smaller owner share | 44.648% | 44.512% |

Both runs validate complete unique 100K membership and saved state in a fresh reader, including vector and code checks. Each has one internal root and depth-one leaves. The reference 100K balance remains 4.962093 seconds and about 17 CPU-seconds, so the corrected runtime still has a large owner-local gap. Its different full-vector persistence path limits total-build comparisons; the balance comparison is the primary question here.

## Interpretation and next decision

Elapsed balance time falls 21.6% and balance CPU falls 8.1% in this pair. Navigation distances also fall 18.2%, while completed jobs fall 39.5%; these are independently constructed trees and schedules. The pair is descriptive evidence, not an isolated or repeatable 21.6% speedup estimate. The deterministic diagnostic proves that avoidable history scanning is removed. Keep that correction, while leaving its share of the 100K latency gap unassigned.

The next experiment samples CPU during the same corrected 100K initial-owner balance. It separates source-posting membership/compaction, navigation/copying, clustering/scoring, active reservation work and other CPU. It uses the existing executable and a bounded 10-second observation. Sampling measures CPU execution, not blocked time. Retain phase work counts and sample-coverage limits. No larger input or automatic tuning sweep follows.

## Evidence and resource closure

Raw before/after results and phase summaries are in [epoch9](../operations/window-epoch9/). Both job supervisors confirm cleanup. The peer stop helper encounters its recorded transient process-environment permission race in both arms; the original errors remain preserved. Independent exit receipts confirm cleanup, and AWS confirms both instances stopped. This collection failure does not turn a validated result into a clean launcher exit, and it does not justify rerunning the workload.

The closed window adds a compute upper bound of $0.9592716291809082 once. Covered research compute totals $31.924788907646596683. Identified recurring storage remains $19.52/month. These are task-specific receipts rather than an account-wide bill. No new storage is allocated.
