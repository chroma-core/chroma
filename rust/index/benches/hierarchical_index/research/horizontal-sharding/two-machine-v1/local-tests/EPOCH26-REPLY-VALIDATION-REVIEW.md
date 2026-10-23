# Epoch26 remote reply validation review

The reviewed local candidate has no remaining correctness blocker. It removes repeated child-row scans and complete reply-body clones while preserving the current navigation acceptance rules. This review performs no Cargo, cloud, SSH or Git operation. Source closure, archive creation and remote admission remain the root agent’s responsibility.

## Reviewed source

Direct comparison against frozen `source-epoch25b.tar.gz` covers only the following two files. Their current hashes match the implementation checkpoint.

| File | SHA256 |
| --- | --- |
| `rust/index/benches/hierarchical_index/distributed/routing.rs` | `0d36a2527523acf2adec9afbb90e601ae3ea8aa53d754607670e883209e1844d` |
| `rust/index/benches/hierarchical_index/distributed/routing_tests.rs` | `8353c35cb7f8f29cd52df350f58d1f7f40c8f367ffb5c2989ef8c9a705ffffbc` |

## Correctness and bounds

The coordinator validates reply identity before considering duplicate suppression. Owner, run, epoch, shared sequence, navigation attempt, step, query slot and complete status checks remain intact. The duplicate index borrows ordered parent keys and complete immutable reply bodies from a stable response collection. An identical duplicate contributes no candidates; a differing body for the same key fails. Reordered parent sets still encounter overlap rejection rather than silently bypassing coverage. Rust lifetimes prevent moving the borrowed bodies before the index is dropped; no unsafe code or mutable alias is added.

Each first-seen reply validates complete parent coverage in one child-row pass. Declared parent identifiers must be unique and cover exactly the returned nodes. Their checked sum must equal the actual child-row length before ordinal storage is allocated. Every returned node must match its expected full identity, including authority and placement, and appear only once across accepted chunks. Each child must name one declared parent, have a finite score and one in-range unused ordinal, and agree with the other children of that parent on observed revision. Final per-parent row counts require complete coverage. Zero-child parents remain legal, matching the existing behavior.

Allocation remains bounded by actual received metadata and child rows. The ordinal vectors’ combined declared length equals the already received child-row count, so an unchecked remote count cannot request an independently large vector. Parent maps and the expected-frontier map add linear metadata space. The borrowed duplicate index and acceptance flags replace full cloned replies. Ordinary allocator failure is not newly recovered; existing frame and codec limits remain the outer wire bounds. The optimization does not eliminate all allocation or candidate movement.

Accepted child vectors move into candidates only after all returned replies pass validation. Missing expected parent coverage still rejects the complete navigation attempt before any result is returned. A later invalid reply cannot expose a usable prefix. Canonical parent/ordinal assembly, full candidate selection, reference unstable score sorting and tie input order, tau/beam rules, structural retry checks, wire encoding and adaptive framing are unchanged. Mutating temporary coverage state before an error is safe because the whole attempt is discarded.

## Evidence and measurement limits

The actual TCP fixture exercises64 queries with16 remote parents and100 children per parent, wide identifiers, equal scores, signed zero and mixed leaf/internal rows. Every output matches the actual reference writer’s navigation oracle. The fixture also checks reversed and duplicate replies, missing/partial rows, conflicting duplicates, stale/publishing status, incorrect identity fields, duplicate ordinals, nonfinite scores and overlapping chunks after replies originate from the real remote owner. Separate coverage tests exercise count overflow, oversized declarations, repeated/unknown parents, wrong placement/authority, mixed revisions, invalid ordinals and legitimate empty parents. Existing tests retain partitioned reply and canonical assembly coverage.

The fixed workload receives103,424 rows. The retained red fixture records1,750,016 coverage-validation visits and8,421,376 logical reply-field clone bytes. The candidate records103,424 coverage-validation visits and zero reply-body clone bytes. These counters establish the removed scan and clone mechanisms. Coverage visits exclude the separate equality traversal for exact duplicate bodies, metadata checks and candidate work. Clone bytes describe the instrumented logical field copies rather than all heap allocations or transport bytes. This distinction is agreed with the implementation author and must remain in the final receipt.

The retained timings do not establish a latency gain. The first red observation is0.469091 seconds and the first green observation is0.488146 seconds; subsequent green fixture observations range from0.468536 to0.476965 seconds. These are individual native fixture observations, not a repeated matched performance study. The candidate is justified by removed work with preserved decisions, pending physical measurement.

`epoch26-routing-focused-02.log` records23 routing/codec tests passing, with zero failures. `epoch26-expansion-focused-01.log` records eight bounded-expansion tests passing, including actual TCP, fitting64-query batching, complete-parent splitting, singleton oversize failure, one absolute deadline, and no retry or prefix return for ambiguous codec/EOF failures. The implementation author reports all three native CLIs build successfully in16.88 seconds; the root agent must bind the final validation receipt and artifacts before admission. No unrelated full-suite result is claimed here.

No epoch26 cloud run is admitted by this review. The running epoch25 save experiment and its evidence remain separate. A future physical comparison must retain the same input, construction settings, wire format and work accounting and preserve adverse results.
