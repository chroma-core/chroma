# Batch one source’s destinations on one peer owner

The smallest coherent candidate makes one transaction cover a source leaf and several destination leaves on one other owner. It keeps the existing two-owner acknowledgement sequence and removes repeated per-destination transactions and source compactions. General transactions spanning three or more owners, mixed local/remote atomic commits and partial destination acceptance are outside this first change. This is a design note only; no protocol or source changes are authorized by this note itself.

## Scope and expected work

Group only routes with the same complete source identity, run, epoch, peer authority, reassignment exclusion and producer context. Deterministically assign each ID to one destination before grouping, as the local bulk/reference path does. Keep current same-owner bulk processing separate. Bound the whole peer batch by the existing raw-code budget and pending-ID limit; partition oversized input deterministically, completing each transaction before the next. One destination group can span multiple chunks as it already does.

| The fixed eight-destination fixture | Current measured path | One proposed owner batch |
| --- | ---: | ---: |
| Protocol endpoint calls | 88 | 11 |
| Remote protocol calls | 48 | 6 |
| Remote destination routing lookups | 8 | 8 |
| Total remote calls | 56 | 14 |
| Source routing lookups | 8 | 1 |
| Total engine calls, including those routing lookups | 104 | 20 |
| Source compactions | 8 | 1 |

The prediction retains all eight destination routing lookups. It does not assume a new routing cache or batch-read API. The source snapshot appears once in the batch manifest. Code generation and destination bytes still depend on the same captured centroids. A batch must fit before admission; raising frame limits is unnecessary. These are work-count predictions, not measured physical latency or CPU gains.

## Immutable identity and state

Introduce an explicit versioned batch payload containing the source reference and centroid digest, destination owner, fixed exclusion/evacuation and producer context, ordered destination references and centroid digests, each ordered ID list, code lengths and the digest of all ordered codes/metadata. Broad routing revisions remain diagnostic; posting-only revision changes do not invalidate centroid-dependent codes. Source IDs must be globally unique across the batch. The phase fixes the complete payload under one attempt; preparation records both accepted and known-rejected outcomes so replay cannot become a different transaction after topology changes.

The source owner remains one endpoint role; the peer’s complete destination bundle becomes the other role. The existing `TransferDriver` still tracks two roles. Extract the payload-independent transition core from `TransferRecord` so the existing single-leaf record and new batch record share exactly the same prerequisites and replayed-step rules. Keep the old single-transfer API and tests. A batch endpoint stores the immutable manifest, prepared data buffers and one accepted outcome per step. A duplicate must bind the same run/epoch/attempt/kind/body and return that step’s retained outcome without repeating edits. A step request cannot reinterpret a single-transfer phase record as a batch.

No acknowledgement barrier disappears. The sequence stays two Prepare calls; destination Stage; BeginCompletion on both roles; source RemoveSource; destination ActivateDestination; Complete on both; Release on both in reverse owner order. The destination role’s Applied acknowledgement means every destination in that batch is applied. The driver’s existing proof booleans then keep their meaning. Status and lost-acknowledgement recovery refer to the complete batch, never an inferred subset.

Rollback remains limited to prepared or inactive staged data before the completion decision. Once completion is chosen or either membership edit may have occurred, resolve the same immutable attempt forward with retained status/acknowledgements, or fail the run closed. An unknown timeout is not proof that an endpoint rejected preparation or skipped an edit. No compensating reverse transfer or new attempt can erase that uncertainty.

## Protection and data edits

Reserve each owner’s complete affected leaf bundle in the same globally ordered owner acquisition order. Within an owner, take short resident locks in node order with an all-or-retry policy; do not hold one node lock while blocking for another, or wait for a split/transfer claim while holding the region bundle. Reject a conflicting claim promptly, return its existing producer identity and release uncommitted protection. No network call occurs under a resident lock, routing gate or operations mutex.

Under granted protection, revalidate live handles, placement, leaf type, exact centroid digests, current source membership and destination absence. Prepare all destination capacity and inactive code buffers before any membership edit. Prepare the replacement source posting list once from the union of accepted IDs. Claims protect postings and centroids while allowing compatible parent-only structural changes; installation must not overwrite newer parent metadata. Reuse the local/reference source scan, deterministic target assignment, code preparation and single-compaction logic as pure preparation helpers, not the local path’s immediate destination-append/source-replacement ordering.

At RemoveSource, replace the protected source posting list once. At ActivateDestination, install every prevalidated destination buffer under its retained claims. No allocation, capacity reservation or other fallible preparation can occur after the first destination edit. Publish current routing revisions while the corresponding resident guards still protect those edits. If an unexpected failure or unwind follows a partial edit, poison the run and retain unresolved evidence/protection; do not return an ordinary retry or allow save to succeed.

Admission uses one immutable all-or-nothing batch in the first candidate. If any proposed ID, target or dependency is invalid before completion begins, abort the cleanly prepared roles and return the entire still-pending proposal to existing current-membership filtering/rerouting. Do not silently delete a failed target from the admitted manifest. This is more conservative than the reference’s available-target selection: negotiating an accepted subset requires a separate fixed accepted-manifest decision and is deliberately deferred. After a fully acknowledged clean abort, bounded fresh smaller/single transactions may process unblocked targets if whole-batch contention proves harmful; unknown outcomes never permit that fallback.

Carry the actual scheduler producer identity into batch claims and clear only the matching attempt’s claim fields. Rejected protected work can use existing producer-aware deferral. Standalone calls retain existing finite progress bounds. A batch’s phase remains nonterminal until both local role records are safely released or aborted. Close/seal/save must count unresolved batch records and active handlers, just as single transfers do. Terminal checks should use exact role keys rather than scanning all retained history; replay records remain retained for the phase lifetime.

## Failure and validation gates

Use the accepted fixed fixture as the first oracle: the same 2,048-row source, eight destinations, 24 moves and existing/untouched rows. Require identical posting maps and recomputed codes, one source compaction, the predicted calls including routing reads, and unchanged peers on a pre-edit rejection. Preserve the existing pair path as a differential oracle, not as a performance target.

Before construction, test the following forced interleavings at real endpoint/driver seams:

- Repeated/changed Prepare and every step, lost successful acknowledgements, and late packets after seal must retain exact outcomes or fail closed.
- Reciprocal callers must obey owner order. A claimed, retired, reparented, moved, internal or changed-centroid target must not deadlock or lose/duplicate IDs. Parent-only changes can remain compatible.
- One unavailable destination must abort the admitted batch cleanly; a clean smaller retry must progress without replaying a changed body. Source IDs already moved must be filtered by a fresh attempt.
- Inject failure before edits, after source replacement and after the first destination append. Only the pre-edit case can abort ordinarily; partial application must block save and poison completion.
- Verify inactive staging, source-before-destination ordering, complete-bundle acknowledgements, exact region/claim release, queue drain and close/seal races. A local bulk arm and mixed local/remote sources must preserve the same current membership.
- Exercise an aggregate payload over the existing bound and a target group split across chunks. Each chunk must complete before another begins; no partial batch is reported as success.

This two-owner, one-source scope is the recommended first implementation if physical26 leaves transfer coordination material. It has one batch decision and reuses the existing two-role proof rather than introducing a multi-owner transaction coordinator. CPU attribution remains unknown: the fixed native loopback test establishes calls/scans/copies, not protocol-versus-transport CPU. The metadata dictionary comparison should finish before choosing the next physical benchmark.

Reference points are `distributed/engine.rs:1882–2170` (current pair driver), `:876–1285` (endpoint protection and edits), `distributed/protocol/transfer.rs` (state/proof rules), `distributed/local_transfer.rs` (available-target claims and one source scan), and matched-reference `writer/writer.rs:1865–2186`. Exact fixed-work evidence and its preserved setup failure are in `FIXED-TRANSFER-DIAGNOSTIC.md` and `FIXED-TRANSFER-DIAGNOSTIC-VALIDATION.json`. The note uses those frozen mechanisms and does not inspect or modify the concurrent codec experiment.
