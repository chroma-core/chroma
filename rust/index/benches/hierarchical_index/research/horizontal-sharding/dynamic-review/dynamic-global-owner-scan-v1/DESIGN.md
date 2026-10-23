# Scan globally discovered neighbors at their local owners

This slice sends eligibility scans to distinct local owner threads while preserving global neighbor discovery and the reference predicates. It operates on one stable writer-batch snapshot. It returns ordered candidate identities; it does not move memberships or run a complete mutation-interleaved split transaction.

## The pieces

- The reference writer supplies the actual global navigation result, including order, beam policy, per-level widths, neighbor limit and excluded old/replacement leaf identities.
- The placement directory maps every globally discovered leaf to its owner. A missing directory entry or owner fails the operation.
- Each local owner thread receives owned copies of its source postings, current versions and available vectors at bootstrap. It scans its own source data. Requests contain centers and previously evaluated identities rather than centrally computed eligibility results.
- Shared predicate helpers preserve the exact full-precision and quantized reference arithmetic. The existing reference loops still perform their original version checks, deduplication, evaluation count updates and immediate reassignment in their original order.
- A bounded report stores the actual request, response, source hash, owner, placement epoch, sequence and worker-thread identity for each dispatched neighbor.

## The scan flow

1. **Capture a stable source.** Check that the reference tree still matches the directory image. Copy source postings, versions and vectors into owner-specific data. Full source bootstrap is charged separately.
2. **Discover neighbors globally.** Invoke the actual reference navigation policy around the old center. Apply the same neighbor-count limit before skipping the old and replacement leaves.
3. **Dispatch in reference order.** Resolve each leaf through the directory and send one request to that owner thread. Wait for its result before dispatching the next neighbor so cross-leaf duplicate suppression preserves reference scan order.
4. **Evaluate on the owner thread.** Reproduce reference scrub's swap-removal order, version comparisons, evaluated-ID insertion, missing-vector handling and exact predicates. Return ordered inserted/evaluated and eligible identities.
5. **Verify the result scope.** Match owner, leaf, source hash, epoch and request sequence. Preserve the reference image and return the bounded scan report.

The reference scrub and scan compare stored versions with `current_version.unwrap_or(0)`. This slice deliberately preserves that behavior, including missing-version default zero and an artificial matching high-bit deleted version. It does not strengthen the predicate or claim such a returned identity can be reassigned: the existing reassignment path separately checks validity. The tests cover these cases explicitly.

## Correctness evidence

The paired local test constructs one ordinary 64-vector reference tree, places its existing leaf branches across owners, and chooses two actual vectors from different owners as replacement centers. The old center is deliberately set to a distant finite value to produce affected candidates across the owner boundary; this is a controlled eligibility test, not a captured real split event. Global discovery visits actual reference leaves. The test requires eligible identities from at least two owners and distinct worker-thread identities, then compares the complete ordered discovery, evaluated-ID and eligible-ID outputs with a one-owner control on the same exact tree and centers.

The actor contract test compares full-precision and quantized results against an independent transcription of the pre-extraction arithmetic for both Euclidean and inner-product metrics. It checks stale, missing, matching-deleted, duplicate and missing-vector cases, preserving scrub and evaluation order. Missing owner, wrong owner, malformed source and wrong source-hash cases fail without central fallback. Two existing reference reassignment tests verify saved version advancement and refusal to invalidate a current copy when its target disappears.

## Bounds and accounting

The source fleet admits at most 128 owners, 100,000 physical source entries, 100,000 source leaves and 4,096 dimensions. Copied logical source payload is capped at 64 MiB after bounded source construction; this is not a preallocation or RSS bound. Each owner has a one-entry request channel. Requests contain at most 100,000 previously evaluated IDs; the report rejects aggregate serialized request/response payload above 8 MiB. Each response wait has a five-second timeout. Local thread teardown joins owned threads; no hard aggregate timeout or process-failure cleanup guarantee is claimed.

Bootstrap reports the byte lengths of copied centroid, ID, version, code, current-version and vector payloads. It also reports source JSON length separately. These are logical payload counts, excluding allocation headers, capacities and allocator overhead. Ownership handoff moves these Vec-backed values into the owner thread; there is no shared Arc source or remote network transfer. Each scan clones its source before scrubbing, and the response reports that pre-scrub clone payload. Request/response JSON lengths describe retained evidence, not actual wire traffic: local channels move Rust values. Hashing and bounded-report serialization can allocate additional temporary buffers.

Full source capture, complete tree equality checks and membership scans remain centralized whole-tree work. The counters do not measure all CPU, reads, cache traffic or allocation. No latency, quality or capacity improvement is claimed.

## Remaining integration

The opt-in `scan_global_neighbors` operation uses actual global discovery and actual owner execution, but ordinary insertion and split callbacks do not automatically invoke it. Construction remains centralized. The default reference path uses the shared predicates and preserves its immediate `reassign` calls. The local scan API does not apply reassignment between rows, so it does not claim equivalence with all side effects of a complete split or recursive reassignment event.

A next construction slice must dispatch at the live split callback, preserve visibility of versions changed by earlier reassignments, coordinate global destination navigation and versioned registration, update directory coverage after structural edits, and implement cross-owner merge/parent updates. These are explicit missing interfaces, not completed behavior. No cloud, controller, production protocol, saved-posting census or matched two-million-vector quality result is part of this package.
