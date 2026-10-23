# Cache and vector-access findings

Vector access includes identity resolution, reader creation, block fetches, decoding, validation and copying. A low decoder timer does not establish that vector access is cheap, and a reported encoded-byte count is not automatically physical disk or network traffic.

## Decoded-vector retention

The 100K cache pair tests whether retaining immutable decoded vectors removes repeated access work. The [epoch11 assessment](../dynamic-review/100k-owner32-epoch11-assessment.json) compares the default 32-row cache with a 384 MiB payload budget. Elapsed time is 54.012 versus 40.709 seconds; reader opens are 377,590 versus 6,475, and copied vector bytes are 3,047,743,488 versus 279,220,224. Both arms pass membership and exact-bit checks.

The larger cache can contain the fixture's vector payload. It therefore does not establish behavior when a representative working set exceeds capacity. Split and reassignment counts differ between arms, and run order is fixed. Cache payload excludes caller references, indexes, provider caches, staging and allocator overhead; it is not a bound on process memory.

**Decision:** Retain explicit bounded retention as a useful mechanism. Diagnose representative cache pressure only when it is the stated question; do not infer a 100B gain from this cache-fitting fixture.

## Reader reuse and provider accounting

The [batch-reuse review](../dynamic-review/bounded-vector-batch-reuse-v1/independent-focused-review.json) covers a fixed sequence of 80 immutable requests. Reusing one reader for consecutive same-root requests reduces opens/decodes from 80 to 5 and encoded returns from 964,128 to 60,258 bytes, with identical vector payload. A 16-request alternating-root control stays at 16 opens/decodes. This is request-level reuse, not an end-to-end build gain.

The [provider-charge review](../dynamic-review/provider-byte-charge-v1/independent-focused-review.json) separates cache accounting from content correctness. Retained fresh-provider tapes show legacy 12/12 decodes over two passes versus byte-charge 12/0. A cache-clear accounting defect affects earlier fixture behavior; the frozen construction path has no corresponding provider-cache clear. That defect is not established as the cause of the construction slowdown. The [continuation snapshot](../archive/2026-10-19-document-consolidation/CONTINUATION-STATE.json) retains the adverse tapes, retracted explanation and source audit under `provider_byte_cache_work`.

## Routing metadata and postings

The full-record adapter stores leaf routing metadata separately from membership and compressed posting rows. Reading a leaf's metadata therefore need not load its posting list. This does not establish bounded owner posting residency, minimum storage reads or the absence of full-list copying during cleanup.

The [packed-centroid fixture](../dynamic-review/packed-centroid-fixture-v1/root-review.json) compares equivalent traversal over a fixed 132-node, 768-dimensional tree. Across 32 queries per pass, ordered IDs and distance bits match, with 4,192 distance evaluations per pass. Full-record requests fall from 4,672 to 480, but packed access adds 544 chunk-file reads, records zero cache hits and slightly increases returned payload. All three internal nodes are expanded.

**Decision:** Packed access is a plausible reference-aligned design, but this fixture establishes no byte or build-time benefit. Connected centroid updates, actual provider cost and useful pruning remain unproven. Avoid adopting it solely because record-request counts fall.

## Storage correctness

The [cadence validation review](../dynamic-review/reference-cadence-validation-v1/root-review.json) accepts fixes for split item accounting and identity preservation of an indivisible oversized posting block. Its 8,194-row, 32-dimensional fixture checks current contents and preserved old readers. This supports those storage mechanisms; it does not establish memory feasibility or throughput for the 100K-vector, 768-dimensional workload. Failed attempts remain in the [validation directory](../dynamic-review/reference-cadence-validation-v1/).
