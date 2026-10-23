# Streaming candidate review contract

The streaming operation must return the same candidates while bounding the additional arrays it owns. Provider decoding, loaded block caches, allocator retention and process RSS remain separate limits unless they are measured and enforced directly.

The complete request first validates selected-leaf uniqueness, query shape and finiteness, budget and capacity. Persisted posting ID/version/code lengths must agree with checked arithmetic. A physical-entry admission must count stale and deleted entries before allocating corresponding owned arrays; a limit checked after provider decoding does not bound provider memory.

Every physical row is checked against its authoritative version. Missing or out-of-range versions fail the whole request; deleted or superseded versions contribute no candidate. All current copies of an ID across selected leaves contribute to one minimum approximate distance. Candidate count means unique current IDs, while physical rows and version checks have separate counters. The operation preserves every tie at the candidate-budget boundary before the coordinator maps local IDs and applies its global-ID ordering.

A candidate-capacity limit must reject the complete operation rather than truncate results or ties. Late malformed data, nonfinite scores, version failures and resource-limit errors return no partial candidates. Operation-owned posting arrays and provider pins are released on both success and failure; a subsequent valid request must succeed and match the retained reference path. Cleanup counters measure released materializations, not an operation peak or RSS bound.

The smallest acceptance fixture compares the exact response and work counts with the reference over repeated IDs with unequal residual scores, stale/deleted copies, current-version duplicates, cutoff ties, empty leaves and malformed late leaves. It also tests physical-row and unique-candidate limits independently, checked size arithmetic, exact-limit success and one-over-limit failure. The source and binary are reviewed before native execution. No default behavior changes or performance claims follow from these correctness fixtures.
