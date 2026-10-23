# Dynamic transaction review responses

The current implementation publishes immutable placement and topology transactions through one deployment-wide fence. The milestone-v7 source and exact Linux output are frozen in `milestone-v7`; its source archive SHA-256 is `0850357977a389d97e6897f0d403a024f68fa4b183ab36a501741b1b5cad6fa1`.

## Astra 02 correctness findings

1. A cosine topology transaction rejects a centroid whose arithmetic mean cancels to zero before lower-tree construction and publication. Dynamic tree validation independently rejects zero cosine routing centroids. The real metric fixture uses opposite nonzero raw vectors, proves that the merge leaves the published pointer unchanged, and verifies a valid cosine reassignment and an IP merge against complete exact references. The deployment explicitly binds little-endian FP32 row interpretation and the worker's metric-specific normalization policy.

2. Replica preparation fsyncs every file, then child directories, the replica root and its parent chain before recording readiness. Changed topology output receives the same treatment; publication also fsyncs the model and canonical corpus file entries. A focused test checks ordering and injects directory-fsync failure before transfer readiness. These tests prove the exercised filesystem calls and failure gate; they do not emulate an actual machine power loss or certify every filesystem implementation.

3. Transaction reconciliation treats a successful publication receipt as definitive even when a descendant becomes current. When publication raises before returning a receipt, reconciliation follows the current deployment's immutable, hash-checked parent chain. It preserves uncertainty when commitment cannot be established. Real transaction fixtures inject both a post-commit descendant interleaving and a journal-write failure, then verify that the transaction is never mislabeled unpublished.

4. Placement admission counts logical subtrees in addition to storage, memory and load. Each worker defaults to the existing 128-subtree request bound and can declare a lower limit. The heterogeneous-capacity regression assigns 128 unit subtrees to the larger worker and the remaining subtree to the smaller worker rather than preparing an unpublishable placement.

## Additional acceptance evidence

The Linux gate passes 23 tests in 14.605 seconds, including fourteen real Rust-backed test methods across L2, inner product and cosine. The log prints the actual immutable V4 binary hash. Tests cover separate-process placement versus prepared-topology contention, generation-specific HTTP endpoint readiness, exact and bounded direct/HTTP trace parity after each topology change, complete snapshot audits, old pinned readers, append after split, source-offline migration followed by another topology change, and durable failure journals. A targeted corruption fixture identifies raw blocks referenced by the vector sparse index, removes those private replica blocks, demonstrates successful root opening followed by failed lazy reranking, and verifies that publication is rejected.

## Remaining scope

The authority and replica-transfer implementation use a local filesystem and trusted local worker processes. Cross-host transport supervision, authenticated admission, production consensus, automatic telemetry collection and garbage collection remain unimplemented. Old serving generations are retained until their caller explicitly closes them. The canonical live-ID contract is a dense prefix; sparse IDs, document deletion and replacement of existing document values require another explicit contract. Resource plans use declared estimates and bounded rebuild input, rather than claiming measured cluster capacity. Repeated append and a full crash/restart campaign remain additional acceptance work.
