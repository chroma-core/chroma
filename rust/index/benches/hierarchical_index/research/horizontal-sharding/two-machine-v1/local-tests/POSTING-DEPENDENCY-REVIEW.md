# Posting-transfer dependency review

The observed failure comes from rejecting prepared work after unrelated posting changes, even when the leaf's centroid and placement remain unchanged. The correction under review separates the properties needed to encode and transfer these vectors from the broader node revision used to protect structural changes. No repaired-performance claim is admitted before coherent source review and focused test results.

The deterministic endpoint reproduction fails in 0.01 seconds. A transfer prepared for vector 7 encounters unrelated additions of vectors 8 and 9. The centroid dependency is unchanged, but the current implementation rejects the transfer because the routing revision advances. The retained receipt is `posting-dependency-red-01.log`.

The 4,096-vector fixture uses 32 workers, a split threshold of 128, a merge threshold of 32 and branching factor 100. It reaches a shallow tree with concurrent reassignment into the same leaves. It fails with a membership-revalidation progress-budget error after repeated routing-revision rejection. At failure it records 37 nodes, 35 splits and 469,129 full-vector fetches. Its 65.85-second failure tail includes outstanding work after the first failure; it is not a successful construction timing. The retained receipt is `dense-shallow-red-01.log`.

The focused review must establish that transfer preparation binds the exact centroid contents, node type and placement needed by its one-bit codes, while membership checks still execute under the complete local region reservation and node edit lock. Source vectors must remain current and destination vectors absent. Real centroid, placement or node-type changes must reject the prepared work. Broad revisions must remain available for structural and other dependent operations. Duplicate attempt identity, acknowledgement ordering, fail-closed unresolved outcomes and epoch fencing remain unchanged. The scheduler must also cancel unneeded waits after its first failure and finish draining admitted handlers before reporting completion; it cannot mark unresolved work successful.

The implementation author remains the sole compiler and will supply the coherent correction snapshot and its focused results. This reviewer performs no Cargo or AWS execution.

## Corrected dependency and safety checks

The coherent correction has no identified source blocker for a new bounded correctness attempt. Transfer preparation compares SHA-256 over centroid length and exact floating-point coordinate bits, rather than rejecting every intervening broad node revision. It compares the current node identity and placement and requires leaf postings. Under the complete local region reservation and node write lock it checks every source ID remains present, every destination ID remains absent and the code payload has the required size. These checks precede endpoint admission and mutation. The immutable operation payload includes the new centroid digests, and duplicate equality still covers them. Broad node revisions continue advancing after posting edits and remain structural snapshot dependencies.

Failure handling polls owner health before repeated reassignment and in queued local region and transfer-region admission. A queued request cancels without data edits; if a grant wins cancellation, it releases that unmodified grant. Scheduler workers stop admitting further queued work after failure. Accepted transfer completion, unresolved-operation poisoning, saved-state rejection and existing bounded waits remain in force. This is fail-closed termination, not recovery of a failed writer.

## Focused retained results

The implementation author's `posting-dependency-green-01.log` records 143 passed and zero failed. Its dense fixture completes with 47 nodes, 45 splits and 349,263 vector fetches. The subsequent `posting-dependency-final-suite.log` records 144 passed and zero failed in 4.13 seconds, including the added adverse matrix. Its dense fixture completes with 46 nodes, 44 splits, 415,391 fetches and 2,659 completed jobs. Different clustering decisions and scheduling produce different work counts. The successful runs establish resolution of the mechanism and exercised correctness; their time or fetch ratios against the failed red run are not matched performance speedups.

The adverse matrix tests both source and destination against an actual centroid change at the same broad revision, placement mismatch, internal-node type and changed membership. Every rejection leaves routing and postings unchanged and leaves no resident reservation. The unrelated-addition test accepts preparation despite advanced broad revisions with identical centroids. The queued-failure test first observes the actual queued reservation, marks the owner failed, then requires a response within two seconds and a Cancelled outcome. Existing integrated transfer tests continue covering actual membership edits and codes. The reviewer inspects the retained results and source but runs no Cargo or AWS commands.

## Fresh execution namespace

The epoch3 operation copies retain the frozen workload, host bindings, reference binary and verified epoch2 corpus paths. They change output, job, shutdown-window and new runtime-binary namespaces to epoch3. New performance/reader hashes load from `runtime-binaries-epoch3.json`; a missing file prevents specification generation. The actual Linux build receipt and resulting hash file must be frozen and checked before admission. These scripts launch one explicitly selected arm with the existing absolute deadline and cleanup behavior; they do not retry or advance arms automatically. A new physical 125-vector correctness attempt must pass before the replacement failed 100K arm or remaining performance arms use the changed binaries.

| Reviewed file | SHA256 |
|---|---|
| `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/engine.rs` | `257c26b4cb9760be2efa331134f5e071a6b68657d4b6e38bc0938631f65d40c5` |
| `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/construction.rs` | `257c084b0e031133a4e054e8d0d7cb4a9acd3694266cfde10949de45a185b8d0` |
| `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/protocol/transfer.rs` | `fd157c8e414350439c1a84683860a1e06c530f046dd55f62f8cd4dabd50c0ad4` |
| `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/engine_endpoint_tests.rs` | `1f4336da44ba455bae447742ea4972f2c6a57ca9c901793cd5c3140dc4b1ae6b` |
| `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/construction_tests.rs` | `8b0b6420ddbbc69ef471b378bfde8b591f2785e7941472d70fbe4a4fb10ef61e` |
| `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/two-machine-v1/operations/make-performance-spec-epoch3.py` | `6635d299d22f88be21eb7d080215d75f9004e7e6984365fad17ce7f6837d1df5` |
| `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/two-machine-v1/operations/run-one-arm-epoch3.py` | `de393706ca4599e111dccabbedb7777266ce578ed1097e6b60b97884413cd2ac` |
