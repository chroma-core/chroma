# Epoch15 local transfer, packed navigation and execution preparation review

The corrected source has no remaining blocker within this review scope. This review covers owner-local posting transfer, packed navigation publication and the narrow epoch15 execution-script changes. It does not establish a performance gain; the fixed 500K candidate run supplies that measurement after normal runtime admission.

## Membership transfer

The owner-local path follows the reference writer’s source-claim approach. It selects one deterministic destination per proposed vector, takes available source and destination claims without waiting while holding another leaf lock, rechecks current source membership and centroid-based eligibility, prepares vector reads and posting codes before editing membership, and compacts the source once across accepted destination groups. Unavailable destinations retain only current source IDs for a later attempt. Cross-owner changes retain the staged, digest-bound transfer protocol and chunking.

One concrete publication race is resolved. Releasing commit leaf locks before publishing their cloned routing images allowed an ancestor split to publish a newer parent link that the transfer could overwrite. The transfer now keeps its ordered commit leaf locks through routing-image publication. This matches the structural writer’s resident-lock-before-routing-gate order; publication performs no resident-lock wait or network call. A controlled test starts an actual parent split at this boundary and checks that every leaf’s resident and immutable routing image agree on parent and revision after both operations finish. A separate test covers parent replacement during transfer preparation.

The targeted tests also cover duplicate proposals, two destinations with one compaction, grouped construction dispatch, current-centroid eligibility, reciprocal callers, selective retries behind a split claim, vector-read failure before mutation, and fail-closed behavior after an injected partial mutation. Protocol-specific progress-budget tests explicitly exercise the protocol path so owner-local dispatch does not silently bypass their hooks.

## Navigation

Each immutable routing image contains matching packed child rows, child identities and leaf flags. Publication and deserialization rebuild those derived rows before readers can observe the image. The wire fields retain their existing order, and candidate order, metric and beam selection remain unchanged. Scoring borrows rows rather than cloning all child centroids and aggregates distance counters for the successfully scored prefix.

Packed rows duplicate the existing child-centroid metadata in memory. Shared-view preparation also rebuilds rows for unchanged shared nodes. These are explicit memory and preparation-cost caveats; no measured memory reduction or isolated end-to-end speedup is claimed. Tests cover row replacement and reordering, centroid refresh, invalid dimensions, wire roundtrips, coherent publication, exact distance counts and the existing reference candidate-selection oracle.

## Execution preparation

The epoch15 scripts select one explicit candidate-only 500K run with 32 workers and a 500K owner capacity, using the retained epoch14 input and reference result. They preserve the existing 600-second supervisor and 590-second workload bounds, fresh instance-window guards and cleanup. There is no size sweep, profiler or reference rerun. The supervisor’s process-exit correction skips an unreadable environment only after a second process-state read proves the process disappeared or became a zombie; a live unreadable process still fails closed.

The inherited epoch14 run template matches the intended settings. Final admission should retain its SHA256, `0d485e7d80b2960091d41cf36a047cddf7d2396f278b26ec48d0f4addb3f6fbc`, because the generator inherits some immutable workload settings from that file. Script hashes are bound by `operations/preparation-epoch15.json`. Runtime binary hashes remain a separate post-build admission requirement. The source packager recursively includes the new distributed Rust modules and tests; the final archive manifest must bind these same reviewed files.

## Validation and limits

The consolidated native receipt `epoch15-suite-02.log` reports 185 passed, zero failed and one filtered test in 5.26 seconds. It includes the new local-transfer and packed-navigation tests, TCP transfer/handoff coverage and existing routing parity tests. The filtered test is the known unrelated reference-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`. This reviewer inspected the receipt and source and did not run a competing Cargo process or any cloud operation.

The first integrated attempt is preserved in `epoch15-suite-01.log`: localhost socket tests met sandbox permission errors, and one real protocol progress-fixture mismatch required selecting its intended protocol path. The corrected consolidated suite runs through the normal approved localhost execution path. Operations preparation reports five focused tests passing in `epoch15-preparation.log`. Native CLI build, exact archive closure and remote binary readback remain the root operator’s admission checks.

## Reviewed source hashes

All paths below are relative to `rust/index/benches/hierarchical_index/distributed` in `/private/tmp/chroma-hierarchical-spann-sharding/single-shard`.

| File | SHA256 |
| --- | --- |
| `local_transfer.rs` | `71a0ca57543d7a94ec08947d64d1009c42ff31d4484307326196b787dbe5ee68` |
| `local_transfer_tests.rs` | `1833cd3f336bc0d33a35198d43c6032e93f862c21c556885eb951256fd0a5d1f` |
| `routing.rs` | `5fba403f826f39e05189d1e07ead7c70e897518765eecd0a6d95e340b04681f5` |
| `engine.rs` | `6d30f046a8915bf0f91c2796c8a42c8939589182c09a8fc2998a7dfd6508be6b` |
| `construction.rs` | `024ed760ff3b06a884536085bbccc15bcf9d509a7c63bf9ac9a754eaf0832cbd` |
| `shared_runtime.rs` | `03dc92f3d94afbff76e9861e6b5c00692ac94bcd24e563ae782678144f737547` |
| `placement_runtime.rs` | `fea5ee62aecb43c26421cbd62d4240fef478e250edcbe8c0e4159030662453cb` |
| `engine_endpoint_tests.rs` | `3720aa41588bc851f327604daeb3fb7fba8cbb330f270042d1f1d9775826d8f8` |
