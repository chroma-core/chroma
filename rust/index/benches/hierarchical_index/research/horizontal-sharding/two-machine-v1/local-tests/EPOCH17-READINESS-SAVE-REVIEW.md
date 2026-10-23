# Epoch17 scheduler readiness, save profiles and execution preparation review

The coherent epoch17 source has no remaining blocker within this review scope. Deferred reassignment now waits for the exact scheduled operation that holds a posting claim. Save changes add coarse measurements without changing stored data or durability ordering. The fixed 500K benchmark still determines whether the scheduler correction improves performance.

## Dependency readiness

A scheduled split can retire its original leaf and then synchronously move a neighbor’s postings. The active queue entry names the original split, while the current claim names the neighbor. Using the neighbor node ID as a proxy for the producer lets retries run before the producer finishes. The controlled regression reproduces this through actual split, navigation, neighbor reassignment and claim acquisition; it does not inject producer metadata directly.

The queue dispatch now supplies its existing operation attempt explicitly through a single scheduled-job wrapper. Synchronous split, neighbor, reassignment and merge calls pass that identity to their posting claims. The coordinator tracks the full authority and sequence of active attempts, withholds dependent retries while those attempts remain active, and removes an active attempt only after the owner response and claim cleanup finish. Independent work remains runnable. The wrapper replaces the previous job request and introduces no per-transfer coordination request. There is no thread-local context to lose across a network call or nested execution.

Direct unscheduled APIs do not invent a producer identity. Their untracked claims retain the bounded reassignment retry path. Cross-owner known rejections return an explicit producer attempt through the existing error response; parsing preserves both authority and sequence. Cross-owner membership transfer payloads, protected commit and release acknowledgments remain unchanged. Claim cleanup clears producer metadata only when its reservation token still matches, preserving replacement claims and fail-closed handling of incomplete mutation.

Tests cover the actual retired-source/neighbor-claim regression, independent work during a blocked retry, withholding through owner acknowledgment, reciprocal dependencies, equal sequence values on different authorities, and an unscheduled claim that reaches the existing retry bound without losing membership. The unscheduled fixture reads resident memberships while its claim is held, then releases the claim before requesting a full snapshot; production snapshot drain checks remain intact.

## Measurement meaning

The new dependency counter counts queue visits withheld by an active producer attempt. It counts neither unique jobs nor elapsed waiting time. The benchmark captures its per-balance before/after delta outside the timed balance boundary. The new local-transfer admission scope includes identity serialization and admission. Cleanup includes its nested terminalization scope, so cleanup and terminalization times must not be added together. These timers add observation overhead to this epoch; results describe the measured patch rather than an isolated scheduler-only speedup.

The readiness fix addresses a concrete cause of repeated attempts but does not coalesce compatible deferred jobs or prove full worker utilization. Remaining scheduler hypotheses are recorded in `SCHEDULER-PATH-AUDIT.md`. Work counts, phase CPU and wall time remain necessary when comparing independently randomized construction.

## Save observations

Save behavior retains the existing sequential close/seal, owner materialization, owner-object copies, final manifest and publication order. Coarse wall timers separate those phases. Owner materialization excludes the measured snapshot-copy interval; store synchronization times are nested measurements rather than extra phases. Some setup and store-open work lies outside the coarse intervals, so their sum is an attribution aid rather than an exact partition of the outer save timer.

Store counters belong to one store handle and its clones. They count supplied immutable payload bytes, completed temporary writes, names created or replayed, and synchronization attempts and time. They do not measure physical device bytes. A repeated SaveOwner request checks its frozen identity and performs a new invocation rather than returning a cached receipt; its profile therefore includes that invocation’s replay writes. The two-leaf fixture checks exact object and synchronization counts for owner staging and coordinator copying, confirms that duplicate local writes remain visible in the counts, and reopens the result with a fresh reader. No save optimization or reader-format change is included.

## Execution preparation

The epoch17 adapters retain one explicit candidate-only 500K run, 32 workers, the 500K owner capacity and the verified epoch14 input. They use fresh namespaces and binary bindings while inheriting the completed epoch16 workload template. The 600/590-second supervisor/workload bounds, independent shutdown guards, exact sandbox resources and cleanup controls remain unchanged. The epoch15 supervisor is reused byte-for-byte. There is no profiler, size sweep, reference rerun or automatic retry.

Every hash in `operations/preparation-epoch17.json` matches its file, including the inherited template SHA256 `f2c7eb800121d4b17702c07033660a63bd9bba904ff3067924c2c2a06e4d43cd`. The five preparation tests pass in `epoch17-preparation.log`. Native CLI builds, final source-archive closure and Linux binary readbacks remain separate root-operator admission checks; this review does not extend an execution window.

## Validation and preserved failures

The actual scheduler regression fails in `scheduler-readiness-red-01.log` and passes in `scheduler-readiness-green-01.log`. The save-profile fixture passes in `save-observability-02.log`; the first attempt remains in `save-observability-01.log` and corrects only a fixture expectation about its root node count. The consolidated corrected suite reports 192 passed, zero failed and one filtered test in 5.36 seconds in `epoch17-suite-02.log`.

The first epoch17 suite remains in `epoch17-suite-01.log`: 191 tests pass and the new unscheduled-claim fixture fails because it requests a full snapshot while deliberately holding a claim. Its correction changes the fixture’s observation, not the production drain rule. The same known unrelated reference-writer test, `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`, remains filtered. This review does not claim the unfiltered suite passes. The root agent runs all Cargo and cloud operations; this reviewer inspects source and receipts only.

## Reviewed source hashes

The distributed paths below are relative to `rust/index/benches/hierarchical_index` in `/private/tmp/chroma-hierarchical-spann-sharding/single-shard`; the benchmark file is in `rust/index/benches`.

| File | SHA256 |
| --- | --- |
| `distributed/construction.rs` | `b5df72d9501218d02b01c5fd71e24527a5916977632bd6ee26612a822d721fa2` |
| `distributed/construction_tests.rs` | `9d952cc4502b66cf8f6cadc5168e18abbc1a18e7ec872eed8b52a2cc9fef8588` |
| `distributed/engine.rs` | `860afc2259b363b210e92dbeea54ca57226d01200e45257fc626f06964f40482` |
| `distributed/generation.rs` | `2ced58f568ba6aa07744670b9ab7c631d3c507d579cca85a038b89c9b2d856aa` |
| `distributed/local_transfer.rs` | `31f845b0d6508dc5388fa6bebb5845d6b4afbd7f11e700b65804e78f4049e962` |
| `distributed/observability.rs` | `52d1ee2c1feeefce9f70ee79e4dcea07a8b2000c64c3df7daec3637249b846dd` |
| `distributed/placement_runtime.rs` | `2bbc2ecd402433b5e91defc68de7b6e64ad069d1cb6ae180e19221e593f5c30e` |
| `distributed/save_runtime.rs` | `e7b0d16350e810d3b0c4f77e9bac3fa831cf199708cf9736300fca75bff62021` |
| `distributed/source_leaf_claim_tests.rs` | `f5ed8c62766e985e59704fe1fe0eb96947c680bd531877f7928c81e8c048e1af` |
| `two_machine_spann.rs` | `fdca1dfbf2cd3c2cdd83a5cbda694109208609218bf18f73b85c6457ad527039` |
