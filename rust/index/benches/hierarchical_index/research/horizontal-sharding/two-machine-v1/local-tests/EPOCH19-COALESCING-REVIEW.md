# Epoch19 deferred reassignment coalescing review

The coherent epoch19 change has no remaining blocker within this review scope. It combines compatible queued reassignment work before dispatch and preserves current-membership revalidation during execution. A controlled fixture establishes reduced repeated work for fixed decisions; the fixed 500K benchmark remains necessary to measure the effect on randomized construction.

## Identity and lifecycle

The queue retains the hash of each original incoming request before normalization. An identical replay adds no work and allocates no new execution identity; a changed body under the same incoming identity rejects. A fresh accepted request receives a private coordinator execution envelope only after request bounds, replay, capacity and epoch checks succeed. Normalized IDs and later pending unions therefore do not change an operation body that an owner may already have executed.

At dispatch, compatible private queued envelopes become one selected execution body. Absorbed envelopes have never entered owner mutation admission or the active set, so discarding them leaves no unresolved operation. The selected attempt enters the active set before its request executes and remains active until its owner acknowledgment and claim cleanup finish. Its body stays fixed for replay. New arrivals during execution remain separate queued work and cannot mutate the active body. Incoming attempt authority and execution authority remain distinct; source ownership still controls which owner receives the request.

## Compatibility and progress

Coalescing requires the same complete source reference, run, epoch and exclusion choice. The source reference includes node identity, owner and placement. It absorbs only jobs whose producer dependencies are currently inactive, never active work, and never crosses an intervening balance or neighbor job for that source. The union contains at most 32,768 unique IDs; an oversized union leaves the candidate queued. The retained incoming-request table also has an explicit bound. Rejected requests and identical replays allocate no new execution envelope.

The selected IDs are sorted and deduplicated, as in the reference retry grouping approach. Actual execution still filters current source membership, navigates the current tree, checks current centroids and preserves exact posting ownership and codes. No per-vector persistent directory or new cross-owner transfer protocol is introduced. Queue producer checks and structural barriers remain necessary; combining queued work does not establish general fairness or eliminate every cause of retries.

## Evidence and measurement limits

The fixed-tree execution fixture applies the same three incoming ID groups individually and through the actual queue and scheduled execution path. Both produce identical memberships and one-bit posting codes. Current-membership filtering leaves five actual query rows in the individual path and three in the combined path; reassignment rounds fall from three to one. These are measured work reductions for that fixture, not a randomized build speedup or a guarantee that every removed pending ID saves a query.

The six new tests cover active and producer-blocked work, independent readiness, same-source structural and neighbor barriers, exclusion and owner-placement separation, stale run and epoch rejection, immutable incoming replay and conflicting bodies, fresh arrivals after dispatch, immutable execution replay, union size and retained-request bounds. Existing source-claim and cross-owner tests remain in the applicable suite.

The benchmark reports per-balance dispatches, extra queued jobs combined and duplicate IDs removed between combined requests. The duplicate counter excludes normalization within one incoming request. It counts pending-set overlap, not guaranteed executed rows saved. Queue union construction sorts bounded temporary ID vectors while holding the scheduler mutex; its CPU and allocation costs remain part of the measured patch. Tree shape, clustering work, retry counts and phase CPU/wall remain necessary when interpreting the next run.

## Validation and execution preparation

The focused actual-execution fixture passes in `coalescing-focused-01.log`. The applicable native suite reports 203 passed, zero failed and one filtered test in 6.55 seconds in `epoch19-suite-01.log`. The exclusion remains the known unrelated reference-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`; an unfiltered clean suite is not claimed. The root agent runs Cargo, including separate native CLI builds and final source closure checks.

The epoch19 adapters differ from epoch18 only in fresh namespaces and prior-result/binary bindings. They retain one explicit candidate-only 500K run, 32 workers, 500K owner capacity, the verified epoch14 input and five 100K construction intervals. The existing 600/590-second supervisor/workload limits, independent shutdown guards and cleanup controls remain unchanged. The epoch15 supervisor is reused exactly. There is no profiler, reference rerun, size sweep or automatic retry.

Every hash in `operations/preparation-epoch19.json` matches at review time, including the completed epoch18 run template SHA256 `b53ca2ac9f5a04560006f5c5197bd26c48aa3b0107fcb7bff4b2450f247e3a82`. The five preparation tests pass in `epoch19-preparation.log`. Final configuration status and source hashes are admission metadata; the root operator must bind their final bytes while preserving the reviewed settings and limits. Native CLI artifacts, final archive closure against epoch18 and fresh Linux binary hashes remain separate admission checks. This reviewer runs no Cargo, cloud operation or Git write.

## Reviewed source hashes

Distributed paths are relative to `rust/index/benches/hierarchical_index` in `/private/tmp/chroma-hierarchical-spann-sharding/single-shard`; the benchmark file is in `rust/index/benches`.

| File | SHA256 |
| --- | --- |
| `distributed/construction.rs` | `6a45fbcfcc6625752349a617d5b26dc175ea4c34b712e63f3451c9bb2dd37039` |
| `distributed/engine.rs` | `9a827a653ac06af26f1a4c8a89bd021fa34d930b9304477eab84d2f0f7dd72f3` |
| `distributed/source_leaf_claim_tests.rs` | `c0fa217b3091309c99c2d1b1d5507d739e3705341f8fe0bdcb335a00a1d29ede` |
| `distributed/reassignment_coalescing_tests.rs` | `1ecdc4ef163e12663f8224af6a3a650c25930658e0863a07b391c2e68cc0a68e` |
| `two_machine_spann.rs` | `4ef00edafbad3545e0633223d8677b318ad2bffa603c6ae19102eba4bed4c975` |
