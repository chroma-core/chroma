# Epoch25 bounded concurrent owner save review

The reviewed candidate is ready for the root operator’s final admission checks. I found no remaining correctness blocker in the bounded concurrency change or its prepared workload. This review covers source and retained native test receipts; it performs no Cargo, cloud, SSH or Git operation and makes no performance claim before measurement.

## Exact reviewed source

The final candidate archive is `packages/source-epoch25b.tar.gz`, 23,081,449 bytes, SHA256 `fe3fd6d0b0ebe475f50a7e4fac1c0263521dddf1cbba002997bb520b3d2a639c`. Its manifest SHA256 is `290324334047bf741da46641397969416206db7ce852598c3921b43f3ab4bda2`. Direct archive comparison against frozen24b finds exactly these three changed or added files and no removals. The initial25 archive is retained and is not this admission target.

| File | SHA256 |
| --- | --- |
| `rust/index/benches/hierarchical_index/distributed/save_runtime.rs` | `edb85623b069871f26ffc44242cb2b46fcdef864ffea2a83382befda363e773a` |
| `rust/index/benches/hierarchical_index/distributed/save_concurrency_tests.rs` | `f294afe938fded64289a3d4b7a05a3fb749660aab9cb3bff0be3f702cfa9dec6` |
| `rust/index/benches/two_machine_spann.rs` | `cc342498c44ae96f2acf5b8b1152a934e63e32dd9e18133554fb9852f3708ef7` |

## Save ordering and failure behavior

Concurrent work begins only after every owner acknowledges close and seal. The coordinator then captures one immutable shared view, owner-root coverage, precision/metric contract and vector descriptor, and constructs every owner request from those values. It holds no owner node or scheduler lock across the scoped calls. Each owner retains its existing quiescence, request-identity and immutable-write checks. The local owner writes its final namespace; remote owners write their separate staging namespaces.

At most two owner calls start per scoped batch. Every started handle is joined before results are inspected, including when another call returns an error or panics. A failed batch prevents subsequent batches and all copying or combined publication. Successful results retain sorted owner order, so the subsequent copy and validation order remains deterministic. There is no detached writer when the coordinator returns. The scoped panic conversion is supported by source inspection; the new deterministic failure fixture injects an ordinary owner error rather than a thread panic.

The barrier verifies successful owner acknowledgments and their owner identities before any copy. Full manifest and payload integrity checks remain in the serial copy loop: local immutable objects are verified in place; remote payloads are copied with existing checksum checks, followed by manifest identity and reference validation. This is not prevalidation of every owner payload before the first copy. No combined generation or latest pointer is published until all these checks and complete generation validation succeed. Failed materialization can leave immutable evidence, while the previous generation remains readable and its publication pointer remains unchanged.

The candidate preserves serial object copying, owner-root coverage checks, format/vector checks, immutable naming, file/directory synchronization, manifest-last generation creation and latest publication. The benchmark’s third-file change only clarifies measurement labels. `owner_calls_nanos` remains the sum of overlapping individual call wall times; `owner_materialization_wall_nanos` measures the joined elapsed phase; `owner_materialization_max_concurrency` is the configured cap, not observed CPU or disk concurrency. Summed call times must not be added to joined wall time. Nested storage synchronization times must not be added again.

## Validation evidence and limits

`OWNER-SAVE-CONCURRENCY-VALIDATION.json` records nine save-runtime tests passing, including both new concurrency fixtures and existing chunk/replay/readback checks, plus the existing failed-owner-save fresh-process regression passing separately. It records three native CLI builds passing and a successful writer rebuild after the label-only clarification. This focused scope is appropriate because the production change is confined to saving; no unrelated full suite is claimed.

The success fixture uses an actual local owner and an actual localhost TCP owner. A bounded gate requires both calls to start before either may materialize, so a serial implementation cannot pass. Holding the remote reply after both materializations proves that no remote payload copy, combined manifest, latest pointer or descriptor appears early. Releasing it produces a generation whose memberships and posting codes validate against a separately opened vector source. Replaying the same save returns the identical descriptor with no new immutable owner names.

The failure fixture makes the peer fail while the local writer remains held. The coordinator does not return until the remaining writer is released and joined. The failed generation remains absent; previous latest and descriptor bytes remain exact; the prior reader’s memberships and codes still validate. These tests establish overlap and publication ordering without a timing threshold. They do not establish a save speedup, physical disk parallelism, or more-than-two-owner performance. Concurrent snapshots can increase peak memory and write contention; the cap limits exposure to two calls, and remote copying remains a separate serial cost.

## Prepared remote comparison

All file hashes in `operations/preparation-epoch25.json` match the reviewed preparation snapshot. The generator admits exactly one explicit candidate3M arm with fresh25 paths and runtime bindings, retaining the successful24 template and valid21 reference result. The workload remains 3,000,000 vectors, 768 dimensions, L2, 32 workers, 100K additions through the first million and one million thereafter, capacity 2,160,000 per owner, and no first-owner override or reserve. The retained epoch2 descriptor and corpus digest remain bound. The final30% owner-share check and actual final-million work on both owners remain required evidence.

The runner admits only an open fresh25 window with at least660 seconds remaining and fixes the absolute job deadline to590 seconds. The unchanged15 supervisor, peer cleanup/error receipts, matching reader/writer hash registry, and candidate-only reviewed-source build checks remain in place. The preparation adds no sweep, automatic retry, reference rerun, profiler, new storage or deadline extension. Its five mocked checks pass in `epoch25-preparation.log`; mocks do not establish cloud identity, guard readiness, actual binary equality or input integrity.

The root operator must bind the final archive, native artifacts, finalized config metadata, fresh Cops identity, resource/rate checks, both independent shutdown guards, remote binary/input readbacks and spending before execution. Final admission metadata updates are separate from the reviewed workload settings. The planned window remains at most1,800 seconds and $4.032 of task compute. No cloud execution is authorized by this review itself.
