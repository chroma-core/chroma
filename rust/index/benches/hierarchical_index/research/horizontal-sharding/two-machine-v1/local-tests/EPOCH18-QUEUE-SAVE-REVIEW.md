# Epoch18 queue admission and local save reuse review

The coherent epoch18 changes have no remaining blocker within this review scope. Queue waiters release mutation admission while they wait for scheduler access. Local save reuses verified durable files in their final namespace instead of writing a second copy. The next fixed 500K run measures balance and save separately; correctness tests do not establish either performance gain.

## Queue and phase ordering

The three enqueue handlers and phase-close handler acquire the scheduler queue before mutation admission. A waiter therefore holds no admission lock while waiting for the queue. Enqueue checks the run, epoch and open phase while holding both locks before insertion; close checks current run and epoch, active owner jobs and a drained queue while holding both locks before closing. This preserves a single decision between accepting work and closing the phase. A close request that waited through an epoch change revalidates its identity after acquiring the locks.

The cross-file review finds no production path that holds mutation admission and then acquires the scheduler queue. Existing operation-lock holders finish before queue insertion or network calls. Worker queue sections do not acquire mutation admission; their health check uses only owner health state. This correction leaves producer-attempt readiness, posting claims, cross-owner transfer barriers and bounded retry rules unchanged.

The controlled convoy regression holds the queue and proves an unrelated real mutation can still be admitted and terminalized for each of the four handlers. Its retained red result demonstrates the opposite ordering blocks that progress. Separate tests cover enqueue-before-close and close-before-enqueue for all three job types, duplicate enqueue, execution and draining of accepted work, rejection after closing or sealing, and a serialized request from another authority that waits through seal and epoch reopening. The obstruction test’s update reflects that queued waiters no longer monopolize admission; it does not weaken production drain rules.

## Durable local reuse

The coordinator-local owner writes its immutable payloads and frozen owner manifest directly into the final generation store. The coordinator verifies the manifest reference’s frozen filename and checksum, validates every referenced payload, checks save/run/generation identity, and requires the acknowledged object set to equal the verified manifest’s object set before reusing it. Final generation validation still checks format, unique owner and root coverage, vector counts and compatible full-vector references before publication.

Remote owners retain staging stores and the bounded copy protocol. Their payload bytes and manifest references still undergo integrity validation before final generation publication. Save synchronization and immutable-file installation order remain unchanged. Removing the local second write does not remove verification reads or establish crash-recovery support.

The local two-leaf fixture checks reduced duplicate-write counts and fresh-reader reopening. The mixed-owner TCP fixture verifies that only local objects are reused and remote objects remain copied and checked. Missing, corrupt or misnamed immutable results reject reuse or generation creation; a failed later generation preserves the previous latest descriptor and its readable contents. Existing save identity, publication conflict and root-coverage tests remain in the consolidated suite.

## Profile interpretation

Local reuse has separate logical object, byte and elapsed counters. Those bytes describe verified immutable references rather than physical disk reads or writes. Owner materialization counters include the one local write; the coordinator store handle has its own counters. Consumers must not count reused logical bytes as an extra write or assume the coordinator handle includes the owner handle’s writes. Remote-copy counters keep their existing meaning. Coarse save phases remain wall times with nested synchronization times, and local-transfer cleanup still contains terminalization; nested measurements must not be added as independent latency.

## Execution preparation

The reviewed epoch18 adapters differ from epoch17 only in fresh namespaces and previous-result/binary bindings. They retain the fixed 500K input, 32 workers, 500K owner capacity, five 100K construction intervals, final redistribution requirement and existing 600/590-second supervisor/workload limits. There is one explicit candidate arm, with no profiler, reference rerun, sweep, retry loop or guard change. The epoch15 supervisor is unchanged.

Every file hash in `operations/preparation-epoch18.json` matches at review time, including the completed epoch17 run-template SHA256 `caa18bc4935d08eb4ae5231761e3c988b2cb77b23d9b764734880f6af87bbda2`. The five focused preparation tests pass in `epoch18-preparation.log`. The configuration’s pending status and source-hash fields may receive final admission metadata; the root operator must bind those final bytes while preserving the reviewed workload and limits. This review does not extend an execution window.

## Validation

The controlled queue failure is retained in `queue-convoy-red-01.log`. The final applicable suite reports 197 passed, zero failed and one filtered test in 7.94 seconds in `epoch18-suite-01.log`. The root agent reports all three native CLI builds passing in `epoch18-cli-01.jsonl` and its stderr receipt. The exclusion remains the known unrelated reference-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`; an unfiltered clean suite is not claimed.

The root operator still binds the final source archive, checks its closure against the epoch17 manifest and validates fresh Linux binary hashes before remote admission. This reviewer runs no competing Cargo process, cloud operation or Git write.

## Reviewed source hashes

Distributed paths are relative to `rust/index/benches/hierarchical_index` in `/private/tmp/chroma-hierarchical-spann-sharding/single-shard`; the benchmark file is in `rust/index/benches`.

| File | SHA256 |
| --- | --- |
| `distributed/engine.rs` | `e0134f48a55ae8163e694d8b922f60a2dcab2f0abb505e6193019d8f10af517f` |
| `distributed/engine_endpoint_tests.rs` | `7ddd80f88fa3ac1f8c46ff8277b7f3cc229004967fe469ffb716f4900cf18da4` |
| `distributed/generation.rs` | `4062f5645646c9f27fee021bda9c01d76b1140dce4ba584ee607473a8d2ae5ea` |
| `distributed/generation_tests.rs` | `f00bd5f2b69dce87a50ba063bbbae65e8cd301cc5902203fa13445b7d35ad40a` |
| `distributed/save_runtime.rs` | `6356f932c0e4de5646c0a56708920d723c8a74b10295752f5819839dd72e2ede` |
| `distributed/construction_tests.rs` | `8adb1412d27e2614e613ec14dcfa2b294e7d7d99aecafff75fe0265a2b230a00` |
| `two_machine_spann.rs` | `f7cc44dee32c3e0c1e24763ca92e7a177dde03a77e91a2380e3b6b09f9b62131` |
