# Candidate29b independent deadline review

The reviewed correction has no identified source correctness blocker for a guarded rerun. It gives owner saving a bounded transport budget while preserving the ordinary construction timeout, immutable integrity checks and conditional generation publication. Acceptance applies to the six source hashes below and remains conditional on final executable builds and the guarded execution supervisor. No successful remote save is established by this review.

## Scope and evidence

The reviewer compares every regular source-archive member directly with its current workspace contents, including files not tracked by Git. Exactly six existing files differ from frozen29. The baseline `source-epoch29.tar.gz` SHA256 is `de891e2e22813bddf12e52b3843b3ec199c076cdbe61fa6098fc0015ccb67969`. The reviewer reads the experiment29 failure report, the transport and save implementation, and the two added regressions. The reviewer performs no source edits, Cargo invocation or cloud operation.

The final local suite receipt records 268 passing tests, zero failures, one ignored real-S3 test and one documented pre-existing filtered test (`epoch29b-distributed-tests-final.log`). Both new regressions are included. The first transport compile receipt contains a type error; the final passing suite supersedes it and must be used for admission. The source hashes below are taken after that successful suite. The implementation receipt `epoch29b-reuse-validation.json` records successful native builds; the reviewer checks its six-file source inventory against current bytes and this report’s hashes. Linux executable builds and physical execution remain admission requirements.

## Deadline behavior

Owner saves share one absolute deadline across all concurrent batches. The deadline is the earlier of 600 seconds from the start of the save and the configured process shutdown deadline. The remote call uses that absolute deadline through connection, frame send and reply receive; the typed save override does not reset it for a new batch. The coordinator checks the deadline before starting, after each joined owner batch, and before invoking final publication. Expiry after materialization prevents starting the combined-generation phase.

Ordinary remote construction calls retain the existing 30-second transport cap. Remote expansion continues to use the original bounded deadline and recursive retry behavior. Server frame admission and authentication retain the ordinary timeout. Only after authenticating and decoding the request does the benchmark server select the longer cap for an owner-save request. The server also limits that cap by the client’s advertised remaining budget and the process shutdown deadline. No wire format, request-size limit or handler concurrency limit changes.

The deadline does not cancel synchronous owner computation or storage calls. Scoped owner-call tasks remain joined before the coordinator returns, including the local owner’s blocked work. A remote client timeout does not cancel its remote handler; it may finish writing immutable evidence until the server or external supervisor stops it. The existing process supervisor remains the hard execution limit. Likewise, a publication method admitted before expiry may finish its synchronous storage validation and conditional write afterward. This is a checked operation/socket budget, not a guarantee that no storage instruction executes after precisely 600 seconds.

## Regression and cleanup checks

The transport regression demonstrates that a classified save taking 100 milliseconds can finish with a 400-millisecond cap despite the ordinary 40-millisecond test cap. It verifies that ordinary requests still time out, a caller’s shorter absolute budget is preserved, and an unclassified request cannot obtain the longer server cap merely by requesting it.

The save regression starts from a committed generation and blocks both owners past a one-second shared shutdown deadline. It verifies that the coordinator still waits for its local writer to join, then returns a deadline error after release. No combined expired-generation object is published, the previous latest descriptor remains exact, and a reader reopens its memberships. This specifically exercises the save-level check that a transport-only test cannot establish.

Owner work still joins before failed save results are returned. Integrity verification, immutable replay/collision handling, frozen publication predecessors and latest-pointer conditional replacement are unchanged. Deadline expiry can leave unreferenced immutable owner objects, which is allowed by the existing manifest-last contract; it does not establish success or permit publishing a partial generation.

## Reviewed source identity

| File | SHA256 |
| --- | --- |
| `rust/index/benches/hierarchical_index/distributed/engine.rs` | `62a0c77dfc0887c19f08036fb4c1d4baa403b97f45eae57a3ccf536235ba76bb` |
| `rust/index/benches/hierarchical_index/distributed/save_concurrency_tests.rs` | `470f75e5a47b03ee83c9dae88c6de44ac0c9e380c6eecfec878ab644cd2ebf38` |
| `rust/index/benches/hierarchical_index/distributed/save_runtime.rs` | `ed44e60188ad3521c61b330f39768fa36009accd1ca7ec99ab76fd030e480163` |
| `rust/index/benches/hierarchical_index/distributed/transport.rs` | `871be3555a7a94fc21853700c62ab933da9e5c3cf413b0c71c48676d5feed791` |
| `rust/index/benches/hierarchical_index/distributed/transport_tests.rs` | `4db2d1a52204df7ed29892931e03ee6760803332ac33f2a9d244068e20bb0fd2` |
| `rust/index/benches/two_machine_spann.rs` | `3c2e46e2c96579262de1197c31cf1e760fc492e830e20eba7525acb46417540f` |

The reviewed inventory file SHA256 is `d4043c2548de7d00873aeb800465c664a791d57aebe16a0821376e9f1bf0f832`. The final suite log SHA256 is `8b406c3d8762398a0030355f1fb1df3c5a4ca799586999ee617c66275742ad0a`.
