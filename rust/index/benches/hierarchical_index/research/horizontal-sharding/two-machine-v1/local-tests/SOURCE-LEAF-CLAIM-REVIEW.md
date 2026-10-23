# Source-leaf claim review

The coherent correction protects a leaf’s current posting membership throughout split preparation and installation. It reuses resident reservation identity without holding a node write lock or a structural region during clustering. The reviewer finds no remaining source or epoch13 preparation blocker in the frozen snapshot. The requested 100K run must still validate its actual outcome; the earlier 324 prepared, 252 discarded and 72 committed measurements are a baseline, not a predicted improvement.

## Protection and concurrency

The claim pins the resident node and stores a unique token under its short node lock. Preparation then reads stable membership, fetches vectors and clusters without retaining that lock or a region grant. Navigation continues reading the published routing image. Append, posting transfers, merge/removal, migration and saved-state paths reject conflicting protection rather than mutating the claimed posting list. The active construction job and reserved-node checks prevent premature phase close/save.

Ancestor edits can reparent the claimed leaf while preserving its membership and centroid. Installation rereads its current routing image and checks identity, leaf kind, centroid and claim token, then acquires the appropriate structural region. It accepts its own source token in both local and boundary publication paths. Installation takes over protection before unlinking the old node. It retries changed ancestry with the already prepared payload rather than reclustering merely because the parent revision changes.

The claim’s scope guard clears only its own token. An error, early return or unwind releases that claim; an installation that has taken over protection keeps its independent token. No preparation write-lock upgrade or claim-held structural region creates a wait cycle. Transfers and shared parent/migration preparations encountering protection reject promptly and release their unmutated grants rather than waiting for a claim while holding the region required by installation. Independent leaf preparation and ancestor structural work remain available.

## Retained work and rejection cleanup

A controlled regression exposes a necessary progress correction: repeatedly proposing a transfer into a legitimately claimed leaf consumes the existing 1,024-round limit in 1.72 seconds while its split is still progressing. The corrected path retains affected IDs and exclusion semantics in deferred reassignment jobs, releases the worker and schedules them after the active dependency. It does not increase retry budgets or drop membership work. Each job contains at most 32,768 IDs; coordinator queueing retains a canonical body digest so identical replay is harmless and changed-body replay conflicts. Deferred jobs remain part of queue-drain state and are phase-fenced like other owner jobs.

The scheduler checks active source/dependency identities across owners and permits unrelated jobs to progress. When resumed, reassignment rechecks current source membership and navigates current destinations. Replaced or moved source IDs are handled by the existing membership revalidation. Forced-removal exclusion is retained. The focused test holds preparation beyond 5.1 seconds, observes one deferred reassignment round, then drains the retained work and verifies exact membership.

Migration export can now reject a claimed subtree without stranding a region or active phase operation. An uncommitted-export guard releases/cancels unmutated protection and retains a terminal rejection for replay. Handoff validation occurs before outgoing capacity reservation, including the empty-subtree rejection case. Once an endpoint exists, its existing explicit handoff protocol owns cleanup; the guard does not release committed ownership-transfer protection.

## Verification and scope

Four focused tests pass in 5.12 seconds. They exercise real paused preparation, readable navigation, append/transfer/merge/export rejection, independent leaf work, actual ancestor reparenting, preserved preparation with zero discards, deferred work/drain, cross-owner scheduling dependency, duplicate-body fencing and token-specific release after failure/unwind. The applicable suite passes 172 tests in 5.23 seconds while explicitly filtering the known separate local-writer regression. All three native CLI builds pass. These results establish the tested mechanisms rather than a benchmark speedup or physical two-machine result.

The source closure comparison contains four changed source/test files and one new focused test module, with no other delta from the instrumented epoch12 source. The correction changes protection and retry scheduling intentionally; it preserves the clustering kernel, thresholds, seed selection and vector order. Actual tree/work differences in the next run still require accounting.

## Fixed execution preparation

The epoch13 operations differ from the working epoch12b preparation by fresh namespace/window identities and artifact paths. The distributed candidate alone is rebuilt from a verified fresh closure using retained build settings/cache. Missing or null final artifact hashes fail admission. The optional reference uses the retained exact epoch12 reference binary; there is no automatic reference run, retry or larger input. The root can populate actual archive/build hashes without another code review when they match this frozen closure.

The existing full-phase profiler handshake, actual five-byte acknowledgement handling, exact-process privilege scope, 120-second recording ceiling, 240-second collector ceiling, 256 MiB raw-file cap, 1 MiB text report cap, cleanup and final report collection remain unchanged. The workload retains its 600-second ceiling and shared 590-second absolute deadline. Eleven pure/mocked preparation tests pass in 0.023 seconds. Fresh actual guards, sandbox identity, rates/budget, Linux build hashes and validated benchmark evidence remain the sole operator’s gates. This reviewer runs no Cargo, AWS, SSH, Git or source mutation and edits only this receipt.

## Reviewed identities

The source archive SHA256 is `f8abad2404a85a9561a82ecb4b2da3c4ae103492255dd85b5e32c98ac722f6c0` and contains 23,024,443 bytes. The validation receipt SHA256 is `9421739258a2f28821e393e03d52398763eb8d690667485b052a4b542ab1b057`.

| Source | SHA256 |
| --- | --- |
| `construction.rs` | `7b73caef4aad365e430b8cb566806fa713bef3a4655507fe117c6304d399e38d` |
| `engine.rs` | `163284982521d059a540604becbb89f080a718311039a28c762ed3b3b3d540f1` |
| `placement_runtime.rs` | `75e800d4eb5ede5cac75ac5f8ed0947615db8cb7ecb359e2cf8e2f583ac87ce7` |
| `construction_tests.rs` | `d7d441180da8963f874ec7be3c5845fd6af739245bc66a9f5b2b5533e0f0563e` |
| `source_leaf_claim_tests.rs` | `02f0bcafe880606f97ffa613970364a6b146b2b8e475a5d4ba3a0c0a3ecf04f2` |
| `make-owner-local-spec-epoch13.py` | `03777ef2c8db896b85baed57e040f08513ca4562f34e242852f64c120e147918` |
| `remote-build-epoch13.py` | `8d10da7eeaebac67f889d552498df6cea0be5e5f849b5f3df012041b8a76f68a` |
| `remote-job-epoch13.py` | `465fdb59388cbc06bdc349d9aeacf476b069b522d58d774f4f39c6f7823edca9` |
| `remote-profile-epoch13.py` | `3df46705bada2fe6dac14b52ef3ce28204b78e0c535acf8d492cb25352db208b` |
| `run-one-arm-epoch13.py` | `fa2db3ae59b57c1273778edd135cb18c22064f081660c9dac332a7b8359534f6` |
| `owner_local_epoch13_test.py` | `68fd62b5cc58147f5b86bd16dde1dcf474c807bdd99fd034288e42fc3cbeb17a` |
