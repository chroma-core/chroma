# Performance preparation and single-arm execution review

The reviewed preparation and execution scripts have no remaining identified admission blocker after the corrections below. This review performs no AWS or Cargo execution. The generator's three local tests validate all six arms and three peer specifications through the unchanged remote-job validator using mock time. The reviewer independently runs the two input-preparation tests; both pass. Remote input preparation and benchmark results remain unexecuted evidence at this review boundary.

The specification generator binds the frozen six-arm workload plan, verified epoch1 performance/reader binaries and retained reference binary. It places owner1 and the reference on the worker instance and owner2 on the peer. Every job uses 32 workers, the existing supervisor's 32-physical-core check, a 600-second cap, at least 32 GiB available memory, and at least 12 GiB disk for the 3M reference or 2 GiB otherwise. It records the exact input hash and requested cadence. It writes a new specification and SHA-256 sidecar and refuses overwrite. It does not execute jobs or advance the plan.

The single-arm runner launches exactly the named arm. The peer and coordinator use one absolute job deadline 590 seconds after initial preparation starts, so the peer's earlier startup cannot cause its separate 600-second timeout to expire before the coordinator's deadline. Failed or timed-out foreground execution triggers an explicit targeted owner1 stop. Peer cleanup must be positively confirmed. Cleanup and evidence-collection failures prevent a successful FINISHED outcome. Each arm still requires an explicit root-operator invocation; there is no automatic next arm or retry. Its TCP readiness probe creates an empty connection outside algorithm work and may appear in the peer's connection-failure counters.

Input preparation verifies the supplied reviewed plan hash before parsing, creating output or contacting services. It then checks the exact task instance through fresh metadata, the artifact-role account identity, the active matching absolute shutdown timer, exclusive workload lock and disk headroom. It downloads each of the 32 pinned objects once using exact version IDs and expected bucket owner, checks the complete object bytes and hashes, and emits only the specified canonical prefix. It tees the first object into the separate 100K file without another download. Both assembled corpus hashes must pass before final files and descriptors are emitted. Failed incomplete bytes and receipts remain preserved.

The reviewed input plan SHA-256 is `401542791f43e5bb80fd4aa4e94fef7be727db09408fac35bfde5ce78ea7756c`. Its maximum full object is 307,200,000 bytes, its aggregate downloaded payload is 9,483,936,768 bytes, and its canonical 3M output is 9,216,000,000 bytes. The additional 100K local output is 307,200,000 bytes. The plan-hash gate corrects the identified gap where a self-declared plan could retain the 3M output size while expanding full-object download sizes.

The existing remote-job supervisor remains unchanged. Actual execution still needs fresh window/instance/source/binary/input receipts, both shutdown guards, bounded progress and runtime, exact output validation, meaningful two-owner occupancy for the distributed arms, prompt task shutdown and reconciled spending. Correctness and physical cloud-publication receipts belong to the preceding milestone and are not benchmark performance results.

| Reviewed file under operations | SHA256 |
|---|---|
| `make-performance-spec.py` | `913350a1b59fd84a8a0fa9cbfee1b466d077fbc03ad77d2d6993fc690155e966` |
| `performance_spec_test.py` | `3a86bf6122f0237f1ba3d4f69b5925907183269ad4b38b465cc52b536634c03d` |
| `run-one-arm.py` | `b8d98046dc63758fff741a3d70875d9062db3b954dcfe79be678848b8d608588` |
| `prepare-performance-input.py` | `8268bee59559d3191310e71449a3088b0ff9a4cf3420708f1e42e61ab6f8d50d` |
| `prepare_performance_input_test.py` | `7668c2885f36fd7ab41ee44bf722412acd24c0e3096b5b17b3a4828b8e887f34` |
| `workload-plan.json` | `39f31ba042f79d29e678278a5147343f218088a6675fd9cb31746f606ccb46b4` |
