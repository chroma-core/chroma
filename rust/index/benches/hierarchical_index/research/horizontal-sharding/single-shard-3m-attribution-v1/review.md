# Independent review of the 3M attribution harness

The harness changes pass focused source review. No source correctness blocker is identified for the authorized paired construction comparison. Admission is conditional on the final native build and multibatch smoke receipts passing for both exact source identities below, followed by the operator’s normal remote execution checks. This review does not assess cloud scripts, provision resources or run additional tests.

## Scope and source identity

The candidate and reference use byte-identical `rust/index/benches/single_shard_attribution.rs`, SHA256 `400e48df6d143ada927f050cbff38da73ba71f8f2b13a7c7e05c46649e61b327`. Comparison against the complete frozen 100K source manifests confirms that the only modified existing files in either checkout are the index Cargo manifest and instrumentation module. The Cargo change declares the new benchmark. Instrumentation changes add serialization derives to existing counter snapshots and balance-iteration records. The new harness is the only added source file; writer algorithms, scheduler behavior, locks, storage and query implementations match the independently reviewed 100K sources.

The candidate scoped source digest is `5b4e02801d844634d393522981f543cb3f26af6efdc1c5ec5c75f067155b55c7` across 26 files. The reference scoped source digest is `1051d7f5755d14430a56f16257065fa67842d88ae3c6ec314516b39e82e76c85` across 21 files. Each digest is SHA256 of compact JSON containing sorted pairs of relative path and file SHA256. The included files are root `Cargo.toml`, `rust/index/Cargo.toml`, both `single_shard_matched.rs` and `single_shard_attribution.rs`, every Rust source below `rust/index/benches/hierarchical_index`, and `rust/index/src/quantization/packed_centroid.rs`. The operator must additionally freeze complete source archives and exact executable identities. A source change requires refreshing this identity and reviewing its material effect.

## Schedule and matching

The schedule advances by 100K through exactly 1M, then by 1M through 2M and 3M. Its calculation also handles a final partial batch without skipping vectors or crossing the transition early. Positive batch sizes and input sizes prevent a zero-progress loop. Both arms load identical ordered vectors, use the same default construction settings, execute the same 12 add/balance cycles and save once after the final completed balance.

The small native smoke uses 64 vectors with batches 16, 16 and 32. It validates the new multibatch harness path rather than representing the intended structural workload. The 3M run must record actual internal-job counts, tree depth and per-level counts before claiming it exercises deeper construction behavior. Exact saved-state validation remains unchanged: every input has one reachable posting, links match parent records, full vector bits and one-bit posting codes match, and persisted four-bit centroid codes match after writer retirement. Reader smoke uses an indexed vector and establishes execution, not held-out recall or quality equivalence.

## Phase and failure evidence

Every completed cycle records add/balance wall time, process CPU, resident memory, work counter deltas, current tree shape and cumulative candidate coordination counters. Counter snapshots run after worker joins, so their deltas do not race active construction. Existing snapshot subtraction preserves newly appended split samples and balance-iteration records while subtracting additive counters.

JSONL events are flushed after each completed add and cycle. A failed later add, balance or save therefore preserves earlier completed evidence even when final `result.json` is absent. An `add_start` without its completion identifies an interrupted add; an add completion without the cycle completion bounds an interrupted balance. Flush reaches the operating system and does not promise crash-durable synchronization to disk. The output directory must be new, so failed attempts remain intact.

Unix timestamps support coarse alignment with CPU samples. Add completion is emitted after counter snapshots; cycle completion follows the balance timer, work snapshots and tree-shape serialization. Samples between these events can therefore include diagnostic overhead. They are approximate phase windows rather than exact instruction boundaries. The complete build/save timer includes snapshot, tree-shape, JSON serialization and event-writing overhead outside the separately measured add/balance timers; retain that residual in total accounting.

## Counter interpretation

Process CPU divided by elapsed phase time measures effective process cores, including runtime work. Worker duration and scheduler waiting are overlapping elapsed times and cannot be added to end-to-end latency. The running process RSS high-water mark is observed at each phase end; it is not an independently reset peak for that phase. End-of-phase RSS is a separate observation.

Candidate coordination records are explicitly cumulative. Difference additive counters between cycles when needed; `peak_active_jobs` remains a running maximum and must not be subtracted or treated as phase CPU occupancy. Structural reservation hold/wait sums describe those measured sections only. The reference has no equivalent reservation counters, so their absence is not a zero-cost measurement.

Deferred transfer retries count routed groups, not unique vectors, completed transfers or machine messages. Queued row counts can repeat vectors that are later coalesced or filtered as superseded. Leaf split sizes count input rows, not k-means iterations or distance operations, and do not include internal clustering. Split/NPA/navigation timers can overlap; a parent operation can contain measured child work. Per-level navigation counters and NPA evaluation counts expose work differences without making these sums a wall-time decomposition.

The harness explicitly reports unpopulated reassignment substep counters, unmeasured k-means iterations and the unavailable per-job critical path. Existing counters do not isolate resident-node handle/lock overhead. CPU samples can support an implementation-backed explanation, but a different random tree or work count prevents attributing every timing difference to sharding overhead. Fresh resident construction does not measure lazy storage-fetch or cache-capacity costs merely because the tree grows to 3M.

## Local receipt status

The final candidate and reference build and 64-vector multibatch smoke receipts all exit zero under `local-tests/final`. Both result files contain cycles 0–16, 16–32 and 32–64 and pass exact membership, reachable-link, vector-bit and saved-code validation. The local source-validation condition is satisfied. The prior coherent engine review and saved-state tests remain applicable because no engine source changes occur in this harness update.

## Focused execution-control inspection

The prepared operator controls bind startup to sandbox account 079819354890, approved worker `i-011ff180f03584f40`, its dedicated task tag, r6i.16xlarge capacity and preserved root volume `vol-08bda85902f848a75`. A new local background watchdog fixes the absolute deadline before startup. The independent remote systemd timer checks instance identity through instance metadata and is verified active before uploads, input restoration or builds. Start requires a reviewed artifact manifest; individual commands reject insufficient remaining deadline. The operator must retain the actual timer/readiness and fresh identity receipts, not treat source inspection as proof that guards run.

The materializer reads exact versioned sandbox S3 objects with expected bucket ownership, validates each complete object SHA256, emits only the selected canonical prefix and records the final file identity. It verifies the previous 100K prefix identity and the exact 9,216,000,000-byte 3M extent. The runner independently rehashes that local input before construction. The final partial source object is fully checked even though its suffix is omitted from the emitted prefix. No corpus payload passes through the Mac.

Source admission checks every extracted regular file against its manifest, rejects unadmitted source symlinks and requires exact source closure. Both arms use the same locked Rust 1.92.0 compiler, native target, frame pointers and debug-symbol settings. Cargo compiler-artifact records identify each source target; the runner copies each admitted executable before building/running the next arm and hashes the retained binaries. One external flock covers the runner. Timing order is reference, candidate, candidate, reference, with fresh processes and preserved output directories.

The benchmark command runs through GNU `timeout` without `--foreground`. Its default process-group control covers the time wrapper, profiler and benchmark descendants, sends TERM at 600 seconds and sends KILL after ten more seconds. The outer Python timeout provides an additional allowance beyond that kill interval. Any nonzero command exits the fixed campaign without automatic retry. The absolute independent guards remain in force across builds and all four processes.

The live profiling probe records only a bounded child process using 99 Hz user-space CPU-clock events and frame-pointer stacks. It changes no kernel permission settings. Its successful result is required before benchmark builds. Profile capture requests the realtime clock to align sample timestamps with the harness events. The compact phase-summary helper also passes focused review. It streams timestamped samples, assigns phases and cycles from event boundaries, caps sample/stack depth, reports unknown and unparsed samples, and retains both leaf and inclusive symbol counts. A live parser probe is required before builds; a static synthetic parser check alone does not prove compatibility with the host perf format. Inclusive symbol counts overlap and cannot be added into CPU shares. Inlining or truncated stacks can hide specific lock/handle functions even when their costs remain in callers. Phase boundaries retain the approximation limits described above.

## Frozen operational review receipt

The final coherent operational set passes focused static review with no unresolved execution-control blocker. The guard now detects the attribution harness and profiler alongside other competing construction commands. No cloud action or additional test is performed by this reviewer. The operator must freeze these exact files into admission and preserve actual local/remote guard activation, materialization, live profiler/parser probe, artifact, command and shutdown receipts. The allowed run remains reference, candidate, candidate, reference with 600-second process limits inside one fresh 90-minute absolute window; a failed required step stops the campaign.

| Operational artifact | SHA256 |
|---|---|
| `lifecycle.py` | `7e4152c2acdefc0918b2a48e9ef292e75946c56b465ff246e7981e89be8c9054` |
| `remote-guard.sh` | `ac045cdec99933f9aa1bb8074224de63c638cdb8362472b015b4720838cb4230` |
| `remote-run.sh` | `92412789d829627d0ad96048efad86d226b5636c6a69b40fde5a8be6a7e1b80f` |
| `remote-run.py` | `7ea8d20090cbe13a74bdce72f2aa83b6330b8555eb316df2548f83056fc32d77` |
| `remote-storage-preflight.py` | `06b4d59f2aa0356fab14eba45c4f45226fc29d34fa2bd37ab5d8aaa6b1a0ab31` |
| `remote-perf-probe.py` | `1cc806117242790758bdc1b329ffe9122708d3a07be01a16cbda1ff766df3a77` |
| `materialize-input.py` | `7948a9ef6acad39c1066f2be04d41500299c8443f22af9431dbcdf55b360fdd1` |
| `perf-phase-summary.py` | `31d6b11cf7da1196ad24fcf98aa613b0840cf6352f12ba908d1e239a647f299d` |
| `input-plan.json` | `401542791f43e5bb80fd4aa4e94fef7be727db09408fac35bfde5ce78ea7756c` |
