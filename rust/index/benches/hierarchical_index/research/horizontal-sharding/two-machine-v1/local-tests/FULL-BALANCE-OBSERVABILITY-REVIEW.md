# Full-balance observability review

The reviewed change adds construction-work counters and a profiler handshake to one matched standard 100K reference/distributed pair. It preserves the inspected algorithm, settings, random initialization calls, input order, placement and scheduling decisions. The reviewer finds no remaining source or execution-preparation blocker in the frozen closures. Actual Linux builds, fresh guarded-window admission, profiler attachment, complete coverage and validated benchmark outputs remain separate operator gates.

## Source and work accounting

The same instrumented two-means kernel exists in both source closures. Its assignment-iteration count increments only for iterations actually executed. Row-iterations sum the input length for each executed assignment pass. Distance-call counts include eight seed-evaluation calls per row, two assignment calls per row per iteration, one center-distance call per iteration and two center-movement calls only when that branch executes. Trivial inputs record a call without initialization/iteration work. The general sampled clustering API remains outside this profile. No random call, stopping condition, arithmetic order or point ordering changes in the inspected kernel.

Kernel counters accumulate locally and merge once per split call. Calling-thread CPU and elapsed time stop before the global counter merge; outer balance timing still includes instrumentation. Kernel calls include leaf-vector and internal-child-centroid inputs, so input rows are not uniformly full-vector rows. The counter contract correctly separates these kernel distances from navigation distances. Linux CPU support is reported by supported-call counts; zero unsupported CPU is not interpreted as zero work.

Distributed counters expose actual reservation requests/candidates/active-conflict visits, ancestry steps, queued waits, transfer protocol attempts, completed groups, compactions, explicit copied payload bytes, navigation materialization/validation work and split preparation/installation outcomes. Membership lookup uses the unchanged optimized slice scan. Its probe count is exact, while its summed haystack lengths are a conservative comparison bound rather than measured hardware comparisons. Reference hash-lookup probes have a different cost unit and must not be compared as equal scalar comparisons. Byte counters cover explicitly identified payload copies rather than all allocation, memory traffic, encoding or I/O.

Reference changes add acquisition-duration timers around existing claim/tree locks and local counters around existing bulk transfers. Its copied-byte and lookup-probe definitions remain explicit. No source transfer, claim or split decision is changed. The focused test verifies actual bulk compaction/copy counts. Distributed source compaction and reservation tests exercise the real endpoints/manager rather than mirrored arithmetic alone. Navigation now separately counts shared-upper scoring, closing the earlier distance-counter omission.

Operation timers report inclusive sums and maxima, with supported calling-thread CPU. Nested navigation, request, transfer, compaction and split scopes overlap and cannot be summed into phase time. Cumulative maxima cannot be subtracted into per-phase maxima. Exact coordinator process CPU now starts/stops immediately around the distributed timed balance, matching the reference phase clock. Owner snapshot intervals separately include external gate waits and reporting. Gross build totals explicitly include these waits and are unsuitable for direct comparison with earlier uninstrumented totals.

No shared counter update runs inside each distance or membership-element comparison. Measurements still add clock reads, per-operation local increments and owner-local merges. Some operations are individual query-expansion requests; the merge cost is therefore potentially material for many small requests. This perturbation is uncalibrated and must remain a limit of the instrumented pair. Preserving algorithms does not establish unchanged latency.

## Full timed-phase observation

Both benchmarks publish a fresh ready record and wait for the collector before starting their balance timers. The collector starts exact-PID userspace profiling disabled, sends enable through private control/acknowledgement pipes, waits for the acknowledgement, then publishes go. The benchmark stops its timer before publishing done. Only after validating completed done and receiving disable acknowledgement does the collector stop/report and publish release. Gate waits occur outside balance timing. Timestamped events and gate records allow the completed ordering to be checked. The sample can include small pre/post-timer bookkeeping, so full coverage does not mean samples contain only timed instructions.

Gate records bind format, phase identity and process ID and refuse overwrite. Each benchmark wait is bounded by 60 seconds and the absolute workload deadline. Failed balance status is published without converting failure into completion. Collector tests prove enable-ack precedes go and disable-ack follows done, and that profiler exit before done never claims full coverage or releases the benchmark as completed.

The preparation admits only the exact retained 100K inputs, 32-worker matched settings and rebuilt reference/distributed artifact hashes. It uses the ordinary 600-second workload supervisor and 590-second absolute job deadline. The collector retains checked-source privileged execution, verified PID/start-tick/boot/executable/spec/marker binding, userspace-only sampling and separate marked-group cleanup. It changes no kernel permission setting. Recording has an independent hard bound no greater than 120 seconds, shortened by remaining job/host/collector time. The complete collector has a 235-second internal ceiling and 238-plus-two-second outer bound. Raw files have a 256 MiB size cap; the text report has a 1 MiB limit and bounded generation time. Raw profile data stays remote. Existing final-after-result small-report collection avoids the prior asynchronous collection race. There is no automatic retry, size sweep or tuning sweep.

## Build provenance and evidence

The packaging helper materializes only reviewed internal source links into regular archive files, rejects external links/unreviewed linked files, and writes exact file hashes. The build supervisor verifies the complete regular-file closure, refreshes timestamps only after verification, and rejects cached selected artifacts reported fresh. Both builds use the same fixed compiler/native/frame-pointer/debug settings. The reference build passes no owner cfg; the distributed binary alone receives `distributed_owner`. Inherited wrapper/encoded flags remain cleared. Process-group cleanup and absolute host guards remain unchanged.

The frozen distributed closure contains 2,297 files, with eight changed files, two new measurement/gate modules and no removals relative to epoch9. The reference closure contains 2,257 files, with four instrumented files changed and no additions/removals relative to the retained reference. The comparison receipt reports both live closures matching their archives. No unrelated source change is identified.

The retained final applicable distributed suite passes 168 tests in 5.14 seconds, with the known independent local-writer regression explicitly filtered. The focused shared-kernel and reference bulk-transfer tests pass. Native distributed reader/correctness/performance and reference benchmark builds finish successfully. Earlier compiler failures remain retained. Nine pure/mocked operational tests pass in 0.019 seconds; they do not establish actual remote profiler control or coverage. This reviewer runs no Cargo, cloud, SSH or Git operations and edits only this review.

## Frozen identities

| Artifact | SHA256 |
| --- | --- |
| Distributed source archive | `f2979b3bb5fe940c8e508de3f3fbfe7c9471d1cb1ef50d407c19504f9549df44` |
| Reference source archive | `a172455362381ee8c520abbfead6f13280fe12664847698d0bb1ad117e3b8b3c` |
| `operations/remote-build-epoch12.py` | `93100ae73297cf76125ccee70e8f32358889e386e51f3200faf485b4c83558ca` |
| `package-observability-sources.py` | `677f5df7a4f387ebbdede5a319af09eba9d18a7b38867c0168b5c8620244443c` |
| `operations/remote-job-epoch12.py` | `ff7d2302ece730f4f800d58571ecfce611469b2f3791e2b472f178c9000d1d6b` |
| `operations/remote-profile-epoch12.py` | `186d641584f1b1d73444bb931e53c32bfe9c393186870bc9c81a55a3b94d05db` |
| `operations/run-one-arm-epoch12.py` | `75c5f24f765f3a5431322857e4029fe27bf6760dd8708315df8ec74859f5eef7` |
| `operations/make-owner-local-spec-epoch12.py` | `8a7d219289b99c50a56d6bde536f96d523c7e3c0429010f3338dc3d6dbef366f` |

The result must pair elapsed/CPU time with actual kernel iterations, distance work, transfers and tree shape. Uncontrolled construction seeds and scheduling still prevent isolated sharding-overhead or optimization-speedup claims. The full-pair observation addresses missing evidence; it does not presume that all remaining latency has one cause.
