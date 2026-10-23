# Reducing insertion and balancing time while preserving durable saving

The combined configuration reduces median final-million construction time from 37.512 to 29.234 seconds, a 22.1% reduction. It uses 64 insertion workers across the existing two machines and permits one centroid refresh at a time for each shared boundary. Bounded parallel durable writes reduce the measured first save from 131.141 to 7.996 seconds and are part of the accepted design alongside owner-direct saving and unchanged-block reuse. The combined configuration also passes first-save and unchanged-save validation with fresh readers.

## Accepted construction configuration

All three combined timing runs pass exact 3M membership and posting-code validation. The following values are medians of three lightweight runs from the same full-precision 2M checkpoint. Construction includes placement and other measured construction overhead, so its median is not the sum of the independently computed phase medians.

| Measurement | Control | Combined configuration | Change |
|---|---:|---:|---:|
| Insertion | 12.957 s | 8.188 s | 36.8% less time |
| Balancing | 23.071 s | 18.364 s | 20.4% less time |
| Construction | 37.512 s | 29.234 s | 22.1% less time |
| Insertion CPU | 368.92 CPU-s | 484.75 CPU-s | 31.4% more CPU |
| Balancing CPU | 565.00 CPU-s | 655.86 CPU-s | 16.1% more CPU |

Accept the combined configuration for latency while explicitly retaining its CPU tradeoff. The benchmark profile uses 64 insertion workers, 32 balancing workers per owner, a 64-query navigation window, per-boundary refresh coordination and the original polling listener. Saving uses at most 32 posting workers per owner. The accepted compact navigation gather and conditional routing-view transport remain included. Use the recorded profile through `operations/development31/accepted-run.py`; experimental source defaults remain conservative for insertion and refresh coordination.

The three combined construction times are 29.324, 27.835, 29.234 seconds. The control range is 37.058–38.682 seconds. These small samples establish a useful result on this workload, rather than a production latency distribution or scalability proof at 100B vectors.

## What determines insertion time

Insertion spends almost all its elapsed time finding destination leaves. The detailed control measures 12.602 seconds routing and 0.367 seconds appending the resulting groups. The coordinator originates every navigation call while both owners score routing candidates and store vectors. The previous 32-worker limit therefore constrains navigation across the pair, rather than allowing 32 independently initiated insertion calls on each machine.

Increasing insertion workers to 64 keeps balancing at 32 workers per owner. The three insertion timings are 8.203, 8.171 and 8.304 seconds, compared with a 12.957-second control median. Median insertion CPU demand increases from 368.92 to 485.25 CPU-seconds. CPU time divided by insertion elapsed time rises from approximately 28 to 59 occupied cores across the 64 physical cores. This is a latency-versus-CPU tradeoff, not a reduction in CPU demand.

The query-window experiment increases each navigation call from 64 to 256 queries. Median insertion rises to 13.663 seconds versus 13.059 for its readiness-listener control. CPU demand rises from 369.35 to 400.69 CPU-seconds, and traffic remains approximately 8.835 GB. Keep the 64-query default.

## What delays balancing

After a local tree edit, its owner refreshes the centroid in the shared routing view before the job reports completion. A refresh prepares a new view, waits for the global publication turn, and rechecks the current owner revision and view sequence. Concurrent changes invalidate prepared work, causing another attempt. The turn remains held through publication and broadcast.

The detailed control records 4,942 attempts, including 3,840 changed-state retries and 348 stale-prepare retries. Concurrent refresh calls accumulate 887.105 wall-seconds, including 833.444 seconds waiting for the publication turn. These overlapping durations are not elapsed phase time and must not be added to it. A separate readiness-listener diagnostic retains a refresh lasting 12.243 seconds, of which 12.189 seconds is publication-turn waiting.

The candidate admits only one refresh per boundary before snapshot preparation. Every waiting request still takes a fresh snapshot, validates its own owner revision and epoch, and follows the existing publication and broadcast protocol. It never reuses another request's success as proof of its own completion. Weak references retain gates only while needed, and a finite bound prevents unbounded active entries. The existing deadline includes gate waiting.

Across three lightweight runs, median balancing falls from 23.071 to 18.665 seconds. Median balance CPU demand increases from 565.00 to 676.49 CPU-seconds. In the separate detailed comparison, attempts fall to 2,371 and changed-state retries to 1,195. Publication-turn waiting falls to 85.890 summed seconds; the new gate adds 135.712 summed waiting seconds. Total refresh time falls to 252.715 summed seconds. This supports reduced speculative publication contention as a contributor to the latency improvement. Work counts and tree evolution vary with scheduling, so it is not an exclusive decomposition of the entire phase.

## Listener experiment

Replacing a 1 ms sleep after an empty accept with operating-system readiness waiting reduces native debug loopback latency from approximately 1,282 to 85 microseconds. Five alternating pairs contain 1,000 measured calls each after warmup, with no failures. Separate control and data admission caps and per-call deadlines remain intact.

That small-request result does not translate into improved construction. Three-run median construction is 39.187 seconds with readiness waiting versus 37.512 with the original listener; median balance is 24.221 versus 23.071 seconds. Keep readiness mode as an opt-in experiment and retain the original listener default. These three repetitions do not isolate whether the difference is scheduling variation or a stable regression.

## Durable saving

Owners use a dedicated shared pool for independent posting serialization, writes and verification. The default cap is 32 workers, configurable down to one for the sequential control. All tasks join, including failures, before publishing owner manifests. Ordered result collection retains deterministic references and error selection. Existing conditional generation publication and unchanged-block checks remain intact. The cap bounds additional block buffers, not the entire cloned snapshot.

The real-storage comparison reduces first-save time from 131.141 seconds with one posting worker per owner to 7.996 seconds with 32. The slower owner’s materialization falls from 127.739 to 4.844 seconds. The sequential arm writes 2,129 posting blocks and the parallel arm writes 2,126; both contain exactly 3M vectors. This is one pair of runs with different final trees, so the result establishes a large practical improvement without isolating a precise repeatable speedup factor. Both owners already save concurrently in both arms.

Unchanged saves take 3.270 and 3.410 seconds respectively. Both reuse every posting block and materialize zero new posting blocks. The optimization therefore improves first or changed saves while preserving the fast unchanged path. Four fresh readers pass exact membership, all 3M posting-code checks, every centroid-code check and the full-vector hash. The first and unchanged generations return identical results for all 16 saved-reader queries within each arm. Focused native tests also cover overlap, concurrency bounds, upload failure, corrupted verification, joining before publication, replay and unchanged-block reuse.

The fresh validator fetches and decodes immutable posting blocks in bounded waves of 32, then consumes them in deterministic leaf order. It retains all integrity, membership and recomputation checks, with at most one wave of decoded blocks resident. The separate reader source passes 287 native tests, including nine reader tests under both one-worker and 32-worker settings. Fresh-reader elapsed time falls from 303.475 and 288.459 seconds in the serial-save arm to 56.365 and 56.362 seconds in the parallel-save arm. These are development-throughput observations across different saved trees, not a matched reader benchmark. The reader build overlaps only post-save functional validation; measured construction and save phases are complete before compilation begins. Writer artifacts remain unchanged.

The combined construction configuration saves its 2,127 posting blocks in 11.072 seconds and saves the unchanged generation in 3.296 seconds with no new posting blocks. Its two fresh readers complete in 52.368 and 55.162 seconds and pass every required check. All 16 query results agree between first and unchanged generations. Across the two parallel-save observations, first-save time is 7.996–11.072 seconds; storage timing remains variable.

The [completion receipt](EPOCH31-COMPLETION.json) binds the source, executables, checkpoint, configuration and raw results. The [accepted profile](../operations/development31/accepted-profile.json) supplies reproducible settings, and the [final cloud audit](../operations/development31/final-cloud-audit.json) records machine identities, artifact verification and process cleanup. Both machines remain running at the user's request under the current budget guards. The recurring follow-up remains paused.

## Experimental controls and limits

All runs restore the exact same full-precision 2M checkpoint, use the same retained input and two 32-core owners, and validate exact 3M membership and all posting codes. Saving runs additionally use fresh readers to validate durable output. Source, configuration, executable and raw receipt identities remain under [development31](../operations/development31). The [experiment design](../operations/development31/experiment-design.json) and [comparison data](../operations/development31/comparison.json) record controls and outcomes. The [candidate plan](EPOCH31-CANDIDATE-PLAN.md) links back to the [current plan](../../CURRENT-PLAN.md).

Timing arms use three lightweight repetitions; diagnostic runs remain separate. The detailed control's 25.581-second balance is slower than its lightweight 23.062–23.646 range, so detailed instrumentation is not assumed free. The gated diagnostic takes 19.220 seconds. CPU profiles from epoch30 remain separate evidence and are not mixed into timing medians. The reference21 final-million result remains a historical single-run comparison with a different tree, not a matched checkpoint control.

The full native suite passes 284 tests with the refresh gate disabled and enabled with detailed diagnostics. Three tests remain ignored: two opt-in actual-S3 tests and the loopback timing benchmark. One documented unrelated parent/leaf concurrency test remains excluded. After restoring the original listener default, all ten transport correctness tests pass. Independent reviews find no blocking transport, saving or refresh-gate correctness defect. The existing concurrent centroid-return test exercises two refresh requests and an intervening owner mutation with the gate enabled.

Nested RPCs can extend total refresh duration beyond the deadline used for gate and retry waiting. A timed-out gate acquisition currently leaves its per-record gate-wait field at zero; successful-run measurements are unaffected, and errors remain retained. The new implementation does not claim to resolve all starvation or publication-scaling limits.
