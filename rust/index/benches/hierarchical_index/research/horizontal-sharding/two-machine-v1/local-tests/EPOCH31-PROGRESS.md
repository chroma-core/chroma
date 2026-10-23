# Construction and saving experiments

The user authorizes implementing and benchmarking promising insertion and balancing improvements, with bounded parallel durable saving included in the next accepted baseline. The [candidate plan](EPOCH31-CANDIDATE-PLAN.md) supplies the hypotheses. The recurring follow-up stays paused.

The isolated source is `/private/tmp/spann-research31`. It combines compact navigation result handling, conditional shared-view transport and bounded parallel posting writes. Independent changes add an event-driven listener wait, configurable insertion navigation windows and detailed attribution of shared boundary-centroid refresh. The default insertion window remains 64. The listener supports `SPANN_ACCEPT_MODE=poll_1ms` as a same-binary control; the candidate uses `readiness`. Saving supports `SPANN_SAVE_WORKERS=1` as a sequential control and defaults to 32. Query-window experiments support 1–256 through `SPANN_INSERTION_NAVIGATION_WINDOW`.

The full native suite passes 280 tests, with two opt-in actual-S3 tests and one timing benchmark ignored; the known unrelated parent/leaf test remains excluded. Both detailed boundary-refresh tests pass. Five alternating loopback pairs show median run-mean request latency of 1,282 microseconds with the original listener and 85 microseconds with readiness waiting; these native debug loopback measurements establish the idle-listener effect but do not predict distributed throughput.

The original two machines remain running without a restart. Their shutdown guards are renewed to October 22 at 19:57:07 UTC (3:57:07 p.m. Toronto), within the existing budget. All cloud benchmarks run serially under the retained exclusive machine lock. Source, input and compiler caches remain on the machines. The exact epoch30 writer checkpoint is reused.

The first build captures a diagnostic revision before its final wide-identifier serialization correction. A second build unexpectedly reuses the same executable because archive timestamps do not invalidate the compiler cache. Neither binary is benchmarked. A third build touches source dependencies before compiling, produces a distinct executable and binds the final manifest. This failure is retained as a build-cache finding; future source extraction must invalidate changed-file timestamps explicitly.

The final Linux executable SHA-256 is `bc31c7e0a4e5a66c157c5d6ebfa5e8abdaa7844927d0a8d86b2fba98ac67f54c`. Immutable source and binary manifests, native logs and run receipts are under `../operations/development31`. The source archive is `../packages/source-epoch31v3.tar.gz`.

An additional insertion-only worker override is implemented and passes 24 construction tests at 64 workers. It leaves balancing worker counts unchanged. This candidate will use a separate immutable binary after the current timing series; no machine build runs concurrently with measured construction.

The detailed readiness run identifies repeated centroid-refresh publication contention: 950.498 summed refresh wall-seconds, 897.608 summed turn-wait seconds, 5,074 attempts, 4,004 view/owner-change retries and 326 stale-prepare retries. There are 423 actual publications and 306 validated no-ops. These concurrent durations overlap. The longest retained refresh lasts 12.243 seconds with 12.189 seconds in turn waits. The optional per-boundary gate therefore has a measured target; it retains fresh validation for each request.

The v4 binary retains the original listener as default because readiness waiting does not improve median construction across three trials. It adds separate opt-in insertion-worker and boundary-refresh controls. Both full native suites pass 284 tests with refresh serialization enabled and disabled; the restored default listener separately passes all ten transport tests. The v4 executable SHA-256 is `92da2dbc3b368b1e7cad118252eab3151e6dd1d0b09af2008aa16e85028988b8`. The immutable archive is `../packages/source-epoch31v4.tar.gz`.

Early independent runs show that 64 insertion workers reduce insertion to approximately 8.2 seconds while raising insertion CPU demand to approximately 485 CPU-seconds. Per-boundary refresh serialization reduces balancing to approximately 18.5–18.7 seconds while raising balance CPU demand to approximately 680 CPU-seconds. These are provisional observations; repetitions and the combined configuration remain required before selecting the accepted settings.

The gated detailed run reduces changed-state refresh retries from 3,840 to 1,195 and total attempts from 4,942 to 2,371. Summed publication-turn waiting falls from 833.444 to 85.890 seconds; gate waiting adds 135.712 seconds. Total refresh wall time falls from 887.105 to 252.715 summed seconds. These inclusive concurrent times must not be added to phase latency. Detailed control balancing takes 25.581 seconds versus 19.220 gated; lightweight medians remain the timing comparison.


## Saving and validator results

The sequential and parallel first saves complete with four fresh-reader validations. First-save time falls from 131.141 to 7.996 seconds, with 2,129 versus 2,126 posting blocks. Unchanged saves take 3.270 and 3.410 seconds and reuse every posting block. A separate reader artifact bounds concurrent immutable fetches and retains all membership, integrity and code checks. Its two reader validations take 56.365 and 56.362 seconds versus 303.475 and 288.459 seconds with the sequential reader. This reader comparison is observational across different saved trees; compilation overlaps only completed-save functional validation.

An attempted lease renewal during active validation is rejected by the existing competing-job check. The old guards remain active. Renewal succeeds at the idle boundary after validation and reader compilation, with no restart. The new absolute deadline is October 22 at 20:58:09 UTC, and the open window compute cap is $37.548365. Costs remain in the active reservation until the machines stop; they are not added to closed compute.


## Completed combined configuration

Three combined timing runs and a combined save pass. Median insertion is 8.188 seconds, balancing 18.364 seconds and construction 29.234 seconds. The combined first save takes 11.072 seconds; the unchanged save takes 3.296 seconds and materializes no new posting blocks. Both fresh readers pass and reproduce identical query results across the first and unchanged generations. Use the [final report](EPOCH31-CONSTRUCTION-AND-SAVING.md) and [completion receipt](EPOCH31-COMPLETION.json) as the current outcome. Earlier progress sections retain the experiment sequence and failures.
