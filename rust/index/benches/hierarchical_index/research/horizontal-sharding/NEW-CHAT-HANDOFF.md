# Active work: epoch34 diagnostic and latency improvement

Follow `two-machine-v1/operations/development34/experiment-plan.md`. The user authorizes a checkpoint-based diagnostic followed by implementing and benchmarking the best supported improvement, with parallel work. Source is `/private/tmp/spann-research34`; epoch33 remains immutable. The prior guarded window is closed. The new window uses the same two machines and retained checkpoint. Keep recurring follow-up paused. Current work adds per-job request-kind attribution and remote expansion chunk counts; owner fanout is already concurrent, while chunks within one owner are sequential. Select the candidate after the cloud diagnostic.

# Current status: epoch33 complete

Connection reuse, bounded server pools and the navigation parent cache are accepted. Read the [epoch33 report](two-machine-v1/local-tests/EPOCH33-TRANSPORT-AND-NAVIGATION.md) and [completion receipt](two-machine-v1/local-tests/EPOCH33-COMPLETION.json). The final source is `/private/tmp/spann-research33view`, frozen in `two-machine-v1/packages/source-epoch33v3.tar.gz`. Use `two-machine-v1/operations/development33/accepted-run.py UNIQUE-NAME [save] [detailed] [profile]` for the accepted configuration. Both peers must use this executable because complete-view installation has a new bounded wire representation.

All six fresh timing arms, both exact navigation replays, both detailed constructions, and first/unchanged-save fresh-reader checks pass. The native suite passes 315 tests with three opt-in ignores and one documented unrelated exclusion. The cache lowers insertion time by 6.6%, insertion CPU by 7.6% and balancing CPU by 6.3%; balancing and overall elapsed-time effects remain inconclusive. Preserve the incomplete v1 cohort and its unconfirmed frame-limit failure separately from the fresh v3 cohort.

No epoch33 validation remains unfinished. The next research action is to correlate late jobs with remote request types and receiving-handler queue/execution times, then select one measured navigation or request-overlap improvement. Keep coordination enabled, preparation-under-turn disabled, notification listening enabled and all accepted saving changes. Do not resume cancelled epoch32 queues. Coordination's larger-index revisit remains in CURRENT-PLAN.md.

Both authorized machines remain running by user request, with no benchmark processes left. Existing budget guards expire at 2026-10-22T23:09:15Z. Reuse retained inputs, checkpoints and compiler caches. The recurring follow-up remains paused. The final audit and active spend reservation are recorded in the completion receipt and SPEND-LEDGER.json. The peer has approximately 6.1 GB free; review verified disposable caches/copies before another large artifact-producing batch, preserving raw evidence and checkpoints.

## Retained earlier context

# Current decision: retain coordination and defer further investigation

Coordination remains accepted. Read the [epoch32 findings](two-machine-v1/local-tests/EPOCH32-BALANCING-RESULTS.md). Six whole-workload pairs and four exact-state pairs reproduce faster balancing with higher CPU. The complete CPU explanation remains unresolved. The user closes this investigation and defers revisiting coordination to larger-index benchmarks specified in CURRENT-PLAN.md. The preparation-under-turn option remains experimental and disabled.

The next recommended action is to inspect retained navigation profiles and detailed scope costs, then choose one reduction in candidate processing, validation or ordering for exact replay and complete final-million validation. Keep notification-based listening and all accepted saving improvements. Do not restart the cancelled epoch32 orchestration scripts. The recurring follow-up stays paused; machines stay running within existing budget guards. Reuse inputs, binaries and the retained 2M and post-insertion 3M checkpoints.

# Current status: epoch31 complete

The accepted configuration reduces final-million construction from a 37.512-second median to 29.234 seconds across three timing repetitions. Insertion falls from 12.957 to 8.188 seconds and balancing from 23.071 to 18.364 seconds. CPU demand increases; retain that tradeoff in comparisons. Bounded parallel posting writes are accepted design and implementation. First saves take 7.996–11.072 seconds versus 131.141 seconds in the sequential control; unchanged saves remain approximately 3.3 seconds and reuse every posting block.

All construction runs pass exact 3M membership and posting-code checks. Six fresh readers pass all membership, posting-code, centroid-code and full-vector-hash checks across three first/unchanged save pairs. Each pair returns identical results for all 16 saved-reader queries. Read the [report](two-machine-v1/local-tests/EPOCH31-CONSTRUCTION-AND-SAVING.md), [completion receipt](two-machine-v1/local-tests/EPOCH31-COMPLETION.json), and [accepted profile](two-machine-v1/operations/development31/accepted-profile.json).

## Continue from the accepted implementation

Use `two-machine-v1/operations/development31/accepted-run.py UNIQUE-NAME [save] [detailed] [profile]` for future final-million runs. It loads the accepted profile: 64 insertion workers across the pair, 32 balancing workers per owner, a 64-query navigation window, per-boundary refresh coordination, the notification-based listener and 32 posting-save workers per owner. Compact navigation gathering and conditional routing-view transport remain included. The wider navigation window remains experimental. The user accepts the notification-based listener for its lower request latency despite no demonstrated construction speedup. The reported 29.234-second median uses polling; `benchmarked-profile.json` preserves those settings. The existing binary already supports notification-based listening, so the accepted launcher enables it without rebuilding frozen artifacts. Carry this listener default into subsequent source builds.

The frozen writer source is `/private/tmp/spann-research31`, archived as `two-machine-v1/packages/source-epoch31v4.tar.gz`. Its binary SHA256 is `92da2dbc3b368b1e7cad118252eab3151e6dd1d0b09af2008aa16e85028988b8`. The separate validated reader source is `/private/tmp/spann-reader31`, archived at `operations/development31/reader-source.tar.gz`; its binary SHA256 is `0d66c8fdfeb0c20b26f5fc9a5bdb1bfee1b028656c35b87146c2ef28d1a94271`. Reader validation fetches immutable blocks in bounded waves while preserving all checks. Preserve independent changes in the shared checkout.

Both machines retain immutable binaries, input, compiler caches and the exact full-precision 2M writer checkpoint under `/home/ubuntu/spann-two-machine-v1/development30`. Reuse them. The reader artifact is required only on the coordinator. The scripts retain source/configuration hashes and raw receipts; avoid new input downloads, repeated full input hashing and machine restart cycles.

## Next research action

No epoch31 experiment remains unfinished. The next candidate investigation should measure remaining global publication serialization and the extra CPU/distance work under higher concurrency. The per-boundary gate reduces speculative retries but retains one global publication turn. Determine which operations can safely prepare or publish independently before altering that protocol. Scheduler eligibility and persistent-connection work remain secondary candidates. These 3M results do not establish behavior at 100B vectors.

## Machine and automation state

Both machines remain running at the user's request. Only sandbox account `079819354890`, region `us-east-1`, worker `i-011ff180f03584f40` and peer `i-00268d1e1dfea0357` are authorized for this workflow. The combined rate is $8.064/hour. The current guard deadline is October 22 at 20:58:09 UTC (4:58:09 p.m. Toronto); the open window compute cap is $37.548365. Inspect `two-machine-v1/operations/window-epoch29c/start-intent.json` and live state before reuse. Renew only at a safe idle boundary within the existing budget, with new guards verified before old guards retire. Keep the recurring follow-up paused. Never touch protected or excluded instances.

The [final cloud audit](two-machine-v1/operations/development31/final-cloud-audit.json) records fresh identity, artifact hashes, process cleanup and guards. Costs remain an active reservation while machines run; do not add them again to closed compute.

## Retained evidence

Experiments [28](two-machine-v1/local-tests/EPOCH28B-3M-RESULT.md) and [29](two-machine-v1/local-tests/EPOCH29-3M-RESULT.md) establish batching, owner-direct saving and unchanged-block reuse. The epoch30 [navigation](two-machine-v1/local-tests/EPOCH30-BASELINE-AND-NAVIGATION.md) and [transport](two-machine-v1/local-tests/EPOCH30-TRANSPORT-COMPARISON.md) reports establish the accepted CPU savings now integrated into epoch31. Preserve their failed attempts, immutable controls and comparison limitations. The [current plan](CURRENT-PLAN.md), [design](DESIGN.md), and [research goal](RESEARCH-GOAL.md) supply broader context.
