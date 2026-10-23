# Compare construction on two machines at 500K vectors

This experiment measures construction after an early ownership split, using the retained source-leaf-claim binaries and a matching unsharded reference. It is prepared locally in the side conversation at Lyon's request. It must run after the main conversation releases the benchmark machines. This folder does not admit or launch a cloud window, change the active plan, or modify the implementation.

## Question and decision

Does the current distributed writer remain reasonably close to the reference when both machines perform useful construction on an increasingly developed tree? Report the first 100K interval separately from the following four intervals. Use the resulting time, CPU and work counts to choose whether local implementation overhead, communication or coordination deserves the next investigation. A single execution does not estimate timing variance, establish causality for the slower reference, or establish quality parity.

The hypothesis is that the first interval's shallow tree and large initial split make it a poor predictor of subsequent construction. Five matched intervals provide direct observations as the tree develops. Measure actual tree shape and work rather than assuming that 500K reaches a particular depth or extrapolates to millions. Cache-capacity crossing is not the question: the 500K full-vector input is 1,536,000,000 bytes, below the 12 GiB per-owner vector cache budget.

## Workload

| Setting | Value |
| --- | --- |
| Input | Identical first 500,000 vectors of the retained, verified 3M corpus, in the same order. Reuse the main experiment's prefix after verifying both hosts' receipts and bytes. |
| Vector representation | 768 dimensions, squared L2; full-precision construction, one-bit posting codes, four-bit saved centroids. |
| Insertion and balancing cadence | Five 100K batches in both implementations. The 1M transition and later 1M batch settings remain unchanged and are not reached. |
| Concurrency | Reference: 32 workers on one machine. Distributed: up to 32 workers per owner on two machines. Report CPU and allocated resources alongside elapsed time. |
| Tree settings | Branching 100, split threshold 2048, merge threshold 512, existing unchanged construction search settings. |
| Initial ownership | One global tree on owner 1; no initial static partition. |
| First placement budget | 80,000 vectors per owner after the first 100K batch. The existing 80% trigger is 64K; its 70% relief target is 56K. This forces branch redistribution before the remaining 400K additions. |
| Later placement budget | 400,000 vectors per owner. The trigger is 320K and relief target is 280K. Existing branch placement and admission rules remain unchanged. |
| Replications | One reference run and one distributed run, sequentially; no automatic retries or size sweep. |
| Observation | Existing counters and phase timings; no external profiler or profiling gates in either arm. |

Whole-branch sizes determine the exact placement. Verify actual shares and work rather than claiming an exact 50/50 split. Final occupancy must be at least 30% on each machine. For this experiment's stronger participation criterion, each owner must receive at least 120,000 of the 400,000 newly added IDs after the first redistribution and commit balancing work during those later intervals. Derive new-add counts from the before/after insertion snapshots, excluding migration and reassignment. Failure of this criterion makes the run unsuitable for the intended distributed-construction comparison; preserve it and do not automatically rerun.

The main conversation's 500K configuration keeps construction on the initial owner and redistributes only after the final balance. This experiment uses the same binaries and input but deliberately changes placement timing. Keep separate results; neither replaces the other or the 100K results.

## Execution handoff

The existing sole operator should execute this after the active experiment closes. Do not start a second controller, use its locks, or extend its deadline. Use the existing lifecycle and remote-job supervisor with a fresh window, fresh output paths, current instance identity and independent local and remote shutdown guards. This preparation has no live deadlines or IP bindings.

Use only sandbox account 079819354890 in us-east-1 and task instances i-011ff180f03584f40 and i-00268d1e1dfea0357. Both are the existing 32-physical-core r6i.16xlarge machines. The proposed maximum is 30 minutes for the window and 590 seconds for each arm including validation. At the retained rate of $4.032 per machine-hour, the conservative two-machine window bound is $4.032. Recheck identity, rates, available headroom and remaining cumulative budget at admission. No new instance, volume growth, corpus download or build is required.

`prepare_specs.py` creates three local, explicitly unadmitted job specifications: reference, distributed coordinator and distributed peer. It reads the immutable epoch12/13 job templates and the main run's completed prefix receipts. Pass fresh private addresses, a fresh window name, absolute deadlines and an unused local output directory. It does not contact AWS, start a process remotely, upload, or change any existing file. Its outputs deliberately contain `combined_review_passed: false`; the operator must review and admit the exact specifications through the normal process before execution. Regenerate each arm's absolute job deadline immediately before admission if preparation has consumed its time budget.

The input prefix receipts must agree on hash and exact dimensions, size and row range. The remote supervisor and corpus adapter must recheck actual bytes when run. Preserve all existing input and output roots. Stop owned benchmark processes and idle task compute after completion or failure, verify stopped instance state through the API, and count the window's compute once.

## Measurements and result document

Write a separate `RESULT.md` beside this plan after execution. Record insertion, balancing, placement, save, validation and gross end-to-end time separately. Present all five intervals, first interval versus subsequent intervals, and their totals. Do not hide initial construction costs by reporting only later intervals.

For each interval, record per-owner memberships and newly added IDs, splits and merges, prepared/committed/discarded split work, transfer rows, compactions, reservation examinations and waiting, navigation work, framed network bytes and peak memory where available. Report coordinator CPU measured around balance separately from owner snapshot CPU intervals; the latter include boundary observation. Sum both owners' comparable CPU intervals for resource accounting, and do not present owner 1 clustering counters as cluster-wide kernel CPU. Mark unavailable per-owner counters explicitly. Nested CPU scopes and aggregate worker waits are not additive elapsed time.

Require fresh-reader validation of exact membership, full-vector bits, posting codes and saved centroid codes. The existing 16 training-vector queries remain smoke tests. Record source and binary hashes, input hash, hardware, per-process cache conditions, instrumentation, random-seed and scheduling limits. Compare this new reference with the main conversation's 500K reference as an additional observed execution, without assuming that two executions establish a variance distribution.

Stop and preserve partial evidence on correctness failure, insufficient useful participation, process failure, timeout, or failed resource/identity checks. A timeout is not a completed timing. After the two arms and result interpretation, stop; no optimization implementation is authorized by this preparation request.
