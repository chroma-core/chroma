# Epoch 34 bounded expansion candidate review

The candidate is suitable for paired benchmarking after its tests pass. Independent source review finds no blocking correctness issue in `engine.rs` and `engine_endpoint_tests.rs` under `/private/tmp/spann-research34`. The reviewer made no source edits or cloud changes. Transport diagnostics remain outside the candidate change.

## Behavior and correctness

The candidate overlaps two groups of remote navigation queries sent to the same owner. Each initial group contains at most 64 queries. A single-group call remains inline. A multi-group call uses the calling thread and one scoped child worker; assigning alternating initial groups to these two lanes bounds simultaneous calls to two per invocation regardless of group count. This is a per-invocation bound, not a process-wide thread pool.

Both lanes use the same absolute deadline. Completed results retain their original initial-group index, are sorted by that index, and are exposed only after the child worker has joined. Existing left-before-right subdivision order remains within each oversized group. The navigation caller still validates complete replies and chooses its next beam only after the full expansion returns. The overlap changes the timing of read-only routing snapshots; it retains the existing revision, topology, completeness, and ordering checks.

An arbitrary transport or application error is recorded as an error and is not retried. The shared cancellation flag prevents later initial groups from starting once a lane observes failure. A group already in progress may finish its size-proven subdivisions before returning; all subdivisions retain the common deadline. An error causes the complete expansion to fail, so the caller receives no successful prefix. Existing subdivision occurs only for explicit request or reply size errors and preserves complete parent rows.

The child worker captures and installs the initiating job context. Its request timings and ordinary work scopes therefore belong to the same job as the caller lane. The paired-overlap test now checks that all four direct expansion timings, across both lanes, appear in the initiating balance job. The historical `ExpansionSequentialGroups` scope now encloses the whole invocation, including overlap; it must be interpreted as invocation elapsed time, not a sum of sequential group times.

## Pool dependency assessment

Remote expansion runs on the control pool. Its handler snapshots local immutable routing image handles under short read locks, releases those locks, and scores locally. It issues no nested remote call. Adding the second request therefore adds terminal control work and does not introduce a new control-handler-waits-for-control-handler edge.

At 32 balancing jobs per owner, two expansion requests per active job imply an expansion-only upper bound of 64 incoming requests at one destination, below the configured 128 control handlers. Other control requests can compete for those handlers. The existing server rejects excess admitted work rather than promising progress for arbitrary recursive control graphs. The benchmark must check admission rejections and failures; this review does not claim universal deadlock freedom or unlimited concurrency safety.

## Validation requirements

The implemented endpoint tests force two handlers to overlap and complete in reverse group order, then check byte-value-equivalent output ordering and a peak of exactly two active handlers. Additional tests cover application rejection without retry or prefix and an already-expired deadline that sends no request. The implementation agent reports all 11 bounded-expansion tests pass with concurrency one, concurrency two, and concurrency two with detailed diagnostics. This includes the added direct job-context coverage. The reviewer inspected the final test assertions and panic-to-error handling; no blocking issue remains. Native test results and exact replay results must be retained separately; source review alone is not an execution result.

Paired measurements should use the same binary, checkpoint, and accepted transport/navigation settings, with concurrency one and two as the sole arm difference. Insertion mostly uses one group and serves as a useful unchanged-path check. Balancing latency, CPU, work counts, failure/rejection counts, and full validation determine whether to retain the candidate.
