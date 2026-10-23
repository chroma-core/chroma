# Reduce sequential remote navigation waits

The focused diagnostic validates exact 3M construction and identifies a concrete balancing candidate: execute two bounded chunks of one owner's navigation request concurrently. Different owners already execute concurrently. The remaining sequential loop divides a large batch into at most 64 queries per request, then waits for each reply before submitting the next chunk. Candidate implementation and same-binary performance validation are in progress.

## Diagnostic method and correctness

The diagnostic restores the accepted full-precision 2M checkpoint, inserts the final million and balances with all epoch33 accepted settings. Both owners use the same frozen executable. Detailed requests ask for reply-attached server durations and accumulate direct request-kind totals in their initiating job. Existing thread-context propagation connects spawned owner requests to the job. The longest 32 jobs and latest 32 completions remain separate bounded sets.

The server reports decode admission wait, request decoding, decoded-result dispatch/worker delay and handler elapsed time. Handler time includes nested calls but excludes response encoding and writing. The client separately records elapsed and reply decoding time. These durations use local monotonic clocks and require no cross-host subtraction. Queue timing availability and timed-reply counts distinguish missing measurements from zero delays. Endpoint totals, concurrent calls and nested scopes must not be summed into a critical path; unexplained residual time is not network-only latency.

Both detailed and ordinary native suites pass 316 tests, with three opt-in ignores and one existing unrelated parent/leaf concurrency exclusion. Native benchmark compilation passes. Independent review checks bounded metadata, unchanged ordinary wire format, no mutation replay and cross-thread attribution. Matching peers are required for detailed timing envelopes. Detailed mode adds CPU and wire overhead and remains disabled in primary candidate timing runs.

The diagnostic source archive is `two-machine-v1/packages/source-epoch34v1.tar.gz`, SHA256 `b57e5b362dbba3cfb0ff636b0bd5398d1614755c6b0636ce8feb8cc62f45c2b8`. The Linux executable SHA256 is `c0d6fcc537c21b6b3d83f11097590b72f62c74cb03444546b78b6538402733ed`. The cached Linux build takes 66.083 seconds. The [operations directory](../operations/development34) retains source manifests, both native test logs, reviews, binary receipts, configuration and raw cloud evidence.

## Findings that select the candidate

Balancing makes 9,216 remote owner expansions. Of these, 2,763 require multiple initial chunks, producing 18,022 subcalls. The initial-group distribution is 6,453 single-group calls, 1,021 two-group calls, 661 with three or four groups, 920 with five to eight groups and 161 with nine or more. No size-triggered subdivision occurs. Insertion makes 15,680 remote expansions, all single-group. Therefore ordinary chunk concurrency principally targets balancing in this workload; an insertion latency benefit is not predicted.

A late owner-2 job lasts 0.581 seconds and finishes approximately 0.158 seconds before balance completion. It makes 24 expansion calls over ten owner-expansion invocations. Those expansion calls take 0.169 seconds in total, including 0.026 seconds decoding replies and 0.051 seconds in remote handlers. Remote decode and execution queues sum to approximately 0.008 seconds. A later 0.181-second job makes nine expansion calls over three owner-expansion invocations, spending 0.073 seconds in these requests. Their sequential groups create work that can potentially overlap; the diagnostic alone does not establish an end-to-end gain.

The longest owner-2 jobs last 2.748 and 2.736 seconds. Their centroid-refresh requests take approximately 1.519 and 1.470 seconds, almost entirely inside the remote handler. They finish 6.317 and 8.397 seconds before balancing completes. These observations retain the distinction between a longest job and the final dependency. Coordination remains accepted and its further investigation is deferred to larger-index benchmarks, as requested.

Across the coordinator's balancing client endpoint, 9,017 expansion requests accumulate 98.936 seconds of elapsed time, 34.603 seconds in server handlers and 15.106 seconds in client decoding. Decoder and execution queues total 3.542 seconds. This aggregate is not the two-owner total or a completion-time decomposition. Remote scheduled-job requests accumulate 485.737 seconds, of which 482.938 is handler time; those calls wait for the entire remote job and do not imply network overhead.

## Candidate and comparison plan

Test two active query chunks per owner expansion against the existing one-at-a-time path. Preserve the 64-query ceiling, shared absolute deadline, complete-result barrier, stable original-group assembly, explicit size-only subdivision and rejection of ambiguous failures. Propagate job attribution to any new chunk workers and avoid creating workers for single-group requests. Expansion handlers are terminal local scoring operations and introduce no nested control-call dependency. Other control work still competes for bounded capacity, so the benchmark must check rejection counters and CPU as well as elapsed time.

Use one executable with a configuration switch for exact fixed navigation replays and three interleaved control/candidate pairs. All runs restore the identical 2M checkpoint. Keep detailed collection off for timings and run a separate candidate diagnostic. Preserve failed attempts and work-count differences. The candidate remains experimental until these results support acceptance.

## Implemented candidate and local validation

A multi-group expansion uses the calling thread and one scoped child worker, assigning alternating initial groups to the two lanes. Each lane processes its groups sequentially. Replies carry their original group index and are assembled in that order after the child joins. A shared cancellation flag prevents new initial groups after an observed error; already-started read-only work may finish within the same absolute deadline. No successful prefix is exposed. Single-group calls stay inline. This is bounded per invocation, not a new process-wide worker pool.

The control uses `SPANN_EXPANSION_GROUP_CONCURRENCY=1`, and the candidate uses `2`; the source default remains one pending acceptance. Both use the same executable. The existing scope named `ExpansionSequentialGroups` now measures the full invocation including overlap; it must not be treated as a sequential-duration sum for the candidate.

Independent review finds no blocking source issue. The full ordinary concurrent suite passes 319 tests with three opt-in ignores and one known unrelated exclusion. Eleven focused bounded-expansion tests pass with serial and concurrent settings. The final detailed focused suite passes twelve tests, including one test-only multi-group size-bisection case added after the full run. Tests exercise two in-flight requests, reversed completion order, bounded concurrency, no successful prefix or arbitrary retry after error, deadlines, size subdivision and propagation of all four test requests into the initiating job. Native benchmark checking passes.

The candidate archive is `two-machine-v1/packages/source-epoch34v2.tar.gz`, SHA256 `8b4041f880a593d2702fa2745cd8a9e4acb006dab13ff7bee5c4e1595404ca31`. The source is `/private/tmp/spann-research34`; the immutable diagnostic remains in its v1 archive. The [candidate review](../operations/development34/candidate-review.md) and [local validation receipt](../operations/development34/candidate-local-validation.json) retain scope and limits.

## Bounded verification extension

After observing the first two pairs, the comparison adds exactly three verification pairs because balancing improves in both but the magnitude varies while CPU increases. This decision is recorded before pair three's candidate result is collected. The additional order is on/off, off/on, on/off; the original order is off/on, on/off, off/on. All six pairs are retained, with initial and verification subsets reported separately. No further extension for statistical significance is planned. Combined intervals are exploratory because the sample-size extension follows initial observations.

The initial three pairs all validate exact 3M construction. Mean balancing is 14.782 seconds serial versus 13.772 concurrent, with 521.093 versus 545.173 CPU-seconds. Insertion is effectively unchanged at 7.502 versus 7.505 seconds. Construction is 23.925 versus 22.144 seconds. The initial paired balancing interval still crosses zero (candidate minus control −2.236 to +0.217 seconds); the extra verification is therefore relevant to accepting the CPU tradeoff, not grounds to claim a precise speedup from three pairs.
