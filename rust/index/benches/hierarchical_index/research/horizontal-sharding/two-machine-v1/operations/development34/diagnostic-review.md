# Epoch 34 diagnostic review

The focused diagnostic changes are suitable for the matched diagnostic run after the implementation tests pass. Independent source review found no blocking correctness issue. This review compares `/private/tmp/spann-research34` with immutable `/private/tmp/spann-research33view`; the reviewed files are `transport.rs`, `observability.rs`, `transport_tests.rs`, and the expansion counters in `engine.rs`. The existing context propagation in `routing.rs` was also checked. The reviewer made no source edits.

## Correctness and attribution

Ordinary requests retain the existing result wire format. A request explicitly opts into a timing envelope; the server returns the ordinary success or error inside that envelope. Typed deserialization preserves full-width identifiers. The existing frame bound applies to the envelope, including oversized-result fallback. The changes add no retry and preserve exclusive ownership of reused sockets until the full response is decoded.

A direct request records timing in the initiating job context. Owner expansion workers capture that context before spawning and install it inside the child thread. Their scoped joins finish before the enclosing job record is captured. Independent server worker threads do not inherit that context. The extended observability test verifies inherited-child attribution and independent-server exclusion. This provides direct-call attribution, not a distributed causal graph for nested server requests.

Request kinds come from a fixed classification, so the new maps have bounded key cardinality. Existing longest-job and last-finishing-job limits remain 32 each. No payload or authentication token is recorded. New timing collection and request aggregation are conditional on detailed mode. Ordinary calls avoid new timing snapshots and map updates, although fixed fields remain in pending-frame storage.

## Interpretation limits

The decode queue begins after the complete frame is read and ends when a decoder takes it. Request socket read time is outside that queue. Decode time includes envelope deserialization and authentication checking. The execution queue begins after decoding and ends just before the handler starts; it includes decoded-result delivery, reactor dispatch, and worker scheduling. It is broader than time waiting in the worker channel alone.

Handler time includes the handler's computation, locks, and nested remote calls. It excludes response serialization and socket writing. Client elapsed time includes connection acquisition, request serialization and transmission, server time, reply transmission and deserialization, and small client bookkeeping costs. Client decode time is separately available. Subtracting measured server and client decode durations from client elapsed does not isolate network latency; serialization, socket transfer, scheduling, and instrumentation remain in that residual.

Concurrent owner calls overlap. Summing their elapsed durations does not yield the job's critical path. Nested server handler time also overlaps the direct request that contains it. Maximum request elapsed is a per-request-kind maximum, not a complete individual request trace. Request maps exist at transport totals and retained individual jobs; they do not currently populate all-job-kind `by_origin` totals.

A review finding identified that a detailed client can request timing from an ordinary server, which then lacks decode and queue measurements. The implementation now adds an explicit queue-timing availability indicator and reply count, rather than treating those zeros as measured. The parent confirms both cloud peers enable detailed mode for the diagnostic run. Older servers do not support the opt-in timed response, so the run must use the matching binary already required by the harness.

## Validation gate

The implementation agent owns native compilation and tests. Relevant tests cover ordinary and detailed reused connections, timed application errors, full-width values, oversized request and response handling, and inherited job context. This source review does not independently claim their execution passed. Cloud timing comparisons must use detailed mode off in both arms and preserve the same checkpoint and transport implementation.
