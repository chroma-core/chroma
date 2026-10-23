# Epoch24 expansion codec and bounded transport review

The corrected epoch24b source and fixed 3M candidate preparation have no remaining blocker within this review scope. Complete expansion data uses a lossless private wire encoding, and read-only remote expansion subdivides only after a specific size failure. The global navigation algorithm still receives every required parent row before selecting its beam. The next physical run measures performance and correctness; smaller wire records do not prove a latency gain.

## Exact field representation

Queries use counted little-endian finite f32 bits, and scored children use counted fixed 69-byte records carrying the full parent ID, ordinal, target ID, authority, placement, leaf flag, distance bits and observed revision in original order. STANDARD padded base64 carries those records inside the existing JSON envelope. Finite values, signed zero and subnormals retain exact bits. Nonfinite values, invalid flags, truncated or trailing data, inconsistent counts, invalid padding/alphabet and arithmetic overflow reject.

Encoding checks the decoded field size before filling its buffer. Decoding checks encoded length before base64 allocation, independently checks decoded length, and verifies exact count/width agreement before allocating typed rows. A max-plus-one decoded payload can share the maximum encoded length and is explicitly tested. The 12MiB-minus-two field limit does not increase the independent 16MiB frame limit; encoded fields plus envelopes may still exceed a frame.

Required versioned field names and unknown-field rejection make old, new and ambiguous mixed messages fail closed. Both endpoint binaries must match. Semantic run/epoch/attempt/step/query-slot identity, parent coverage, child-count/ordinal completeness, finite distances, duplicate/replay and revision checks remain in the navigation driver. Persisted trees, posting codes and saved-reader formats do not change.

## Size proof, subdivision and deadlines

Serialization finishes in a frame-limited buffer before any response prefix is written. A server that cannot fit a reply sends a reserved small size error instead of closing the connection with an ambiguous EOF. Generic callers receive a typed size error; this does not assert a mutation was unapplied and adds no transport-level mutation retry.

Only the read-only Expand path adapts. It starts with at most 64 query requests, preserving the fitting insertion batch. A typed request/reply size rejection halves the query group; only after one query remains can it split that query’s complete parent set. It never splits a parent’s child rows or applies owner-local truncation. A single complete parent that still exceeds transport or codec limits fails explicitly. Each split reduces a finite request or parent count.

Every subcall keeps the original query slot, navigation identity, step, run, epoch and shared sequence. Parent subsets remain subject to full global coverage validation and canonical ordering before beam selection. Replies stay private until every subcall succeeds; a later failure returns no successful prefix. All proactive groups and adaptive calls share one initial absolute deadline already capped by the client’s operation limit. Subdivision does not renew that deadline or enlarge a frame.

Review identifies and resolves one size-classification blocker. A buffer whose length happens to equal its limit is not proof of overflow if the serializer subsequently returns an unrelated error. The corrected buffer records an explicit flag only when a write would exceed the limit. Classification uses that flag or the codec’s exact size-limit/size-overflow messages. Nonfinite data, allocation errors, arbitrary codec failures, application rejections and EOF remain fatal without adaptive resubmission. The retained red regression reproduces the wrong classification; the corrected test distinguishes exact-limit arbitrary failure from real overflow.

## Evidence and limits

The six codec tests and 15 navigation tests pass in `EXPANSION-CODEC-VALIDATION.json`. They cover full-width identities, finite float bits, version mismatch, malformed and oversized payloads, actual TCP parity and unchanged frame limits. A representative fixed reply falls from 267,587 to 94,516 framed bytes. That is a representation result for the fixture, not a universal compression ratio or an end-to-end prediction.

The controlled retained-array transport fixture produces a complete 17,762,811-byte reply and reproduces source23’s reply rejection followed by caller EOF. The explicit-size correction passes. Other tests exercise adaptive query grouping, whole-parent subdivision, single-parent failure, codec-field size signaling, rejection of arbitrary codec/EOF/application errors, no successful prefix after a later failure, one absolute deadline, and one RPC per fitting 64-query/16-parent level with reference-compatible global results.

The corrected applicable suite reports 236 passed, zero failed and one filtered test in 24.05 seconds in `epoch24-suite-02.log`. The exclusion remains the known unrelated reference-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`; an unfiltered clean suite is not claimed. The earlier passing suite and failed classification regression remain retained. Native CLI builds and fresh Linux artifact verification remain root-owned admission gates.

Physical epoch23 logs establish reply-size errors before construction failure, but lack the first request identity and a cross-host total order. Expansion attribution is supported by source and the real-engine fixture, not an exclusive trace reconstruction of every EOF. Oversized reply attempts may already have completed scoring before rejection; those actual calls, bytes and distances remain counted. Base64 CPU, temporary encoding buffers and repeated read-only scoring are part of measured cost. Memory bounds apply to fields and serialized frames, not to all live tree state. No frame limit, deadline or algorithm setting increases.

## Source closure and execution preparation

This review binds only corrected `packages/source-epoch24b.tar.gz`, 23,075,911 bytes, SHA256 `9e5f4d1ed698acaf22fddb29dc4ae50e336e48e9280f896bfc7eba48391869eb`, with manifest SHA256 `497c0e3fca98c75a950ff8b719249c0190802ec3d9ffbbaf8798abb12067ef10`. Independent comparison with frozen source23 confirms five modified files, the two new codec files and no removals. The initial source24 archive remains unadmitted and retained; final configuration metadata must bind 24b.

The epoch24 adapters preserve one candidate-only 3M run against the valid epoch21 reference, exact retained input, 768 dimensions, 32 workers, established cadence and thresholds, 2,160,000 capacity and zero first-owner override. Runner and build adapter retain their controls with fresh namespaces. The epoch15 supervisor, 590-second job limit, 660-second remaining-window check, 1,800-second host window and independent shutdown guards remain unchanged. There is no reference rerun, profiler, automatic benchmark retry or sweep.

All hashes in `operations/preparation-epoch24.json` match at review time, including the inherited epoch23 candidate template SHA256 `e60162bde9a17f2bcaf991c49c1b065663dfad263021a63877cb970cbe7c3994`. Five preparation tests pass in `epoch24-preparation.log`. Pending runtime bindings are placeholders, not verified artifacts. The root operator binds final configuration/archive metadata, native and Linux build receipts, matching binaries on both hosts, fresh identity/input checks and absolute guards before admission. Final 1M construction participation and full fresh-reader validation remain mandatory. This reviewer performs no source edits, Cargo commands, cloud operations or Git writes.

## Reviewed source hashes

Paths are relative to `rust/index/benches/hierarchical_index/distributed` in the corrected frozen archive.

| File | SHA256 |
| --- | --- |
| `engine.rs` | `3ec0e8568f54e5446e920aa0b79c92466b0ff9f66ac04b2ae1a4bba39ad758c0` |
| `engine_endpoint_tests.rs` | `39b55e2822d9921ce6022cd4550b676219491b6326a5c174315d08339590b6fd` |
| `expansion_codec.rs` | `45777ad8174bf430f77d9a16e885c71e887004773e7ec16b49e2914e1efc219c` |
| `expansion_codec_tests.rs` | `7c93ea2a99f167b144204b28acfe4bb3682654b5ff3c1588098d0b7d9503fdaf` |
| `routing.rs` | `ce5c98d2863e478b6a2fb3cc07633b749fb554ba875f848a3c64719c7ac1665b` |
| `transport.rs` | `bf1e66b801ab6bf7a32a0c09166deb8f1dbe75e18a9a269128dbe91edfba91ad` |
| `transport_tests.rs` | `7b8380bdb59fdc8b4685da7558008ea82a46aaf9e904af2c16ecf9fb7c8dc177` |
