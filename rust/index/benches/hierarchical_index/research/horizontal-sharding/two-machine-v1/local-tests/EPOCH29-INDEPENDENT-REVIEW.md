# Candidate29 independent correctness review

The expanded review finds no remaining source correctness blocker after two corrections: exact-byte reconciliation for ambiguous immutable uploads and a frozen publication predecessor that prevents old save attempts from rolling back the latest generation. This acceptance covers the reviewed source hashes below, with local test receipts and limits stated explicitly. Actual S3 validation and the final native/Linux builds remain admission requirements.

## Scope and method

This is an independent read-only implementation review against the frozen28 archive, including untracked source files by comparing archive member contents with the workspace files. The review reads the repository AGENTS.md and CLAUDE.md, the target design, owner-direct feasibility assessment, saved-object copying assessment and experiment28 failure report. It inspects the existing S3 implementation to check the adapter assumptions. No Cargo command, cloud operation or Git write is performed by the reviewer.

## Resolved finding

**P1: The new durable adapter fails identical multipart immutable replay.** The selected existing storage client uses multipart upload for objects of at least 5 MiB. Its completion helper maps every failure to a generic storage error, including conditional-create precondition failures. The generation adapter resolves existing bytes only for its typed conflict error. Consequently an identical replay of a large immutable object returns an error instead of the same reference. Separately, the existing byte-upload multipart path lacks the abort-on-error cleanup present in its stream-upload path. That cleanup behavior is inherited from the reused storage API, not introduced by candidate29; selecting its existing stream API avoids inheriting it without broader storage changes. Relevant code is `generation_backend.rs` at the durable put call, `generation.rs` at immutable error handling, and `rust/storage/src/s3.rs` at `finish_multipart_upload`, `multipart_upload`, and `multipart_upload_stream`. This is a concrete mismatch with selected-backend behavior, not evidence that a measured candidate29 object has crossed the threshold.

The reviewed correction selects the existing stream API for cleanup and reconciles every failed immutable write by exact stored-byte equality. Different bytes, an absent object or a failed read preserve the original write error. Mutable latest-pointer conditional replacement is unchanged. The new deterministic regression covers generic failure with identical bytes, generic failure with different bytes, an absent object and a lost successful write response. The explicit ignored cloud smoke exercises small and multipart immutable replay/collision, expected-generation replacement and a publication race; the reviewer has not run it. Its thread barrier synchronizes entry into publication, not the individual S3 reads, so the result establishes one winner but does not guarantee both writes reached the ETag race. The model test gates the writes themselves and covers that precise race.

## Checks without findings

Owners perform complete immutable payload validation before publishing their owner manifests and returning acknowledgements. The coordinator reads owner manifests and routing metadata, validates strong posting references and coverage, and avoids posting payload relay. Save requests bind the backend destination into the frozen request identity. The selected durable reader rejects references without SHA256 and verifies length, checksum and SHA256 when reading bytes.

The generation publication path reads the current descriptor and its ETag together, checks the expected generation, and performs conditional replacement. Failed conditional replacement is reconciled only against the exact desired descriptor. Owner worker batches join every started sibling before returning a failure. Immutable evidence may remain after failure; publication remains last. The tests cover concurrent publication, stale-reader retention, owner failure, lazy opening, writer-file removal and zero saved-object relay requests.

The refresh change retains the prior dependency and publication checks while replacing the retry count with a single elapsed-time deadline and a yield between attempts. Every retry after acquiring a publication turn aborts the proposal and releases the turn. Deadline expiry while waiting aborts the proposal before returning. The finite revision-churn test targets the observed retry failure rather than weakening publication validation. The deadline bounds retry admission and publication-turn waiting; it cannot preempt a blocking owner RPC and is not a hard wall-clock cancellation of all I/O.

## Acceptance limits

The conditional test backend is a model, not proof of actual S3 conditional writes, acknowledgement handling, multipart cleanup or independent remote reading. A guarded real-backend probe and fresh-reader validation remain necessary. The expanded implementation reuses unchanged posting blocks but still scans all membership and writes routing metadata; it does not restore writers from a saved 2M checkpoint.

The high-level completed-save method now publishes against a captured previous generation. The expected predecessor is now frozen per save identity as described below.

The coordinator still downloads routing metadata and independent full-vector descriptors, which are required for compatible publication. No claim of zero coordinator storage reads or zero metadata bytes is justified. Full vectors remain independently located and must be available to a fresh reader through the corpus adapter.

## Expanded review: unchanged posting reuse

**Resolved P1: Replaying an older successful save could roll back the latest generation.** The failure condition is recapturing the current latest descriptor as predecessor on each invocation. With that behavior, saving `first`, saving `second` from the same sealed state, and replaying `first` supplies `second` as the expected predecessor of `first`, allowing conditional publication to move the latest pointer backward. The correction retains the original expected predecessor under a mutex in a map bounded to 1,024 identities, keyed by run, save attempt and destination configuration. Concurrent same-attempt calls retrieve the same predecessor. A current descriptor remains idempotent; a superseded attempt fails its original expected-generation check. The passing regression saves `first`, saves `unchanged`, replays `first`, and asserts failure with `unchanged` still current and the first reader valid. The map persists only for the live coordinator, consistent with the explicit exclusion of coordinator recovery.

The cache retains one saved image per leaf from one most recent successful owner save. Each entry holds a weak resident identity, a strong immutable routing image and a saved posting reference/count; it holds no posting payload. The weak identity prevents a replacement allocation or handoff from inheriting an earlier resident’s saved posting. The strong routing image ensures subsequent `Arc::make_mut` revision edits allocate a distinct image. Replacing the complete cache after an owner manifest succeeds bounds retained metadata by that owner snapshot, including any leaves removed since that save until the next successful save.

The mutation audit finds that append, pairwise transfer, local bulk transfer and owner-batched transfer replace or mutate the routing image when they change membership or codes. Split/merge replacement and ownership installation create new resident allocations or routing images. Reservation-only edits do not change posting bytes. Failed unresolved mutations prevent saving. No posting mutation path inspected can successfully preserve both identities while changing saved posting bytes.

The owner still scans all current membership IDs to detect duplicates and validate code lengths before omitting unchanged posting bytes. Routing metadata and manifests are serialized for each new generation. This is reuse of unchanged posting objects, not work proportional only to changed vectors and not reuse of routing blocks. Content-addressed posting names preserve equal bytes even after a conservative routing-only cache invalidation, although that case still serializes and issues a conditional upload.

The repeat benchmark supplies each generation’s explicit descriptor to its own reader process and validates both after coordinator shutdown. It records the unchanged repeat save separately from first-save timing. The structural cache tests exercise controlled saved split/merge/handoff shapes and replacement identity; they do not execute those full mutation protocols between saved generations. Transfer tests do execute the actual transfer paths and preserve the first reader’s references and content.

## Isolated corrected28 acceptance

The isolated refresh correction has no identified source correctness blocker. It is acceptable for its own benchmark after the isolated full suite passes and the frozen source comparison confirms only the refresh implementation and its regression differ from frozen28. The parent reports ten focused shared-runtime tests passing and the original exact retry-budget failure reproduced in 0.18 seconds. This reviewer does not rerun Cargo concurrently. The 30-second budget limits retries and publication-turn waiting; it does not preempt a blocking owner RPC.

## Reviewed source identity

The following hashes identify the expanded source after both corrections. The baseline archive remains `source-epoch28.tar.gz`, SHA256 `7cb1a5eeb090f53a258217dc9c098c01be9e65e03b721e47b809eb5f2866ed08`.

The reviewer reads the final save-suite receipt: 13 tests pass, zero fail (`epoch29-reuse-save-tests-review.log`). The implementation worker reports 266 tests passing before the final predecessor correction, with one explicit real-S3 test ignored and the pre-existing `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership` exclusion. The reviewer does not characterize that filtered receipt as all repository tests passing. Native rebuild and real-backend smoke are not established by this review.

| File under `rust/index/benches/` | SHA256 |
| --- | --- |
| `hierarchical_index/distributed/engine.rs` | `ec54a88365a2d94c2e39549e76fbf8dd995416e0a051109daa336d64c55fdf4a` |
| `hierarchical_index/distributed/generation_backend.rs` | `9a35bd2406db875490c0b6290efb91b6264361f9d48b9cca59868b280a96e989` |
| `hierarchical_index/distributed/durable_generation_tests.rs` | `146436af3592cb4409ac5c079df22ca94a3030d64bcb623e4578851bc055a26c` |
| `hierarchical_index/distributed/generation.rs` | `a794ba3c6c31a08e236acbdc0102de21732ec269d195e5ad15312df554e68531` |
| `hierarchical_index/distributed/save_runtime.rs` | `e7df444ea4599fa24c4a64eb7f5c6a4d848a9ed39cf1d07104c9526d5b476c5d` |
| `hierarchical_index/distributed/save_concurrency_tests.rs` | `1cf935be6c096ccbf3e899c9530a7ba34a856c2010aac1e0ce35bc43c1a2e51c` |
| `hierarchical_index/distributed/saved_chunk_codec_tests.rs` | `0df083f501f41eb62a4c5321a463dcd5a6672dbe189d8c40e6b4a3690b3e7857` |
| `hierarchical_index/distributed/saved_reader.rs` | `c6b6029405d0d5e7024adb43dc79d8b705bc08d58b65def8c736d53ca61d9983` |
| `hierarchical_index/distributed/mod.rs` | `a2e6a20c907566926332782ac3be0542e348e74e7a5894e5990a09c5c5a57b31` |
| `hierarchical_index/distributed/shared_runtime.rs` | `a485a8489d2531ef192527a3f9487f12602e5d46a3efbf4454584235ef932214` |
| `hierarchical_index/distributed/shared_runtime_tests.rs` | `9fc8a9a038c4dffd6cc5161ad7fbca84d507e67fdde9b270c86d4e5aec599f20` |
| `two_machine_spann.rs` | `a005c095e38b471c0e08cfade7c6e5e6a5cef5b40f6530e5ba8ae76cccdb7c7c` |
| `two_machine_reader.rs` | `aa3ead36aa54991bdd6672c9cf0c3990fc948bfa0f055ae2f168192136592789` |
