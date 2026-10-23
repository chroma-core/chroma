# Epoch20 saved-object chunk codec review

The bounded saved-object response codec has no remaining blocker within this review scope. It changes only the private RPC byte representation and preserves decoded bytes, request bounds, response validation and the whole-object integrity check. Persisted owner manifests, posting files and reader formats remain unchanged. The fixed 500K run measures the effect on save latency and network bytes.

## Representation and bounds

Saved-object responses encode their byte vector as a STANDARD padded base64 string under an explicit new field name. Serialization rejects decoded data beyond the existing chunk limit. Deserialization checks encoded length before allocating the decoded vector and then checks decoded length independently, because one extra decoded byte can share the maximum permitted encoded length. Strict alphabet, padding and trailing-bit checks come from the existing base64 STANDARD engine and are exercised by malformed-input tests.

The response rejects unknown fields and requires the new string field. A legacy integer-array response fails in the new decoder, and the new response fails in the legacy decoder because its required byte-array field is absent. Matching endpoint binaries are therefore required on both hosts. The framing and transport limits are unchanged; this is not a mixed-version compatibility layer.

## Copy integrity and lifecycle

The owner still requires a sealed, healthy current save phase and checks run, epoch, offset, requested chunk size, file size and relative object path before reading. Reassembly still checks the exact object reference, expected offset, nonempty progress, chunk and remaining-object bounds, and the completion flag. A stale or incompatible response cannot become a partial accepted object.

The extracted copy helper retains the same final integrity boundary: the completed decoded bytes are installed through the immutable store, and the resulting size/checksum reference must equal the requested reference before the owner manifest and generation can be accepted. Valid base64 is not evidence of correct object contents. As in the baseline, an integrity failure can leave an unpublishable new immutable artifact; existing immutable bytes are not overwritten, and latest-generation publication does not occur. Local immutable reuse, frozen save identity, root coverage and saved-reader validation retain their existing paths.

## Tests and measurement limits

The four focused tests pass in `saved-chunk-focused-01.log`. They cover zero, arbitrary and maximum byte-vector roundtrips; serialization bounds; encoded and decoded oversize cases including max-plus-one at equal encoded length; malformed alphabet, padding and trailing bits; and fail-closed legacy/new decoding.

The actual TCP fixture reads an object larger than one chunk, verifies exact reassembled bytes, writes and reopens the copied object, and repeats the read with identical decoded bytes and frame counts. It matches observed received framed bytes to the actual serialized new responses and compares the same fixed responses with the legacy integer-array representation. The legacy representation exceeds twice the new framed bytes for that fixture. This ratio describes the fixture’s byte distribution, not a guaranteed ratio for every posting or manifest.

Adverse TCP responses change identity, offset, completion or progress and reject during reassembly. A separate fault changes data while retaining valid canonical base64 and the original object reference; the real copy helper rejects the whole-object checksum mismatch and leaves latest/descriptor unpublished. The fixture also rejects stale run and epoch and invalid request offset or size. The applicable native suite reports 207 passed, zero failed and one filtered test in 5.40 seconds in `epoch20-suite-01.log`, retaining mixed-owner save, durability, identity and replay coverage. The exclusion remains the known unrelated reference-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`; an unfiltered clean suite is not claimed.

The codec does not compress object contents or reduce the immutable source bytes read. Base64 encoding and decoding also consume CPU. Network-byte accounting must distinguish framed RPC bytes from decoded payload bytes. Native CLI builds and final Linux measurements remain root-operator evidence; this review does not infer a save speedup from representation size alone.

## Execution preparation

The epoch20 adapters preserve the fixed candidate-only 500K workload, 32 workers, 500K owner capacity, verified epoch14 input and five 100K construction intervals. Their code differs from epoch19 only in fresh namespaces and previous-result/binary bindings. The 600/590-second supervisor/workload limits, independent shutdown guards and cleanup controls remain unchanged, and the epoch15 supervisor is reused exactly. There is one explicit candidate arm, with no profiler, reference rerun, sweep or automatic retry. Missing runtime hashes fail closed.

Every hash in `operations/preparation-epoch20.json` matches at review time, including the completed epoch19 run template SHA256 `236ac9379b26b8911d695aa7c3110bc25bd8d7a2244841db022474844d5dac17`. The five preparation tests pass in `epoch20-preparation.log`. The root operator binds final configuration status/source metadata, source-archive closure against epoch19, CLI build receipts and fresh matching Linux binary hashes before admission. This reviewer runs no Cargo, cloud operation or Git write.

## Reviewed source hashes

Paths are relative to `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed`.

| File | SHA256 |
| --- | --- |
| `save_runtime.rs` | `dff87a81bfcf9862e1ab8ef42d7fc79833c342c9b939afed03b1b5a057f116d4` |
| `saved_chunk_codec_tests.rs` | `7971a1f3d6d823fdbdd7bd48bac7f6d7b666361994bd557c65ce4d81f4be891c` |
