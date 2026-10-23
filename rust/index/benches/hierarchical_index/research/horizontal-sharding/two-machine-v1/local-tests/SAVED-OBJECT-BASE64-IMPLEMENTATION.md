# Saved-object chunk encoding candidate

The candidate replaces JSON integer arrays with base64 strings only in saved-object chunk responses. The internal byte vector, request fields, generic JSON transport framing, immutable file bytes and reader format remain unchanged. The frozen epoch19 archive remains the benchmark baseline.

The response uses the explicit `bytes_base64` key and the existing base64 0.22 dependency's standard padded encoding. Matching endpoint binaries are required. A legacy response fails the new decoder; a new response lacks the byte-array field required by a legacy decoder. The new response struct rejects unknown fields rather than accepting a mixed representation.

The encoder rejects raw chunks above 1,048,576 bytes. The decoder bounds encoded length before allocating decoded bytes, then checks decoded length too. This second check matters because a raw chunk of max + 1 bytes can have the same encoded length as max. The decoder allocation is bounded by the encoded-length cap, with at most two padding-estimate bytes beyond the raw limit. Existing request phase, object identity, offset, completion and progress checks remain active. Reassembled bytes still pass the existing whole-object reference/checksum comparison before publication. Sync barriers and manifest-last publication remain intact.

The focused tests exercise arbitrary bytes, empty and maximum chunks, invalid alphabet/padding, both oversize cases and old/new wire rejection. An actual TCP test transfers a 1 MiB + 17-byte object under a 2 MiB frame limit, checks exact frame-byte accounting against serialization and compares representation size with the legacy response. Additional tests cover immutable read replay, malformed response metadata, stale run/epoch, request bounds and corruption encoded as valid base64. The corruption test calls the same extracted checksum-checking copy helper as the coordinator. Existing mixed-owner save and fresh-reader tests remain regression requirements.

Native validation is pending with the root build owner. No benchmark latency, measured byte reduction or end-to-end saving improvement is claimed by this preparation record.

| Source relative to the single-shard checkout | SHA256 |
| --- | --- |
| `rust/index/benches/hierarchical_index/distributed/save_runtime.rs` | `dff87a81bfcf9862e1ab8ef42d7fc79833c342c9b939afed03b1b5a057f116d4` |
| `rust/index/benches/hierarchical_index/distributed/saved_chunk_codec_tests.rs` | `7971a1f3d6d823fdbdd7bd48bac7f6d7b666361994bd557c65ce4d81f4be891c` |
