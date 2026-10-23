# Lossless complete expansion codec candidate

The candidate encodes complete expansion query vectors and child-score rows as bounded base64 strings of explicit little-endian numbers. It preserves the decoded types, every row and its order, full logical/placement identities, exact finite f32 bits, and the navigation driver's complete reply validation. Six focused codec tests and 15 existing navigation tests pass. This is local mechanism evidence, with no latency claim or remote admission.

The measured reason to investigate is epoch 21's 49.011 GB of received expansion traffic, approximately 167 framed bytes per remote row. The current epoch 23 failure also exposes an oversized remote reply during final balancing. Compact encoding must not mask that batching defect: a separate bounded-expansion correction and combined review are required before another admission. A per-field codec ceiling and the unchanged whole-frame ceiling are rejection bounds, not a protocol for splitting an arbitrarily large remote batch.

## Representation

The query uses the required key `query_f32le_v1_base64`. Its raw payload is a four-byte u32 coordinate count followed by four bytes per coordinate, preserving `f32::to_bits()` in little-endian order. The child rows use `children_le_v1_base64`: a four-byte u32 row count followed by 69 bytes per row. Each row stores parent ID16, ordinal8, target ID16, target owner8, target placement8, leaf flag1, score bits4 and observed parent revision8. Complete JSON operation envelopes, requested parent identities, child counts, view/attempt/level/query-slot identity and status stay in their current form.

The decoder reconstructs the same in-memory types consumed by the owner and navigation driver. The direct local path does not encode its borrowed query or scored rows. Required versioned keys and rejection of unknown fields make old/new request or reply combinations fail closed; there is no fallback. Saved objects, reader generations, engine algorithms, transport architecture and dependencies remain unchanged.

Each raw field is capped at 12 MiB minus two bytes (12,582,910 bytes), and its encoded string is capped at 16,777,216 characters. The cap's padding boundary exercises the case in which an extra decoded byte has the same encoded length. This does not enlarge the default 16 MiB whole-frame limit: enclosing keys, metadata and other replies further reduce the payload that fits, and a lower configured frame limit still rejects the request or response. The maximum count allowed by the raw child layout is 182,360; a claimed count never allocates that many rows until exact decoded length is proved.

Serialization checks count multiplication/addition, the field limit, usize-to-u64 ordinal conversion and finite floats before encoding. Decoding checks encoded size before base64 allocation and decoded size afterward, validates the count against exact checked payload length, and reserves decoded rows fallibly. It rejects illegal flags and nonfinite floats. The strict existing base64 engine rejects malformed alphabet, missing/extra/noncanonical padding and unused nonzero padding bits. Query dimension and semantic navigation validation remain in their existing handlers. There is no lossy float conversion, identity truncation, per-parent metadata compression, implicit ordinal reconstruction, row pruning or local top-k selection.

## Verification

The actual TCP payload fixture receives exactly 94,516 framed reply bytes for 1,024 complete rows, compared with 267,587 bytes from the retained JSON numeric-row representation. Its 768-dimensional query body is 4,295 bytes compared with 7,993 bytes for the retained numeric query array. These values include the fixture's actual integer identities and score values; they are not universal bytes per row or a projected speedup. The received transport counter equals the exact new response serialization plus its four-byte frame header.

Bitwise tests cover signed zero, subnormals, finite extrema, thousands of fixed finite bit patterns, u128/u64 identity maxima, usize ordinal maxima, flags, complete row order and empty fields. Maximum query and row fixtures verify the actual field caps. Malformed cases include invalid base64, the same-encoded-length max-plus-one case, oversized encoding, short headers, false and overflowing counts, truncated/trailing rows, illegal flags and nonfinite input on both encode and decode. Actual TCP tests reject legacy requests at new endpoints, new requests at legacy endpoints, legacy responses, and a lower transport frame ceiling. The expected diagnostic EOF/JSON errors in these rejection cases are not failed tests.

The existing 64-query mixed-owner fixture reaches a real second owner over localhost TCP using the new field representation and retains exact canonical input ordering, selected IDs/scores/order and traversal work against its legacy ordering oracle and unsharded reference. Existing navigation tests preserve rejection of malformed/partial/overlapping/late/stale responses and retain packed-image publication checks. This fixture uses two owner services in one process and does not establish physical distributed benchmark performance.

The focused codec command passes 6/6 tests after a 17.25-second native rebuild; the selected tests take 2.99 seconds. The focused navigation command passes 15/15 tests in 0.71 seconds. The existing `placement.rs:99` unused-assignment warning is unchanged. No test attempt fails in this candidate. No full suite, three-CLI build, Linux runtime, benchmark, cloud action, Git mutation or frozen-archive change occurs in this work. Exact commands, counts, expected rejection diagnostics and source/index hashes are recorded in [the validation receipt](EXPANSION-CODEC-VALIDATION.json).

## Source boundary and next decision

The only modified existing source file is `routing.rs`, which contains the field annotations and the new private module declaration. The new source files are `expansion_codec.rs` and `expansion_codec_tests.rs`. `routing_tests.rs` remains byte-for-byte at the epoch 23 candidate hash. The files live under `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/`.

| Source | SHA-256 |
|---|---|
| `routing.rs` | `ce5c98d2863e478b6a2fb3cc07633b749fb554ba875f848a3c64719c7ac1665b` |
| `expansion_codec.rs` | `45777ad8174bf430f77d9a16e885c71e887004773e7ec16b49e2914e1efc219c` |
| `expansion_codec_tests.rs` | `7c93ea2a99f167b144204b28acfe4bb3682654b5ff3c1588098d0b7d9503fdaf` |
| `routing_tests.rs` | `eb445c667d6adb11e2c8657bdb82f2754c4c96381377c68d116e3a058c5ace14` |

The observed staged-index SHA-256 is `5d95a4de29df0d2533f4bdb59da68b25c0b3a5b062e29e6bf3ed0a89576d75dd` and remains equal to the observation before writing this receipt. No staging operation runs. Independent review should assess this representation together with the bounded remote expansion repair. After coherent integration, one combined native suite and the three CLI builds can provide the next admission evidence; a matched benchmark can then measure traffic and CPU rather than assuming a latency benefit from smaller frames.
