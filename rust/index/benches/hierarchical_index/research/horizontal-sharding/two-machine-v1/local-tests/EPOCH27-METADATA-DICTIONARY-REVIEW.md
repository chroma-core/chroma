# Epoch27 response-local metadata dictionary review

The reviewed candidate has no remaining source correctness blocker. It shares identical ordered child metadata within one response and reconstructs complete existing replies before navigation validation. It changes the private response wire version, so matching binaries are required. This independent review performs no source edit, Cargo, cloud, SSH or Git operation. Root admission must bind the final complete archive and actual runtime artifacts to these reviewed hashes.

## Reviewed source

The comparison base is frozen `source-epoch26.tar.gz`, SHA256 `e80f8b914a4d35ca02217d12e661169d8d1cc2c4f3d79b9c0b24e82c720d34c1`. The final source closure must include both new codec files and the separately reviewed test diagnostic.

The dictionary production delta comprises a versioned response adapter, its batch codec, module registration and visibility changes. The already reviewed epoch26 one-pass reply validation and its tests remain present. Three endpoint test assumptions adapt to the new compact response sizes and aggregate expanded-row ceiling. The separate transfer diagnostic is test-only.

| File under `rust/index/benches/hierarchical_index/distributed/` | SHA256 |
| --- | --- |
| `engine.rs` | `943c13538dfe5c0cba0a6adac5cb5e694aacab64f67822d572bfebb2c7f3ee07` |
| `routing.rs` | `13bc8269000d6dd40adb780e035725476809a476c1a2926626c954b8c0fec474` |
| `routing_tests.rs` | `8353c35cb7f8f29cd52df350f58d1f7f40c8f367ffb5c2989ef8c9a705ffffbc` |
| `expansion_codec.rs` | `e5687abf5f4f72a25fdbf3ecc212912e7af945d6f23579c43cbd82cc19638068` |
| `expansion_batch_codec.rs` | `023b62dccd973c3c9df4db63ae1c9d4ba81d3cbdee1fd236bca8737cf3636c98` |
| `expansion_batch_codec_tests.rs` | `3c22c435277b7f01ffd2e8407a4bdbf4be22b1bcd2c2ca18124417e5e734b7ee` |
| `engine_endpoint_tests.rs` | `e671f511014faab310e613fda35c04c5355f5239221bfa0ee690b3c939b126cb` |
| `local_transfer_tests.rs` | `8518f838f16aff5da22541700cefc72c739011d7d9dbdd96efe859592ac41cf3` |

## Exact reconstruction and rejection

Reuse requires equality of every metadata field in its original order. The hash key includes parent, observed revision, ordinal, full128-bit target ID, authority, placement and leaf flag for each row. Hash lookup is followed by complete equality; matching parent/revision or colliding hashes do not establish identity. Scores remain separate exact32-bit words, including signed zero and finite subnormal values. Contiguous parent/revision runs preserve interleaving and unusual ordinal order instead of silently repairing them.

Decoding restores the existing reply vector with every outer identity field, requested-node list, child count, status and child row intact. Complete reply validation, canonical assembly and global reference beam/tie selection continue afterward. Invalid partial coverage, duplicate ordinals, mixed revisions and overlapping replies remain invalid; the wire adapter neither prunes nor accepts them as successful navigation. Encoding and decoding reject nonfinite scores. Full-width integer conversion and legal leaf flags are checked. There is no unsafe code, cross-request cache, pointer identity or new mutable routing state.

The required `ExpandedMetadataV2` response variant and required versioned dictionary/run keys reject legacy and mixed wire representations. Both directions of the version-mismatch test fail closed. Dictionary duplicates and unused entries, missing/trailing bytes, unknown run references and malformed base64 also fail. This requires paired endpoint deployment rather than silent fallback.

## Allocation and size boundaries

A compressed response cannot allocate arbitrarily many decoded child rows. The encoder checked-sums the old69-byte-per-row equivalent across the entire batch. The decoder first bounds raw dictionary/run blobs, parses only available dictionary metadata, validates run references and exact score extents, and checked-sums that same expanded equivalent across every reply. Only after the complete batch passes this aggregate budget does it allocate rich child vectors. Repeated references to a small dictionary therefore cannot bypass the expanded-row ceiling.

Dictionary counts are bounded by available header bytes and the maximum row count before table reservation. Each nonempty entry’s checked row length must fit the remaining bytes; flags and ordinal conversion are validated before use. Run counts are bounded by available reference bytes and the row limit. A separate bounded reply-envelope visitor avoids treating64 as a semantic limit while preventing unbounded envelope slots. Existing16MiB transport framing and strict bounded base64 provide additional outer limits. Ordinary metadata/envelope vectors are still materialized within these limits; this is bounded buffering, not a streaming or zero-allocation decoder. The equivalent encoded-row budget bounds rich rows proportionally rather than claiming that all process allocations total12MiB.

Known payload-budget and size-overflow serializer errors retain their exact typed size signals. Production transport classification, adaptive query/complete-parent subdivision and one absolute deadline are unchanged. Arbitrary codec, EOF or application errors receive no new retry. The aggregate decoded-row ceiling can require subdivision even when compressed bytes fit the frame; the endpoint regression now tests this explicitly. One parent that cannot fit still fails clearly, and no partial candidate prefix is returned.

## Tests and interpretation

`EXPANSION-METADATA-REUSE-VALIDATION.json` records four new dictionary tests passing, eight bounded-expansion tests passing and27 routing/module tests passing. The four dictionary tests are included in the27 routing/module run and must not be summed as39 unique tests. `epoch27-routing-focused.log` records27 passing with zero failures, including actual mixed TCP owners, the reference navigation oracle, canonical ordering and malformed remote reply rejection. The final native CLI build log records a successful17.98-second build; `EXPANSION-METADATA-REUSE-VALIDATION.json` binds all three resulting native artifacts, and root must verify those bindings before remote admission. This focused review does not claim an unrelated full-suite run.

Dictionary tests force hash collisions and alter both same-revision and different-revision metadata. They verify every decoded row and score bit, interleaved runs, reordered ordinals, arbitrary full-width values, empty/stale replies and a65-envelope batch. Hostile tests exercise false counts, invalid flags/references, truncated/trailing score or dictionary bytes, duplicate/unused entries, nonfinite scores and repeated references whose small compressed input exceeds the rich-row budget. Existing semantic navigation tests run through the new real RPC adapter; representation round trips alone are not described as proof of valid memberships.

The actual framed fixed64-query/two-parent/32-child payload falls from413,199 to62,266 bytes with exact restored replies. These are measured representation bytes for one reuse fixture, not a projection for all index traffic. The native thread CPU helper returns unsupported (`None`), so encoding CPU is unknown rather than zero. The owner still scores every child and constructs rich replies; grouping, hashing, metadata equality and reconstruction add work. End-to-end latency and memory improvement remain unmeasured. The reviewed epoch26 counter caveat also remains: coverage visits exclude duplicate body-equality traversal, and logical clone bytes are not all allocations.

The added fixed transfer diagnostic changes no production dispatch or protocol. It builds resident vectors, initial codes and fixed destinations before timing, moves24 IDs from a2,048-row source into eight destinations, checks exact source/destination IDs and recomputed posting codes, verifies two untouched leaves, and compares final staged/local posting maps. Its retained successful `fixed-transfer-run-02` evidence is separate from dictionary performance. The remote endpoint uses the current codec-capable engine but this diagnostic does not navigate. It does not establish physical network or end-to-end speedup, and its test-only source must be retained in archive closure.

## Admission boundary

Every file hash in `operations/preparation-epoch27.json` matches the reviewed preparation snapshot. Its fresh candidate-only3M namespace retains the existing supervisor, input/template bindings,32 workers, capacity2,160,000, fixed590-second job, required remaining window time, no automatic retry and no sweep. Five mocked preparation checks pass; these do not establish cloud identity or actual binaries. Root must freeze the complete source closure, finalize config/build metadata, verify matching writer/reader binaries on both hosts, establish fresh independent shutdown guards and check input/resources/rates/budget before one remote run. Final archive closure and runtime admission are separate from this source review.
