# Lossless expansion encoding assessment

The smallest next encoding change replaces only the query-vector and scored-child JSON arrays with bounded base64 strings containing fixed-width little-endian numbers. Keep the JSON operation envelope, transport framing, every child row, and every current routing validation. This avoids a transport rewrite and adds no dependency. This is a read-only recommendation; no codec, test, Cargo build or cloud run is implemented here.

## Recommended representation

The request's query field becomes an explicitly versioned key such as `query_f32le_v1_base64`. Its decoded bytes contain a u32 coordinate count followed by that many f32 bit patterns, four bytes each. Encode floats with `to_bits().to_le_bytes()` and decode with `from_bits(u32::from_le_bytes(...))`; do not convert through f64, decimal text or a lower-precision format. Coordinate order and every finite f32 value, including signed zero and subnormals, remain exact. The existing dimension and finite-vector validation still runs.

The reply's children field becomes `children_le_v1_base64`. Its decoded bytes contain a u32 row count followed by fixed-width 69-byte rows in exactly the original row order. Keep all current outer fields, including owner, run, epoch, shared sequence, attempt, step, query slot, selected parent references, child counts and status.

| Per-child field | Bytes | Reason |
|---|---:|---|
| Parent logical ID | 16 | Logical IDs are u128. |
| Child ordinal | 8 | Encode the current usize value as checked u64, then check conversion on decode. |
| Target logical ID | 16 | Preserve the complete u128 identity. |
| Target owner | 8 | A child can belong to another owner. |
| Target placement counter | 8 | Preserve the full placement identity. |
| Leaf flag | 1 | Accept only zero or one. |
| Distance bits | 4 | Preserve the exact f32 score. |
| Observed parent revision | 8 | Preserve current revision validation. |
| **Total** | **69** | Explicit widths; never serialize native struct memory or padding. |

Use the existing standard padded base64 dependency and the saved-object codec's strict string visitor pattern. The shared pattern is encoded-length checking before allocation, strict alphabet/padding decoding, and a second decoded-length check. Its saved-object-specific 1 MiB limit is not an appropriate expansion limit: an expansion reply can already exceed that size. A small new expansion codec module with two field adapters is sufficient; leave the saved-object format unchanged. Explicit width handling is clearer than serializing the current Rust struct with implicit bincode configuration, especially for usize conversion, exact row-size validation before allocation, and future struct-field additions.

Matching endpoint binaries are required. Renamed required fields make old-to-new and new-to-old expansion decoding fail closed. Do not accept both legacy and new fields or silently fall back. Include the layout version in the required key and reject unknown binary versions if a separate version header is chosen. Construction state, saved generations, query readers and other engine requests do not change.

## Expected bytes, without a CPU claim

A 768-dimensional query contains 3,072 raw float bytes, or 3,076 bytes with the proposed count header. Base64 needs 4,104 characters for that payload, plus its JSON key and quotes. Without a header, the numeric data alone needs 4,096 characters. This replaces decimal formatting and commas while preserving full precision. The current JSON query size depends on its actual float values; no constant JSON byte size is assumed.

A reply with N rows contains `4 + 69*N` binary bytes and `4*ceil((4 + 69*N)/3)` base64 characters. The large-N cost is 92 characters per row, plus the retained envelope and parent/count metadata. Epoch 21 observes approximately 167 received framed bytes per remote row, including that surrounding data. Its 293,199,682 remote rows imply approximately 26.974 GB of new encoded row characters before retained envelopes, compared with 49.011 GB of measured received traffic. This is a scale-of-opportunity calculation, not a prediction of achieved traffic or latency: actual batching, envelope bytes, errors and the next run's row count must be measured. The existing 58.846 GB total traffic and 945 process-CPU seconds do not establish how much CPU decimal serialization consumes.

Do not remove repeated parent metadata in the first candidate. Parent IDs and observed revisions repeat, so a parent table can reduce bytes further, but it adds index validation and must preserve arbitrary row order, exact duplicate-reply equality and detection of mixed revisions. Never replace each target's owner with the reply owner, truncate logical IDs to the small IDs in test fixtures, or drop placement counters. Do not infer ordinals from arrival order. A later parent-table candidate can be judged separately after the simple representation has measured bytes and CPU evidence.

## Bounds and unchanged validation

Keep the current configured frame ceiling and deadline behavior. The default frame ceiling is 16 MiB, and the transport already bounds both JSON serialization and incoming frame allocation. The field codec also needs a fixed decoded-payload ceiling, conservatively at most 12 MiB for that frame configuration, with checked base64 length arithmetic. Exactly 12 MiB of raw data encodes to 16 MiB before the envelope, so the existing whole-frame check still rejects that boundary when the envelope does not fit. Use the stricter configured frame when it is lower; do not enlarge transport limits to admit the codec. A configurable higher frame limit should not silently lift the codec's explicit maximum.

After base64 decoding, read the count without allocating rows. Check `4 + count*width` with checked arithmetic and require exact equality with the payload length, rejecting truncation and trailing bytes. Bound count from the actual decoded length and the payload limit, not merely from the claimed header. Reserve the output vector fallibly only after these checks. Convert u64 ordinals to usize with a checked conversion. Reject illegal flags. Float bit reconstruction itself is lossless; the existing request validator rejects nonfinite coordinates and the existing navigation validator rejects nonfinite scores before selection. Preserve zero-row stale/publishing responses and any complete empty-parent expansion allowed by the present protocol.

Decode into the existing request/reply types so validation continues to check the full remote authority, run/epoch/shared view, attempt, level, query slot, requested parent identities, complete counts, no missing/overlapping parents, complete unique ordinal ranges, matching parent revisions and finite distances. Retain the complete-attempt retry policy. Do not introduce local top-k pruning or remove child rows. Expansion replies have no saved-object-style whole-payload SHA check today; reusing base64 does not introduce one. Existing operation and semantic validation remains necessary, and the codec is not authentication or integrity protection against arbitrary valid-value modification.

## Proposed mechanism tests

Use one bounded fixture with 64 fixed queries, multiple selected parents and actual local plus TCP-connected owners, reusing the candidate-assembly fixture where possible. Compare complete decoded rows bit-for-bit, selected IDs/distances/order, exact expansion/distance/request work and measured framed bytes against a test-only legacy serializer. Retain large tie groups, mixed-depth leaves and the current tau policy. This is a proposal only; no new benchmark or sweep runs for this assessment.

Test zero/one/max rows and coordinates, arbitrary integer identities including u128/u64 maxima, checked ordinal conversions, positive/negative zero, subnormals and finite extrema. Test round-trip bit equality rather than ordinary float equality. Inject malformed alphabet/padding, encoded and decoded oversize bounds, the max-plus-one padding-length case, impossible claimed counts, multiplication overflow, truncated headers/rows, trailing bytes and invalid leaf flags. Verify old/new endpoint combinations fail closed. Feed nonfinite query/score bit patterns through the actual endpoint/driver to prove the existing validation rejects them. Repeat wrong parent/owner/placement/view/attempt/step/slot, duplicate ordinal, inconsistent revisions, partial parent coverage, overlapping replies and exact/conflicting duplicate-reply cases. Assert explicit encoded-byte reduction on a representative fixed payload without timing thresholds.

## Source boundary

The implementation needs only field annotations/types in `distributed/routing.rs`, a small `distributed/expansion_codec.rs` module and focused codec/routing tests. `engine.rs` should need no algorithm change: its handlers consume the same decoded types. `transport.rs` should retain its JSON envelope and existing counters/limits. The direct local path continues using borrowed queries and in-memory scored rows without encoding. Do not generalize every protocol message or modify saved-generation files for this candidate.

The inspected files are under `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/`. Hashes describe the local source observation, not a new admitted archive; another worker may advance engine correctness afterward.

| File | SHA-256 |
|---|---|
| `benches/hierarchical_index/distributed/routing.rs` | `fda48067d70d04f11dad29173e6304473b630ae02ac4777ad764473944c932bd` |
| `benches/hierarchical_index/distributed/engine.rs` | `cfd4bcce3aab37478c29814a439b09bd8c85edce73b25611dc74485400bc11be` |
| `benches/hierarchical_index/distributed/transport.rs` | `b54a9a0490511b533a21650a559f20527c16c3ad30be05d363131d763a627f1c` |
| `benches/hierarchical_index/distributed/save_runtime.rs` | `cca129118d445c97251c26675cc20f2e2a00486300e346769c4d58f99aeb0d86` |
| `Cargo.toml` | `ab1d5910230a6ab9b6393b508a79c843f827e3bbcd76caba06f1bb16ee794858` |

Measured epoch 21 context is recorded in [the insertion analysis](EPOCH21-DISTRIBUTED-INSERTION-ANALYSIS.md). The current saved-object base64 mechanism and its earlier admission are recorded in [the saved-object implementation receipt](SAVED-OBJECT-BASE64-IMPLEMENTATION.md).
