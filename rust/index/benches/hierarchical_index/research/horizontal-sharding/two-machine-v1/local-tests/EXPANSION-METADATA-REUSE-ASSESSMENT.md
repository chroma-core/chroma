# Reuse expansion metadata within one batch

The next candidate can send each identical ordered parent/child metadata array once per RPC response, followed by each query's complete score array. This preserves every scored row and the current navigation algorithm. It is more promising for bytes than compressing integer widths or changing query precision, because a 64-query batch can repeat the same metadata dozens of times. CPU and wall-time improvement remain unmeasured. This assessment changes no source, runs no Cargo or benchmark, and does not admit a remote candidate.

## Ranked recommendation

1. Implement a response-local dictionary of exact ordered metadata runs, with each reply retaining its own ordered run references and exact f32 scores. Reuse only after full equality, and reconstruct the existing `Vec<ExpandReply>` before the current validators. This is the preferred first candidate.
2. Consider implicit canonical ordinals only after measuring the first candidate. Keeping explicit u64 ordinals in each dictionary row makes arbitrary reply order and malformed-row validation easy to preserve. Removing them adds permutation rules for little benefit once metadata is shared across many queries.
3. Defer cross-request caching, compression libraries, narrower IDs and sender-side pruning. They add identity/lifetime or algorithm changes that are unnecessary for this byte-reduction question.

## Source and safe reuse boundary

The owner expands each request separately and retains immutable routing-image handles while scoring it. A batch therefore does not imply that every query sees the same parent image. A parent can publish a replacement between two request expansions. The existing driver permits coherent rows per parent within a reply and validates their observed revision, while final beam selection waits for all required replies.

A wire dictionary must reuse metadata only when the complete ordered metadata matches. Use parent ID, revision and row count to find possible matches, optionally with a hash, then compare every ordinal, full target ID, target owner, target placement and leaf flag. Never treat matching parent ID/revision or a matching hash as proof of equality. Same revision with different child contents becomes two dictionary entries. Changed revision also becomes another entry, even if child targets happen to match. A short-lived immutable image pointer can be an optimization hint on the owner, but it is neither a serialized identity nor sufficient when the codec receives only scored rows.

The current scored reply contains no centroids. Its complete child rows contain parent ID, ordinal, full target reference, leaf flag, score and observed revision. Reusing metadata reduces repeated identities, not centroid traffic. Parent placement and full owner identity also remain in each reply's existing requested-node list; do not remove those fields from the envelope.

## Small lossless schema

Keep all current reply envelope fields unchanged. Give the response variant an explicit new wire version, with a dictionary blob and an array of reply envelopes whose child payloads reference that dictionary. A batch-level adapter is necessary: a field adapter on one reply cannot share data across other replies. The adapter returns exactly the existing decoded reply vector, so callers, local execution and navigation validation do not change.

Partition each reply's child rows into maximal contiguous runs having the same parent ID and observed revision. A dictionary entry stores that run's complete metadata in its original order. Normal owner output forms one complete run per parent, which follows the reference's child-array organization. Retaining the original ordinals makes the representation equally lossless for an unusual ordering or an invalid partial/mixed-revision reply. An interleaved parent creates multiple runs; it is not silently reordered or repaired. Existing validation must still reject incomplete counts, duplicate ordinals and mixed revisions after decoding.

| Item | Raw bytes |
|---|---:|
| Dictionary entry header: parent u128, observed revision u64, row count u32 | 28 |
| Dictionary child metadata: ordinal u64, target ID u128, owner u64, placement u64, leaf flag u8 | 41 per row |
| Per-reply run reference | 4 per run |
| Per-query score bits | 4 per row |
| Dictionary entry count and per-reply run counts | 4 each |

For each reply, write the run references in its original run order. The referenced entry determines the exact number and metadata of following score words. Concatenating these decoded runs reproduces every row in its exact original order, including equal scores and signed-zero bits. Query slots, selected parents, counts, status, attempt/view identity and all other outer fields remain untouched. Empty/stale responses remain representable with zero runs.

Standard padded base64 can carry these bounded binary blobs inside the current JSON transport. Reuse the current strict base64/bounds pattern and explicit little-endian widths; no new dependency or general serializer is needed. Keep the complete queries' current lossless representation. The response variant/key must be required and versioned so old/new binaries reject one another, rather than accepting an empty dictionary or falling back silently.

## Fixed-fixture byte arithmetic

The retained 64-query mixed-owner fixture has four internal parents, two owned remotely, with 32 children each. For its fixed-beam case, assume all 64 queries expand those same two remote parents and their metadata remains identical across the response. This gives 4,096 remote scored rows. The following counts exclude unchanged JSON envelopes, requested-node lists, child-count lists and keys.

| Child payload calculation | Current complete-row codec | Dictionary candidate |
|---|---:|---:|
| Raw payload per reply | 4 + 69×64 = 4,420 | 4 + 2×(4 + 4×32) = 268 |
| One shared raw dictionary | None | 4 + 2×(28 + 41×32) = 2,684 |
| Base64 characters for all 64 replies and dictionary | 64×5,896 = 377,344 | 64×360 + 3,580 = 26,620 |

This is approximately 14.18 times fewer child-payload characters for this fixed reuse case. It is arithmetic, not a measured transport or speed result. If every query sees unique metadata, the dictionary saves only the repeated parent/revision within each run and adds headers/references. Single-row unique runs can be larger than the current 69-byte encoding. Different frontiers, publications, adaptive subdivision and small batches reduce reuse. Epoch 24's 36.834 GB of final-add traffic and approximately 345 million remote rows justify investigating the opportunity; multiplying the fixed-fixture ratio into that total would be unsupported.

## CPU, memory and bounds

Start with source-level metadata grouping and hash-assisted exact equality, not a cache or new scoring loop. Building and comparing the dictionary costs CPU and touches all metadata. It can replace repeated byte emission with score-only emission, but the owner still constructs rich scored rows and the receiver still reconstructs them. Current reply cloning, semantic validation and scoring costs therefore remain. Measure encoding/decoding CPU and temporary bytes before claiming a CPU benefit.

A small encoded response can expand into many rich rows, so the current compressed-byte/frame ceiling alone is insufficient. Before allocating decoded rows, checked-sum all referenced run lengths across all replies and enforce a separate expanded-row/byte budget consistent with the current complete-row codec. A conservative first bound is the existing equivalent total `sum(4 + 69×reply_rows)` under the current decoded payload budget, in addition to dictionary/blob and unchanged whole-frame limits. Validate count multiplication, exact buffer lengths, every dictionary reference, every inferred score extent, legal flags, usize ordinal conversion and finite scores. Bound dictionary entry/row counts and reject unused or excessive dictionary material; do not allocate from header claims before checking available bytes.

Retain the current adaptive RPC subdivision and absolute deadline. If the new dictionary payload, outer frame or expanded-row budget does not fit, return a typed size failure so the client splits the query group and, when necessary, the selected parent set. Each smaller response remains complete for its requested parents; there is no prefix beam selection or child truncation. One complete parent that cannot fit remains an explicit error. Do not retry an arbitrary codec failure, EOF or application rejection as though it were a size signal. Subdivision may reduce metadata reuse, which is acceptable for bounded memory and correctness.

## Smallest proposed mechanism test

Reuse the fixed 64-query/two-remote-parent/32-child fixture, with actual mixed local/TCP owners and no timing threshold. Capture complete legacy reply vectors, encode/decode the candidate, and compare all outer fields, every row identity/ordinal/revision/flag, and every f32 bit in order. Then run the existing global navigator and compare selected IDs, scores, order and distance/expansion/request work with the unsharded reference. Record actual framed bytes separately from the arithmetic above.

Within that one payload family, alter one query's parent metadata under a different revision, then alter metadata under the same revision. Require separate dictionary entries and exact decoded equality. Include tied scores, signed zero, interleaved runs, reordered child ordinals, duplicate ordinals, mixed revisions, missing children and conflicting duplicate replies; malformed semantic inputs must remain rejectable by the current validators. Add invalid/out-of-range dictionary references, duplicate/unused table entries, false counts, missing/trailing score words, oversized expanded-row totals behind a tiny dictionary, malformed base64, nonfinite bits and version mismatches. A deliberately small frame/expanded-row budget must trigger the existing bounded subdivision without losing rows or restarting the deadline.

## Implementation seam and inspected source

The likely source changes are a narrow custom serializer/deserializer on the `EngineResponse::Expanded` variant, a batch codec next to `routing::expansion_codec`, and focused codec/routing/endpoint tests. A versioned response variant should distinguish new from old payloads while preserving the in-memory variant and decoded types. The engine algorithm, request grouping, current adaptive subdivision, transport framing and saved-generation formats should remain unchanged. No cross-request state is required.

The following hashes identify the inspected local source, not a new admitted archive. Paths are under `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/`.

| File | SHA-256 |
|---|---|
| `routing.rs` | `0d36a2527523acf2adec9afbb90e601ae3ea8aa53d754607670e883209e1844d` |
| `engine.rs` | `3ec0e8568f54e5446e920aa0b79c92466b0ff9f66ac04b2ae1a4bba39ad758c0` |
| `expansion_codec.rs` | `45777ad8174bf430f77d9a16e885c71e887004773e7ec16b49e2914e1efc219c` |
| `routing_tests.rs` | `8353c35cb7f8f29cd52df350f58d1f7f40c8f367ffb5c2989ef8c9a705ffffbc` |
| `transport.rs` | `bf1e66b801ab6bf7a32a0c09166deb8f1dbe75e18a9a269128dbe91edfba91ad` |
