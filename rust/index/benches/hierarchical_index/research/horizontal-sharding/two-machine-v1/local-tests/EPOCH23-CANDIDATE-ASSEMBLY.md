# Epoch 23 candidate assembly

The navigation candidate replaces a mixed-owner ordering sort with checked placement into the same parent/child order as the reference. The focused native navigation module passes 15 tests. This establishes correctness of the ordering mechanism and a reduction in ordering work; it does not establish an end-to-end latency improvement. The candidate has no remote admission, and epoch 22's separate stale-image failure remains a correctness blocker owned by the engine worker.

## Mechanism and limits

Navigation gathers scores from local immutable images and remote owner replies before choosing one global set of candidates for the next tree level. Remote replies can arrive in any order, so their rows must first be ordered by the parent's position in the current frontier and then by the child's ordinal within that parent. The reference-compatible score sort then receives exactly the same input order, including ties.

The candidate builds one parent-ID-to-frontier-position hash map, records each row's parent position once, counts the actual rows per parent, and computes contiguous offsets. It moves each row into the slot at its parent offset plus child ordinal, rejects occupied or out-of-range slots, and gathers those slots in order. It reuses the existing candidate vector's allocation during the gather. Hash-map iteration never determines output order. The reconstruction takes expected linear time in parents plus rows, because hash lookup has expected constant cost; this is not a worst-case deterministic linear-time claim.

The complete remote reply validation remains in place before reconstruction. It checks owner and construction-view identity, query slot, navigation attempt and level, complete status, exact duplicate replay versus conflicting replay, requested parent coverage, nonoverlap, declared child counts, complete unique ordinal ranges, finite distances and coherent parent revisions. The slot check additionally rejects duplicates across local and remote rows. Unknown parents and repeated frontier parent IDs fail explicitly. Count-derived offsets use actual row counts rather than an untrusted declared count, allocations use fallible reservation, candidate counts retain the reference's u32 limit, and prefix addition is checked. Any failure aborts the complete traversal attempt before beam selection.

Only mixed-owner reconstruction changes. All-local ordering, centroid scoring, full-precision construction distances, the narrow reference score sort, tau policy, beam width, leaf retention across mixed depths, exclusion, retry behavior, request/reply encoding, storage and placement protocols retain their current behavior. Nonfinite scores remain invalid; the existing NaN tau policy remains unchanged.

The new temporary storage consists of the parent map, per-parent count/offset vectors, one narrow parent-position entry per row, and one optional-row slot per candidate. The final ordinal tree set and the wide-record ordering sort are removed. Peak memory and latency are not measured by this fixture; neither is claimed to improve from these source changes alone. The retained remote validation still contains its own maps, ordinal sets, repeated per-parent scans and reply clone, and the reference score sort still runs afterward.

## Fixed regression and work counts

The new 64-query fixture uses two actual owner services, with owner two reached over localhost TCP. Its tree has four selected internal parents split between both owners, 32 leaf children per internal parent, and an extra leaf attached to the root so traversal retains leaves across mixed depths. It uses fixed finite query vectors, equal centroid distances, signed-zero coordinates, and three fixed semantic cases: fixed beam, tau with a floor, and the existing NaN tau behavior. These are semantic checks rather than a timing or tuning sweep.

The fixture executes the retained key-sort implementation through a test-only oracle and the candidate through a private generic assembly seam. It compares the complete pre-score-sort rows, selected IDs, scores, order, expansion counts, distance counts, level counts and owner-request counts. It also runs the public production entry point and compares selected IDs and distances against the actual unsharded writer. The old assembler exists only in test code; there is no runtime flag for choosing it.

| Reconstruction work in the three fixed semantic cases | Retained oracle | Candidate |
|---|---:|---:|
| Candidate rows reconstructed | 21,376 | 21,376 |
| Wide-record ordering comparisons | 118,228 | 0 |
| Parent lookup calls during row ordering | 236,456 | 21,376 |
| Final global ordinal tree-set insertions | 21,376 | 0 |

The retained lookup count comes from instrumenting the exact key extraction closure in the retained unstable key-sort call. The candidate count is one lookup for each actual input row. These counts exclude building the parent lookup table and all unchanged remote validation and reference score sorting. They describe this fixture's work rather than a projected percentage of total navigation cost.

The same 64-query TCP fixture changes replies after they arrive to verify reorder and exact-duplicate acceptance, plus missing replies, incomplete children, duplicate ordinals, overlapping parent sets, wrong levels and nonfinite scores. Old and new paths agree on acceptance or rejection. Direct helper checks also reject repeated frontier IDs, unknown parent IDs, missing/interrupted ordinal ranges and usize-max ordinals, while permitting empty parent expansions. The remaining navigation tests cover late or stale replies, mismatched ownership/view identity, exclusion, large tied frontiers, per-level tau/floors, packed-image publication/serialization and whole-attempt stale-image rejection.

## Verification and source boundary

The final command is `CARGO_TARGET_DIR=/private/tmp/chroma-single-shard-target cargo test -p chroma-index --test hierarchical_distributed distributed::routing::tests --offline -- --nocapture`. It passes all 15 selected tests, with 201 tests filtered out, in 0.96 seconds after a 16.54-second native rebuild. The existing unused-assignment warning at `placement.rs:99` is unchanged. The new tests are `canonical_assembly_64_queries_mixed_tcp_owners_matches_old_and_reference` and `canonical_assembly_rejects_repeated_unknown_and_incomplete_ordinals`.

The first build invocation omitted the retained target directory and was interrupted; it produces no admitted test evidence. The first sandboxed focused run compiles, passes the helper test, and fails the TCP fixture at socket binding with macOS permission error 1. The permitted outside-sandbox localhost runs then pass. No full suite, Linux runtime, performance benchmark, cloud operation, Git mutation or change to the frozen epoch 22 archive occurs in this task.

Only the following implementation files change under `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/`:

| File | SHA-256 |
|---|---|
| `routing.rs` | `fda48067d70d04f11dad29173e6304473b630ae02ac4777ad764473944c932bd` |
| `routing_tests.rs` | `eb445c667d6adb11e2c8657bdb82f2754c4c96381377c68d116e3a058c5ace14` |

The next decision follows correctness repair and independent review. If admitted, one existing matched workload can quantify the navigation and insertion difference while reporting the unchanged distance and expansion work. This note provides no authorization or evidence for a remote epoch 23 run.
