---
name: Multiple sparse indices
overview: Allow a Chroma collection to have multiple enabled sparse vector indices (one per metadata key, multiple sparse vectors per record) by lifting the single-index caps and making segment storage + the compacted read/query path key-aware, with backward-compatible reading of existing single-index collections.
todos:
  - id: pr1-engine
    content: "PR1 hammad/sparse_multi_engine: per-key file_path helpers + enabled_sparse_keys + keyed segment writer/reader/flusher (+legacy fallback & migration) + per-key SparseIndexKnn/Idf + consistency check; Rust segment/query/types/GC tests (caps still on)"
    status: pending
  - id: pr2-enable
    content: "PR2 hammad/sparse_multi_enable: lift single-index caps in Rust validators.rs + collection_schema.rs, Python types.py, TS schema.ts; docs; tests for multiple enabled sparse keys"
    status: pending
  - id: pr3-fusion
    content: "PR3 hammad/sparse_multi_fusion: verify/lock arithmetic + rrf score fusion across multiple sparse $knn leaves in worker; confirm rank validation admits multiple sparse leaves"
    status: pending
  - id: pr4-clients
    content: "PR4 hammad/sparse_multi_clients: Python + TS e2e (multi-sparse collection, multiple sparse vectors per record, per-key query + fusion) and multi-key sparse embedding generation; docs polish"
    status: pending
isProject: false
---

# Add support for multiple sparse vector indices

## Background (current state)

Sparse config is already modeled per metadata key (`schema.keys[<key>].sparse_vector.sparse_vector_index`), and the query API already carries a `key` on `RankExpr::Knn`. Three things block multiple sparse indices:

1. Validators hard-cap the collection at one enabled sparse index (Rust/Python/TS).
2. The metadata segment stores all sparse vectors in ONE shared inverted index per shard — `set_metadata` drops the metadata-key prefix for `SparseVector`, and the writer/reader fields are singular (`Option<...>`).
3. The compacted query path (`SparseIndexKnn`, `Idf`) ignores `key` and reads that single shared index.

Decisions confirmed:
- Identity = metadata key.
- **Create-time only**: schemas are effectively WORM. The only schema-mutation path (`UpdateSchemaFromConfig`, Go [table_catalog.go](go/pkg/sysdb/coordinator/model/collection_configuration.go) line 819 / Python `update_schema_from_collection_configuration`) updates **dense** vector params only; there is no API to add a sparse index to an existing collection. Multiple sparse indices are declared at collection creation and never added later.
- Backward compatibility is narrow: the ONLY pre-existing on-disk case is a collection created under the old cap with a single anonymous sparse index. Migration just remaps that anonymous index to its single schema key. We never add a new sparse key to an existing collection.
- Scope = Rust end-to-end + Python and TS clients.

## 1. Lift the single-index caps (schema + validation)

- [rust/types/src/validators.rs](rust/types/src/validators.rs) `validate_schema` (~lines 266-347): remove the `sparse_index_keys.len() > 1` rejection and update/relax the "Sparse vector index cannot be enabled by default ... At most one" default-key message (keep the defaults-cannot-enable rule; drop the "at most one" wording).
- [rust/types/src/collection_schema.rs](rust/types/src/collection_schema.rs): remove `SchemaBuilderError::MultipleSparseVectorIndexes` (line ~86) and the check that raises it in `create_index` (~lines 2768-2786). Keep `SparseVectorRequiresKey`.
- Add a helper `Schema::enabled_sparse_keys(&self) -> Vec<String>` near `is_sparse_index_enabled` (line ~406) for the storage layer to enumerate per-key indices.
- Python [chromadb/api/types.py](chromadb/api/types.py): delete `_validate_single_sparse_vector_index` and its two call sites (~lines 2413, 2473).
- TS [clients/new-js/packages/chromadb/src/schema.ts](clients/new-js/packages/chromadb/src/schema.ts) (~lines 868-877): remove the "Only one sparse vector index is allowed" check.
- Docs note: update [docs/mintlify/cloud/schema/index-reference.mdx](docs/mintlify/cloud/schema/index-reference.mdx) limitation line (~84).

## 2. Make segment storage key-aware (indexing layer)

The whole sparse pipeline (writer shard -> commit -> flusher shard -> flush) currently carries exactly one sparse index. We thread a metadata-key dimension through each stage. The on-disk distinguisher is the `segment.file_path` map key: today there are fixed global keys, and we move to per-metadata-key entries pointing at independent blockfile UUIDs (all blockfiles already share one collection `prefix_path` per [blockfile_metadata.rs](rust/segment/src/blockfile_metadata.rs) lines 512-520, so unique UUIDs are what separate indices).

### Where the key -> blockfile mapping is stored

There is no separate side table; the metadata-key -> blockfile-UUID mapping IS the structure of the segment's `file_path` map (we overload its keys).

- Rust: `Segment.file_path: HashMap<String, Vec<String>>` ([rust/types/src/segment.rs](rust/types/src/segment.rs) line 127). The map key is the index/file-path name; each value is a list of blockfile paths formatted as `{prefix}/{blockfile_uuid}` by `ChromaSegmentFlusher::flush_key`. Per shard this narrows to `SegmentShard.file_path: HashMap<String, String>` (line 450).
- Proto/persistence: `Segment.file_paths: map<string, FilePaths>` with `FilePaths { repeated string paths }` ([idl/chromadb/proto/chroma.proto](idl/chromadb/proto/chroma.proto) lines 42-52). Persisted in the sysdb segment record and snapshotted into the collection version file (what GC reads in `list_files_at_version`).
- Today (single index) the map key is a fixed global constant and the metadata key is NOT recorded on disk. Proposed: the map key encodes the metadata key, e.g. `{"sparse_posting::my_field": ["prefix/uuidA"], "sparse_posting::other_field": ["prefix/uuidB"]}`. The metadata key is recovered on read by parsing the map key (`parse_sparse_file_path_key`, below); the blockfile UUID in the path value remains the actual index identity. No schema or new persistence surface is required.

### 2a. file_path naming helpers

File: [rust/types/src/segment.rs](rust/types/src/segment.rs) (near the existing consts at lines 27-29)

- Keep `SPARSE_MAX` / `SPARSE_OFFSET_VALUE` / `SPARSE_POSTING` as the legacy/global constants (still recognized on read).
- Add helpers that derive per-key map-key names, e.g. `sparse_posting_key(key) -> format!("{SPARSE_POSTING}::{key}")`, plus `sparse_max_key`, `sparse_offset_value_key`. Add an inverse `parse_sparse_file_path_key(name) -> Option<(SparseKind, String)>` so the reader can classify a map entry as MaxScore/WAND-max/WAND-offset and recover the metadata key. The map key is internal to Chroma (never a storage path component), so the `::` delimiter only needs to be unambiguous for parsing; pick a delimiter not allowed in metadata keys, or length-prefix/escape the key to be safe.

### 2b. Writer side (build + route)

File: [rust/segment/src/blockfile_metadata.rs](rust/segment/src/blockfile_metadata.rs)

- `MetadataSegmentWriterShard.sparse_index_writer: Option<SparseIndexWriter>` (line ~250) -> `sparse_index_writers: HashMap<String, SparseIndexWriter>` keyed by metadata key.
- Writer creation in `MetadataSegmentWriterShard::from_segment` (~lines 803-936): replace the single 3-way branch with a loop over `schema.enabled_sparse_keys()`. For each `key`, decide its on-disk source (see migration matrix in section 2e) and build a `SparseIndexWriter`, inserting into the map. Fresh indices pick WAND vs MaxScore from that key's `config.algorithm` (read per-key instead of the collection-wide `is_maxscore_enabled()`); forked indices keep whatever the existing blockfiles are, regardless of schema (preserving the current "on-disk format wins over schema" rule documented at lines 56-66).
- Write routing for `MetadataValue::SparseVector` in `set_metadata` (~line 1012), `delete_metadata` (~1145), and (transitively) `update_metadata` (~1215): change `match &self.sparse_index_writer` to `self.sparse_index_writers.get(prefix)` where `prefix` is the metadata key. If the key has no writer (index not enabled), skip rather than error — `apply_materialized_log_chunk` already gates on `schema.is_metadata_type_index_enabled(key, SparseVector)` (~lines 1376-1378, 1426-1427), so this is defensive.

### 2c. Commit + flush side

File: [rust/segment/src/blockfile_metadata.rs](rust/segment/src/blockfile_metadata.rs)

- `MetadataSegmentFlusherShard.sparse_index_flusher: Option<SparseIndexFlusher>` -> `sparse_index_flushers: HashMap<String, SparseIndexFlusher>`; the commit step (~lines 1705-1708) iterates the writers map and commits each into the flushers map.
- `SparseIndexFlusher::flush` (~lines 108-143): take the metadata `key` and return per-key file_path map keys via the 2a helpers (e.g. `(sparse_posting_key(key), vec![path])` for MaxScore; the `sparse_max_key`/`sparse_offset_value_key` pair for WAND) instead of the global constants.
- Flusher wiring (~lines 1849-1853): iterate `sparse_index_flushers` and insert every per-key entry into the `flushed` map. Because `metadata_segment.file_path` is fully replaced by this map each compaction ([rust/segment/src/types.rs](rust/segment/src/types.rs) line 1784), legacy global keys are dropped automatically once a migrated segment is rewritten.

### 2d. Reader side

File: [rust/segment/src/blockfile_metadata.rs](rust/segment/src/blockfile_metadata.rs)

- `MetadataSegmentReaderShard.sparse_index_reader: Option<SparseIndexReader>` (~line 1865) -> `sparse_index_readers: HashMap<String, SparseIndexReader>` plus `legacy_sparse_index_reader: Option<SparseIndexReader>`.
- `MetadataSegmentReaderShard::from_segment` (~lines 1889-2014): instead of hardcoding three `load_index_reader` calls on the global constants, scan `segment.file_path` keys, classify each with `parse_sparse_file_path_key`, and build a reader per metadata key into the map. Any remaining legacy global entries (no key suffix) build `legacy_sparse_index_reader` exactly as today (MaxScore first, else WAND pair). `from_segment` needs no schema — classification is purely from file_path key names.

### 2e. Consistency check

- `Segment::check_metadata_consistency` in [rust/types/src/segment.rs](rust/types/src/segment.rs) (~lines 391-399): for each enabled sparse key, require its per-key entries OR accept the legacy global layout as a valid pre-migration state for the (single) enabled key. Keep the WAND-vs-MaxScore key-presence rules per index.

## 2f. Migration plan (backward compatibility)

Because schemas are create-time only (see Background), there are exactly two shapes a collection can ever take:

- **New collection (created after this change)**: schema may declare 1..N sparse keys. Its first compaction has no legacy entries, so every enabled key is written directly to per-key layout (`sparse_*::<key>`). No migration logic involved.
- **Existing collection (created before this change)**: schema has exactly one enabled sparse key (old cap), and on disk it has a single anonymous index under the global keys (`sparse_posting`, or `sparse_max` + `sparse_offset_value`). This is the ONLY migration case: remap that anonymous index to its one schema key. The schema can never gain a second sparse key, so we never have to merge or split indices.

### Identifying the legacy index's owner

- `from_segment` receives the schema; `schema.enabled_sparse_keys()` returns exactly one key for any legacy collection. That key owns the anonymous index.
- Defensive guard: if global sparse entries exist but `enabled_sparse_keys().len() != 1`, log a warning and attribute the legacy data to the first enabled key (unreachable for normally-created collections; guards hand-edited schemas).

### Read path before any compaction (no rewrite needed)

- The reader builds `legacy_sparse_index_reader` from the global entries; the per-key map is empty.
- Query operators resolve `readers.get(&self.key).or(legacy_sparse_index_reader.as_ref())` (section 3), so the single sparse query falls back to the legacy reader — identical behavior to today. Reads keep working with zero rewrite.

### Write path on next compaction (lazy rewrite)

Per the single enabled `key` in `from_segment`:

- Per-key entry already present (`sparse_*::<key>`) -> fork it (already migrated; only happens after the first post-upgrade compaction).
- Else global legacy entries present -> fork the legacy blockfiles (`SparseWriter`/`MaxScoreWriter` `old_reader` = legacy reader) to preserve existing postings, then flush under the per-key name.
- Else -> fresh index using `config.algorithm` (e.g. a legacy collection that had sparse enabled but never compacted any sparse data).

On flush, the rebuilt `file_path` contains only the per-key entry; the global legacy keys are not re-emitted and disappear (full replacement, [types.rs](rust/segment/src/types.rs) line 1784). The old legacy blockfiles become unreferenced and are reclaimed by GC.

### Migration safety properties

- No backfill job, version bump, or schema-mutation API needed; migration is lazy on the normal compaction path and touches at most one index per legacy collection.
- Algorithm continuity: the forked index keeps its existing on-disk format (WAND/MaxScore) regardless of schema, matching the current documented rule.
- Rollback consideration: once a segment is rewritten to per-key layout, an older binary that only understands global keys would not find the sparse index. If rollback safety is required, gate per-key flush behind a config flag for one release while still reading both layouts (optional safeguard).

## 3. Make the compacted query path key-aware (query layer / execution engine)

What is already key-aware in the execution engine (NO change needed):

- Fan-out: `WorkerServer::orchestrate_search` ([rust/worker/src/server.rs](rust/worker/src/server.rs) lines 769-788) already spawns one `SparseKnnOrchestrator` per sparse `$knn` leaf, passing `knn_query.key`.
- `SparseKnnOrchestrator` ([rust/worker/src/execution/orchestration/sparse_knn.rs](rust/worker/src/execution/orchestration/sparse_knn.rs)) already threads `self.key` into `SparseLogKnn` (line 151), `SparseIndexKnn` (line 169), and `Idf` (line 245), and reads per-key BM25 config from the schema (lines 226-240). No orchestrator change required.
- `RankOrchestrator` fuses per-leaf results in DFS order — works for any number of sparse leaves.
- `KnnFilterOrchestrator` is vector-type agnostic.

What must change (the operators that actually open the reader):

- [rust/worker/src/execution/operators/sparse_index_knn.rs](rust/worker/src/execution/operators/sparse_index_knn.rs) `run` (~lines 69-77): the `key` field already exists but is unused on the compacted path; select the reader via `self.key`: `readers.get(&self.key).or(legacy_sparse_index_reader.as_ref())`; return empty when absent.
- [rust/worker/src/execution/operators/idf.rs](rust/worker/src/execution/operators/idf.rs) (~lines 141-142): select the per-key reader (with legacy fallback) for `dimension_counts`; `self.key` is already used for log adjustments.
- [rust/worker/src/execution/operators/sparse_log_knn.rs](rust/worker/src/execution/operators/sparse_log_knn.rs): already key-aware (reads `merged_metadata.get(&self.key)`) — no change expected.

Out of scope (pre-existing limitation): the local/single-node executor rejects sparse indexing entirely ([rust/frontend-core/src/collection_ops.rs](rust/frontend-core/src/collection_ops.rs) ~lines 211-217), so this feature is distributed-only. Lifting local sparse support is a separate, larger effort — not included unless explicitly requested.

## 3b. Query-language score fusion across multiple sparse indices

Goal: let a single search fuse scores from several sparse indices (and dense) with arithmetic, e.g. `sparse_a * 0.5 + sparse_b * 0.5`, `(sparse_a + 1).log() + dense`, or `rrf([sparse_a, sparse_b, dense])`.

Good news: the rank language already supports this. `RankExpr` ([rust/types/src/execution/operator.rs](rust/types/src/execution/operator.rs) lines 1152-1192) provides `$sum`, `$mul`, `$div`, `$sub`, `$min`, `$max`, `$abs`, `$log`, `$exp`, `$val` plus `rrf(...)`; each `$knn` leaf targets an arbitrary `Key::MetadataField` (lines 1787-1866); `RankExpr::knn_queries()` (lines 1203-1230) already DFS-collects every leaf; the search server spawns one `SparseKnnOrchestrator` per sparse leaf keyed by `knn_query.key` ([rust/worker/src/server.rs](rust/worker/src/server.rs) lines 769-788); and `RankOrchestrator` fuses the per-leaf result vectors in DFS order. So no new operators are needed.

The work is to make multi-sparse fusion actually resolve and validate, then prove it:

- Per-leaf index resolution: depends on section 3 — each sparse `$knn` leaf must hit its own per-key reader (`readers.get(&self.key)`), so `sparse_a` and `sparse_b` score against different indices instead of one shared one. This is the core enabler; without it, two sparse leaves would fuse identical scores.
- Validation: confirm rank validation admits multiple sparse leaves over distinct enabled keys. Check `validate_rank` ([rust/types/src/validators.rs](rust/types/src/validators.rs) ~lines 173-184) and frontend `Schema::is_knn_key_indexing_enabled` ([rust/types/src/collection_schema.rs](rust/types/src/collection_schema.rs) ~lines 2432-2455) / [service_based_frontend.rs](rust/frontend/src/impls/service_based_frontend.rs) (~lines 2390-2402) walk every `$knn` leaf and validate each key independently. After section 1 lifts the caps, all referenced sparse keys are enabled, so each leaf validates. Ensure there is no assumption of a single sparse leaf (e.g. no dedup by query type).
- Same-key leaves: two `$knn` leaves on the same key with different query vectors must both run independently (each builds its own orchestrator) — verify no collapsing keyed solely by metadata key.
- Clients: confirm Python (`chromadb/execution/expression/operator.py` `Knn` + operator overloading + `rrf`) and TS expression builders allow composing multiple sparse `Knn(key=...)` leaves with arithmetic, with no client-side single-sparse restriction. No protobuf change (arithmetic ops and `Knn.key` already exist).

## 4. Garbage collection

- GC already treats any non-HNSW/non-bloom file_path key as a sparse index ([list_files_at_version.rs](rust/garbage_collector/src/operators/list_files_at_version.rs) ~lines 162-173; [garbage_collector_tool.rs](rust/garbage_collector/src/bin/garbage_collector_tool.rs) `is_sparse_index_file_type` ~line 37). Per-key keys are picked up automatically; add a focused test to confirm multiple per-key sparse entries are all retained.

## 5. Clients (Python + TS)

- Python: confirm `CollectionCommon._get_sparse_embedding_targets` iterates all enabled sparse keys (it does) and that string-query embedding resolves per-key EFs; remove single-index validation (item 1).
- TS [clients/new-js/packages/chromadb/src/schema.ts](clients/new-js/packages/chromadb/src/schema.ts): remove the cap and verify per-key sparse embedding/search wiring mirrors Python; `Knn(key=...)` already targets a key.
- No protobuf changes needed (`RankExpr.Knn.key` and `SparseVector` already exist).

## 6. Tests

- Rust segment (new collection): multi-key write/commit/read round-trip with two sparse keys written directly to per-key layout.
- Rust segment (migration): a legacy single anonymous-index segment is still readable via the legacy fallback, then rewritten to the single per-key entry after one compaction; old global keys gone.
- Rust query: hybrid search across two sparse keys returns independent results; `SparseIndexKnn` selects the correct index by key.
- Rust fusion: arithmetic fusion across multiple sparse indices produces correct scores — e.g. `sparse_a * w1 + sparse_b * w2` and `rrf([sparse_a, sparse_b, dense])` — and two leaves on the same key with different queries both run independently.
- Schema/validator: two enabled sparse keys now pass `validate_schema` and the builder at create time (`collection_schema.rs` test at ~6349-6374 must be inverted/removed).
- Python + TS: a collection created with multiple sparse keys builds; a record with multiple sparse vectors ingests and each key is independently queryable.

## PR stack (incrementally reviewable)

Graphite-style stack of `hammad/` branches, each independently reviewable and safe to merge. Ordering guarantees every intermediate state compiles and preserves behavior: the engine is made key-capable while still externally single-key (caps on), then the caps are lifted once the engine supports multi-key, then fusion/clients/e2e land on top.

```mermaid
flowchart LR
    P1["PR1 hammad/sparse_multi_engine"] --> P2["PR2 hammad/sparse_multi_enable"]
    P2 --> P3["PR3 hammad/sparse_multi_fusion"]
    P3 --> P4["PR4 hammad/sparse_multi_clients"]
```

### PR1 - `hammad/sparse_multi_engine`

Make the distributed engine key-capable without changing external behavior (caps still enforce a single sparse key, so this is exercised in single-key + migration mode). Tightly coupled because the segment reader struct and its operator consumers must change together.

- Scope: sections 2a-2f and 3.
  - [rust/types/src/segment.rs](rust/types/src/segment.rs): per-key file_path helpers + `parse_sparse_file_path_key`; per-key `check_metadata_consistency` with legacy acceptance.
  - [rust/types/src/collection_schema.rs](rust/types/src/collection_schema.rs): add `Schema::enabled_sparse_keys()`.
  - [rust/segment/src/blockfile_metadata.rs](rust/segment/src/blockfile_metadata.rs): writer/reader/flusher keyed by metadata key (`HashMap`) + `legacy_sparse_index_reader`; route `set/delete/update_metadata` by key; per-key commit/flush; legacy fork-on-compaction migration.
  - [rust/worker/src/execution/operators/sparse_index_knn.rs](rust/worker/src/execution/operators/sparse_index_knn.rs) and [idf.rs](rust/worker/src/execution/operators/idf.rs): select per-key reader via `self.key` with legacy fallback.
- Commit: `[ENH](segment): Key sparse vector index by metadata field`
- Test plan:
  - `cargo test -p chroma-types segment` and `... collection_schema` - helper round-trip/parse, `enabled_sparse_keys`, consistency check (per-key + legacy).
  - `cargo test -p chroma-segment` - new: multi-key write/commit/read round-trip (construct schema with 2 keys directly in-test, bypassing the API caps); legacy single anonymous-index segment still readable via fallback, then rewritten to per-key after one commit (old global keys gone).
  - `cargo test -p worker sparse` - `SparseIndexKnn`/`Idf` select correct per-key index; legacy fallback path.
  - `cargo test -p garbage_collector` - multiple per-key sparse file_path entries are all retained (regression).

### PR2 - `hammad/sparse_multi_enable`

Flip the feature on: allow multiple enabled sparse indices at collection creation across all schema validators.

- Scope: section 1.
  - [rust/types/src/validators.rs](rust/types/src/validators.rs): drop the `> 1` rejection (keep defaults-cannot-enable).
  - [rust/types/src/collection_schema.rs](rust/types/src/collection_schema.rs): remove `MultipleSparseVectorIndexes` and its check in `create_index`.
  - Python [chromadb/api/types.py](chromadb/api/types.py): remove `_validate_single_sparse_vector_index`.
  - TS [clients/new-js/packages/chromadb/src/schema.ts](clients/new-js/packages/chromadb/src/schema.ts): remove the single-index check.
  - Docs: [docs/mintlify/cloud/schema/index-reference.mdx](docs/mintlify/cloud/schema/index-reference.mdx).
- Commit: `[ENH](schema): Allow multiple sparse vector indices`
- Test plan:
  - `cargo test -p chroma-types` - `validate_schema` accepts two enabled sparse keys; builder allows a second key; invert/remove the single-index builder test (~6349-6374).
  - Python: `pytest chromadb/test/api/test_schema_e2e.py -k sparse` - schema with two sparse keys builds and serializes.
  - TS: `yarn test schema` in `clients/new-js/packages/chromadb` - multi sparse-key schema builds.

### PR3 - `hammad/sparse_multi_fusion`

Verify and lock in arithmetic score fusion across multiple sparse indices end-to-end in the worker.

- Scope: section 3b (mostly verification + tests; confirm no single-sparse assumption in `validate_rank` / frontend `is_knn_key_indexing_enabled`).
- Commit: `[TST](worker): Fuse scores across multiple sparse indices`
- Test plan:
  - `cargo test -p worker` - hybrid search across two sparse keys returns independent results; `sparse_a * w1 + sparse_b * w2` and `rrf([sparse_a, sparse_b, dense])` produce correct fused scores; two `$knn` leaves on the same key with different queries run independently.
  - `cargo test -p chroma-types validators` - rank validation admits multiple sparse leaves over distinct enabled keys.

### PR4 - `hammad/sparse_multi_clients`

Client end-to-end: create a collection with multiple sparse keys, ingest records with multiple sparse vectors, query each independently, and fuse.

- Scope: section 5 (client wiring/tests) + docs polish.
- Commit: `[TST](clients): Multiple sparse vectors per record e2e`
- Test plan:
  - Python: `pytest chromadb/test -k "sparse and multi"` - create multi-sparse collection; add records with 2 sparse vectors; query each key; fused search (`Knn(key=a)*0.5 + Knn(key=b)*0.5`, `rrf`).
  - TS: `yarn test` in `clients/new-js/packages/chromadb` - mirror of the Python e2e.
  - Confirm `_get_sparse_embedding_targets` (Python) and TS equivalent generate embeddings for all enabled sparse keys.

## Out of scope / notes

- Go sysdb stores schema JSON opaquely and needs no logic change for per-key sparse indices.
- Algorithm (WAND vs MaxScore) becomes effectively per-key via each writer's config; collection-wide `set_sparse_algorithm` remains valid.
