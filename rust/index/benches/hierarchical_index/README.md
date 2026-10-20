# Insert-only hierarchical index

The index stores each immutable embedding in exactly one leaf. A writer appends new IDs, balances its tree, and commits a snapshot. Queries open a reader on a flushed snapshot.

## The pieces

- An embedding is the immutable full-precision vector stored under its ID.
- A leaf stores IDs and quantized codes relative to its centroid. Its length counts actual rows, including rows that remain on disk after a lazy open.
- A leaf claim reserves the postings, centroid, and lifetime of its selected leaves. Claims exist only during active work. A routed transfer can reserve its source and all currently available destinations together.
- The tree lock protects parent links and internal-node changes.
- A transfer proposal names a source leaf, a destination leaf, and candidate IDs. The proposal can become obsolete before it runs.
- A retry queue retains transfers and merge-source retirement work that cannot finish during the current balancing round.

The implementation lives in `writer/writer.rs`. Its active claims use a short `Mutex<HashSet<NodeId>>`; no vector-to-leaf ownership map is retained.

## Mutation phases

1. **Validate the whole input batch.** Reject an ID that appears twice in the batch or already has an embedding. IDs above the checkpoint maximum avoid storage reads. Lower IDs use an exact embedding existence check, so holes remain usable. Validate vector dimensions and finite coordinates before mutation.
2. **Append against a stable tree.** Parallel workers route new IDs and prepare leaf buffers. Their buffers are flushed after the workers join.
3. **Balance in parallel rounds.** Workers claim leaves for splits, merges, and grouped transfers. Every transfer rechecks membership and eligibility against current centroids. A routed transfer compacts its source once across all available destinations. Compatible queued retries for the same source and exclusion rule are combined before routing.
4. **Commit and flush the snapshot.** Exclusive access keeps mutation phases apart. Dirty state clears only after every blockfile flush succeeds. An outstanding flusher borrows the writer, so another mutation phase cannot start before that flusher is consumed or dropped.

The public mutation entry points require `&mut self`. Batch append and balance return errors that the harness propagates. Balancing checks its retry queue independently of leaf sizes. After eight rounds it returns an incomplete error if transfer or merge-source retirement retries remain. If only oversized or undersized leaves remain, it logs those sizes and returns a valid snapshot for commit. A panic during mutation poisons the writer; subsequent append, balance, and commit calls reject it. Reopen the last published checkpoint after such a panic.

A routed transfer atomically claims its source and all currently available destinations without waiting. It deduplicates overlapping proposals before choosing available targets, and it returns current source rows with unavailable or missing targets for retry. It loads postings, checks source membership, checks destination existence, and recomputes eligibility. It prepares destination codes, destination capacity, and compacted source buffers before changing rows. It prepares every accepted destination before appending any rows. It then appends accepted destination rows and replaces the source buffers once, while retaining every selected claim. Node-map guards are released between leaf accesses, including when both IDs share a map shard. Leaf claims are released before scheduling additional balance work. Tree-lock holders never wait for leaf claims, and code releases node-map guards before taking the tree lock.

Splits claim their source through construction and publication of replacement leaves. Replacement node IDs are fresh and retired IDs are never reused. Merges share the transfer operation and retire only sources that are empty under a leaf claim. Node tombstones remain necessary for deleting retired nodes from forked checkpoints.

## Checkpoint format and conversion

Format `2` stores only posting IDs and codes. The live writer and reader require the scalar `format` marker and reject unmarked checkpoints. No authoritative vector versions, version cache, stale-row filtering, embedding-update API, or vector-deletion API remains in the live path.

Convert an unmarked versioned checkpoint before opening it with this writer. The destination must be a new directory:

```sh
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized -- \
  --save-dir /path/to/source-checkpoint \
  --upgrade-legacy-to /path/to/new-checkpoint \
  --max-cache-bytes 536870912
```

Conversion runs before dataset loading and exits after publishing its output. It filters legacy rows using authoritative versions and deletion bits, rejects multiple live copies, validates reachable topology and embedding availability, and rejects live authoritative IDs that have no reachable posting. Accepted posting codes and node centroid codes are recomputed from stored full-precision data. The source directory is read only. Output is staged in a sibling temporary directory and renamed into place after all flushes and checkpoint metadata succeed; a handled failure removes the staging directory.

Conversion retains topology and live IDs in memory. Embedding payloads are processed incrementally: output vector data flushes and forks every 4096 embeddings, and posting output flushes and forks after approximately 32 MiB. Authoritative metadata is scanned in bounded ID windows, including checkpoints without a maximum-ID summary. Configure a bounded block cache for large conversions. Conversion can reject ambiguous or incomplete legacy checkpoints; it does not choose an arbitrary live copy.

## Instrumentation

Each checkpoint prints leaf claim conflicts, transfer retries, total claim hold nanoseconds, successful source-compaction batches, transferred vectors, and source entries scanned. The `transfer_batches` counter counts source compactions; source scans include attempts that accept no rows. These counters also appear in `WriterStatsSnapshot`. Existing timing tables omit stale-row scrubbing, and memory reports omit version storage.

Serial insertion checks have one timer for the whole batch and separate timers for coordinate validation and ID checks. The measured total is an exclusive wall-report phase; total insertion time includes it. Coordinate validation and ID checks run in two passes so their durations do not require per-vector clocks. This changes validation iteration order relative to the uninstrumented revision while preserving every check; compare the transfer optimization against the instrumentation revision to hold that change constant. Counts distinguish checked IDs, IDs requiring no checkpoint probe, and persisted existence probes.

Each balance iteration records wall time for work detection, sequential retry drain, scheduling, and parallel workers, including terminal retry-only or capped iterations. Rows include pending entry/ID counts before and after the sequential retry drain, empty retirement entries, partition count, worker count, and worker maximum and summed durations. Worker maximum and sum overlap the worker wall span and must not be added to exclusive wall totals. Final navigation refresh has a separate cumulative wall timer. These rows print after the measured balance call. Merge duration measures all merge attempts, including early returns, with one timer per attempt.

Full-precision transfers construct no quantized query state. Quantized transfers prepare that state only when eligibility uses it; forced merge evacuation also skips it. This removes unused residual, zero-vector, norm, dot-product, and query allocations from the full-precision transfer path. The optimization retains the same serial insertion validation and instrumentation as the preceding measurement commit. Source compaction is once per routed group and once per compatible coalesced retry group, not once per entire balancing phase.
