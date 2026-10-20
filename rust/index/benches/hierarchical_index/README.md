# Insert-only hierarchical index

The index stores each immutable embedding in exactly one leaf. A writer appends new IDs, balances its tree, and commits a snapshot. Queries open a reader on a flushed snapshot.

## The pieces

- An embedding is the immutable full-precision vector stored under its ID.
- A leaf stores IDs and quantized codes relative to its centroid. Its length counts actual rows, including rows that remain on disk after a lazy open.
- A leaf claim reserves the postings, centroid, and lifetime of one leaf or a pair of leaves. Claims exist only during active work.
- The tree lock protects parent links and internal-node changes.
- A transfer proposal names a source leaf, a destination leaf, and candidate IDs. The proposal can become obsolete before it runs.
- A retry queue retains transfers and merge-source retirement work that cannot finish during the current balancing round.

The implementation lives in `writer/writer.rs`. Its active claims use a short `Mutex<HashSet<NodeId>>`; no vector-to-leaf ownership map is retained.

## Mutation phases

1. **Validate the whole input batch.** Reject an ID that appears twice in the batch or already has an embedding. IDs above the checkpoint maximum avoid storage reads. Lower IDs use an exact embedding existence check, so holes remain usable. Validate vector dimensions and finite coordinates before mutation.
2. **Append against a stable tree.** Parallel workers route new IDs and prepare leaf buffers. Their buffers are flushed after the workers join.
3. **Balance in parallel rounds.** Workers claim leaves for splits, merges, and grouped transfers. Every transfer rechecks membership and eligibility against current centroids. Transfers to the same destination compact their source once per group.
4. **Commit and flush the snapshot.** Exclusive access keeps mutation phases apart. Dirty state clears only after every blockfile flush succeeds. An outstanding flusher borrows the writer, so another mutation phase cannot start before that flusher is consumed or dropped.

The public mutation entry points require `&mut self`. Batch append and balance return errors that the harness propagates. Balancing checks its retry queue independently of leaf sizes and returns an incomplete error after eight rounds if work remains. A panic during mutation poisons the writer; subsequent append, balance, and commit calls reject it. Reopen the last published checkpoint after such a panic.

A transfer atomically claims both leaves without waiting. It loads postings, checks source membership, checks destination existence, and recomputes eligibility. It prepares destination codes, destination capacity, and compacted source buffers before changing rows. It then appends accepted destination rows and replaces the source buffers. Node-map guards are released between leaf accesses, including when both IDs share a map shard. Leaf claims are released before scheduling additional balance work. Tree-lock holders never wait for leaf claims, and code releases node-map guards before taking the tree lock.

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

Each checkpoint prints leaf claim conflicts, transfer retries, total claim hold nanoseconds, applied transfer batches, transferred vectors, and source entries scanned. These counters also appear in `WriterStatsSnapshot`. Existing timing tables omit stale-row scrubbing, and memory reports omit version storage.
