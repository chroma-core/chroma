## Summary

The hierarchical index stores one posting per immutable vector. Parallel balancing moves rows under exclusive leaf claims, validates each proposed move against the current source and destination, and removes accepted source rows explicitly. The public append, balance, and commit phases require exclusive access. Duplicate input IDs return an error before batch mutation.

```text
append a validated batch
  route workers against the stable tree
  join and flush leaf buffers
balance in parallel rounds
  claim a leaf or pair without waiting
  validate and prepare each grouped transfer
  append accepted rows and compact the source
  retain contended work for another round
commit and flush a reader snapshot
  persist IDs and codes in format 2
  clear dirty state after every flush succeeds
```

A separate conversion command filters versioned checkpoints using their authoritative versions and deletion bits, validates topology and exact coverage, and writes a new checkpoint. It recomputes codes, bounds embedding output with periodic flushes, and preserves the source. Live readers and writers require the new format marker. See the [prototype README](rust/index/benches/hierarchical_index/README.md) for conversion usage and recovery semantics.

The transfer protocol, insert-only contract, persisted schema, and format gate form one correctness boundary in this PR. The candidate includes the complete converter so legacy stale rows cannot enter the version-free live path unchecked.

## Evidence

- **Before:** Reassignment invalidates source rows through per-vector versions, and legacy checkpoints can contain stale posting rows.
- **After:** 128 tests pass across `hierarchical_posting_delta`, `hierarchical_policy_cache`, `hierarchical_query_only`, and `hierarchical_reader_packed_centroids`. Empty-posting serialization and size-only cap regressions pass in that suite. Tests cover conflicting and reversed transfers, leaves in one node-map shard, ownership round trips, split/merge arrivals, retained retries, injected partial-transfer poisoning, duplicate rejection, exact codes and embeddings after reopen, conversion filtering and rejection cases, output chunk boundaries, source preservation, and flush retry.
- The benchmark target passes `cargo check`; formatting and whitespace checks pass. No performance benchmark is run in this PR task.
- Checkpoint diagnostics expose claim conflicts, retries, claim hold time, source rows scanned, transfer batches, and transferred vector counts.

## Merge Danger

**Door:** Two-way for code. Format 2 checkpoints require this implementation; converting into a separate directory preserves the legacy checkpoint for rollback.

**Blast Radius:** Prototype.

The hierarchical benchmark writer and reader use an insert-only API and a new posting schema. Embedding updates and vector deletion are unsupported. Balancing keeps its eight-round size optimization limit. Residual oversized or undersized leaves are logged and can commit when all transfer and retirement work is complete. Pending retries still return an incomplete error and prevent commit. A panic during mutation poisons the writer and requires reopening the last published checkpoint.
