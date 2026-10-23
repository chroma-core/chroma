## Summary

The hierarchical index stores one posting per immutable vector. Parallel balancing moves rows under exclusive leaf claims, validates each proposed move against the current source and destination, and removes accepted source rows explicitly. The public append, balance, and commit phases require exclusive access. Duplicate input IDs return an error before batch mutation.

```text
append a validated batch
  route workers against the stable tree
  join and flush leaf buffers
balance in parallel rounds
  claim the source and available targets without waiting
  validate and prepare each grouped transfer
  prepare every destination before appending
  compact the source once across destinations
  retain contended work for another round
commit and flush a reader snapshot
  persist IDs and codes in format 2
  clear dirty state after every flush succeeds
```

A separate conversion command filters versioned checkpoints using their authoritative versions and deletion bits, validates topology and exact coverage, and writes a new checkpoint. It recomputes codes, bounds embedding output with periodic flushes, and preserves the source. Live readers and writers require the new format marker. See the [prototype README](rust/index/benches/hierarchical_index/README.md) for conversion usage and recovery semantics.

The transfer protocol, insert-only contract, persisted schema, and format gate form one correctness boundary in this PR. The candidate includes the complete converter so legacy stale rows cannot enter the version-free live path unchecked.

## Evidence

- **Before:** Reassignment invalidates source rows through per-vector versions, and legacy checkpoints can contain stale posting rows.
- **After:** 161 tests pass across `hierarchical_posting_delta`, `hierarchical_policy_cache`, `hierarchical_query_only`, and `hierarchical_reader_packed_centroids`. Empty-posting serialization, size-only cap, multi-destination compaction, retry coalescing, superseded retry filtering (including lazy posting loads), and partial multi-target failure regressions pass in that suite. Tests cover conflicting and reversed transfers, leaves in one node-map shard, ownership round trips, split/merge arrivals, retained retries, injected partial-transfer poisoning, duplicate rejection, exact codes and embeddings after reopen, conversion filtering and rejection cases, output chunk boundaries, source preservation, and flush retry.
- The benchmark target passes `cargo check`; formatting and whitespace checks pass. Performance comparison runs separately from these correctness gates.
- Checkpoint diagnostics expose claim conflicts, retries, claim hold time, source rows scanned, source compactions, and transferred vector counts. Serial validation has separate coordinate/ID timers and an exclusive wall phase while total insertion time includes every check. Balance iteration records separate work detection, sequential retry drain, setup, worker wall, and final navigation refresh; worker sums and maxima are overlapping diagnostics.

The measurement revision is `8581e80920036247715da0b40a4d0a4702118122`, and the transfer optimization revision is `fc39a953806ef1d9cb02f1985b2fcf1228e5cdb8`. Both use the same two-pass validation and timing. Routed transfers reserve their source and available destinations together, prepare all destinations before publication, and compact the source once. Compatible queued retries coalesce by source and exclusion rule. Full-precision and forced transfers skip unused quantized query construction. The `transfer_batches` counter counts successful source compactions in the optimized revision; compare source rows scanned per transferred vector alongside wall time and contention.

## Wikipedia benchmark follow-up

The comparison measures insertion (including production preflight), balancing, commit fork/flush, and writer reopen. It excludes benchmark posting validation, input loading, diagnostics, and cleanup. Separate saved-index audits verify correctness. The reference is instrumentation-only `8581e809`; source compaction is `fc39a953`; the final retry-membership filter is `eea4ada6`. Wikipedia uses identical FP navigation and 32-thread settings. The 1M builds commit every 100k vectors; 3M builds commit every 1M.

| Core indexing time | Instrumented reference | Source compaction | Compaction + retry filter |
|---|---:|---:|---:|
| 1M, two runs in opposite order | 26.045 s | 27.443 s (+5.4%) | Not run |
| 3M | 66.753 s, two-run mean | 70.841 s, two-run mean (+6.1%) | 68.931 s, one run (+3.3%) |

Production insertion checks take 0.716 seconds at 1M and 2.180 seconds at 3M. Coordinate checks account for 0.674 and 1.999 seconds respectively; fresh sequential IDs require no persisted existence probes.

Source compaction reduces scanned posting entries by 87.4% at 3M, but serial retry drain grows from 3.772 to 7.487 seconds and accounts for about 95% of the balancing regression. The membership filter finds 57.8% of coalesced queued IDs are superseded and lowers retry time to 5.641 seconds. Its overall 2.7% improvement over unfiltered compaction is a single-run result within observed timing variation; the combined candidate has not demonstrated a net performance win over the reference. The implementation handoff reports 161 passing targeted tests for the final filter.

Checkpoint 9's long balance is reproduced at 44.08 seconds and explained by coarse subtree scheduling. In the independent 8M-to-9M replay, one round has 38 leaves needing work, but one indivisible subtree contains 27 of them: its worker takes 8.193 seconds while the next slowest takes 0.322 seconds. Process samples show roughly one active CPU core during much of the tail, without sampled disk reads or major faults. The PR also defers destination balancing to outer rounds, unlike the versioned baseline's recursive destination balancing, so these costs cannot be attributed to version removal alone.

All nine quick-run indexes and the diagnostic/replay indexes pass separate saved-index audits. The final 3M filtered index contains exactly 3,000,000 physical/live postings, no missing or unreachable tree nodes, and 100 source-matching embedding/code samples. Exact L2 recall is 77.12% / 98.30% at tau 1.5, rerank 1 / 8, and 77.16% / 98.51% at tau 2. The retained 8M snapshot also passes its audit. This follow-up runs no full 10M build and makes no readiness or merge decision.

Full commands, hashes, logs, resource samples, and the report are retained locally under `version-free-writer/followup/benchmarks/` and remotely under `/mnt/msmarco/version-free-followup-20261020/`.

## Merge Danger

**Door:** Two-way for code. Format 2 checkpoints require this implementation; converting into a separate directory preserves the legacy checkpoint for rollback.

**Blast Radius:** Prototype.

The hierarchical benchmark writer and reader use an insert-only API and a new posting schema. Embedding updates and vector deletion are unsupported. Balancing keeps its eight-round size optimization limit. Residual oversized or undersized leaves are logged and can commit when all transfer and retirement work is complete. Pending retries still return an incomplete error and prevent commit. A panic during mutation poisons the writer and requires reopening the last published checkpoint.

