## Summary

Balancing workers take individual leaves from a shared queue, allowing idle workers to help when many leaves under one parent need work. Each round snapshots eligible leaves, starts the largest postings first, and waits for all workers before discovering the next round's jobs.

```text
begin round after pending transfers drain
  collect eligible leaf IDs, largest postings first
  each worker repeatedly takes the next leaf
    recheck its current type and size
    balance through existing claims and structural locks
  join workers
  discover newly created or oversized leaves next round
```

This PR is stacked on #7927 (`codex/version-free-hierarchical-writer`), which supplies the version-free writer. Its single commit changes scheduling and adds regression tests. Existing leaf claims, topology locking, eight-round size limit, transfer completion checks, and persisted format remain in place.

## Evidence

**Before:** Whole subtrees are assigned to workers. One measured round assigns 27 of 38 leaves needing work to one subtree; that worker runs for 8.19 seconds while the next-slowest finishes in 0.32 seconds.

**After:** Two matched Wikipedia replays add 1M vectors to independent copies of the same saved 8M checkpoint, with 32 writer/balance threads, 1024 dimensions, identical full-precision settings, and matching precomputed recall queries. The second pair reverses run order. The reference is `eea4ada6f21554adb535d1ffdea95f74cb76650b`; the candidate is `14dc9698eb6e400ba6f677575eff7a5659deeed6`.

| Measure | Reference | Shared leaf queue | Reduction |
| --- | ---: | ---: | ---: |
| Balance, pair 1 | 47.856s | 16.692s | 65.1% |
| Balance, reversed pair | 45.264s | 16.101s | 64.4% |
| Insert + balance + commit/reopen, mean | 62.01s | 31.92s | 48.5% |

Sampled utilization rises from about 5.4 cores to 17–17.5. Sampled CPU work increases slightly, supporting reduced idle time rather than less computation. Retry time increases from roughly 0.4s to 1.6s, but the shorter worker tail dominates. Recall differences stay within 0.13 percentage points and peak RSS remains comparable. Both variants reach the eight-round limit with a few remaining size violations; this is not a claim of identical resulting trees or full convergence. A single fresh 1M run is 3.4% slower, so the large gain is established for this saved-tree workload, not every build.

The first pair uses one local NVMe volume and the reversed pair another; each pair uses the same volume for both variants. Timings include production insertion checks, balance, commit preparation/flush, and reopen. Benchmark validation runs separately. Input loading and other harness work are excluded from this phase sum. The 9M ground truth is existing precomputed data with verified query identity and prefix metadata, not independently recomputed for these runs.

- All 173 test executions pass across `hierarchical_posting_delta`, `hierarchical_policy_cache`, `hierarchical_query_only`, and `hierarchical_reader_packed_centroids`. New tests cover unique eligible jobs, next-round discovery, and concurrent split/merge with exact-once IDs, topology, codes, and persisted reopen.
- Benchmark compilation, formatting, whitespace checks, and commit hooks pass. Independent concurrency and storage reviews find no blocking defect.
- All four replay outputs pass complete exact-once ID and topology audits, plus 200 sampled embedding/code checks covering existing and newly inserted vectors.
- The diagnostic `partitions` counter now counts eligible leaf jobs rather than subtree roots; those counts are not directly comparable across revisions.

A separate fresh-build comparison applies finer scheduling to the versioned writer as well. Version-free plus finer scheduling reduces measured writer phases by 29.4% at 1M/100k checkpoints and 10.4% at 3M/1M checkpoints, averaging two reversed-order runs each. Peak RSS increases 8.6% and 6.4%, and all eight saved-index audits pass. That comparison includes version removal and its replacement coordination, plus retained versioned cleanup/recursive balancing; it does not isolate this scheduler's benefit.

Full commands, hashes, raw CLI output, audits, and caveats are retained in `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/version-free-writer/balance-experiments/benchmarks/REPORT.md` and `version-free-writer/versioned-finer-comparison/benchmarks/REPORT.md`. Remote scheduler artifacts are under `/mnt/msmarco/balance-experiments-20261020` and `/mnt/data/balance-experiments-repeats-20261020`.

## Merge Danger

**Door:** Two-way. Reverting this scheduler commit restores subtree assignment without changing the checkpoint format.

**Blast Radius:** Prototype.

The change affects hierarchical writer balancing order and concurrency. New children wait until the next round, and more simultaneous neighboring work can increase contention. Existing claims and structural locks remain the safety boundary; the queue itself does not grant ownership. Pending required transfers still prevent commit, while bounded residual leaf-size violations remain permitted. Merge this stacked PR into the version-free branch, or retarget to `hierarchical-spann` after #7927 lands.
