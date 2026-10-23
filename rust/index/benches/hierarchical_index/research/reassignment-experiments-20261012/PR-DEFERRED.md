## Summary

Finish a leaf's reassignment scans before balancing the leaves that receive its vectors. This reduces repeated cleanup and tree changes during a scan while keeping vector moves immediately visible.

Each balancing worker owns a stack of destination leaves that need maintenance. Reassignment searches from the root, publishes the new posting, and queues the destination. The worker drains that stack after the source split or merge finishes. Pending requests for the same leaf and causal depth are deduplicated, and a completed leaf can be queued again if further moves change it.

The existing per-leaf balancing guards, merge reservations, maximum causal depth of four, and maximum of eight outer balancing rounds remain in use. Missing queued leaves are skipped through the existing lookup path. Both full-precision and quantized reassignment use the queue. This PR does not group destination writes or bypass root navigation.

The benchmark output adds reassignment outcomes, exclusive reassignment elapsed time, and destination queue counters. These are summed worker timings, not percentages of checkpoint wall time. A standalone saved-index audit checks that an undeleted benchmark prefix contains every expected reachable current-version vector ID (`hierarchical_spann_audit`). The implementation and tests are in `rust/index/benches/hierarchical_index/writer/writer.rs`; the counters are in `instrumentation.rs`.

## Benchmark results

The comparison uses three interleaved repetitions per variant on the lyon EC2 `r6id.16xlarge` instance, with 32 add threads and 32 balancing threads. Each run adds 1M Wikipedia vectors to its own copy of an existing checkpoint. Write navigation and neighbor reassignment use full precision, replication is one, and the configured cache is 1 GiB. All values below are arithmetic means.

| Metric | Current writer | Deferred destination balancing | Change |
| --- | ---: | ---: | ---: |
| 9M→10M balancing | 15.666s | 10.531s | −32.8% |
| 9M→10M full build wall time | 60.020s | 54.956s | −8.4% |
| 39M→40M add | 13.087s | 13.416s | +2.5% |
| 39M→40M balancing | 41.935s | 26.630s | −36.5% |
| 39M→40M commit, fork and flush | 21.456s | 21.760s | +1.4% |
| 39M→40M add, balance, and commit | 76.478s | 61.806s | −19.2% |
| 39M→40M full build wall time | 186.184s | 169.716s | −8.8% |
| 40M recall@100, tau 2 and rerank 8 | 94.363% | 94.373% | +0.010 percentage points |

Full build wall time includes posting validation, memory cleanup, and tree diagnostics. It excludes query recall and the separate persisted-index audit. At 40M, the control takes eight outer balancing rounds in all three trials; deferral finishes in one. Posting cleanup calls fall from approximately 554K to 75K, and reassignment attempts fall from approximately 564K to 510K.

The uninstrumented 40M control averages 41.940s balancing, compared with 41.935s for the instrumented control. Timer overhead does not explain the measured balancing benefit. A separate grouped-write variant averages 26.277s balancing, only 0.35s below deferral; that variant is excluded from this PR.

## Validation and limits

The PR branch passes 68 test executions across `hierarchical_policy_cache` and `hierarchical_posting_delta`. The tests cover queue depth, pending-work deduplication, requeueing completed work, immediate vector visibility before a queued split, removed destinations, and existing concurrent merge and split behavior. Clippy passes with warnings denied for both test targets, the writer benchmark, and the saved-index audit benchmark. `git diff --check` passes.

All three deferred trials at each size pass the pre-commit and reopened-index coverage checks and the separate persisted-index audit. The 40M audit finds exactly 40,000,000 reachable current-version IDs covering the expected vector prefix. Before-and-after file hashes confirm that the saved source checkpoints are unchanged.

Recall uses 100 fixed queries with exact answers for the matching corpus prefix. These results screen for a recall regression; they do not establish equivalence within a small recall margin. A larger paired recall check and a follow-on write remain needed to check whether any maintenance cost moves into the next checkpoint.

The benchmarks compare variants based on `ba8a8c764643582eac295cd5df004a15ee46b65d`. This PR is based on `083ef95d6d3474f76cd703246294ae28c5f50368`, which also contains the reader precision change in #7897. The writer implementation is byte-identical to the deferred benchmark variant. The measured recall and query latency use the benchmark base's reader, so they do not measure #7897. Unused grouped-write counters are excluded from this PR.

The design builds on the deferred maintenance experiment in #7866. This PR provides the implementation on the current feature branch with fresh checkpoint comparisons.

## Reproduction

The 39M→40M run uses the following arguments. Each trial supplies a separate working copy through `--save-dir`; the original snapshots are never used as writable output.

```sh
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized -- \
  --dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 \
  --branching-factor 100 --split-threshold 2048 --merge-threshold 512 \
  --threads 32 --balance-threads 32 --write-navigation fp --fp-npa \
  --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 \
  --write-level-taus _,_,0.30 --max-replicas 1 \
  --write-rng-epsilon 0 --write-rng-factor 0 \
  --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ \
  --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 \
  --recall-tau-values 0.7,1,2 --num-queries 100 \
  --max-cache-bytes 1073741824 --compute-gt-clusters false \
  --verify-valid-postings --save --resume --save-dir <working-copy>
```

Exact command lines, full CLI logs, persisted audits, checkpoint metadata, and process timing are saved on the benchmark instance under `/home/ubuntu/benchmark-data/reassignment-experiments-20261012/cp-40-exact-gt` and `cp-10`. The 40M run uses an experiment-local exact-prefix ground-truth cache; shared ground-truth files are preserved.
