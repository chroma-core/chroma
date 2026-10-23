# Deferred destination posting-list loading: EC2 results

## Conclusion

Deferring the destination posting-list load does not establish a reliable performance improvement by itself. Four repeated runs from the same saved tree show less than a 1% difference in balancing time. Profiling shows that the subsequent cleanup step performs approximately the same loading work that destination registration omits. A larger improvement requires changing when cleanup runs or allowing cleanup to operate without loading the complete list.

## What changes

A vector moves between leaves by appending a replacement posting to its destination and publishing a new vector version. The candidate appends to the destination's resident delta without first loading its persisted posting list. The leaf's complete length increases by one, so its size continues to include persisted rows. The replacement posting exists before the new version becomes visible to other readers.

The control and candidate also share a correctness fix. A reopened vector can have its current version only in the checkpoint cache. First reassignment promotes that value into the writable version map before comparing and updating it. Without this fix, first registration can fail because the writable map has no entry. Comparing the candidate to the earlier writer would therefore compare different amounts of valid work.

The corrected control is `39626fb14` on `codex/reassignment-checkpoint-version-fix`. The candidate is `000ac1eb6f9a7436b8a95719973e961244521163` on `codex/reassignment-destination-deltas`. Both descend from the prior profiled writer at `85d41e1de`. No pull request is opened for this experiment.

## Measurements

The repeated checkpoint runs are the strongest comparison because both versions start with independent copies of the same 800k MS MARCO tree. Each run adds two batches of 100k vectors, commits and reopens after each batch, and measures recall at 1M. Runs are sequential in control, candidate, candidate, control order.

| Run | Balancing | Checkpoint total | Peak process RSS | Cold recall at 100 |
| --- | ---: | ---: | ---: | ---: |
| Control 1 | 11.17 s | 42.30 s | 7.02 GiB | 80.46% |
| Candidate 1 | 10.43 s | 41.47 s | 7.06 GiB | 80.72% |
| Candidate 2 | 11.10 s | 42.10 s | 7.04 GiB | 80.73% |
| Control 2 | 10.54 s | 41.34 s | 6.88 GiB | 80.71% |
| Control mean | 10.855 s | 41.820 s | — | — |
| Candidate mean | 10.765 s | 41.785 s | — | — |

The candidate mean is 0.83% lower for balancing and 0.08% lower for checkpoint totals. Both differences are smaller than the variation between the two runs of each version. These measurements do not establish a speedup or memory reduction.

Fresh builds provide additional context but use only one run per version and dataset. Each build adds 1M vectors in ten 100k checkpoints. The checkpoint total includes source-vector loading, addition, balancing, commit, and reopen; it excludes final recall and final diagnostics.

| Dataset and version | Balancing | Checkpoint total | Peak process RSS | Cold recall at 100 |
| --- | ---: | ---: | ---: | ---: |
| Wikipedia control | 57.32 s | 84.16 s | 7.41 GiB | 77.80% |
| Wikipedia candidate | 50.36 s | 76.36 s | 7.69 GiB | 77.81% |
| MS MARCO control | 51.17 s | 144.02 s | 6.87 GiB | 81.26% |
| MS MARCO candidate | 55.71 s | 148.70 s | 7.04 GiB | 80.51% |

The first checkpoint changes its number of balancing passes between versions. Wikipedia takes three passes in the control and two in the candidate; MS MARCO takes two in the control and three in the candidate. That difference accounts for much of the mixed full-build result. Excluding the first checkpoint, Wikipedia balancing is 40.44 versus 38.59 seconds, and MS MARCO balancing is 39.69 versus 37.66 seconds. Those single-run differences remain insufficient to establish a repeatable benefit. Recall uses 100 queries and the parallel builds can produce different trees; the MS MARCO recall difference needs repeated runs before attributing it to the change.

## Where the loading time goes

The candidate removes the destination-load timer, but the following balancing call cleans the destination leaf and loads its persisted postings. The measured work therefore moves from registration into cleanup. These numbers sum elapsed time across workers; concurrent work overlaps, so they are not build wall time.

| Measurement | Control destination-load work | Control cleanup work | Candidate cleanup work |
| --- | ---: | ---: | ---: |
| MS MARCO replay 1 | 61.69 s | 41.28 s | 95.63 s |
| MS MARCO replay 2 | 57.17 s | 41.14 s | 100.57 s |
| Wikipedia full build | 68.33 s | 71.88 s | 140.58 s |
| MS MARCO full build | 138.71 s | 141.26 s | 273.33 s |

The code path that explains this result is the unconditional cleanup at the beginning of destination balancing. Cleanup materializes the posting list. Skipping the earlier load changes its timing without eliminating it.

## Correctness checks and limits

The candidate passes 37 test executions across the posting-delta and policy/navigation harnesses. The corrected control passes the new persisted-version regression. The regression checks an absent destination, concurrent attempts to move the same vector, exactly one successful publication, complete leaf lengths, later materialization, and persisted postings, quantization codes, and versions. The candidate additionally checks that registration performs no posting load. Formatting and commit checks pass.

Separate EC2 verification runs check root-reachable valid posting counts before commit and after reopening. The initial assertion that all 1M vectors have valid postings fails for the control: 2,076 are missing. A read-only audit confirms that the starting 800k snapshot already has only 797,924 reachable valid IDs. Both control and candidate then report 897,924 at 900k and 997,924 at 1M, with no missing child nodes, no resident indexed embeddings lacking a valid posting, and the same final counts after reopen. This confirms matching counts and an increase of 200,000 valid IDs; it does not prove equality of every ID or repair the inherited deficit. The original assertion failure is preserved in `verify-progress.log`; the source-adjusted count checks are saved in `validation-summary.json`.

The saved source checkpoint metadata has the same SHA-256 checksum before and after the timing replays and read-only audit. Mutable replay checkpoints use independent copies rather than hard links. The audit instrumentation exits immediately after inspecting the reopened writer and is excluded from the measured binaries. Its temporary source edit is restored after the audit build.

## Environment and settings

The EC2 machine is the existing lyon instance, an r6id.16xlarge with 64 virtual CPUs and 512 GiB of memory. Both datasets have 1,024 dimensions. The benchmark uses local NVMe, full-precision writer navigation and neighbor reassignment, parallel balancing, a 1 GiB block cache, 100 precomputed queries, L2 distance, recall tau 1.5, and rerank multiplier 1. Wikipedia uses 32 add/balance threads, beam tau 0.5, level taus `_,_,0.30`, and RNG factor 0. MS MARCO uses 64 add/balance threads, beam tau 1.5, and RNG factor 4. The user-requested removal of `--write-level-min-pcts` is applied.

Exact commands, logs, process resource measurements, revision files, and checksums are saved beside this report. `summary.json` contains parsed timing results, and `analyze.py` regenerates it. Remote artifacts are under `/home/ubuntu/benchmark-data/destination-deltas-7842`. The EC2 instance remains running.
