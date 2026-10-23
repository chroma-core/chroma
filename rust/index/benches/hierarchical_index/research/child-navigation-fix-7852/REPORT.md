# Child navigation: candidate-limit fix and simplification

## Summary

Balancing scores a parent's children while moving vectors between leaves. A shared parent payload stores their IDs beside contiguous full-precision centroids, so scoring avoids fetching every child's node from the shared map. Search resolves candidates in distance order and counts only live nodes toward its candidate limit. Removed nodes establish neither the closest distance nor the limit that cuts off further candidates.

The child IDs and centroids live in one private, validated payload. One method edits child lists and publishes matching centroids. A second method changes an internal node's centroid and refreshes its entry in the next parent. Searches retain a stable reference and release the parent map guard before scoring. The node map remains the source of actual leaves and internal nodes; the parent payload supplies their scoring data. The persisted index format is unchanged.

The PR also fixes first reassignment after reopening an index. Registration imports an unchanged vector's persisted version into the writable version map before updating it and preserves the old version until a replacement posting exists. Both benchmark variants include this fix, so the comparison holds version handling constant.

The implementation is in the prototype writer under `rust/index/benches/hierarchical_index/writer/`. A separate stacked cleanup, #7855, removes unused navigation-dirty tracking.

## EC2 results (provisional)

The comparison measures control `049cc0eb1` and the exact PR head `22d32265e` on the lyon r6id.16xlarge instance. It uses 1,024-dimensional datasets, full-precision navigation and neighbor reassignment, parallel balancing, and 100k checkpoints. Repeated MS MARCO runs independently copy the same validated 800k checkpoint and add 200k vectors. The two control runs are reused from the preceding comparison on this instance; the two final-head candidate runs execute sequentially afterward. The final comparison therefore does not interleave control and candidate runs.

| Comparison | Control balancing | Candidate balancing | Reduction | Control checkpoint total | Candidate checkpoint total | Reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MS MARCO 800k→1M mean, two runs each | 9.04 s | 4.83 s | 46.5% | 39.62 s | 35.84 s | 9.6% |
| Wikipedia fresh 1M, one run each | 49.90 s | 32.75 s | 34.4% | 75.72 s | 58.61 s | 22.6% |
| MS MARCO fresh 1M, one run each | 47.53 s | 34.50 s | 27.4% | 139.69 s | 127.87 s | 8.5% |

Checkpoint totals include dataset loading, addition, balancing, commit, and reopen. They exclude final recall and separate correctness checks. The first 100k checkpoint takes longer in the candidate's full builds: Wikipedia balancing is 12.41 versus 13.60 seconds; MS MARCO is 11.67 versus 14.27 seconds. Later checkpoints improve. Single full-build comparisons do not establish why startup differs.

Recall across 100 precomputed queries is 81.05% versus 81.15% for the replay means, 77.72% versus 77.88% for fresh Wikipedia, and 81.14% versus 80.61% for fresh MS MARCO. The MS MARCO full build therefore measures a 0.53 percentage-point recall reduction. These small samples and single full-build runs do not establish recall equivalence. Peak process memory is 7.27 versus 7.52 GiB for Wikipedia and 7.14 versus 7.34 GiB for MS MARCO.

## Validation

Both local test harnesses pass 51 executions. Three regressions fail on the preceding PR head and pass on the current head: a removed nearest child consuming the candidate limit, a removed merge source consuming the limit, and reassignment missing a live sibling. Tests also cover 125 combinations of distance cutoffs and candidate limits, a child returning after being absent during publication, parallel splits, cascading internal splits, retained payloads, and persisted versions.

Final-head correctness validation finds an intermittent failure. The first MS MARCO run fails before its 200k commit: 200,000 vectors are expected, but only 199,750 have reachable valid postings. The table includes every final-head validation run, including that failure.

| Final-head run | Successful exact-ID checks | Outcome |
| --- | ---: | --- |
| final-validate-msmarco | 2 | before commit: 200000 expected ids, 199750 root-reachable valid ids; missing sample [132381, 128356, 133456, 136011, 121409, 128301, 120162, 129671], extra sample [] |
| final-validate-msmarco-recheck | 20 | Passes through 1M before every commit and after every reopen. |
| final-validate-wikipedia-1 | 20 | Passes through 1M before every commit and after every reopen. |
| final-validate-wikipedia-2 | 20 | Passes through 1M before every commit and after every reopen. |

A deterministic regression reproduces a vector-loss mechanism on both the control and final candidate. A merge removes its source posting before transferring every vector. If a concurrent internal split temporarily removes the root, reassignment can exhaust three immediate attempts without publishing a replacement. That preexisting defect remains unresolved; the reproduction does not prove the cause of the specific 250 missing vectors. The timing results are provisional because validation has not established that parallel builds consistently preserve every vector.

An earlier Wikipedia validation also found a missing child reference at 800k. A separate deterministic regression reproduces that error on both revisions when a merge sees a replacement leaf before its parent pointer is assigned. This PR repairs that case by always finding and removing the reachable parent link. The regression passes with the fix, but the reproduction does not prove the cause of the specific missing child in that earlier run.

Independent replay copies leave the validated source checkpoint's metadata checksum unchanged.

## Benchmark settings

Common settings are `--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false`. Wikipedia adds `--dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0`. MS MARCO adds `--dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4`. Replays add `--resume --save-dir <independent-checkpoint-copy>`. Correctness checks add `--validate-postings` and are measured separately. The benchmark target is `hierarchical_spann_profile_quantized` in `chroma-index`.

Logs, exact commands, resource measurements, revisions, and parsed results are saved on EC2 under `/home/ubuntu/benchmark-data/child-navigation-fix-7852`. The validated source checkpoint is preserved on EBS under `/mnt/benchmark-source/checkpoints/child-navigation-800k`.
