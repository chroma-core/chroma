# Reference-style local transfers and navigation at 500K vectors

The three requested changes improve the measured sharded build, but it remains slower than the retained reference. Insertion falls from 5.487 to 1.340 seconds, balancing from 54.346 to 19.379 seconds, and construction plus saving from 68.566 to 30.002 seconds. These are reductions of 75.6%, 64.3% and 56.2%. The reference takes 0.699, 9.054 and 12.601 seconds respectively. Keep the changes: correctness passes and the measured coordination and scoring waste fall substantially. This is one combined experiment, so it does not isolate each edit’s individual speedup.

## What is implemented

Same-owner reassignment groups destinations, claims the affected leaves, rechecks current membership and centroids, prepares codes, then compacts the source once across accepted destinations. The local path preserves readable navigation and independent-leaf concurrency. The explicit staged protocol remains for cross-owner transfers. Construction executes reassignment at the source owner; a controller directly asking a remote owner to move between two of that owner’s leaves still uses the retained protocol.

Navigation reads child centroids from one contiguous array paired with child identities and leaf flags in the parent’s immutable image. Publication and deserialization build this array once. Construction uses full precision, saved query navigation uses four-bit centroids, and postings retain one-bit codes. Distance counts accumulate locally and publish once per owned expansion, including the successful prefix when a later comparison fails.

The packed array duplicates the existing child-centroid fields to preserve mutation and wire compatibility. It improves the scoring layout but does not reduce memory by itself. Further removal of those duplicate fields is not part of this experiment.

## Workload and comparison

All arms use the same retained 500,000 FineWeb vectors, 768 dimensions, squared L2, 32 insertion and balance workers, and five 100,000-vector insertion/balance intervals. Branching is 100, split threshold 2,048, merge threshold 512, neighbor count 32, and construction beam 10–16 with tau 1.5. Both candidate owners have capacity 500,000, with the same 80% trigger and 70% placement target. There is no external profiler or tuning sweep.

All timed construction runs on the initial owner. The first four intervals perform no migration; final redistribution leaves 265,479 vectors on the initial machine and 234,521 on the peer, a 53.10%/46.90% split. This measures initial-owner construction and a real final handoff, not distributed-construction throughput. The input is 1.536 GB and remains within the 12 GiB vector cache. Final tree shapes are `[1,5,354]` for reference, `[1,6,347]` for the previous candidate and `[1,6,344]` for the new candidate: two internal levels and then leaves. There is no cache-capacity crossing.

The reference and previous candidate are retained epoch14 runs. The new candidate runs in a separate guarded window on the same task hardware, using an incremental Linux release build with the matched compiler and flags. Seeds and scheduling are not controlled, trees differ, and runtime/cache variability remains. The measured ratios apply to these runs, not a universal speedup or a 100B extrapolation.

| Measured phase | Retained reference | Previous sharded | New sharded | New/reference |
| --- | ---: | ---: | ---: | ---: |
| Insertion wall time | 0.699 s | 5.487 s | 1.340 s | 1.92× |
| Balancing wall time | 9.054 s | 54.346 s | 19.379 s | 2.14× |
| Balancing process CPU | 52.70 s | 117.80 s | 66.54 s | 1.26× |
| Clustering CPU within balancing | 35.296 s | 30.918 s | 31.522 s | 0.89× |
| Balancing CPU outside clustering | 17.404 s | 86.882 s | 35.018 s | 2.01× |
| Placement wall time | — | 1.194 s | 1.261 s | — |
| Saving wall time | 2.840 s | 7.411 s | 7.884 s | 2.78× |
| Construction plus saving | 12.601 s | 68.566 s | 30.002 s | 2.38× |
| Peak construction process memory | 2.077 GB | 3.081 GB | 3.271 GB | 1.58× |

Saving means commit plus flush for the reference and distributed save for the candidate. The candidate total includes redistribution and small orchestration costs; the reference has no redistribution. Fresh-reader validation takes another 4.204 seconds outside the candidate build/save timer. Peak memory is the process high-water mark, not summed fleet memory or an isolated measurement of the packed-array allocation. The 190 MB increase versus the previous candidate cannot be attributed to that array from this combined run.

| Vectors after interval | Reference add / balance | Previous sharded add / balance | New sharded add / balance |
| --- | ---: | ---: | ---: |
| 100K | 0.098 / 6.740 s | 0.180 / 14.859 s | 0.166 / 10.839 s |
| 200K | 0.119 / 0.574 s | 0.533 / 10.587 s | 0.139 / 3.291 s |
| 300K | 0.142 / 0.570 s | 1.081 / 10.289 s | 0.253 / 1.925 s |
| 400K | 0.160 / 0.551 s | 1.624 / 9.395 s | 0.340 / 1.961 s |
| 500K | 0.180 / 0.620 s | 2.069 / 9.215 s | 0.442 / 1.363 s |

## What the work counts establish

Local batching removes the measured transfer protocol and most reservation work during balancing. New local batches number 4,673, of which 3,943 compact a source. The new candidate transfers 162,587 rows versus 161,484 previously, so the reduction in bookkeeping is not explained by moving fewer rows. The reference moves 183,585 rows and performs 3,269 bulk transfer batches. Counter definitions distinguish attempted local batches from completed source compactions; zero legacy transfer groups does not mean zero transfers.

| Balancing work | Previous sharded | New sharded |
| --- | ---: | ---: |
| Transfer protocol calls | 321,814 | 0 |
| Reservation requests | 60,617 | 350 |
| Active reservation conflict visits | 3,203,721 | 62 |
| Source compactions | 29,038 | 3,943 |
| Source entries scanned | 41,309,426 | 6,438,225 |
| Split preparations committed / discarded | 349 / 0 | 344 / 0 |

The last insertion interval performs exactly 28.5 million candidate centroid comparisons in both runs. Its owned-scoring CPU falls from 55.691 to 4.789 seconds, while total owner CPU across that insertion snapshot falls from 60.92 to 10.55 seconds. Wall time falls from 2.069 to 0.442 seconds. This strongly localizes the navigation improvement, but the combined counter and packed-layout changes are not separately attributed. The reference performs 28.3 million comparisons and uses 3.29 CPU-seconds for that insertion interval.

Clustering work remains substantial and differs between trees. New clustering performs 349 calls and 88.34 million distances, versus 356/91.26 million previously and 358/79.82 million for the reference. Clustering CPU changes only from 30.918 to 31.522 seconds while non-clustering CPU falls from 86.882 to 35.018 seconds. The main measured CPU reduction therefore occurs outside clustering.

## Remaining explanations and next decisions

Direct local navigation is the leading CPU investigation. The new candidate still records 19.939 inclusive CPU-seconds in balancing navigation, including 6.789 in scoring, 5.519 in candidate handling and 4.670 in reply validation. It copies 2.219 GB of query data and clones 5.426 GB of reply data during balancing. These scopes nest and overlap; they must not be summed as separate elapsed-time costs. A direct local traversal that preserves global candidate selection is the next reference-style comparison to consider.

Saving is a separate measured gap: 7.884 seconds versus 2.840 reference, with no improvement in this experiment. Compare the actual serialization, full-vector access and persistence work before changing it. Do not attribute this gap to transfer coordination, which occurs in another phase.

The balance scheduler and critical path still need examination. After the first interval, balancing falls from 39.487 to 8.540 seconds but the reference takes 2.314 seconds. CPU is 43.03 versus 33.44 seconds; average busy cores are about 5.04 versus 14.45. That leaves a utilization or serial-dependency question in addition to excess CPU. A targeted phase trace should distinguish runnable-work starvation from necessary dependencies; merely increasing threads or vector count is not the next experiment.

Keep all three changes and stop this requested experiment. Reprioritize direct local navigation, saving, and balance scheduling/serial dependencies. No additional implementation or benchmark starts automatically.

## Correctness, failures and shutdown

The applicable native suite passes 185 tests with one known unrelated local-writer test excluded, matching the baseline exclusion. All three native executables build. An independent review finds and resolves a race where local transfer could overwrite newer reparented routing metadata; ordered commit locks now cover publication, and the controlled regression passes. The first suite’s local TCP sandbox denials and obsolete progress-test injection failure remain recorded. Both protocol progress tests now execute the retained protocol explicitly instead of silently bypassing their injector.

The physical benchmark finishes successfully. A fresh independent reader validates exactly 500,000 memberships, verifies the full-vector digest, and recomputes every posting code and centroid code. Sixteen training-vector smoke queries run; they do not establish held-out recall or quality parity.

The peer-stop wrapper still encounters an unreadable-process permission error, so its attempted terminal-process workaround is not considered successful. The independent peer supervisor confirms cleanup after operator stop, the main supervisor exits zero with cleanup confirmed, and AWS reports both exact task instances stopped. No benchmark retry occurs. The 401.109-second window adds at most $0.8984837236022949 compute and no new recurring storage commitment. This is task spending, not a whole-account invoice.

## Reproduction and evidence

- [Machine-readable three-arm comparison](../operations/window-epoch15/comparison.json) and [candidate summary](../operations/window-epoch15/candidate-summary.json) retain per-interval counters and overlapping scopes.
- [Raw candidate receipts](../operations/window-epoch15/500k-distributed/run-receipts.json), [result](../operations/window-epoch15/500k-distributed/result.json) and [fresh-reader validation](../operations/window-epoch15/500k-distributed/durable-validation.json) retain the measured evidence. [Retained reference](../operations/window-epoch14/500k-reference/result.json) and [previous candidate](../operations/window-epoch14/500k-distributed/result.json) remain unchanged.
- [Native validation](LOCAL-TRANSFER-PACKED-NAV-VALIDATION.json), [independent review](EPOCH15-LOCAL-TRANSFER-PACKED-REVIEW.md) and [source changes](epoch15-source-closure-comparison.json) bind correctness to the frozen source.
- [Configuration](../operations/config-epoch15.json), [admission](../operations/admission-epoch15.json), [runtime hashes](../operations/runtime-binaries-epoch15.json), [Linux build receipts](../operations/window-epoch15/linux-build-receipts.json), [cleanup reconciliation](../operations/window-epoch15/cleanup-reconciled.json) and [API closure](../operations/window-epoch15/closed.json) bind the execution and shutdown.

Input SHA256 is `165f6baa847dacb65c1e9c718c1f6edd976f52ec3907136c447de9ebf5972f48`. Frozen source archive SHA256 is `2b36ff49ce3ca38aff2e02fd596cada211d0eaf96036221f7cbd3ebf1fa5a0af`. Writer SHA256 is `b3e134079a6331ba90f5f1c4a96e78172f494a8bb40a9befabca729e457a3040`; reader SHA256 is `92b4793e68045d2385de7eb750cacda323405eaeaf9e9040f2c5633fe0fc1a49`. The retained reference SHA256 is `8566a052e2fe0908188862c5ede2b98215664236198f6ab40a43193248016fb6`.
