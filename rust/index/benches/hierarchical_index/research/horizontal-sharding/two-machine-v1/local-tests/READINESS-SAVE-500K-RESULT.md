# Producer-aware scheduling and save attribution at 500K

The scheduler correction preserves exact membership and prevents the demonstrated premature retry, but this run does not improve balancing latency. Keep it for the verified readiness behavior; do not claim a performance win. The new measurements locate most transfer waiting in admission and terminal bookkeeping, and identify local duplicate saved-object copying as a separate removable cost. These are the next targets.

## Comparable workload and limits

This run uses the retained 500,000 vectors in the same order, 768-dimensional L2 distance, 32 workers, five 100K insertion intervals and the same construction settings as epochs14 and16. Both physical machines have 32 physical cores. All timed construction remains on the initial owner, followed by final redistribution. The vector working set is 1.536GB and fits the configured 12GiB cache; this does not test cache-capacity crossing or distributed construction throughput. The final tree has root/inner/leaf counts 1/5/349, compared with 1/4/340 in epoch16 and 1/5/354 in the reference. Randomized tree construction and scheduling are not fixed across runs. The reference is retained rather than rerun.

The code carries the producing scheduled attempt through synchronous operations and uses it to defer retries. New coarse transfer and save timers also add observation overhead. Saving behavior is unchanged. The source closure contains ten reviewed files; [validation](READINESS-SAVE-VALIDATION.json) records 192 passing applicable native tests, three successful CLI builds and the one unchanged unrelated exclusion. Both failed fixture attempts and the scheduler's expected red regression remain retained. [Independent review](EPOCH17-READINESS-SAVE-REVIEW.md) covers the correction and measurement meanings.

## Measured comparison

| Measurement | Reference epoch14 | Candidate epoch16 | Candidate epoch17 |
| --- | ---: | ---: | ---: |
| Insertion wall seconds | 0.699 | 1.014 | 1.037 |
| Balancing wall seconds | 9.054 | 16.167 | 21.067 |
| Balancing process CPU seconds | 52.700 | 54.300 | 57.130 |
| Clustering CPU seconds | 35.296 | 28.778 | 32.360 |
| Other balance CPU seconds | 17.404 | 25.522 | 24.770 |
| Save wall seconds | 2.840 | 7.781 | 7.413 |
| Construction plus save wall seconds | 12.601 | 26.640 | 30.986 |

Balancing is 30.3% slower in this observation. Its first interval rises from 7.542 to 12.000 seconds; clustering comparisons rise from 78.20M to 91.46M overall and clustering CPU rises 12.4%. Those work changes prevent assigning the elapsed increase entirely to the scheduler. The subsequent four intervals are a clearer remaining bottleneck: they use 34.66 CPU-seconds versus 34.77 previously, but take 9.067 wall-seconds versus 8.626. The reference tail takes 2.314 wall-seconds and 33.44 CPU-seconds. Average occupied cores in the candidate tail are about 3.82 versus 14.45 reference.

Repeated reassignment work falls, without an elapsed benefit. Reassignment rounds fall from 5,241 to 4,737; rejected destination groups from 10,077 to 7,353; local transfer batches from 4,866 to 4,428; and reassignment query copies from 1.657GB to 1.446GB. Independent trees also change these counts. All 348 prepared leaf splits commit, with zero discarded preparations. The queue records 4,149,664 visits withheld by active producers. These are repeated queue visits, not unique jobs, time blocked or proof of worker utilization.

## Where transfer waiting occurs

| Inclusive scope | Sum of worker wall seconds | Sum of thread CPU seconds |
| --- | ---: | ---: |
| LocalTransfer | 190.866 | 2.786 |
| LocalTransferAdmission | 106.931 | 0.120 |
| LocalTransferPreparation | 2.225 | 2.204 |
| LocalTransferCommit | 0.179 | 0.179 |
| LocalTransferCleanup | 81.332 | 0.088 |
| LocalTransferTerminal | 81.301 | 0.081 |

These intervals overlap across workers; their sums are not elapsed phase costs. Cleanup contains terminalization. Admission plus terminalization account for nearly all the outer transfer waiting, while actual preparation and commit remain close to their CPU time. Source inspection shows that both admission and terminalization acquire the owner's operation mutex. Enqueue handlers hold that same mutex while acquiring the scheduler queue mutex. This gives a concrete lock-convoy hypothesis: queue contention can delay unrelated transfer bookkeeping. It does not yet prove which holder causes the measured wait. The next controlled test must hold the scheduler queue and show whether unrelated admission/completion can proceed, while preserving atomic phase close and queue admission.

## Saving costs

Saving takes 7.413 seconds. Owner calls take 2.582 seconds, local copies 1.582, remote copies 3.047, final manifest work 0.104 and publication 0.090. These coarse phases omit some setup and manifest work and do not form an exact partition. File/directory synchronization measurements are nested inside them. The local owner writes 179 objects totaling 32,333,242 bytes; the coordinator then copies those same 179 objects and bytes. The remote owner contributes 176 copied objects totaling 31,811,188 bytes. The coordinator records 359 file and 359 directory synchronization attempts, taking 2.714 seconds combined inside saving.

The local duplicate copy alone occupies 21.3% of the observed save interval. Save the local owner directly into the combined immutable store, verify its manifest and objects, and reuse those references. Keep remote copying, checksums, file/directory synchronization, fresh-reader checks and publication-last ordering. The tiny fixture must show fewer duplicate writes and preserve replay, corruption rejection and previous-generation readability before another fixed-workload measurement. Remote copy representation and packing remain separate candidates.

Reference saving is 2.840 seconds but has a different contract: its measured local backend uses ordinary writes without matching fsync barriers, and it writes full vectors while the candidate references retained immutable input. Neither this ratio nor a future local-reuse speedup establishes equal durability performance.

## Correctness, resources and decision

Final owner memberships are 251,978 and 248,022, so both machines hold about half the index. The fresh reader verifies every membership exactly once, all posting and centroid codes and the full-vector digest without writer memory. The sixteen reused training queries are smoke checks, not held-out recall evidence. Candidate peak process RSS is 2,798,878,720 bytes.

Keep exact producer readiness for its controlled correctness behavior. Preserve the adverse timing and prioritize the operation/scheduler lock interaction, followed by direct reuse of local saved objects. Both can be tested independently at their phase boundaries; do not present their eventual combined total as an isolated speedup for either change. Coalescing deferred work, owner-affinity scheduling and remote save transport remain subsequent hypotheses.

Both approved instances are API-confirmed stopped. The known peer-stop helper PermissionError remains in the launcher evidence; matching independent supervisor cleanup receipts and API states reconcile shutdown. This window adds $0.8265974235534668 compute upper bound and no new storage. It is closed and cannot be reused. Raw results, build/runtime identities, profiles and closure are retained in `../operations/window-epoch17/`; the source archive SHA256 is `0260f7eccf81b9fe6d144beee0d229a3fa88f7cae60fc7cfac8f64726bcf4a64`.
