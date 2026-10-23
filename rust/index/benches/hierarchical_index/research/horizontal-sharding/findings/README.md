# Benchmarks and findings

The [epoch33 transport and navigation report](../two-machine-v1/local-tests/EPOCH33-TRANSPORT-AND-NAVIGATION.md) records the current accepted implementation. Persistent connections and bounded pools pass integration validation. Three navigation-cache pairs reduce insertion time by 6.6%, insertion CPU by 7.6%, and balancing CPU by 6.3%; balancing and total-construction latency differences remain inconclusive. First and unchanged saves pass fresh-reader checks. The [accepted profile](../two-machine-v1/operations/development33/accepted-profile.json) binds the final executable.

The [epoch31 construction and saving report](../two-machine-v1/local-tests/EPOCH31-CONSTRUCTION-AND-SAVING.md) records the preceding construction and saving measurements. The [current plan](../CURRENT-PLAN.md) and [handoff](../NEW-CHAT-HANDOFF.md) identify active work and the accepted baseline.

Epoch31 accepts 64 insertion workers and per-boundary refresh coordination. Median 2M→3M construction falls from 37.512 to 29.234 seconds, with higher CPU demand. Bounded parallel posting writes reduce measured first saves from 131.141 to 7.996–11.072 seconds while unchanged saves retain full block reuse. Six fresh durable readers pass. The [accepted profile](../two-machine-v1/operations/development31/accepted-profile.json) and [completion receipt](../two-machine-v1/local-tests/EPOCH31-COMPLETION.json) bind the implementation and evidence.

Experiments 28 and 29 are complete. [Experiment 28](../two-machine-v1/local-tests/EPOCH28B-3M-RESULT.md) validates physical 3M construction and saving in 98.593 seconds versus 97.369 seconds for experiment 27; it reduces transfer protocol work but establishes no end-to-end speedup. [Experiment 29](../two-machine-v1/local-tests/EPOCH29-3M-RESULT.md) validates owner-direct saving and reuse of all 2,115 unchanged posting blocks. Its first S3 save takes 108.154 seconds and its unchanged save takes 2.929 seconds. Both independent readers pass all membership, vector-hash and code checks.

Both authorized machines remain running at Lyon's request, at $8.064/hour combined. Inspect `two-machine-v1/operations/window-epoch29c/start-intent.json` for the current guarded deadline. The recurring follow-up remains paused; the current manual research is authorized.

[Experiment 27](../two-machine-v1/local-tests/METADATA-DICTIONARY-3M-EPOCH27-RESULT.md) validates physical 3M construction in 97.369 seconds including saving, versus 109.605 for experiment 26 and 63.295 for the retained reference. Response-local metadata sharing reduces final-million insertion traffic from 34.624 to 7.206 GB and CPU from 448.44 to 342.12 seconds with similar candidate work. Balancing also does less work, so its full gain is not attributed to the codec.

The retained [epoch26 result](../two-machine-v1/local-tests/REPLY-VALIDATION-3M-EPOCH26-RESULT.md) validates physical3M construction but takes109.605 seconds including saving, versus104.710 previous and63.295 reference. Reply copies are removed; validation CPU per row improves during final balancing and worsens during final insertion. Preserve the overall slowdown and changed work. Both machines are API-stopped; peer process cleanup receipt is missing. The [fixed-transfer diagnostic](../two-machine-v1/local-tests/FIXED-TRANSFER-DIAGNOSTIC.md) separately confirms repeated small transactions and source compactions. Candidate27 measures metadata sharing; the active plan owns subsequent priorities.

The retained [epoch25 result](../two-machine-v1/local-tests/PARALLEL-OWNER-SAVE-3M-EPOCH25-RESULT.md) validates3M construction and saves in24.258seconds, versus33.305 with sequential owner calls. Complete construction/save is104.710seconds versus63.295reference. Keep bounded concurrent saving; candidate26 reports the subsequent reply-validation result. Both epoch25 cleanup receipts and API stops pass.

The retained [complete physical 3M result](../two-machine-v1/local-tests/COMPACT-EXPANSION-3M-EPOCH24-RESULT.md) validates both-owner construction and the fresh reader. Construction and saving take 117.411 seconds versus 63.295 reference; final ownership is 49.68%/50.32%. Keep bounded compact navigation and prioritize overlapping independent owner materialization plus diagnosis of final-balance overhead. Both instances are stopped. Earlier failed runs remain evidence, with their original limits.

The [final-balance cost assessment](../two-machine-v1/local-tests/EPOCH24-FINAL-BALANCE-COST-ASSESSMENT.md) ranks reply materialization/validation, tiny cross-owner transfer groups and completed transfer-history scans. It separates nested measured scopes from source hypotheses and specifies fixed-work diagnostics.

The [new 500K comparison](../two-machine-v1/local-tests/LOCAL-TRANSFER-PACKED-NAV-500K-RESULT.md) validates reference-style local transfers, local distance counts and packed child centroids. Construction plus saving falls from 68.566 to 30.002 seconds; the retained reference takes 12.601 seconds. Insertion is 1.340 versus 0.699 seconds, balancing 19.379 versus 9.054, and balance CPU 66.54 versus 52.70 seconds. Keep the three changes. Direct local navigation, saving and balance scheduling remain the next investigations. Both machines are stopped and no further run starts automatically.

This folder is the readable evidence index. Each topic records the question, setup, result, interpretation and limits, with links to retained data. Raw benchmark files remain in their existing locations; no measurement data is moved or deleted.

| Finding | What it establishes | Scope |
| --- | --- | --- |
| [Direct local navigation at 500K](../two-machine-v1/local-tests/DIRECT-LOCAL-NAV-500K-RESULT.md) | Removes local envelope copies and validation; construction/save falls 30.002→26.640 seconds, while later balance wall remains about 8.6 seconds. | One candidate versus retained reference and candidate; independent tree work differs. |
| [Scheduler readiness audit](../two-machine-v1/local-tests/SCHEDULER-PATH-AUDIT.md) | Identifies a claim-dependency mismatch and ranks remaining scheduling questions against retained CPU/wall evidence. | Source findings and hypotheses; no scheduler improvement measured. |
| [Save-path audit](../two-machine-v1/local-tests/SAVE-PATH-AUDIT.md) | Identifies duplicate local durable writes, save transport bytes and unequal persistence contracts. | Source audit plus epoch15 counters; dominant save cost remains unmeasured. |
| [Dynamic two-machine construction](../two-machine-v1/REPORT.md) | Physical protocol and durable-restore correctness pass; both 100K comparisons and the 3M reference validate. Owner-local 100K balancing has substantial excess elapsed and CPU cost. | Two actual machines, shared upper tree and dynamically moved subtrees; larger comparisons are deferred while minimal diagnostics investigate owner-local overhead. |
| [Three-million-vector attribution](../single-shard-3m-attribution-v1/REPORT.md) | Four valid builds; candidate construction/save averages 3.14% longer, with most elapsed gap in initial balancing, measurable node-handle CPU work and extra balancing navigation. | FineWeb3M, 32 workers, two runs per arm, three tree levels; causal limits remain and no query-quality parity is established. |
| [One-owner redesign comparison](../single-shard-redesign-v1/REPORT.md) | All four saved-state checks pass; candidate timings vary around a higher mean, with extra navigation work and transient memory. | Fresh FineWeb100K, 32 workers, two runs per arm, one machine; no internal balancing or quality parity claim. |
| [Architecture and implementation](architecture-and-implementation.md) | Identifies reusable components and missing connections to the intended design. | Retained source investigations; not a new full code audit. |
| [Construction locks](construction-locks.md) | Explains the current locks and how each differs from the unsharded writer. | Source inspection of the current sharded path and retained optimized reference; no new benchmark. |
| [Construction costs](construction-costs.md) | Records the 100K construction sequence, gains, adverse results and reference comparison. | One machine, one local owner, 32 configured workers. |
| [Finer balancing jobs](finer-balancing-scheduler.md) | Records the benefit of dynamically assigning individual leaf jobs and the requirements adopted for sharded scheduling. | Unsharded saved 8M-to-9M Wikipedia replay, two matched pairs; fresh 1M shows no established gain. |
| [Cache and vector access](cache-and-vector-access.md) | Distinguishes reader/decode reuse, cache capacity, storage correctness and routing payload costs. | Mechanism fixtures and scoped 100K measurements. |
| [Quality and query cost](quality-and-query-cost.md) | Records the three-arm 16K quality and warm query study. | Reference and one/four local owners; not physical distribution. |
| [Physical fixed-partition experiments](physical-fixed-partition.md) | Retains useful one/two-host construction, query and append evidence. | A fixed-boundary engine; not the complete dynamic target. |
| [Prepared experiments](prepared-experiments.md) | Distinguishes reviewed code, packaged tests and unmeasured performance ideas. | Execution remains paused. |
| [Scaling and evidence limits](scaling-and-evidence-limits.md) | Identifies resource arithmetic, open dependencies and limits of inference. | Conditional 100B model; no demonstrated 100B feasibility. |
| [Evidence locations](evidence-locations.md) | Maps detailed reports, raw data, reviews and other research folders. | Complete directory entry points, with source-specific interpretation retained. |

Read the [design](../DESIGN.md) for what we want to build and the [plan](../CURRENT-PLAN.md) for future work. A finding records evidence; it does not authorize execution or select work independently of the plan.

## Recording a result

Use the [finding template](TEMPLATE.md) for a new question, or add a clearly labeled result to an existing topic. Include input and source identities, tree shape, working set/cache conditions, work counts, timing boundaries, correctness outcomes and links to raw receipts. State whether the result is measured, a source-based hypothesis or arithmetic. Record failures and qualifications as prominently as improvements.

After every experiment, write the interpretation and decision, then reprioritize the plan. Mark an older finding as superseded only for the affected claim; keep its evidence and failed attempts. Reuse a receipt only for the source version and conditions it actually tests.

The [reservation-history result](../two-machine-v1/local-tests/RESERVATION-HISTORY-RESULT.md) establishes removal of completed-history scans with deterministic work counts. Its corrected 100K build validates, but different tree/work counts prevent attributing the entire observed balance-time reduction to the fix. The remaining owner-local overhead needs CPU attribution.

The [initial-owner profile](../two-machine-v1/local-tests/OWNER-LOCAL-PROFILE-RESULT.md) and [independent review](../two-machine-v1/local-tests/OWNER-LOCAL-PROFILE-RESULT-REVIEW.md) put clustering work first in the investigation queue. Roughly 90% of the early sample lands in the distance and split routines. The sample does not cover the entire balance or measure blocked time. Both implementations use the same clustering kernel, so differences in work require examination before claiming a missing kernel optimization.


## Complete balance attribution at 100K

The [complete-balance 100K pair](../two-machine-v1/local-tests/FULL-BALANCE-OBSERVABILITY-RESULT.md) measures 324 prepared splits in the distributed implementation, of which 252 are discarded, versus 69 clustering calls and 69 committed splits in the reference. Extra clustering accounts for 15.698 of 25.780 additional CPU-seconds. Balance wall times are 14.055 versus 12.965 seconds; the reference has a long initial serial round, so the earlier threefold elapsed gap does not reproduce. Both saved indexes validate, both profiles cover the entire timed phase with zero lost samples, and both machines are API-stopped. Preventing repeated split preparation is the next priority; repeated posting compaction and local navigation copies follow. Instrumentation, different random construction work and overlapping scopes limit causal wall-time attribution.

- [Producer-ready scheduling and save attribution at500K](../two-machine-v1/local-tests/READINESS-SAVE-500K-RESULT.md) records a correct but slower run, isolates transfer bookkeeping waits, and measures duplicate local save copying.

- [Queue lock ordering and local saved-object reuse at 500K](../two-machine-v1/local-tests/QUEUE-SAVE-500K-RESULT.md) records the near-elimination of transfer bookkeeping waits, faster later balancing, zero duplicate local saved-object writes, and increased CPU/retry work.

- [Compatible pending reassignment combination at 500K](../two-machine-v1/local-tests/COALESCING-500K-RESULT.md) records fixed-work parity, fewer rounds and query copies, first-interval variability, and increased non-clustering CPU.

- [Compact saved-object transport at 500K](../two-machine-v1/local-tests/SAVED-CHUNK-500K-RESULT.md) records exact-byte validation, lower framed traffic and copy latency, unchanged durability, and storage-timing limits on attribution.

- [Epoch21: 3M construction fails after physical redistribution](../two-machine-v1/local-tests/TWO-OWNER-3M-EPOCH21-RESULT.md). The current reference validates; candidate insertion on both owners exposes 58.846 GB traffic, followed by a navigation retry failure.

- [Epoch21 distributed insertion attribution](../two-machine-v1/local-tests/EPOCH21-DISTRIBUTED-INSERTION-ANALYSIS.md). Fewer comparisons accompany more CPU and traffic; repeated scored-child metadata and extra ordering work become the next optimization candidates.

- [Epoch22: local-route retries still abort 3M construction](../two-machine-v1/local-tests/NAVIGATION-3M-EPOCH22-RESULT.md). The shared-centroid repair passes controlled tests; new diagnostics identify a distinct pre-handoff stale-local-image failure.

- [Local candidate-assembly mechanism](../two-machine-v1/local-tests/EPOCH23-CANDIDATE-ASSEMBLY.md). Exact64-query remote/local parity removes extra ordering comparisons; no benchmark latency claim yet.
- [Remote candidate-pruning assessment](../two-machine-v1/local-tests/REMOTE-CANDIDATE-PRUNING-ASSESSMENT.md). A useful top-K proof needs a fixed total order and a new validated reply contract; current unstable ties prevent an unchanged exact-parity claim.

- [Lossless expansion encoding assessment](../two-machine-v1/local-tests/EXPANSION-CODEC-ASSESSMENT.md) identifies a bounded representation for complete navigation payloads; byte arithmetic is unmeasured until validation and a matched run.

- [Epoch23: candidate assembly and oversized-reply failure](../two-machine-v1/local-tests/CONTINUATION-ASSEMBLY-3M-EPOCH23-RESULT.md). Completed distributed insertion uses less CPU and elapsed time; final balancing fails, and no complete candidate build is claimed.

## Completed checkpoint-based comparisons

The [baseline and navigation report](../two-machine-v1/local-tests/EPOCH30-BASELINE-AND-NAVIGATION.md) records even placement, exact writer restoration, repeated final-million timings and both-owner profiles. The [transport comparison](../two-machine-v1/local-tests/EPOCH30-TRANSPORT-COMPARISON.md) reduces repeated shared-view transmission. Both candidates reduce CPU work without improving end-to-end time; retain the baseline. The [current plan](../CURRENT-PLAN.md) prioritizes critical-job and dependency-wait attribution.
