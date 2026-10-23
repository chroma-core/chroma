# Sharing navigation metadata reduces physical traffic and insertion cost

Keep the response-local metadata dictionary. The physical 3M run validates all saved memberships and codes and finishes construction plus saving in 97.369 seconds, versus 109.605 for candidate26 and 63.295 for the reference. The clearest evidence is the final million additions: similar candidate work accompanies 79.2% less framed traffic, 23.7% less CPU and 26.8% less elapsed time. Balancing improves too, but performs less work, so its full gain cannot be assigned to the codec.

| Measurement | Candidate26 complete rows | Candidate27 shared metadata | Reference21 |
| --- | ---: | ---: | ---: |
| Insertion, seconds | 25.399 | 21.208 | 14.308 |
| Balancing, seconds | 52.732 | 45.173 | 31.874 |
| Redistribution, seconds | 6.726 | 6.628 | 0 |
| Saving, seconds | 24.395 | 24.000 | 17.089 |
| Construction and saving, seconds | 109.605 | 97.369 | 63.295 |
| Insertion CPU, summed across owners, seconds | 665.65 | 568.19 | 377.21 |
| Balance CPU, summed across owners, seconds | 1385.08 | 1108.51 | 501.37 |
| Balance clustering CPU, seconds | 144.781 | 142.123 | 156.523 |

Construction plus saving is 11.2% lower than candidate26 and 7.0% lower than candidate25's 104.710 seconds. It remains 1.538 times reference. The candidate uses two 32-physical-core machines while the reference uses one; this is not resource-normalized parity. Coarse phase timers omit small coordination work included in the overall construction timer.

Each response now sends an identical ordered child-metadata run once, then retains a complete score array for every query. Dictionary entries match every identity, placement, revision, ordinal and leaf flag; matching a revision or hash alone is insufficient. Every finite score bit and row order is preserved. The receiver reconstructs the same complete replies before existing validation and global selection. The implementation has no cross-request cache, pruning or precision change.

The fixed 64-query/two-parent/32-child test measures 413,199→62,266 framed bytes, about 6.64 times fewer bytes for the complete payload. Native CPU timing is unavailable and no debug latency gain is claimed. Physical insertion provides the more representative evidence below.

| Final million additions | Candidate26 | Candidate27 | Change |
| --- | ---: | ---: | ---: |
| Elapsed seconds | 16.638 | 12.183 | −26.8% |
| Summed process CPU seconds | 448.44 | 342.12 | −23.7% |
| Framed application traffic, GB | 34.624 | 7.206 | −79.2% |
| Navigation candidate rows, billions | 1.111 | 1.105 | −0.6% |
| Remote child rows validated, millions | 321.993 | 346.217 | +7.5% |
| Navigation calls | 15,648 | 15,648 | Same |
| Remote engine calls | 16,385 | 16,413 | +0.2% |
| Inclusive backend CPU seconds | 67.564 | 29.671 | −56.1% |
| Owned scoring CPU seconds | 126.805 | 128.715 | +1.5% |

Both insertion intervals contain one million query rows. The lower traffic and CPU persist despite more validated remote rows; candidate counts and scoring CPU are close. This supports the codec mechanism more strongly than an equal-size comparison alone. The inclusive backend scope includes transport and response handling; it does not isolate encoding from decoding or network waits. Do not add overlapping scope durations to obtain total CPU or elapsed time.

Final balancing takes 20.334 seconds and 688.61 CPU-seconds, versus 28.723 and 968.84. Traffic falls 41.151→9.962 GB. Candidate rows fall 920.440→805.349 million, validated remote rows 363.060→303.265 million, splits 747→724 and prepared rows 1,818,011→1,753,231. Transfer protocol calls fall 245,267→201,637 and remote engine calls 199,659→169,431. These workload changes explain part of the difference and prevent a causal percentage claim for the full balance improvement. Clustering CPU is 34.154→33.323 seconds. Final-balance traffic counters include 69 failed connections; the size-subdivision path also increments that counter, so this is not evidence of 69 network outages. Insertion records zero failed connections.

Saving remains approximately 24 seconds because this experiment changes no save code. Independent owner calls totaling 19.790 seconds overlap into 9.981 seconds elapsed. Serial remote copying takes 12.667 seconds for 191,244,394 bytes across 1,064 objects. Local immutable reuse takes 0.261 seconds. File/directory synchronization remains enabled; its times are nested inside these phases. Save contracts still differ from reference, which uses ordinary writes and persists full vectors while the candidate retains its immutable input.

Both machines perform construction during the final million additions and balancing. Final ownership is 1,511,280/1,488,720, or 49.624% on the smaller owner. The tree has [1,37,2127] nodes by depth, versus candidate26 [1,33,2123] and reference [1,35,2123]. The fresh reader verifies all 3M memberships, full-vector digest, one-bit posting codes and four-bit saved centroid codes after writer retirement. Reader validation takes 23.934 seconds outside construction/save. The worker supervisor exits zero after 140.229 seconds.

The workload remains 3M×768 L2 vectors, 32 workers per machine, 100K insertion intervals through 1M and 1M thereafter, full-precision construction navigation, beam 10–16/tau1.5, branching 100, split/merge thresholds 2048/512 and 32 reassignment neighbors. Construction starts on one owner and redistributes after 2M with capacity 2.16M. Input order and settings match; random split initialization and thread schedules are uncontrolled. Each owner preloads the full corpus with a 12GiB vector-cache limit, so this is not cache-capacity crossing or scalable full-vector placement evidence. Sixteen training-vector smoke queries do not establish held-out recall.

Twenty-seven routing/codec tests include the four dictionary tests; eight bounded-expansion tests run separately. All three native CLIs build. Tests cover changed metadata under the same revision, hash collisions, exact full-width values and score bits, malformed dictionary references/counts, aggregate decoded-memory limits, incompatible wire versions over TCP, complete bounded subdivision and the original deadline. One focused independent review accepts the coherent change. The full archive has 2,307 verified files, with five changed and two new files versus26 and no omissions. Both staged indexes remain unchanged.

Source27 SHA256 is `10d628b01cf040fe2be1782f79860f5272deed435272584be258d0f5e2a42059`. The Linux writer is `73e6626f4203c61b27d978d5aee63e58006761e8c3a43e16dc6957f67b4df611` and reader is `fa4041491b2c45c236c93730a8ddfc549c62fa8bb68986cbc1063f25b12a24d2`. Both hosts pass independent input/runtime checks. One candidate runs without a profiler, reference rerun, retry or sweep.

The next priority is batching one source's compatible remote destinations into one two-owner transaction. Candidate27 still proposes 46,634 transfer rows across 18,738 groups, about 2.49 rows per group, and performs 201,637 protocol calls in final balancing. The fixed diagnostic establishes redundant calls and repeated source scans. The implementation must retain per-target validation, source claims, immutable replay identity and every required acknowledgement. Its changes remain outside frozen27 and require separate failure tests and review before benchmarking.

Both task instances are API-confirmed stopped. Worker cleanup is confirmed; the peer exit receipt is missing. The launcher exits 1 after the retained stop-helper permission error. API shutdown and validated results do not turn that missing process receipt into confirmed cleanup. No further cloud workload is admitted at this checkpoint.

The window adds $1.388759104537964 compute once and no storage. Covered compute is $51.720880146583974683; recurring storage remains $19.52/month. The first-month covered bound is $71.240880146583974683, leaving $928.759119853416025317 under the $1,000 authorization. This is covered task accounting, not a whole-account invoice.

Evidence: [raw result](../operations/window-epoch27/3m-distributed/result.json), [phase summary](../operations/window-epoch27/candidate-summary.json), [durable validation](../operations/window-epoch27/3m-distributed/durable-validation.json), [runtime bindings](../operations/runtime-binaries-epoch27.json), [closure](../operations/window-epoch27/closed.json), [cleanup reconciliation](../operations/window-epoch27/cleanup-reconciled.json), [local validation](EXPANSION-METADATA-REUSE-VALIDATION.json), [review](EPOCH27-METADATA-DICTIONARY-REVIEW.md), [fixed-transfer diagnostic](FIXED-TRANSFER-DIAGNOSTIC.md) and [next protocol design](OWNER-BATCHED-TRANSFER-PLAN.md).
