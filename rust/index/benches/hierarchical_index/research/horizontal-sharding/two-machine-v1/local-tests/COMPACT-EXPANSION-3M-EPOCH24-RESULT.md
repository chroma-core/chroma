# Complete physical 3M construction with bounded compact navigation

The candidate completes construction, saving and fresh-reader validation on two physical machines. Both owners perform substantial insertion and balancing work. Final ownership is 1,490,472 and 1,509,528 vectors, giving the smaller owner 49.6824%. Keep the lossless expansion encoding and bounded complete-reply handling: the previous oversized-reply failure is resolved in this run, and distributed insertion uses substantially less CPU, traffic and elapsed time.

## Complete workload comparison

| Phase | Candidate24 seconds | Reference21 seconds | Difference seconds |
| --- | ---: | ---: | ---: |
| Insertion | 25.079 | 14.308 | +10.771 |
| Balancing | 52.389 | 31.874 | +20.516 |
| Redistribution | 6.286 | 0.000 | +6.286 |
| Saving | 33.305 | 17.089 | +16.216 |
| Construction and saving | 117.411 | 63.295 | +54.116 |

The complete candidate takes 117.411 seconds versus 63.295 reference, or 1.855 times as long. The candidate construction timer also includes small coordination work outside the displayed phase timers. Tree-shape inspection takes 1.606 seconds separately; fresh-reader validation takes 24.049 seconds separately. The worker supervisor completes in 160.475 seconds with exit zero. These timings are different scopes and must not be added twice.

Both runs use the retained 3M-vector, 768-dimensional L2 input, 32 workers per machine, 100K insertion intervals through 1M and 1M thereafter, matched construction search and split/merge settings. The reference uses one machine; the candidate uses two machines, each with 32 physical cores. The candidate starts on one owner and redistributes after the 2M balance using 2.16M capacity. Trees are independently constructed with scheduler/randomness variability. Candidate shape is [1,34,2110], reference [1,35,2123]. Both owners preload the corpus; this does not test cache-capacity crossing or scalable vector storage.

Saving does not have identical persistence contracts: the candidate synchronizes files/directories and retains the immutable vector input, while the reference writes full vectors using ordinary writes. The measured save gap is real for these implementations, but it is not isolated sharding overhead under equal durability.

## Final million additions

| Measurement | Candidate23 partial run | Candidate24 complete run | Reference21 |
| --- | ---: | ---: | ---: |
| Elapsed seconds | 25.719 | 16.292 | 7.375 |
| Summed process CPU-seconds | 741.78 | 439.86 | 206.18 |
| Scored candidate rows / reference distance comparisons | 1,082,859,653 | 1,054,710,021 | 1,124,527,033 |
| Application framed bytes | 59,966,776,825 | 36,833,706,065 | 0 |

Candidate24 reduces final-add elapsed time by 36.7%, CPU by 40.7%, and framed bytes by 38.6% relative to candidate23. It scores 2.60% fewer candidate rows, while remote validated child rows increase from the earlier run. Changed tree shape and placement prevent an isolated codec speedup claim. The fixed-tree tests establish exact representation and selection parity; this physical run establishes the combined candidate’s practical improvement. Candidate24 still takes 2.21 times reference insertion elapsed time and 2.13 times CPU.

Before the last additions, owners hold 1,411,209/588,791 vectors. They add 712,807/287,193 vectors, ending at 2,124,016/875,984 before balancing. Owner CPU is 275.45/164.41 seconds. Final balancing commits 506/209 splits and completes 5,789/2,919 jobs. Final redistribution produces the nearly even saved ownership above. This is physical distributed construction, not merely a final redistribution after single-owner construction.

## Remaining balance and save costs

The final balance takes 26.204 seconds and 934.76 summed CPU-seconds, versus 6.846 seconds and 191.66 CPU-seconds reference. Clustering uses 35.222 CPU-seconds versus 42.037 reference, with 715 committed splits versus 740 reference clustering calls. No candidate split preparations are discarded. CPU outside clustering is 899.538 versus 149.623 seconds. The remaining gap is therefore primarily outside the clustering kernel in this workload; those residuals include all other process work and are not one exclusive function.

Cross-machine reassignment and navigation are the next construction questions. Final balancing records 241,602 transfer protocol calls, 196,916 remote engine calls, 21,899 completed transfer groups and 54,591 proposed transfer rows. Same-owner operations already use the direct path. Inclusive transfer-driver CPU totals 415.231 seconds and inclusive navigation totals 343.584 seconds; the former includes navigation, so these cannot be summed. The balance exchanges 40.555 GB of framed application data. A source audit will distinguish repeated protocol bookkeeping, serialization, request frequency and navigation work before choosing a correction.

The transport records 61 failed connections during final balancing even though construction succeeds. Typed size rejections are counted by the transport as failures and can trigger bounded read-only subdivision. These counters are not proof of lost membership or 61 unexpected network outages. The worker stderr is empty; the successful exact validation and bounded-retry tests remain the relevant correctness evidence. Navigation deferral count is zero, so this physical run does not exercise the conditional scheduler-continuation mechanism.

Saving takes 33.305 seconds. Independent owner materialization takes 9.652 and 9.598 seconds, but owner calls run sequentially for 19.433 seconds. Remote object copying then takes 12.514 seconds for 193,673,743 bytes; local object reuse takes 0.264 seconds. Each owner performs about 1,058 file and directory synchronizations; those durations are nested within materialization. The next focused implementation experiment overlaps independent sealed-owner materialization with a bounded concurrency limit, preserving the barrier before validation/publication and all durability/failure guarantees. Remote-copy concurrency remains a separate candidate. The approximately 9.6-second overlap opportunity is a hypothesis, not a promised whole-build gain.

## Correctness, provenance and limits

The fresh reader validates all 3,000,000 unique memberships, the vector SHA256, all 1-bit posting codes and all 4-bit centroid codes. Writer objects are not required. Sixteen training-vector smoke queries execute; they do not establish held-out recall or quality equivalence. Existing local tests cover full-width identity and finite float-bit preservation, malformed payloads, exact reference selection, complete reply coverage, subdivision, fatal unrelated errors and one unchanged deadline. The applicable suite passes 236 tests with the same one known unrelated local-writer exclusion; an unfiltered clean suite is not claimed.

The corrected source archive is `source-epoch24b.tar.gz`, SHA256 `9e5f4d1ed698acaf22fddb29dc4ae50e336e48e9280f896bfc7eba48391869eb`. The Linux writer SHA256 is `d674f3c2dcdb0cd7900d9b051b13e322178254ba73f213a97bc340d83c3773f0`; the reader is `1aee244c4cb00564899937ac3a6b2c9a0908f85bd4e6b6033c0f099cda04cc13`. Both machines pass independent full-input and runtime checks. Reference21 remains the valid retained baseline. No reference rerun, automatic retry or profiler is used.

Both task instances are API-confirmed stopped. Worker cleanup passes; the launcher reports the retained peer-stop permission error and no peer exit receipt is collected. Peer process cleanup therefore remains unconfirmed separately from verified instance shutdown. Retain this operational failure even though the candidate itself succeeds.

The closed window adds $1.4137360649871826 compute once and no storage. Covered compute is $47.435721418861806683; identified recurring storage remains $19.52/month. The first-month covered bound is $66.955721418861806683, leaving $933.044278581138193317 under the $1,000 authorization. This is task accounting, not a whole-account bill.

Raw evidence is in [the result](../operations/window-epoch24/3m-distributed/result.json), [durable validation](../operations/window-epoch24/3m-distributed/durable-validation.json), [phase summary](../operations/window-epoch24/candidate-summary.json), [runtime bindings](../operations/runtime-binaries-epoch24.json), [closure](../operations/window-epoch24/closed.json) and [cleanup reconciliation](../operations/window-epoch24/cleanup-reconciled.json). The [review](EPOCH24-EXPANSION-TRANSPORT-REVIEW.md) binds the local correctness evidence. Inclusive CPU scopes overlap; process CPU is counted once per physical owner; application bytes exclude TCP headers and retransmissions; summed RSS peaks would not establish simultaneous cluster peak memory.
