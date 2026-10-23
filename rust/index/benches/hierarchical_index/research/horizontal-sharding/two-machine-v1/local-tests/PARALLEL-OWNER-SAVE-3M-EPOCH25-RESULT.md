# Concurrent sealed-owner saving on the physical 3M workload

Concurrent owner materialization reduces saving from 33.305 to 24.258 seconds, a 27.2% reduction, while all 3M memberships, full-vector digest, posting codes and centroid codes validate in a fresh independent reader. Keep the change. The two owner calls total 19.737 seconds but overlap into a joined phase of 10.021 seconds. This directly exercises the intended mechanism on separate machines.

| Measurement | Candidate24 sequential owner saving | Candidate25 concurrent owner saving | Reference21 |
| --- | ---: | ---: | ---: |
| Insertion, seconds | 25.079 | 24.541 | 14.308 |
| Balancing, seconds | 52.389 | 48.967 | 31.874 |
| Redistribution, seconds | 6.286 | 6.597 | 0 |
| Saving, seconds | 33.305 | 24.258 | 17.089 |
| Construction and saving, seconds | 117.411 | 104.710 | 63.295 |
| Sum of owner-save call durations, seconds | 19.433 | 19.737 | Not applicable |
| Joined owner-materialization phase, seconds | Not separately measured; calls are sequential | 10.021 | Not applicable |
| Remote object copy, seconds | 12.514 | 12.882 | Not applicable |

Construction and saving take 104.710 seconds, 1.654 times reference. The code change affects saving only. Construction varies by 3.655 seconds between independently built trees, so the full 12.701-second reduction is not attributed entirely to concurrent saving. Coarse construction timers include small coordination work outside the listed phases. Shape inspection takes 1.738 seconds and fresh-reader validation 24.021 seconds outside construction/save. The worker supervisor takes 147.959 seconds and exits zero.

Owner materialization takes 9.913 and 9.629 seconds individually. Their summed call duration is nested/overlapping and must not be added to the joined 10.021-second phase. The configured concurrency cap is two; it is not a measurement of busy CPU cores. Remote copying remains serial: 1,069 objects totaling 194,041,879 bytes take 12.882 seconds. Local reuse verifies 1,070 objects totaling 191,328,326 bytes in 0.257 seconds. File/directory synchronization stays enabled and its durations are nested inside these measurements. Concurrent snapshots may raise memory demand; this run does not isolate that increase.

The protocol seals all owners before starting bounded batches of independent immutable save requests. Every started call is joined, successful owner acknowledgments and identities are checked, and the existing serial copy/payload-validation path runs before combined publication. Deterministic tests verify overlap, no early copying/publication, exact replay and preserving the previous readable generation when one owner fails. No durability guarantee, owner namespace or generation identity changes.

Both machines perform real construction. The final million additions place 753,648 new vectors on owner1 and 246,352 on owner2. Final balancing commits 557/186 splits. Final redistribution leaves 1,488,355/1,511,645 vectors, or 49.6118% on the smaller owner. The tree shape is [1,36,2133], versus candidate24 [1,34,2110] and reference [1,35,2123]. The final add/balance intervals take 15.776/22.456 seconds. Those construction differences are retained as workload variability, not a save-path speedup.

All arms use the retained 3M×768 L2 input, 32 workers per machine, 100K insertion intervals through 1M and 1M thereafter, full-precision construction navigation, beam10–16/tau1.5, branching100, split/merge thresholds2048/512 and32 reassignment neighbors. Neither split-kernel seeds nor thread interleavings are controlled. The candidate starts on one owner, redistributes after2M with capacity2.16M, and uses the same hardware and input as candidate24. Each owner preloads the corpus with a12GiB vector-cache limit; this is not cache-capacity crossing or scalable vector placement evidence. Saving contracts differ from reference: candidate file/directory synchronization and retained input differ from reference ordinary writes and full-vector persistence. Sixteen training-vector smoke queries are not held-out recall evidence.

The next construction experiment removes repeated reply-validation scans and complete reply clones. Its fixed-tree work counts and exact reference-output tests pass, but small fixture timing does not establish a speedup. Measure the same physical workload against candidate25. Keep remote-copy concurrency separate so the next comparison isolates the navigation implementation change; do not start a protocol redesign merely from total CPU residuals.

Source25b SHA256 is `fe3fd6d0b0ebe475f50a7e4fac1c0263521dddf1cbba002997bb520b3d2a639c`. The Linux writer is `5b3fbbb29396fe7c9c369762ee0996da1f97f98f0b9bf621e59383413d54b74e` and reader `ca50b0b8fcf473c8f9336030238fd7d42cc43def62b36135656789ad4d55146c`. Nine save-runtime tests, the existing failed-owner fresh-process regression and all three native builds pass; one focused independent review accepts the change. No unrelated whole-suite claim is made. Both hosts independently pass runtime and full-input checks. One candidate run is performed without profiling, reference rerun, sweep or automatic retry.

Both instances are API-confirmed stopped and both matching supervisor cleanup receipts are collected. The worker exits zero; the peer reports operator stop with signal15. The launcher still exits1 because its helper encounters the retained permission error. Preserve that helper failure separately from the successful candidate, confirmed process cleanup and API shutdown.

The window adds $1.5120071969604492 compute once and no storage. Covered compute is $48.947728615822255883, recurring storage remains $19.52/month and the first-month covered bound is $68.467728615822255883, leaving $931.532271384177744117 under the $1,000 authorization. This is covered task accounting, not a whole-account invoice.

Evidence: [raw result](../operations/window-epoch25/3m-distributed/result.json), [phase summary](../operations/window-epoch25/candidate-summary.json), [durable validation](../operations/window-epoch25/3m-distributed/durable-validation.json), [runtime bindings](../operations/runtime-binaries-epoch25.json), [closure](../operations/window-epoch25/closed.json), [cleanup reconciliation](../operations/window-epoch25/cleanup-reconciled.json), [local validation](OWNER-SAVE-CONCURRENCY-VALIDATION.json) and [review](EPOCH25-PARALLEL-OWNER-SAVE-REVIEW.md).
