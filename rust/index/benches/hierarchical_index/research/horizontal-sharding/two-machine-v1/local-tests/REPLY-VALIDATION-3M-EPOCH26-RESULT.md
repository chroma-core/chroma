# One-pass reply validation on the physical 3M workload

The candidate removes complete reply copies and preserves correctness, but does not improve total elapsed time in this run. Construction plus saving takes 109.605 seconds, versus 104.710 for the previous candidate and 63.295 for the reference. Keep the simpler one-pass coverage validation and borrowed reply comparisons for their demonstrated work reduction; preserve the adverse elapsed result and do not label it an end-to-end speedup.

| Measurement | Candidate25 | Candidate26 | Reference21 |
| --- | ---: | ---: | ---: |
| Insertion, seconds | 24.541 | 25.399 | 14.308 |
| Balancing, seconds | 48.967 | 52.732 | 31.874 |
| Redistribution, seconds | 6.597 | 6.726 | 0 |
| Saving, seconds | 24.258 | 24.395 | 17.089 |
| Construction and saving, seconds | 104.710 | 109.605 | 63.295 |
| Insertion CPU, summed across owners, seconds | 651.23 | 665.65 | 377.21 |
| Balance CPU, summed across owners, seconds | 1288.46 | 1385.08 | 501.37 |
| Balance clustering CPU, seconds | 154.571 | 144.781 | 156.523 |

The change validates each reply's child coverage in one pass and borrows immutable duplicate-reply bodies instead of cloning them. It preserves identity checks before duplicate suppression, complete parent coverage, ordinal uniqueness, finite scores, revision consistency, canonical candidate order and global beam selection. Duplicate equality still examines rows; the coverage-visit counter does not count that separate work. Zero reply-clone bytes means those validation copies are gone, not that navigation allocates or copies nothing.

The final distributed insertion interval has mixed evidence. Reply-validation CPU increases from 16.291 to 19.963 seconds while validated child rows increase from 278.612 to 321.993 million. CPU per validated row is approximately 6% higher. Insertion elapsed time increases from 15.776 to 16.638 seconds. Logical reply clones fall from 22.591 GB to zero. The interval processes one million queries in 15,648 navigation calls in both candidates, but candidate rows increase from 1.076 to 1.111 billion. This does not establish an insertion speedup.

The final balance interval reduces validation CPU despite more validated rows. Validation CPU falls from 45.588 to 37.871 seconds while validated rows rise from 337.000 to 363.060 million, approximately 23% less CPU per validated row. Logical reply clones fall from 27.389 GB to zero. These are descriptive measurements across independently constructed trees, not a fixed-work causal estimate.

Total final-balance time rises from 22.456 to 28.723 seconds and CPU rises from 868.72 to 968.84 seconds. Protocol calls increase from 196,168 to 245,267 and remote engine calls from 165,760 to 199,659. Committed splits are 743 versus 747, prepared rows 1,806,016 versus 1,818,011, and navigation candidate rows 914.938 versus 920.440 million. Changed placement, scheduling and transfer work confound the overall comparison; they neither erase the adverse result nor prove its sole cause. Inclusive navigation scopes overlap and must not be added together as exclusive costs.

Concurrent saving retains its observed behavior. Owner calls total 20.259 seconds and overlap into 10.238 seconds elapsed with a concurrency cap of two. Serial remote copying takes 12.798 seconds for approximately 193.691 MB across 1,078 objects. Saving varies by 0.137 seconds from candidate25. This experiment does not change saving.

All 3M memberships, full-vector digest, one-bit posting codes and four-bit saved centroid codes validate in a fresh reader after writer retirement. Final ownership is 1,492,163/1,507,837, so each physical machine owns approximately half the vectors. The tree has [1,33,2123] nodes by depth versus [1,36,2133] in candidate25. Both owners perform real construction during the final million additions and balancing. Fresh-reader validation takes 23.621 seconds outside construction/save; the worker supervisor exits zero after 152.178 seconds.

The fixed native mechanism test preserves exact reference selection and reduces coverage visits from 1,750,016 to 103,424 and logical reply-clone bytes from 8,421,376 to zero. Its first measured debug interval changes from 0.469 to 0.488 seconds, an adverse result retained in the local receipt. Twenty-three routing/codec tests, eight bounded-expansion tests and all three native CLIs pass; independent review accepts the two-file production change. There is no unrelated whole-suite claim.

All physical arms use the retained 3M×768 L2 input, 32 workers per machine, 100K insertion intervals through 1M and 1M thereafter, full-precision construction navigation, beam10–16/tau1.5, branching100, split/merge thresholds2048/512 and32 reassignment neighbors. The candidate starts on one owner and redistributes after2M with capacity2.16M. Candidate construction uses two32-physical-core machines; reference uses one. Split seeds and thread schedules are uncontrolled. Each candidate owner preloads the full corpus with a12GiB vector cache, so this does not test cache-capacity crossing or scalable full-vector placement. Candidate durable file/directory synchronization and retained input differ from reference ordinary writes and full-vector persistence. Sixteen training-vector smoke queries do not establish held-out recall.

Final-million insertion sends and receives 34.624 GB of framed application data, versus 30.581 GB in candidate25. Final balancing transfers 41.151 GB versus 38.827 GB. These counters include request/reply envelopes and failed or partial frames, observed once per connection; they exclude TCP/IP headers and retransmissions. More remote rows accompany the higher traffic. Candidate26 changes no wire format.

The next candidate shares identical ordered child metadata within each remote response while retaining every query's full score array. Repeated metadata is a concrete byte-reduction opportunity; its CPU and latency benefit remain hypotheses. The separate fixed-transfer diagnostic confirms excessive small protocol groups and repeated source compactions without navigation. Keep transfer batching as the next algorithmic candidate, with per-target correctness and replay barriers preserved.

Source26 SHA256 is `e80f8b914a4d35ca02217d12e661169d8d1cc2c4f3d79b9c0b24e82c720d34c1`. The Linux writer is `dd8e8944892abe9f5c6224346d951dd8ee000490bf7c6f716d493b18f095e14d` and reader is `5bd5a8dad5fef8bf2426a9ce1bb303872b3f949b6c1b867e9afe77daf47ef670`. One candidate runs with no profiler, reference rerun, sweep or retry. Both hosts pass independent input/runtime checks.

Both instances are API-confirmed stopped. The worker exits zero and its cleanup receipt passes. The peer process exit receipt is missing; API shutdown is separate evidence, not proof that this receipt was collected. The launcher exits1 after its stop helper encounters the retained permission error. Preserve this operational failure separately from the validated benchmark.

The closed window adds $1.3843924262237548 compute once and no storage. Covered compute is $50.332121042046010683, recurring storage remains $19.52/month, and the first-month covered bound is $69.852121042046010683, leaving $930.147878957953989317 under the $1,000 allowance. This accounts for covered task resources, not a whole-account invoice.

Evidence: [raw result](../operations/window-epoch26/3m-distributed/result.json), [phase summary](../operations/window-epoch26/candidate-summary.json), [durable validation](../operations/window-epoch26/3m-distributed/durable-validation.json), [runtime bindings](../operations/runtime-binaries-epoch26.json), [closure](../operations/window-epoch26/closed.json), [cleanup reconciliation](../operations/window-epoch26/cleanup-reconciled.json), [local validation](EPOCH26-REPLY-VALIDATION.json), [review](EPOCH26-REPLY-VALIDATION-REVIEW.md) and [fixed-transfer diagnostic](FIXED-TRANSFER-DIAGNOSTIC.md).
