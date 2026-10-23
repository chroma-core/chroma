# Direct local navigation at 500K vectors

The experiment tests whether removing local request/reply assembly reduces construction cost while preserving global navigation decisions. The validated candidate reduces construction plus saving from 30.002 to 26.640 seconds. Keep the direct local path: local query copies and reply validation disappear, and inclusive navigation CPU falls from 19.939 to 10.770 seconds. The later balancing intervals remain about 8.6 seconds, so scheduling and waiting become the next priority.

## Question and decision

Epoch15 spends 19.939 inclusive CPU-seconds in balancing navigation, copying 2.219 GB of queries and cloning 5.426 GB of reply data. Local navigation can borrow queries and use coherent immutable routing images directly. The candidate preserves the same global candidate order, distance-based beam, stale-attempt restart and validated remote protocol. Keep the change if exact semantic tests and saved-state validation pass, the predicted protocol work disappears and the observed timing/work counts support the improvement. Preserve adverse timings and explain construction variability.

## Workload and limits

The candidate uses the retained 500K-by-768 FineWeb input, 32 workers and five 100K insertion/balance intervals. The split threshold is 2,048, merge threshold 512 and branching setting 100. The tree reaches two internal levels in the retained baseline, exercising multi-level local routing; the 1.536 GB input fits within the 12 GiB vector cache. This tests local routing overhead, not cache-capacity crossing. All timed construction remains on the initial owner. Final branch redistribution must leave at least roughly 30% of vectors on each of the two physical machines.

The retained reference is epoch14, and the previous candidate is epoch15. Input, hardware, metric, precision and construction settings remain matched. Seeds and thread scheduling are not controlled; independently constructed trees can differ in work. There is no external profiler, reference rerun or size sweep. The fresh window is capped at 30 minutes and $4.032 compute for both exact sandbox instances, with independent local and remote absolute shutdown guards. A correctness or progress failure stops the attempt without extending the window.

## Implemented and tested

Only the local navigation implementation and its tests change from epoch15. Three focused tests compare the actual local path, envelope-only backend and reference on mixed-depth and tied-score trees, including beam/tau choices and exclusions. They check equal logical distances and results, zero local envelope copies and validation, mixed local/remote selection, malformed remote responses and stale local images. The applicable native suite passes 188 tests with one retained unrelated writer test excluded. Three benchmark/reader executables build, and both Git indexes remain unchanged.

The source archive has SHA256 `ea8ce5c8978f5a6505bb37f5a0b5f26c8fdb2502e930e24982d2e1b2395afaeb`. The [validation receipt](DIRECT-LOCAL-NAV-VALIDATION.json), [independent review](EPOCH16-DIRECT-NAV-REVIEW.md), [three-file source closure](epoch16-source-closure-comparison.json) and [execution admission](../operations/admission-epoch16.json) bind the implementation and controls. The native retired-image fixture uses a controlled seam; it does not establish every possible concurrent interleaving.

## Results and interpretation

| Measurement | Retained reference | Previous candidate | Direct local navigation |
| --- | ---: | ---: | ---: |
| Insertion wall seconds | 0.699 | 1.340 | 1.014 |
| Balance wall seconds | 9.054 | 19.379 | 16.167 |
| Balance CPU seconds | 52.700 | 66.540 | 54.300 |
| Clustering CPU seconds | 35.296 | 31.522 | 28.778 |
| CPU outside clustering seconds | 17.404 | 35.018 | 25.522 |
| Saving wall seconds | 2.840 | 7.884 | 7.781 |
| Construction plus saving seconds | 12.601 | 30.002 | 26.640 |

Insertion improves by 24.3%, balancing by 16.6% and construction plus saving by 11.2% in this comparison. Balance CPU is now 3.0% above the retained reference, while balance elapsed time is 78.6% above it. Saving is essentially unchanged. Its comparison with the reference has different durability and full-vector persistence contracts, documented in the [save audit](SAVE-PATH-AUDIT.md).

The predicted navigation work disappears without reducing logical candidate work. Local navigation query-copy bytes fall from 2,219,271,168 to zero, reply-clone bytes from 5,426,103,408 to zero, and reply validations from 722,039 to zero. Candidate rows increase slightly from 66,174,200 to 66,553,758. Inclusive navigation CPU falls from 19.939 to 10.770 seconds; candidate handling falls from 5.519 to 3.117 seconds. These scopes overlap and are not additive phase costs. Reassignment still copies 1.657 GB of query data before navigation, which is a separate retained cost.

The elapsed gain does not establish that scheduling improves. After the first interval, balancing takes 8.626 seconds versus 8.540 before, while CPU falls from 43.03 to 34.77 seconds. Average busy cores consequently fall from about 5.04 to 4.03; the retained reference uses about 14.45 across its corresponding tail. The candidate tail now uses 34.77 CPU-seconds versus the reference's 33.44, but takes 8.626 versus 2.314 elapsed seconds. This strengthens the case for investigating waiting and dependency ordering. Local-transfer operations accumulate 165.016 overlapping wall-seconds but only 2.853 CPU-seconds. Their preparation and commit scopes account for about 2.45 wall-seconds. Waiting outside these scopes remains unresolved. Reassignment rounds increase from 5,050 to 5,241, and rejected groups from 9,176 to 10,077.

Construction variability explains part of the overall improvement. Clustering comparisons fall from 88.343 million to 78.198 million, with CPU falling from 31.522 to 28.778 seconds. The final shape changes from one root, six internal children and 344 leaves to one root, four internal children and 340 leaves. All 340 counted preparations commit; none is discarded. The reference has one root, five internal children and 354 leaves. The last insertion interval performs 28.2 million comparisons versus 28.5 million before and takes 0.290 versus 0.442 seconds; owner CPU falls from 10.55 to 5.60 seconds. Peak process memory is 2.762 GB versus 3.271 GB before, but this single randomized pair does not isolate memory savings from topology and allocation differences.

## Correctness, closure and cost

Fresh-reader validation checks all 500,000 current memberships, full-vector SHA256, every posting code and every centroid code without writer objects. The 16 training-vector smoke queries are not held-out recall evidence. Construction remains on owner 1 through all five intervals. Final physical redistribution leaves 218,052 and 281,948 vectors, so the smaller owner holds 43.61%. This is initial-owner construction followed by physical handoff, not a measurement of distributed construction throughput.

Both supervisors report cleanup, and both exact task instances are API-confirmed stopped. The peer-stop launcher retains its permission error and nonzero outcome; independent matching supervisor receipts reconcile cleanup. The window costs at most $0.7909735502624512 in compute and adds no recurring storage. It is counted once in the spending ledger. The window is closed and cannot be reused.

## Decision and next experiment

Keep direct local navigation as the candidate baseline. Its exact fixed-tree tests and eliminated protocol work support the mechanism, and the 500K run validates membership and saved data. Preserve the unchanged tail latency as adverse evidence. The [scheduler audit](SCHEDULER-PATH-AUDIT.md) now has a deterministic actual-callsite reproduction of a retry dispatched while its leaf remains claimed. Correct that readiness mismatch with minimal coordination, then compare the same 500K workload. Add coarse save attribution to that later run without weakening persistence guarantees; local duplicate writes are a separate optimization candidate.

Raw evidence is in [the comparison](../operations/window-epoch16/comparison.json), [candidate result](../operations/window-epoch16/500k-distributed/result.json), [durable validation](../operations/window-epoch16/500k-distributed/durable-validation.json), [cleanup reconciliation](../operations/window-epoch16/cleanup-reconciled.json) and [API closure](../operations/window-epoch16/closed.json). The previous [combined-change report](LOCAL-TRANSFER-PACKED-NAV-500K-RESULT.md) is retained unchanged.
