# Candidate assembly reduces insertion work; oversized replies abort balancing

The candidate completes construction through 2M vectors, redistributes ownership and adds the final million on both machines. It then fails 1.298 seconds into the final balance with an unexpected connection closure. Server diagnostics record reply frames exceeding the configured limit before the peer's construction failure. There is no complete 3M candidate generation, final membership validation or whole-build comparison. Both machines are API-confirmed stopped.

## Completed insertion comparison

The final million additions complete faster and consume less CPU than the same phase in epoch21, despite slightly more navigation candidates and network traffic. The candidate ordering scope falls sharply, consistent with the fixed-tree demonstration that canonical assembly removes extra sorting. Independently constructed trees and schedules differ; this is one phase comparison, not an isolated causal speedup or complete construction result.

| Final million additions | Epoch21 candidate | Epoch23 candidate | Valid reference21 |
| --- | ---: | ---: | ---: |
| Elapsed seconds | 31.920 | 25.719 | 7.375 |
| Process CPU-seconds, summed once across participating processes | 945.00 | 741.78 | 206.18 |
| Navigation candidate rows / reference distance comparisons | 1,073,563,761 | 1,082,859,653 | 1,124,527,033 |
| Inclusive candidate assembly CPU-seconds | 312.972 | 82.279 | Not the same scope |
| Application framed traffic, GB | 58.846 | 59.967 | No remote traffic |

Compared with epoch21, this phase uses 19.4% less elapsed time and 21.5% less CPU while assembling 0.87% more rows. The assembly scope uses 73.7% less CPU; inclusive scopes overlap and must not be added to elapsed time. Backend CPU rises from 259.385 to 269.690 seconds, and reply-validation CPU rises from 17.185 to 21.098. Owner2 still consumes 269.41 CPU-seconds. Retain canonical assembly and prioritize remote representation and bounded transport work. The candidate remains about 3.49 times slower than reference in this insertion phase.

Before the final insertion, owners contain 1,499,119 and 500,881 vectors. They receive 740,993 and 259,007 additions, ending at 2,240,112 and 759,888 before the failed balance. Both machines perform construction, but the smaller share is about 25.33%; the required final placement of at least 30% per owner is not reached or validated. Redistribution would follow successful final balancing.

The eleven complete intervals through 2M total 8.933 seconds adding, 27.363 balancing and 2.644 redistributing. These cannot be combined with the failed final balance into a successful 3M build time. First-interval balancing takes 7.592 seconds, compared with 11.585 in reference21, while the 900K interval takes 4.035 seconds. Random work and scheduling remain relevant; these observations do not isolate improvements.

## Correctness correction and remaining failure

The conditional continuation preserves exact pending reassignment when active structural work repeatedly invalidates navigation. Independent review resolves deferred-merge retirement and checks replay, dependency ordering, stable broken routes and phase draining. The combined native suite passes 220 tests with the same one known unrelated local-writer exclusion, and all three native programs compile. The [review](EPOCH23-LOCAL-ROUTE-ASSEMBLY-REVIEW.md) and [validation](EPOCH23-LOCAL-ROUTE-VALIDATION.json) bind the six changed files. Frozen source SHA256 is `5344d8ad2480d8fe9e9b92f0b7a01b4c49ff17b743e8f8e3d956cb68addc32c1`.

No accepted navigation deferral is counted in the completed physical balance phases or the observed failed phase. Keep the controlled correctness evidence, but do not claim that this run exercises or validates that continuation under physical contention. Separate neighbor selection and insertion retain their finite fatal navigation rules.

The worker's failure is `split leaf 18446744073711392315: split self reassignment from 18446744073714345226: transport I/O: unexpected end of file`. The peer also fails during split self-reassignment. Its stderr records three reply serialization/send errors stating that a frame exceeds the configured limit before its first construction-job EOF. This establishes oversized replies as an observed failure mechanism. The exact response variant and earliest causal chain need a controlled reproduction; no panic or network outage is inferred from EOF alone.

The next correctness task is to bound complete remote expansion batches and report oversized replies explicitly. Preserve every scored row, global selection and existing frame/deadline limits. Smaller encoding is an independent optimization, not a substitute for bounding a response. The separate [lossless codec assessment](EXPANSION-CODEC-ASSESSMENT.md) supplies the next local representation candidate; it is excluded from epoch23.

## Evidence, cleanup and accounting

The [raw worker receipts](../operations/window-epoch23/3m-distributed/run-receipts.json), [peer receipts](../operations/window-epoch23/3m-distributed/serve-receipts.json), [partial summary](../operations/window-epoch23/candidate-partial-summary.json), [Linux builds](../operations/window-epoch23/linux-build-receipts.json) and [runtime bindings](../operations/runtime-binaries-epoch23.json) retain this attempt. The partial summarizer explicitly marks it incomplete. No reference rerun, profiler, automatic retry, size sweep or deadline extension occurs.

The worker supervisor confirms cleanup after exit1. The peer stop helper reports a permission error and no matching peer exit receipt is collected, so peer process cleanup remains unconfirmed separately from API shutdown. Both exact instances are API-stopped, as recorded in [closure](../operations/window-epoch23/closed.json) and [cleanup reconciliation](../operations/window-epoch23/cleanup-reconciled.json). The closed window adds $1.2023860912323 compute once and no storage. Covered compute is $46.021985353874624083; recurring storage is $19.52/month and the first-month covered bound is $65.541985353874624083. This is task accounting, not a whole-account invoice.
