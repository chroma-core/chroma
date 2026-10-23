# Reusing an unchanged shared routing view reduces work, but does not improve latency

Both optimizations are accepted for their measured CPU savings. Keep compact navigation result handling and conditional routing-view transport in the intended implementation. Each passes independent validation; their combination still requires integration and validation. Preserve the immutable baseline as the comparison control. Latency improvement is unproven, and the small observed elapsed-time increases remain a possible tradeoff.

The conditional-view candidate reduces transport work and CPU while preserving correctness, but it does not improve end-to-end construction time in three repetitions. The immutable checkpoint baseline remains the preferred latency control. This experiment follows the transport finding in [the baseline and navigation report](EPOCH30-BASELINE-AND-NAVIGATION.md) and [the current plan](../../CURRENT-PLAN.md).

## Mechanism and validation

An owner needs a current copy of the upper-tree routing records and ownership directory before navigation. The candidate sends the version of its already installed immutable view. The coordinator returns a short acknowledgement only when that version exactly matches its published view; otherwise it returns the complete view. A peer with no installed view requests the full contents. An acknowledgement with a different version is rejected, and a concurrent newer local installation is retained.

The version and immutable view are captured together by the existing publication mechanism. The optimization changes neither mutation admission nor navigation result validation. Both owners run the same candidate binary. This candidate starts from the immutable baseline and excludes the compact navigation gather change, so the two mechanisms are evaluated separately.

The native suite passes 274 applicable tests. The existing two actual-S3 tests remain ignored, and the previously documented unrelated parent/leaf concurrency test remains excluded. Focused tests verify exact-version acknowledgement, full replies for different versions, rejection of an acknowledgement with an empty cache, retained installed-view identity on an unchanged TCP reply, changed-view installation, and a smaller unchanged response. The fixed 256-query navigation replay matches the baseline byte-for-byte. All three timing runs and the separate detailed run pass exact 3M membership and every posting-code check.

## Matched-checkpoint results

Every run restores the same two files from the [verified 2M checkpoint](../operations/development30/checkpoint-manifest.json). Preparation and post-construction validation are outside construction timing. Three runs per arm provide a small comparison, not a precise confidence interval; parallel schedules and resulting work counts differ.

| Metric, median of three timing runs | Baseline | Conditional-view candidate | Change |
|---|---:|---:|---:|
| Balance wall time, seconds | 22.144 | 25.595 | +15.6% |
| Gross final-million construction, seconds | 37.994 | 39.405 | +3.7% |
| Balance CPU, seconds | 648.44 | 589.14 | −9.1% |
| Balance application-framed bytes, GB | 13.716 | 12.527 | −8.7% |

| Candidate run | Insert, seconds | Balance, seconds | Redistribute, seconds | Gross construction, seconds |
|---|---:|---:|---:|---:|
| 1 | 12.872 | 25.614 | 0.794 | 39.405 |
| 2 | 13.055 | 23.524 | 0.802 | 37.507 |
| 3 | 12.828 | 25.595 | 0.929 | 39.477 |

The separate detailed candidate run records 66.019 coordinator CPU-seconds in JSON encoding and decoding, versus 79.743 in the detailed baseline. Framed bytes fall from 13.756 to 12.482 GB in those diagnostic runs. This supports reduced transport work. A conditional-hit counter is not present, so these measurements do not establish the exact fraction of view requests that receive acknowledgements. Remaining full replies and other message types are not separated by request kind in this version of the instrumentation.

The timing result does not support promoting this candidate as a latency improvement. Lower aggregate CPU does not guarantee earlier completion of the last dependent job. The per-second traces show uneven active-job counts between owners late in balancing even when vector counts are near even. Those observations motivate a critical-path investigation, but do not prove the cause of the measured regression.

## Decision and next unfinished action

Keep even placement, writer checkpoints, the repeated-run harness, and the diagnostic baseline. Retain both optimization candidates and their evidence without selecting either as the new latency baseline. The navigation candidate reduces candidate-processing CPU per row; the conditional-view candidate reduces transport work. Neither produces an end-to-end win in this workload.

The next research action is to identify the work that delays final completion. Add bounded records for the longest job identities, their node/owner, dependency waits, and per-owner eligible queue counts, then use the retained checkpoint to compare complete phase timelines. Attribute transport bytes and codec CPU by request kind before changing the wire format or adding connection pooling. A further CPU optimization alone is not yet justified as a latency fix. This next action is a documented priority, not a resumed recurring task.

## Retained artifacts

The executable SHA-256 is `cd00645d0f2935cf55b8c051a44c0c1c23bbba68aa4da3168feacf22f1037214`. The [source manifest](../operations/development30/transport1-source-manifest.json), [isolated patch](../operations/development30/transport1.patch), [source archive](../packages/source-epoch30-transport1.tar.gz), [native validation](epoch30-transport-native.log), and [focused protocol tests](epoch30-conditional-view-native.log) are retained. The source archive SHA-256 is `bfd7ea9fa90f7003b8f4eb877e4500be338ab91a512df2cf1bd6ccc7b7342159`. The raw timing and diagnostic receipts remain in the corresponding `transport-*` directories under [development30](../operations/development30).
