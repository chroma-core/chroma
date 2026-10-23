# Experiment 29: owners save directly and reuse unchanged posting blocks

Experiment 29 is complete on 3M vectors across two physical machines. Both independently reopened generations pass exact membership, vector SHA256 and complete posting/centroid-code validation. Keep owner-direct saving and unchanged-block reuse. The unchanged save reuses all 2,115 posting blocks, materializes none and takes 2.929 seconds, versus 108.154 seconds for the first durable save. Both saves copy zero posting-payload bytes through the coordinator.

## Results

| Measurement | Result |
| --- | ---: |
| Construction | 74.367 s |
| First durable save | 108.154 s |
| Construction plus first save | 182.521 s |
| Unchanged second save | 2.929 s |
| First-generation independent reader | 274.151 s |
| Second-generation independent reader | 252.291 s |
| Final ownership | 1,494,039 / 1,505,961 vectors |
| Tree nodes by depth | 1 / 36 / 2,115 |

The unchanged save is 97.29% shorter than the first save on the same sealed state. It still scans all 3M memberships and writes routing and manifest metadata. Total supplied immutable payloads fall from 385,384,469 to 25,300,011 bytes. These counters include logical payload supplied for replay and are not a measurement of network or device writes. Referenced durable bytes include reused objects and must not be interpreted as rewritten bytes.

The first save materializes 1,049 blocks on owner 1 and 1,066 on owner 2. Owner materialization takes 105.301 and 101.538 seconds respectively; those phases overlap. The repeat materializes zero blocks and reuses those same counts. Native mutation regressions separately check that a small mutation rewrites two affected blocks while reusing eight. This physical run measures the completely unchanged case, not a post-save mutation workload.

## Comparison

| Experiment | Insertion | Balancing | Redistribution | First save | Construction plus save |
| --- | ---: | ---: | ---: | ---: | ---: |
| 27 | 21.208 s | 45.173 s | 6.628 s | 24.000 s | 97.369 s |
| 28b | 20.514 s | 45.909 s | 6.797 s | 25.002 s | 98.593 s |
| 29c | 21.401 s | 46.315 s | 6.300 s | 108.154 s | 182.521 s |

Experiment 29 establishes no first-save or end-to-end speedup over 28. Its first save writes directly to durable S3 storage; 28 uses the retained filesystem-based saving contract. The different backends and uncontrolled tree/scheduling variation prevent attributing the elapsed difference solely to payload routing. The unchanged save isolates reuse within one frozen state. Independent-reader validation and the second save are excluded from the construction-plus-first-save metric.

## Validation and retained failures

The final native suite passes 269 tests, with two opt-in S3 tests and one documented unrelated exclusion. Three native CLIs and all required Linux executables build. [Independent review](EPOCH29C-INDEPENDENT-REVIEW.md) accepts the final correction. The final Linux S3 smoke passes immutable replay, multipart conflict preservation, conditional publication, stale-predecessor rejection and concurrent publication. Ten native publication races also pass with reconciliation; the final formatting-equivalence refinement has deterministic coverage.

Both fresh readers run after both saves, so the first generation remains readable after publishing the second. Each validates all 3M memberships and posting codes, every centroid code, and the full-vector hash without writer-side saved objects. The retained immutable vector input remains required. The 16 training-vector smoke queries are not held-out quality evidence. Full-corpus caching and this workload do not establish bounded storage or 100B scalability.

Retain the [construction outlier](EPOCH29-CONSTRUCTION-OUTLIER.md), [smaller-input rejection](EPOCH29-100K-INPUT-REJECTION.md), [save timeout](EPOCH29-SAVE-DEADLINE.md) and [conditional-publication diagnosis](EPOCH29-S3-PUBLICATION-RECONCILIATION.md). The final save-specific deadline is bounded by the process shutdown deadline; synchronous storage still depends on the external supervisor for hard termination. The reconciliation diagnosis does not claim to reproduce the complete cause of the earlier two-error publication result.

## Artifacts and operations

Raw evidence is in `operations/window-epoch29c/3m-distributed`: `result.json`, both validation reports, process receipts and frozen job specifications. `EPOCH29-COMPLETION.json` binds their hashes. The source archive SHA256 is `78bf3556592b90d6b1fbcd69cb5832919d7fb66058c5f9b4f40e29748707e57b`.

The coordinator exits with code 0. The peer stops by the expected supervised signal after completion; both process cleanup receipts are confirmed. Both r6i.16xlarge instances remain running at Lyon's explicit request, at $8.064/hour combined. The independent cutoff is October 22, 2026 at 17:50:37 UTC, or 1:50 p.m. Toronto time. The recurring follow-up remains paused. Closed compute totals and the open-window reservation are recorded separately in the spend ledger.

[Development overhead changes](DEVELOPMENT-OVERHEAD-29.md) retain running machines, use S3 for verified binary transfers and reuse unchanged input hash evidence. The next storage hypothesis is bounded parallel object writes within each owner; no next experiment starts as part of this completion.
