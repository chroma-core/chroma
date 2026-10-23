# Corrected28 validates batching but shows no end-to-end speedup

Keep the batched-transfer mechanism and the centroid-refresh liveness correction. The corrected28 physical 3M run validates all memberships and saved codes with an independent reader. Construction plus saving takes 98.593 seconds versus 97.369 for candidate27 and 63.295 for the retained reference21. This run does not establish an end-to-end speedup. The final balance uses substantially fewer protocol calls and less CPU while processing more proposed transfer rows.

| Measurement | Candidate27 | Corrected28 |
| --- | ---: | ---: |
| Insertion, seconds | 21.208 | 20.514 |
| Balancing, seconds | 45.173 | 45.909 |
| Redistribution, seconds | 6.628 | 6.797 |
| Saving, seconds | 24.000 | 25.002 |
| Construction and saving, seconds | 97.369 | 98.593 |
| Final-million insertion, seconds | 12.183 | 11.669 |
| Final-million balance, seconds | 20.334 | 19.140 |
| Final-million balance CPU, seconds across owners | 688.610 | 563.750 |
| Final-million transfer protocol calls | 201637 | 38638 |
| Final-million proposed transfer rows | 46634 | 53140 |
| Final-million source entries scanned | 38087805 | 17862373 |
| Final-million compactions | 26424 | 11169 |

Final balance protocol calls fall 80.8%, CPU falls 18.1% and elapsed time falls 5.9%. These are observed differences, not an isolated causal percentage: independent random trees and schedules differ, candidate navigation rows rise from 805.349 to 868.209 million, splits rise from 724 to 739 and proposed transfer rows rise 14.0%. The inclusive transfer-driver CPU scope remains approximately 300 seconds in both runs; scope counters overlap and some compaction accounting moves into the batch path. Lower protocol work does not establish that all transfer overhead is removed.

Final ownership is 1,488,051/1,511,949; both owners perform final-million construction. The tree has 1, 31 and 2,127 nodes by depth. The independent reader validates the full 3M membership, full-vector digest, one-bit posting codes and four-bit centroid codes. Full-corpus caches, 16 training-vector smoke queries, two candidate machines versus one reference, unequal persistence contracts and uncontrolled trees remain comparison limits. This is not held-out recall or a 100B capacity result.

The isolated source differs from frozen28 only in the refresh function and its regression test. A bounded elapsed retry policy prevents finite dependency changes from exhausting 32 attempts immediately while preserving all snapshot and publication checks. All 258 applicable local tests pass; the same unrelated local-writer test remains excluded. Independent review verifies the exact two-file closure. The archive SHA256 is `956b32eb3025088f1713f02bd77ab69360f87e9cf568ce4f52d09e65bb78a801`. Candidate29 storage changes are absent.

The worker supervisor exits 0 and confirms cleanup. The peer-stop helper encounters an unreadable process environment and does not establish a process cleanup receipt; the launcher therefore exits 1. Both instances are subsequently API-confirmed stopped. Preserve that distinction rather than treating the launcher as wholly successful. The window adds $1.013453185 compute once and no storage. The original failed28 run remains separate evidence.

Evidence: [raw result](../operations/window-epoch28b/3m-distributed/result.json), [summary](../operations/window-epoch28b/candidate-summary.json), [comparison](../operations/window-epoch28b/comparison.json), [reader validation](../operations/window-epoch28b/3m-distributed/durable-validation.json), [source review](EPOCH28B-REFRESH-REVIEW.md), [closure](../operations/window-epoch28b/closed.json). Next measure candidate29 owner-direct saving and unchanged posting-block reuse, retaining corrected28 as the construction baseline.
