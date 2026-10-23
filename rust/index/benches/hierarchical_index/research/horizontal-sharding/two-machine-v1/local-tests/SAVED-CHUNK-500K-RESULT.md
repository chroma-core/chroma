# Compact saved-object transport at 500K

Keep bounded base64 encoding for saved-object chunks. The fixed-byte TCP test preserves exact bytes with less than half the legacy framed bytes, and the full 500K run validates every saved membership and code. Save traffic falls 60.6%, while remote copying takes 1.876 seconds instead of 2.936. Total saving falls from 6.477 to 4.683 seconds, but faster owner storage writes also contribute; the full 27.7% reduction is not isolated to encoding.

## Comparison

| Measurement | Reference epoch14 | Candidate epoch19 | Candidate epoch20 |
| --- | ---: | ---: | ---: |
| Insertion wall seconds | 0.699 | 1.251 | 1.246 |
| Balancing wall seconds | 9.054 | 9.174 | 9.576 |
| Balancing CPU seconds | 52.700 | 63.520 | 65.440 |
| Clustering CPU seconds | 35.296 | 33.575 | 35.384 |
| Other balancing CPU seconds | 17.404 | 29.945 | 30.056 |
| Save wall seconds | 2.840 | 6.477 | 4.683 |
| Construction plus save wall seconds | 12.601 | 18.317 | 16.962 |

Construction behavior is unchanged by the codec. Balancing varies from 9.174 to 9.576 seconds, with 65.44 CPU-seconds versus 63.52 previously. The subsequent four intervals take 2.094 seconds versus 2.121 previously and 2.314 reference. Independent trees and schedules prevent treating those construction differences as encoding effects. The candidate also performs final physical redistribution; the reference has no ownership handoff. Reference saving uses a different durability and full-vector persistence contract, so its 2.840 seconds is not an equal save-contract target.

## What changes and what is measured

Saved objects still contain the same bytes, identifiers and checksums. Only their transport representation changes: a bounded base64 string replaces a JSON integer array inside the existing framed response. The encoded-length limit is checked before decoding, and the decoded-length limit is checked afterward. Both checks are necessary because a maximum chunk and a chunk one byte larger can have the same encoded length. Identity, offset, completion, phase and whole-object checksum checks remain in place. New writer binaries must run on both endpoints; mixed versions fail closed. Reader formats, immutable writes, synchronization and publication ordering remain unchanged.

| Saving measurement | Epoch19 | Epoch20 |
| --- | ---: | ---: |
| Remote raw object bytes | 30,556,667 | 30,863,895 |
| Save-only received framed bytes | 104,710,027 | 41,225,393 |
| Save-only total framed bytes | 104,798,459 | 41,315,999 |
| Received framed bytes / raw object bytes | 3.427 | 1.336 |
| Remote copy wall seconds | 2.936 | 1.876 |
| Owner staging calls, wall seconds | 3.291 | 2.561 |
| Local object reuse, wall seconds | 0.045 | 0.044 |
| Local copied bytes | 0 | 0 |
| Remote copied objects | 171 | 175 |
| Coordinator file / directory sync calls | 175 / 175 | 179 / 179 |

The representation accounts for the large byte reduction despite slightly more raw remote data. Application byte counters include framing, metadata and control messages, not physical link overhead. The remote-copy interval includes decoding, object writes and checksum work, so its 1.060-second reduction is not pure network time. Coordinator synchronization time also changes from 1.281 to 1.152 seconds. Local owner materialization falls from 1.685 to 0.980 seconds even though this codec does not change local storage; preserve that variability when attributing the total save improvement. No save CPU speedup is claimed from these wall and byte counters.

## Correctness and workload

Four focused tests cover empty, arbitrary and maximum chunks; invalid alphabet, padding and encoded/decoded oversize; exact TCP multi-chunk replay and framed-byte accounting; incorrect identity, offset, completion and phase; and valid base64 carrying corrupted bytes that fails whole-object checksum validation. The same fixture compares the actual received frame count against the legacy representation without a timing assertion. [Native validation](SAVED-CHUNK-VALIDATION.json) records 207 applicable tests and three CLI builds, with the same known unrelated exclusion. [Independent review](EPOCH20-SAVED-CHUNK-REVIEW.md) binds both changed files and checks the unchanged persistence boundary.

The 500K workload retains the same input/order, 768-dimensional L2 metric, 32 workers, five 100K intervals and construction settings. The input fits the vector cache. All timed construction occurs on owner1; final physical redistribution leaves **259,781 / 240,219 vectors**, so the smaller owner holds 48.04%. The tree has one root, five inner nodes and 350 leaves. Peak RSS is 2,966,646,784 bytes. A fresh reader verifies exact unique membership, all posting and centroid codes, and the full-vector digest without writer objects. Sixteen reused training queries remain smoke checks, not quality parity evidence.

## Closure and next decision

Both instances are API-confirmed stopped, both matching supervisor receipts confirm cleanup, and this run's launcher exits cleanly. The unchanged launcher's historical failures remain retained; one clean run does not establish a fix. Compute upper bound is **$0.9477872760772705**, counted once, with no storage change.

The next material question is construction on both physical owners. With the intended 2,160,000-vector capacity and existing batch cadence, first redistribution occurs after 2M and the final 1M phase of a 3M run exercises both owners. The 500K evidence cannot answer that question. Finish the readiness assessment, use the retained 3M input and current reviewed binaries, and run one matched current reference and candidate in a separate fresh window if admitted. Sum distinct owner process and clustering counters rather than interpreting the coordinator-only counters as cluster totals. Do not infer cache-capacity behavior or held-out recall from that workload.

Raw results and save-only transport summaries are in `../operations/window-epoch20/`. Source archive SHA256 is `5a229d7a9b78b76e10fe6212e7ef03719d901b98416441640e33f35a25153ad4`. Reference epoch14 and candidate epoch19 are retained comparisons; this run has no external profiler or reference rerun.
