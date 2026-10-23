# Fixed routes expose transfer coordination and repeated source work

The staged cross-owner transfer path makes 88 protocol endpoint calls and compacts the source eight times for the same 24 moves that the local bulk path applies with one compaction. This mechanism holds with navigation removed. It does not establish the cost of physical network traffic or attribute the 3M construction CPU gap.

The fixture has 2,048 current source postings, eight destinations receiving three eligible IDs each, 768-dimensional resident vectors, one existing posting per destination and two untouched leaves. Routes and destination centroids are fixed. Input creation, initial quantization, eligibility checks and final verification stay outside timing. Two owner objects communicate through actual TCP on the native host; the comparison arm puts the same endpoints on one owner. There are no splits, navigation, disk reads, cache loads or size sweeps.

| Observed work | Staged cross-owner path | Direct local bulk path |
| --- | ---: | ---: |
| Moved vectors | 24 | 24 |
| Protocol endpoint calls | 88 | 0 |
| Remote protocol calls | 48 | 0 |
| Additional remote routing lookups | 8 | 0 |
| Total remote calls | 56 | 0 |
| Source compactions | 8 | 1 |
| Source posting entries scanned | 16,300 | 2,048 |
| Copied code bytes across source/destinations | 1,825,600 | 229,376 |
| Framed bytes observed once at the client | 75,429 | 0 |
| Native debug measured interval | 81.462 ms | 1.770 ms |

The measured call count matches two preparation calls and nine completion/release calls per group. Forty calls execute locally and 48 reach the peer. The driver also reads each destination’s routing record once, so the prediction of 48 remote calls describes the protocol subset; total remote calls are 56. Each source compaction scans the shrinking current list, yielding `2048 + 2045 + … + 2027 = 16300` entries.

Both paths finish with all 2,058 IDs exactly once and identical complete posting maps. Every one-bit code is recomputed. The source retains exactly IDs 24–2047, each destination retains its existing prefix plus its three intended IDs, and both nonparticipant leaves remain unchanged. No connection fails. The runner enforces an absolute 30-second native-process timeout; the successful process finishes in 2.071 seconds, including fixture setup and verification.

The wall intervals include loopback connections, server handler creation and transaction waits. Existing thread CPU clocks are unsupported on this native host, so inclusive CPU fields are null. There is no measured protocol-versus-transport CPU split. The local arm intentionally omits cross-owner consistency work, so its interval is not a cross-machine target or an end-to-end speedup prediction.

The next protocol candidate can batch one source’s compatible destination groups by owner and compact the source once. It must retain per-target placement/centroid/current-membership validation, ordered protection, immutable replay identity, staged destination data, source removal ordering, both owners’ acknowledgements and fail-stop behavior. Merely removing barriers is unjustified. If destination routing lookups stay unchanged, batching eight groups into one transaction predicts six remote protocol calls plus eight routing lookups, rather than 56 total calls. That prediction needs a real implementation and the same fixed-work oracle before a construction benchmark.

`FIXED-TRANSFER-DIAGNOSTIC-VALIDATION.json` binds exact commands, source/binary/log hashes, the strict runner, scope limits and a preserved setup failure before timing. Only the diagnostic test file changes; all relevant production hashes still match frozen source26. No cloud or Git action occurs. Root can prioritize this protocol candidate after interpreting physical26.
