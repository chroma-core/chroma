# Epoch21: insertion after the first physical handoff

The final 1M insertion is slow primarily in distributed navigation and its candidate assembly, rather than because it performs more centroid comparisons than the reference. It takes 31.920 seconds and 945.00 combined process CPU-seconds, versus 7.375 seconds and 206.18 CPU-seconds for the reference. The candidate performs 1.074 billion comparisons versus 1.125 billion reference comparisons. Its remote expansion representation produces 58.846 GB of framed application traffic, including 49.011 GB received by the coordinator. These are completed insertion measurements. The following balance fails its whole-attempt navigation retry budget, so this is not a completed or validated distributed 3M build.

## Comparable phase deltas

The candidate snapshots bracket insertion only. CPU is the difference in each owner's cumulative process CPU; the total includes both machines. RPC bytes are the provided coordinator-side accounting difference, counting each connection once in each direction. Peer counters are not added again. GB means decimal bytes divided by 1,000,000,000.

| Measurement | Candidate 1M→2M, before handoff | Reference 1M→2M | Candidate 2M→3M, after handoff | Reference 2M→3M |
| --- | ---: | ---: | ---: | ---: |
| Insertion wall seconds | 5.537 | 4.681 | 31.920 | 7.375 |
| Combined process CPU seconds | 155.80 | 123.79 | 945.00 | 206.18 |
| Centroid comparisons | 688,000,000 | 707,663,082 | 1,073,563,761 | 1,124,527,033 |
| Navigation batches | 15,648 | Per-vector traversal | 15,648 | Per-vector traversal |
| Remote framed bytes | 0 | 0 | 58,846,054,318 | 0 |
| Queries copied into remote envelopes, bytes | 0 | Not applicable | 3,072,000,000 | Not applicable |
| Reply structures cloned, counted bytes | 0 | Not applicable | 23,763,213,440 | Not applicable |

The handoff leaves 1,491,238 vectors on owner1 and 508,762 on owner2. The completed final insertion adds 746,974 and 253,026 vectors respectively, ending at 2,238,212/761,788. Both physical owners participate; these counts do not establish final placement or full-build admission because balancing fails afterward. The candidate final insertion is 4.328× the reference wall time despite 4.5% fewer scored child rows. Within the candidate, comparison work grows 1.560× from the preceding 1M insertion, while elapsed time grows 5.765×.

The insertion pipeline has substantial concurrency. Owner1 consumes 684.75 CPU-seconds and owner2 260.25 during the final phase, averaging 21.45 and 8.15 busy cores respectively, or 29.61 across both machines. This does not support describing the entire insertion as one serial routing loop. It also does not show that every configured worker continuously performs useful work.

## What the counters attribute

The coordinator's inclusive Navigation scope records 680.624 CPU-seconds and 973.464 summed worker wall-seconds during the final insertion. Its Candidate scope records 312.972 CPU-seconds, ExpansionBackend 259.385, reply validation 17.185, query copying 1.232, owned scoring 81.189 and shared scoring 2.183. The remote owner records 29.172 CPU-seconds in owned scoring. These are nested or overlapping scopes, not additive phase latencies. In particular EngineRequest's 260.808 CPU-seconds largely overlap ExpansionBackend, and summed worker wall time is not elapsed insertion time. The reference's navigation/scoring/sort nanosecond counters are accumulated elapsed durations, not Linux thread CPU, so they must not be presented as equivalent CPU subdivisions.

Candidate assembly is a clear remaining cost. Its inclusive CPU rises from 33.445 seconds before handoff to 312.972 afterward, considerably faster than the 1.560× increase in candidate rows. The source adds ordering and duplicate-ordinal machinery whenever a query touches a remote parent: it sorts wide scored-child records by parent rank and ordinal, performs map lookups in that comparator, and builds a set for all resulting child ordinals. It then performs the separate reference-compatible distance sort. The remote reply validator has already checked complete per-parent ordinal coverage and overlapping parents. This identifies consequential extra work, but the current scope does not isolate each sort, lookup, set insertion or allocation.

Remote expansion is also expensive. The phase validates 1,000,000 remote query replies containing 293,199,682 scored children, grouped into 15,648 expansion RPC batches. The coordinator sends 9,835,351,445 bytes and receives 49,010,702,873. Total client calls are 16,378; the additional 730 calls align with a routing-record fetch and posting append for each of owner2's 365 leaves. Owner-scoped RemoteEngineCalls includes one additional boundary accounting call. The measured wire volume is therefore associated overwhelmingly with remote navigation, rather than per-leaf append traffic.

The 49.011 GB received is approximately 167 bytes per remote scored child, including other response metadata. Each scored-child response repeats its parent ID, ordinal, full target reference, leaf flag, distance and revision as JSON. The replies do **not** contain full centroid vectors or posting lists. Remote append preparation fetches a routing record once per leaf; those few hundred centroid vectors cannot explain tens of gigabytes. The raw 3.072 GB query-copy counter describes full-precision query vectors cloned into remote requests; the first per-window input-vector copies occur for both local and remote navigation and are not included in that counter. JSON numeric representation enlarges the remote query payload further. The saved-object base64 optimization does not apply to these navigation vectors or score records.

The remote process consumes 260.25 CPU-seconds but reports only 29.172 in its scoring scope. This supports investigating serialization, request processing, response construction and append work outside scoring. It does not measure the JSON encoder's exclusive share. The coordinator's ExpansionBackend scope includes request encoding, waiting and response decoding; its 552.148 summed wall-seconds versus 259.385 thread CPU-seconds likewise do not isolate network latency.

## Algorithm and batching

Insertion splits the input among 32 workers. Each worker navigates windows of 64 vectors, then all worker contributions combine by destination leaf before a second worker pool performs posting updates. The one-million-vector interval consequently makes 32 × ceil(31,250/64) = 15,648 navigation calls. There is no per-vector append RPC. See `distributed/construction.rs:804` and `:835`.

Each navigation level scores local immutable packed routing images first, sends grouped remote requests, joins all required remote replies, validates them, and selects one global beam for each query. The per-level barrier is required by this traversal algorithm. The implementation currently performs local scoring before dispatching remote requests, which may reduce overlap within a batch. The measurements establish its existence, not its exclusive latency cost. See `distributed/routing.rs:287` and `:330`, and `distributed/engine.rs`'s expansion backend.

## Smallest useful next work after correctness

First repair and validate the failed final balance. Do not interpret this phase analysis as an admission of the whole 3M result.

The next bounded mechanism should address remote navigation's bulk representation and candidate assembly, keeping the global selection algorithm fixed. Use one fixed 64-query batch with multiple parents and mixed local/remote rows. Compare exact selected IDs, float distances, row order and work counts against the current implementation and reference; include ties, per-level beam policies and stale/duplicate/malformed replies. Record encoded request/reply bytes and candidate assembly work, without elapsed-time thresholds.

Two narrow changes have direct source justification. A compact numeric expansion payload can avoid repeated JSON metadata and float-array parsing, using existing binary/base64 dependencies with explicit bounds and matching endpoint versions; it must retain identity, view, revision, complete-parent coverage and bounded decoding. Separately, assembling already validated per-parent rows into frontier/ordinal slots can remove the extra global wide-record ordering sort and duplicate-ordinal set, while preserving the exact input order required by the reference's tie behavior. Do not weaken remote validation to obtain that reduction. Choose one coherent change after the mechanism establishes parity; the next already-required distributed correctness/performance execution can quantify its effect without another baseline run solely for attribution. Current counters cannot predict its end-to-end speedup.

## Evidence and source identity

The measurements come from `operations/window-epoch21/3m-distributed/run-receipts.json` (embedded `events.jsonl`), `operations/window-epoch21/candidate-partial-summary.json`, and `operations/window-epoch21/3m-reference/result.json`. Candidate preflight identifies the retained epoch20 benchmark binary SHA256 `5efa72832ae7647de5361dd45ed4a4a95f8905a8e47ee77ade79a8da5b539167`. The inspected source files match `packages/source-epoch20.tar.gz.json`; this run does not have a separate epoch21 source archive.

| Source under `rust/index/benches/hierarchical_index/distributed/` | SHA256 |
| --- | --- |
| `construction.rs` | `6a45fbcfcc6625752349a617d5b26dc175ea4c34b712e63f3451c9bb2dd37039` |
| `routing.rs` | `673e5aa893d98072c8952d92cf74503837e2dae4645452471e270764b36ed4a7` |
| `engine.rs` | `9a827a653ac06af26f1a4c8a89bd021fa34d930b9304477eab84d2f0f7dd72f3` |
| `transport.rs` | `b54a9a0490511b533a21650a559f20527c16c3ad30be05d363131d763a627f1c` |

Only this analysis note changes. No implementation, benchmark, cloud resource or Git index changes as part of the investigation.
