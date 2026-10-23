# Reuse transport resources and reduce navigation lookup work

The implementation reuses TCP connections and executes requests on bounded server pools. This is an adopted implementation direction requested by Lyon; cloud construction validation passes. The navigation candidate caches consecutive parent lookups during reply validation and candidate ordering. Accept the navigation cache for consistently lower CPU demand and faster insertion across three fresh pairs. Balancing and total construction latency differences remain inconclusive. Both arms keep the accepted coordination, notification listener and saving implementation.

## Transport behavior and correctness

A client call exclusively borrows a socket and returns it only after receiving and decoding a complete reply. A stale cached socket can be replaced before sending. A failure after sending does not automatically replay the request, because the mutation may already have executed.

Readiness reactors assemble bounded frames without tying idle sockets to execution workers. Separate decoder workers preserve parallel decoding. Data execution has reserved capacity for construction, its publication/export dependencies, and terminal chunk installation. Control execution has independent capacity. Saturation beyond these bounds fails explicitly; arbitrary recursive control saturation is not a universal progress guarantee.

At the benchmark configuration, each server has 192 data execution workers, 128 control execution workers and 16 decoder workers, plus two reactors. These threads are fixed rather than created per request. They are not a claim of 336 runnable cores or higher compute parallelism. Admission permits at most 384 data and 256 control sockets. The manifest records logical frame payload bounds; decoded objects, buffers, allocator overhead and thread stacks add memory. Process RSS remains the practical memory observation.

Independent review finds no blocking correctness issue. The final combined native suite passes 315 tests, with three opt-in tests ignored and one existing unrelated parent/leaf concurrency test excluded. Focused transport coverage passes 19 tests in both ordinary and detailed mode, including idle-budget and concurrent-reply validation. Native benchmark compilation passes. Tests cover reuse, concurrent reply isolation, stale sockets, lost mutation replies, partial frames, overload, shutdown and reciprocal nested requests.

## Navigation hypothesis and validation

The existing profiles show substantial navigation CPU, including hashing in candidate ordering. Grouped child rows repeatedly look up the same 128-bit parent identifier. The candidate remembers the previous parent and map index, falling back to the existing map on every change. All child validation, completeness checks, canonical ordering and tie behavior remain intact. Arbitrarily interleaved rows retain the same results.

Both settings pass 31 focused routing tests. A grouped mechanism replay reduces 409,600 lookups to 4,096. Native debug micro timings are useful for confirming the mechanism, not estimating production speedup. `SPANN_NAVIGATION_PARENT_CACHE=0` selects the control in the same executable; the candidate is enabled otherwise. Both cloud arms use the new transport.

## Job attribution limits and new evidence

The retained epoch32 longest-job set omits the final 2.607 seconds of balancing. It therefore cannot establish the actual last-finishing job. The navigation change targets demonstrated CPU work, not a proven completion-critical dependency.

The new detailed recorder retains the latest 32 completion estimates independently from the longest 32 durations. It also persists peer transport counters periodically, avoiding loss when the supervisor terminates the peer. These counters are cumulative endpoint observations and must not be added to the other endpoint’s byte totals. Job scopes overlap, remote requests lack initiating-job context, and cross-owner completion estimates retain clock-alignment uncertainty.

## Reproducible artifacts

The final isolated source is `/private/tmp/spann-research33view`. The frozen source archive is `two-machine-v1/packages/source-epoch33v3.tar.gz`, with SHA256 `95661fdb9f587aec02bd8a3ddd8c9c537777f3b6ea374ca01efa9b450995a293`. The Linux executable SHA256 is `03f5ad0d0096baedc0f58b2b204691f2a3589521b5eb0eb6e33cb9ab3af78eb5`. Both machines verify the same executable. The source manifest covers 2,315 files. Earlier v1 and diagnostic-only v2 archives, hashes, build logs and failed attempts remain retained separately. All builds reuse the retained compiler cache. An initial cache-path allowlist rejection is corrected without rebuilding; the failed deployment log remains retained.

The [operations directory](../operations/development33) retains the source manifest, validation receipt, independent review, test logs, raw cloud receipts and experiment design. Cloud timing comparisons restore the same 2M writer checkpoint, insert the final million vectors with 64 insertion workers, and balance with 32 workers per owner. Detailed diagnostics remain separate from primary timings. No measured benchmark overlaps compilation.

## Original v1 transport integration evidence

Both 256-query cloud replay settings exactly match the retained reference output. The transport-only detailed construction smoke restores 2M, inserts the final million, and passes exact 3M writer validation. Through the balancing snapshot, the coordinator opens 119 connections and reuses connections for 54,967 calls; all 55,086 sent requests have framed replies. The server records a fixed 336 processing workers and zero admission rejections. The legacy `failed_connections` counter also counts application error replies: its 92 entries are attributed to boundary readiness, node reads and transfer preparation, not evidence of 92 failed TCP connections.

The peer export gap is closed: 73 cumulative transport samples are retained in the smoke. Latest-completion records reach within approximately 37–38 milliseconds of the balance-complete event. One substantive near-final owner-2 job takes 0.587 seconds, including 0.340 seconds in navigation and 0.374 seconds in client request elapsed time; those scopes overlap. Owner 1 finishes with short neighbor jobs. The longest jobs remain approximately 2.25 and 3.29 seconds, but their duration alone does not identify the completion bottleneck. These are single diagnostic observations, not a measured critical-path decomposition or proof that network setup dominates waiting.

## Retained failed attempt and investigation

The third navigation-enabled timing attempt (`33-nav-on-3`) completes insertion in 7.473 seconds and balancing in 16.470 seconds, then exits before placement completes with `transport frame exceeds configured limit`. It does not reach final writer validation and is not a successful construction timing sample. The original three-pair cohort therefore remains incomplete; no replacement is silently substituted.

A diagnostic-only executable preserves the same behavior and frame bound while identifying request kind and request-encoding versus reply-prefix failure. A focused regression confirms that an oversized request on a reused socket is rejected before dispatch and does not corrupt a subsequent request. Another regression constructs a realistic 32-internal-node, 2,131-unit shared routing view with 768-dimensional full-precision centroids: its actual installation request is 22,550,510 bytes, above the 16,777,216-byte frame cap. This proves a reachable monolithic shared-view limitation, not yet the identity of the failed cloud request. The following section records diagnostic reruns and the bounded compact correction. The failed run and all earlier successful results remain retained.

## Bounded shared-view correction

Three diagnostic-only cloud reruns pass and do not reproduce the original failure. The specific failed request therefore remains unconfirmed. The corrected representation addresses the independently reproduced oversized installation case, which is consistent with the failure’s phase and error but is not proven to be its original cause.

Complete-view installation now encodes a versioned, bounded representation of full-width identifiers, revisions, ordered children and exact finite float bits. The regression request falls from 22,550,510 to 9,396,716 bytes and successfully dispatches through the unchanged 16MiB transport limit. Checkpoint and global view representations remain unchanged; both RPC peers must use the new format. Larger views can still exceed the bound and require a future chunked or otherwise bounded publication design.

Independent review fixes a dimension-validation ordering issue before acceptance: malformed child dimensions must be rejected before constructing a derived navigation buffer. Tests include a small malicious payload that would otherwise amplify into a multi-gigabyte allocation, malformed counts/flags/version/trailing data, duplicate metadata, nonfinite values, base64 bounds, negative zero and full-width IDs. The integrated native suite passes 315 tests, with the same three opt-in ignores and one unrelated exclusion. The fresh comparison holds this correction constant in both navigation arms; the original incomplete cohort remains separate.

## Final v3 transport and tail validation

Both exact 256-query replays pass with the final executable. The full 3M detailed transport smoke also passes. Through balancing, the coordinator opens 120 connections and reuses connections for 55,687 requests; all 55,807 sent requests receive framed replies. The coordinator server records 336 fixed processing workers, 62 accepted connections and zero admission rejections. Its 98 client application-error replies are attributed to readiness (2), node reads (37) and transfer preparation (59). These observations establish reuse and bounded worker creation under the workload; no isolated transport latency benefit is claimed.

In this diagnostic, the longest recorded owner-1 and owner-2 jobs take 1.696 and 2.370 seconds. The longest owner-2 job includes 0.803 seconds in navigation and 1.778 seconds in client requests. The final substantive owner-2 job takes 0.584 seconds, including 0.318 seconds in navigation and 0.384 seconds in client requests, and finishes approximately 50 milliseconds before the balance-complete event. Owner 1 finishes a 14-millisecond neighbor job approximately 57 milliseconds before that event. These inclusive times overlap. The latest-completion records now reach the end of balancing, but do not provide a complete dependency chain or independently bounded inter-host clock alignment.

## Fresh navigation comparison and decision

All six final v3 timing runs pass exact 3M writer validation. Three pairs alternate the order (off/on, on/off, off/on), restore the same 2M checkpoint and use the same executable. Both arms include persistent connections, bounded server pools and compact complete-view installation. Detailed diagnostics are disabled for primary timings. The cache is accepted for its insertion improvement and CPU savings, with no established balancing or total-construction speedup.

| Metric | Cache off, mean | Cache on, mean | Relative change |
| --- | ---: | ---: | ---: |
| Insertion elapsed, seconds | 8.057 | 7.528 | -6.6% |
| Balancing elapsed, seconds | 14.326 | 14.619 | +2.0% |
| Construction elapsed, seconds | 23.363 | 22.697 | -2.8% |
| Insertion CPU, seconds across both owners | 468.023 | 432.587 | -7.6% |
| Balancing CPU, seconds across both owners | 562.693 | 527.400 | -6.3% |

Insertion improves in every pair; the mean reduction is 0.528 seconds, with an approximate paired 95% t interval of 0.496–0.560 seconds. Balancing changes by +0.293 seconds, with an interval of −1.214 to +1.801 seconds. Construction changes by −0.666 seconds, with an interval of −2.470 to +1.138 seconds. Three pairs give limited precision, particularly for completion time. CPU reductions occur in every pair: insertion saves 35.437 CPU-seconds (interval 31.920–38.953), and balancing saves 35.293 CPU-seconds (interval 9.547–61.040).

Balancing work counts are similar but not identical: candidate rows average 918.84 million off versus 915.87 million on, validated child rows 452.98 versus 452.16 million, and owned routing distances 909.14 versus 908.54 million. Committed splits average 735.33 versus 739.33, and reassignment input rows 721,746 versus 726,678. Lower CPU therefore accompanies broadly similar work, but these evolving-tree runs do not isolate identical computation. Exact navigation replays establish output equivalence on fixed inputs. The [machine-readable summary](../operations/development33/navigation-v3-summary.json) retains ranges, every paired difference and work counts.

The complete construction timer includes work beyond insertion and balancing, so its difference is not the sum of those two phase differences. The transport implementation has no isolated repeated baseline comparison, as requested; these measurements establish the navigation-cache comparison on the new transport, not a causal transport speedup.

## Detailed navigation evidence and next target

The separate final detailed run passes. Its inclusive navigation CPU is 264.680 seconds versus 312.786 in the cache-off diagnostic; reply validation is 66.945 versus 82.456, and canonical ordering is 21.712 versus 42.688. These are single evolving-tree diagnostics with instrumentation overhead, not another primary timing pair. The reductions are consistent with removing repeated parent hashing; the fixed replay demonstrates that mechanism directly.

The cache-on diagnostic retains longest jobs of 2.636 and 3.992 seconds on the two owners. Its latest substantial owner-2 job takes 0.505 seconds, including 0.223 seconds in navigation and 0.368 seconds in client calls, and finishes approximately 64 milliseconds before balance completion. Another nearby job takes 1.259 seconds, including 0.371 seconds navigating and 1.026 seconds in client calls. The last recorded lightweight completions occur approximately 44 milliseconds before balance completion. Peer transport export retains 74 samples. Scope times overlap and the records do not establish a complete dependency chain.

The next useful investigation should connect each late job to its remote request type and the receiving handler's queue and execution time. Navigation remains a concrete CPU target, especially reply validation and distance work; the late jobs also spend material time in remote calls that may include transfer or publication. Request waiting alone does not identify network latency. Correlating these calls will distinguish a useful batching/overlap change from speeding computation that is already hidden behind another dependency. Scheduler changes require evidence of eligible work waiting; these traces do not establish that condition. Coordination remains accepted, with its larger-index revisit deferred as requested.

## Persistence, completion and retained machines

The final accepted configuration passes exact construction, first saving, unchanged saving and two fresh durable-reader validations. First saving takes 6.756 seconds; the unchanged save takes 3.504 seconds and reuses all 2,136 posting blocks (1,063 and 1,073 by owner), with zero posting blocks rematerialized. Fresh-reader checks take 50.653 and 52.456 seconds. This is a persistence integration check, not a repeated saving performance comparison.

The [accepted profile](../operations/development33/accepted-profile.json) and [completion receipt](EPOCH33-COMPLETION.json) bind the final source, executable, measurements and validation. Both existing sandbox machines are API-confirmed running, with no benchmark processes and their stop timers active for 2026-10-22T23:09:15Z. They remain running by user request. The recurring follow-up remains paused. The open compute reservation is updated without adding it to closed compute a second time. The final filesystem audit finds approximately 54.3 GB free on the coordinator and 6.1 GB on the peer; reclaim only verified disposable caches or copies before another large artifact-producing batch, preserving raw evidence and checkpoints.
