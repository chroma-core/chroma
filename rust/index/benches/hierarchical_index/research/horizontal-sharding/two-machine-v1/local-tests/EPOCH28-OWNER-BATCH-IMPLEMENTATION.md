# One transaction moves one source’s rows to one peer’s leaves

The writer groups a source leaf’s compatible destinations on one other owner into a bounded transaction and compacts the source once. The source owner and the complete destination bundle use the same two-role acknowledgement sequence as a pair transfer. Same-owner transfers keep their direct bulk path, and the pair API remains available. Candidate28 changes no navigation, scoring, metadata dictionary or save behavior.

Preparation fixes the run, epoch, attempt, producer, exclusion, full source/destination identities, centroid digests, IDs and codes. Each owner reserves its complete leaf bundle in owner order and validates current postings under short ordered node locks. Navigation remains readable. All destination capacity and the replacement source list are prepared before membership edits. Source removal precedes destination activation. Exact step outcomes are retained for replay; terminal records release resident handles and replacement buffers. Unknown preparation acknowledgements or uncertain partial edits fail the run closed and block saved state.

Payload chunks contain at most128 destinations,32,768 IDs and512KiB of codes. These are frame and metadata bounds, not routing caps: every remaining group continues in deterministic chunks. Each fully committed construction chunk schedules its source and destinations before a later chunk can reject. The direct diagnostic API leaves scheduling to its caller so its protocol counts remain comparable with the existing pair oracle.

| The fixed24-move fixture | Pair transactions | One peer bundle |
| --- | ---: | ---: |
| Protocol endpoint calls |88|11|
| All remote calls, including eight destination routing reads |56|14|
| All engine calls |104|20|
| Source compactions |8|1|
| Source entries scanned |16,300|2,048|
| Framed bytes |75,429|55,827|

The fixture uses2,048 source postings, eight destinations with three eligible IDs each,768 dimensions, existing destination rows and two untouched leaves. Real loopback TCP connects two owner services in one native process. Posting maps and all codes equal the pair/local-bulk oracle. The diagnostic excludes navigation, construction, input I/O and cache loading. Native thread CPU is unavailable, and inclusive wall scopes overlap; the test establishes work reduction, not physical latency or protocol-versus-transport CPU attribution.

Validation covers exact payload/proof replay, inactive staging, changed dependencies, reciprocal callers, genuine split claims and producer identity, lost successful step acknowledgements, an ambiguous successful Prepare acknowledgement, partial edit faults, chunk bounds and phase close/seal/save behavior. Controlled red tests expose an epoch-opening registry race and a committed-chunk scheduling gap; both corrected paths pass. The chunk regression releases the second target’s real claim, retries, runs balancing and verifies queue drain, all8,058 IDs and codes exactly once, and no oversized leaves. A delayed opener also cannot regress the current epoch or clear its records.

The applicable native suite passes257 tests with one documented unrelated local-writer test excluded. All three native command-line tools build. The exact commands, failure logs, source archive, full file comparison, source hashes, native artifacts and unchanged staged index are bound in `EPOCH28-OWNER-BATCH-VALIDATION.json`.

Whole-bundle contention remains an explicit risk: one unavailable destination rejects the clean predecision bundle; there is no accepted-subset negotiation or automatic fallback. Like the pair driver, preparation validates the newly captured centroid dependencies but does not recompute routing eligibility from those snapshots. Only one source and one peer participate in each transaction. Physical construction overhead and contention are the next measurement, with target-group counts, transaction counts, rejected work, copied bytes and latency reported separately.
