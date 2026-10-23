# Construction investigation and experiment record

The immediate objective is fast, correct single-owner construction on 100K vectors with 32 workers. The same investigation process continues through multi-owner construction, representative search and the full 100B goal. Epoch13 is complete and the sandbox instance is API-confirmed stopped. Direct cleanup remains enabled after the scoped single-owner assessment.

## How decisions are made

Work on the highest-value available question, considering expected impact, strength of evidence and time to learn. After every experiment, record what happened, interpret it, state the conclusion and limitations, and reprioritize this queue before selecting the next experiment. An unsuccessful experiment is useful evidence, not a reason to hide the result or repeat it without a new question.

Use the optimized unsharded implementation as the default design. For each proposed change, identify how the reference performs the operation and whether the dynamic difference is necessary for sharding, supported by measurements, or an explicit hypothesis to test. Remove unnecessary differences where possible. A single-owner shortcut must preserve a credible multi-owner path; do not replace the required global semantics with an easier problem.

Each experiment records its question, hypothesis, reference behavior, justified deviation, structural or working-set condition, source/settings, time and cost bounds, stop rule, measurements, interpretation and resulting decision. Fill execution bounds before admission. Use fixed-work local fixtures when they answer the mechanism question and complete-build measurements to establish practical benefit. Record work counts and correctness alongside timing. Larger tests are appropriate when they answer a question efficiently.

## Ranked investigation queue

| Priority and status | Question and evidence | Next discriminating step | Decision sought |
| --- | --- | --- | --- |
| 1 — Runtime reviewed; exact upload confirmation is pending | Does preparing leaf splits outside the structural lock improve complete construction? Local correctness tests pass; no speedup is measured. | Run the frozen off/on pair on 100K vectors with 32 workers and cadence 4,096 after normal approval and a fresh guarded window. The rejected window is closed. | Adopt only if complete-build evidence justifies snapshots and validation. |
| 2 — Local prototype accepted; native integration active | Can stage-level regrouping reduce repeated appends and destination locking? Seven focused tests pass; the fixed oracle reduces three appends to one for one destination, with no reduction when every row has a different destination. | Validate native policy admission and exact integrity, then review one bounded paired proposal. Memory accounting and external-hook rejection pass local review. | Preserve durable handles and phase visibility while measuring whether fewer appends improve complete construction. |
| 3 — Fixed-tree fixture complete; production integration deferred | Packed centroid scoring reduces full-record requests but adds chunk reads, yields zero cache hits and does not reduce total returned bytes in the tested fixture. | Revisit only after split and regroup measurements identify routing as consequential residual work. | Require actual provider-cost benefit and connected centroid publication before adoption. |
| 4 — Deferred | What vector-access cost remains after the decoded cache? | Revisit lookup, checksum and copying after split and stage-regroup measurements. | Optimize measured residual work rather than guessing from overlapping counters. |
| 5 — Queued | Does publication repeat avoidable reader and metadata work? | Compare reference reader lifetimes with phase boundaries and the published construction view. | Share bounded current readers while preserving immutable query generations. |

The baseline enables direct single-owner cleanup and the 384 MiB decoded cache, uses four queue-cache pages and scalar handles, and disables eligibility batching and local reassignment. The cadence fixture exposes two storage correctness defects; both fixes pass local regression and independent source review. Final package review and fresh shutdown guards precede remote execution. Detailed completed and adverse results remain below.

## Completed experiments and what they change

| Experiment | Observed result | Interpretation and conclusion | Queue decision |
| --- | --- | --- | --- |
| Epoch7: batch immutable-handle callbacks, 100K | Elapsed time is 159.525 seconds with batching versus 118.966 without. Both pass integrity checks. Callback counts fall substantially. | Fewer callbacks do not establish a faster build. Independent tree construction and run order limit causal attribution. | Keep scalar handles for diagnosis. Investigate synchronization rather than assuming larger batches help. |
| Epoch8: raise queue cache from one page to four, 100K | Queue commits fall to 236 and flag reads to 3. Balancing takes 102.218 seconds versus 102.314 in the prior baseline. Integrity passes. | Queue traffic is removable waste, but its removal does not materially improve observed balance time. This is not a controlled timing pair. | Retain four pages for this fixture; stop cache-capacity sweeps and promote authority/cleanup work. |
| Epoch9: eligibility batching off/on with one verified binary, 100K | Off/on elapsed times are 97.375/111.590 seconds; CPU times are 104.310/95.354 seconds. Balancing takes 83.977/98.687 seconds. Both pass integrity checks. The on arm discards 583,044 lookahead rows. | CPU work falls while elapsed time rises. Trees and run order differ, so this does not prove the source of the slowdown. Scalar version-request counters exclude batched checks. No complete-build speedup is established. | Keep eligibility batching off. Prioritize cleanup and synchronization, not another batching sweep. |

These trees have one internal root and roughly 70 leaves. They exercise leaf balancing but do not establish internal-branch pruning, representative recall, physical distribution or 100B economics. Timers for callers, queues, services and locks overlap and must not be added as exclusive time buckets.

Evidence: [epoch7 assessment](dynamic-review/100k-owner32-epoch7-assessment.json), [epoch8 assessment](dynamic-review/100k-owner32-epoch8-assessment.json), [epoch9 assessment](dynamic-review/100k-owner32-epoch9-assessment.json). CURRENT-PLAN.md contains the detailed code-based hypotheses and reference source locations. CONTINUATION-RUNS.md retains earlier results and failed attempts.

## Next result to record

Record the prepared-split paired result once its upload review is resolved. The optimized reference baseline is complete: native elapsed is 16.696 seconds, with 0.049 seconds of displayed insertion and 13.85 seconds of balancing. Integrity passes before commit and after reopen. Input-loading boundaries, allocator, cache policy and construction work differ from the dynamic run, so these values do not establish a causal speedup ratio. The insertion-boundary investigation promotes stage regrouping as the next available local experiment. Any paired experiment must report complete-build time, phase times, CPU, actual work, memory, tree shape and saved integrity before adoption.


## Lock scope and leaf payload checks

Source inspection confirms the frozen optimized reference does have a tree lock for structural updates, including child-navigation publication, internal splits and attachment. This does not justify holding a broad lock throughout dynamic leaf split preparation or ordinary insertion. The investigation must distinguish exclusive placement on one owner from concurrent workers mutating that owner's nodes, and identify the smallest synchronization needed for each operation. Independent leaf work should follow the reference's concurrency wherever possible.

The active full-record dynamic node store separates leaf metadata from posting rows. A leaf metadata record contains centroid, parent, row count and placement information; its adapter supplies empty ID, version and code arrays and asserts that posting rows cannot be written into the metadata record. Thus fetching this leaf metadata does not inherently fetch its posting list. Owner actors still retain posting payload separately, and cleanup can scan/copy memberships. Physical posting paging, residency and unused bytes fetched remain a separate scaling question; metadata separation does not prove those costs are bounded. Add a payload-access experiment if retained evidence shows unnecessary whole-list reads or copying, distinguishing routing metadata reads, cleanup membership scans and query posting reads.

## Epoch9 interpretation refinement

Construction-only counters strengthen the decision against treating fewer messages as sufficient progress. Eligibility batching reduces Current callbacks from 1,726,877 to 795,485, yet aggregate callback caller time rises from 29.65 to 31.70 seconds. Home-location messages fall from 2,999,746 to 2,008,884. Structural lock holding rises from 12.28 to 18.59 seconds and waiting from 5.57 to 10.52 seconds. These spans overlap and independently constructed trees differ; they support investigating synchronization but do not prove its causal contribution. Use the before-save counters for construction: later integrity auditing adds 100K home-location calls. Cleanup remains first; broader lock-scope work remains second. No new runtime is needed for this interpretation.

## Active cleanup experiment specification

The hypothesis is that, with exactly one physical owner, cleanup can check authoritative current versions and compact posting rows in one owner callback instead of copying memberships, resolving home locations and issuing repeated validation callbacks. This follows the reference's local version lookup while preserving the dynamic implementation's missing-version semantics. Placement on one owner eliminates cross-owner transfer gaps for this treatment; it does not eliminate concurrent mutations, so the callback must serialize correctly with version changes. Multi-owner and paged metadata paths retain their existing behavior until separately justified.

The first experiment is a fixed-work local comparison of the existing and direct cleanup paths. It must contain stale and live rows, duplicate IDs, absent current versions, distinct posting codes and enough rows to cross the existing 64-row validation boundary. Compare exact ordered IDs, versions and codes after swap-removal, removed counts, physical counts and published leaf lengths. Exercise concurrent version changes and verify multi-owner transfer-safe fallback and invalid-policy rejection. This fixture answers mechanism and correctness questions; it cannot establish build throughput.

The intended follow-up, conditional on passing tests and review, is one bounded 100K, 32-worker construction comparison with the same optimized binary and retained input, scalar handles, eligibility batching off and four queue-cache pages. Record cleanup rows and callbacks, insertion/balancing/publication time, CPU, lock measurements, tree shape and complete integrity checks. Use fresh output roots and shutdown guards. Final source hashes, execution deadline and cost bounds must be frozen in the runtime admission before launch; no cloud window is opened by this proposal. Accept a mechanism reduction separately from an end-to-end benefit, and reprioritize after the result even if timing is adverse.

Source inspection also finds a semantic difference that must remain explicit: the ordinary cleanup substitutes version zero for an absent version, whereas dynamic cleanup removes a row when its current version is absent. The treatment must preserve dynamic absence handling, including a stored zero version. Both existing paths use swap-removal, so a stable filtering implementation would not be an exact ordered-work comparison.

## Cleanup first local result

The first three local tests pass. On the fixed 515-row fixture, both paths retain the same 274 rows and remove 241 with exact ordered IDs, versions and codes. The existing path performs nine version-check callbacks, two posting callbacks and nine home-location calls; direct cleanup performs one posting callback and no version-check or home-location callbacks. The fixture includes duplicate IDs and an absent current version with stored version zero. A paused-callback test passes for concurrent version mutation, and the connected full-record saved-generation test passes.

This establishes the intended reduction in dispatch for fixed work and preliminary correctness coverage. It does not establish a complete-build speedup or correctness for all configurations; fallback/native tests and independent source review remain pending. Cleanup stays first in the queue. The paired runtime controller is prepared locally with source bindings intentionally unfilled, so it cannot execute before the implementation is frozen and accepted. Evidence: dynamic-review/single-owner-local-cleanup-v1/first.stdout and dynamic-review/single-owner-local-scrub-runtime-v1/HANDOFF.json.


## Cleanup local review decision

The coherent cleanup change passes independent source review, transfer and node-guard regressions, and six native policy cases. Invalid multi-owner, paged, unbounded and reference configurations reject before creating output. The three changed source files are frozen and their hashes match the reviewed checkout. The decision is to package the same-binary 100K off/on comparison; no complete-build benefit is established yet. Cleanup remains the highest-priority experiment. See dynamic-review/single-owner-local-cleanup-v1/root-review.json.


## Cleanup complete-build comparison admitted

The frozen runtime package passes independent verification of all957 source files, the required IDL link, exact three-file source replacement, controller tests and scientific defaults. The sole operator is dispatched for epoch10: one new optimized binary, cleanup off then on,100K vectors,32 workers, scalar handles, eligibility off and queue cache four. The fresh window is capped at1800 seconds and $2.016 compute, with600 seconds for build,300 for each arm and300 for preservation; no retry or extension. Independent shutdown guards and fresh identity/resource checks remain required. Results are pending; this admission is not evidence of a performance improvement.


## Epoch10 cleanup result

Epoch10 completes the100K cleanup comparison with one verified optimized binary. Off/on elapsed time is119.121/48.887 seconds and CPU time109.491/76.023 seconds. Balancing takes106.248/35.735 seconds; insertion remains8.470/8.788 seconds. Both arms pass100K membership and exact vector/query-bit checks. Cleanup snapshot/validation traffic disappears in the direct path, but work differs:39.344M versus32.142M scanned rows and69 versus66 splits. Aggregate cleanup identity waits fall from199.529 seconds to zero; concurrent waits overlap and are not exclusive wall time. The on arm records one add_missing_nodes event, so independent source/retry diagnosis remains open. The observed improvement is promising single-owner evidence, not a general2.4x scaling claim. No additional run is admitted. See dynamic-review/100k-owner32-epoch10-assessment.json.


## Residual costs after direct cleanup

The improved arm still performs947,928 logical vector gets,348,607 reader opens and2.898 GB of return copying; recorded decoding is0.239 seconds. It also performs813,472 node point reads with8.759 seconds of aggregate node-miss time. Structural lock holding totals9.373 seconds and waiting7.597 seconds. These overlapping spans do not identify exclusive bottleneck percentages. They keep structural lock scope and vector-access work high in the queue, while weakening any proposal to focus on decoding alone. The missing-node event is localized by phase counters to balancing at28,672 rows; exact target identity and interleaving are not recorded. Independent source/retry verification is pending before final reprioritization.

## New candidate: an unnecessarily small immutable-vector cache

Source inspection finds that the vector store rejects any decoded cache larger than32 vectors per owner, rather than merely choosing32 as a default. At768 dimensions this permits only98,304 bytes of retained vector payload. The improved100K arm records4,502 hits out of947,928 logical gets. The optimized reference shares immutable embeddings in memory, so this hard cap is a meaningful design difference requiring justification; bounded memory does not itself require a32-vector limit.

Promote this question alongside structural lock scope for the next decision: can a configurable byte-bounded cache recover reference-like immutable access for a realistic owner working set without changing membership semantics? First inspect handle/root invalidation and account for cache metadata, shared caller references and provider memory separately. If justified, choose one prespecified capacity and fixed request sequence with both reuse and eviction; do not run a capacity sweep or equate fitting100K with fitting100B. This is a candidate under review, not an implemented change or a claimed bottleneck. The retry regression and independent cleanup assessment remain the immediate work.

## Next selected experiment: bounded decoded-vector reuse

The hard32-vector cache limit has no identified correctness requirement: lookup already compares the complete immutable handle, and cache guards end before storage access. Select a configurable byte-policy experiment before a broad lock rewrite. Preserve the existing default and use one explicit384 MiB decoded-payload budget per owner for the eventual100K treatment. At768 dimensions this holds131,072 vectors and therefore the100K working set. This is a reference-like in-memory construction scenario, not evidence that a100B owner working set fits memory.

The implementation must check per-owner and aggregate budgets with overflow-safe arithmetic and report metadata overhead separately from retained vector payload. Caller-held shared references, provider buffers and reader pins remain separate memory categories. Local fixed-request tests must exercise reuse beyond32 entries, eviction under a smaller test budget, immutable-root/handle replacement, exact vector bits and corrupt/missing-content errors. Keep checksum and observation implementations unchanged so the eventual comparison answers the cache question alone. Freeze source and integrate native configuration before review. No new cloud execution is admitted yet.


## Cleanup accepted; bounded cache implementation selected

The cleanup assessment is accepted for the bounded nonpaged single-owner baseline: enable direct cleanup in subsequent diagnostic experiments, while preserving the source default and multi-owner fallback. The focused regression proves a failed registration leaves the current version intact and a subsequent real reassignment navigates to a replacement root with one current copy. It does not recreate the exact full-record race or force Retry within that same reassignment invocation; source control flow supplies the retry-loop ordering evidence. The runtime event remains preserved with this limit. The next active change is an explicit384 MiB per-owner decoded-vector payload policy, preserving default32-row behavior and all checksum/observation code. No cloud run is active.


## Cache experiment controls

The comparison keeps direct cleanup enabled, scalar handle lookup, eligibility batching disabled and four queue-cache pages in both arms. The baseline omits the new decoded_vector_cache policy and retains32 rows. The treatment declares402,653,184 payload bytes per owner and the same aggregate payload cap for one owner. Capacity is the budget divided by dimension times four bytes, rounded down. Keep FIFO replacement, insertion warming behavior, checksumming and observation unchanged. Report cache-owned payload and metadata separately from shared caller references and provider allocations. The operator prepares controls locally with source bindings unfilled while implementation proceeds; no machine is started.

## Decoded-cache first local result

Three focused tests pass. Replaying the same64 immutable vectors twice yields64 cache hits and four reader opens when the explicit budget fits64 vectors, versus zero hits and eight reader opens with the default32-row cache. A deliberately small eight-vector budget also produces zero hits on that sequential tape and respects its payload cap. Exact vector bits match in every case; caller-held shared vectors remain valid after eviction.

A concurrent old-root completion can replace a newer cache entry, but full-handle comparison forces a subsequent new-root request to miss rather than return old bits. Tests also reject mismatched SHA, dimension, home, absent key and wrong root/content binding. Admission tests cover aggregate budget failure and arithmetic overflow. These results establish cache reuse and identity/error behavior, not construction speedup. Native wiring, connected coverage and frozen-source review remain pending; cache reuse stays first in the queue.


## Decoded-cache integration result

The connected full-record saved-generation witness and native build pass. Both native default and explicit-byte configurations complete with the same verified vector-bit digest; invalid aggregate budget, too-small budget, overflow and reference-treatment configurations reject. These65-row development-profile runs verify wiring and integrity, not throughput. The decision is to freeze the coherent source change for independent review and package one100K comparison if accepted. No change to queue priority or cloud admission follows from the small-run timing.

### Current cache experiment execution

The reviewed package is admitted for one bounded comparison. The question is whether retaining the 100K-vector decoded working set reduces repeated reader access and total construction time. The default cache holds 32 vectors; the treatment allows 384 MiB of vector payload, enough for this input. Metadata, caller-held vectors, and other caches are accounted for separately. Tree shape and work counts must accompany timing because independently constructed trees can do different work. The sole operator is dispatched; measurements are pending.

## Epoch11 cache result and decision

Epoch11 completes both 100K-vector arms with full membership and exact vector/query-bit checks. Default versus 384 MiB cache elapsed time is 54.012 versus 40.709 seconds; balancing is 40.465 versus 26.980 seconds and CPU is 80.071 versus 62.152 seconds. Reader opens fall from 377,590 to 6,475 and copied vector bytes from 3,047,743,488 to 279,220,224. Logical gets differ (995,416 versus 955,088), as do splits (68 versus 65), reassignments (54,836 versus 53,071) and leaves (69 versus 66). Both trees have depth one and zero missing-node counters. The result supports retaining an explicit bounded cache for this diagnostic, with timing qualified by unequal work and fixed run order. It establishes neither cache-capacity-crossing behavior nor large-scale economics. Structural synchronization becomes the highest-priority implementation question; further cache sweeps are deferred. See dynamic-review/100k-owner32-epoch11-assessment.json.

## Epoch12 result and priority decision

Epoch12 passes complete100K membership and vector/query-bit checks in both arms. Local reassignment performs26,615 successful transactions with no reported errors, retries or missing-node events. Structural acquisitions fall25,331→168, but elapsed time is39.208seconds off versus40.963on; balancing is25.426versus27.347seconds. CPU also increases61.686→63.230seconds. Trees have equal final68-leaf shape but different split/merge/scrub work. Retain the mechanism as an opt-in and keep it disabled in the performance baseline because no build-time improvement is established. Prioritize a reference-like larger balancing interval before a broader split-lock redesign. See dynamic-review/100k-owner32-epoch12-assessment.json. The worker is validating staging and split admission locally; no subsequent cloud experiment is admitted.


## Cadence storage validation and next decision

The cadence correctness fixture now passes all 8,194 current vector checks and preserves the old snapshot. Two narrow storage fixes correct split item accounting and preserve the identity of an indivisible oversized posting block. Independent source review accepts the frozen four-file change for packaging; a fresh optimized storage dependency and benchmark build is required. The next experiment compares balancing every 4,096 versus 100,000 insertions on the same 100K vectors with 32 workers. Both arms use identical split admission and cache settings, with local reassignment disabled. The sole operator is preparing the package; final package review and fresh shutdown guards precede cloud execution. Epoch12 remains closed and API-confirmed stopped.

The local fixture proves admission rejection preserves attached state, successful balancing and save preserve current vector and posting-code bits, and the prior saved generation remains readable. Five focused storage regressions and the native compile pass. Both failed attempts remain retained. This 32-dimensional fixture does not establish 100K throughput or memory feasibility. The next measurement remains cadence; broader lock redesign and cache sweeps remain lower priority. No additional cloud spending occurs during this local validation. See dynamic-review/reference-cadence-validation-v1/HANDOFF.json and root-review.json.


## Epoch13 cadence result and decision

Both 100K-vector arms pass complete current membership and exact vector/query-bit checks. Balancing every 4,096 insertions takes 46.531 seconds versus 59.202 seconds for one 100K insertion interval. Insertion takes 8.623 versus 4.273 seconds; balancing takes 33.523 versus 50.635 seconds. CPU falls from 65.485 to 58.151 seconds. Larger cadence alone is not an accepted elapsed-time improvement.

The long interval has 75 splits and 43,378 scrubs versus 69 and 28,625 for short intervals. Structural lock holding rises from 7.839 to 35.809 seconds; peak distinct subtree workers falls from 22 to 15. These spans overlap and trees differ, so they motivate inspecting split preparation rather than proving its causal cost. The short arm reports one missing-node add event; final integrity passes, but the exact retry sequence is not witnessed. Preserve this adverse counter.

Keep 4,096 as the measured dynamic performance baseline for now. First prepare the genuinely optimized buffered reference with matching input and construction settings, then use that comparison to guide split preparation changes. The implementation worker owns the isolated reference adapter. API shutdown and spending closure remain pending operator confirmation. See dynamic-review/100k-owner32-epoch13-assessment.json.


## Optimized reference validation gate

The isolated retained-input adapter passes three focused tests covering exact input bits/order, input rejection, buffered insertion, a real split, packed navigation and old/current saved-vector readback after writer drop. Native harness integration remains under validation. Root review identifies that saved-vector hashes prove storage contents but not unique tree membership. Existing root-reachable validation compares ID sets and therefore hides duplicate current postings. The worker adds duplicate detection only to benchmark validation; the indexing kernel remains unchanged. The reference controller must enable existing pre/post posting validation and require the additional uniqueness check before reporting a comparable build. One 100K reference run is proposed against retained epoch13 long-cadence evidence; no cloud window is admitted.


## Prepared split first correctness result

Four focused tests pass: paused preparation preserves source visibility while disjoint structural work progresses; fixed prepared groups preserve current and old saved generations; current-version-only and centroid changes reject before detach and re-queue to eventual balance completion; failure of the second scratch reservation releases the first charge before copying or detach. The receipt is dynamic-review/prepared-leaf-split-v1/tests-second.stdout. This establishes the exercised mechanisms, not construction speedup, all possible mutations or physical distribution. Explicit allocation accounting and frozen independent source review remain pending; the policy remains default off.

## Prepared split local review

The optional split path passes five focused tests and the existing default-path regression. It keeps the source leaf visible while fetching vectors and clustering outside the structural lock, then validates the source before installation. A stale preparation explicitly queues another balance attempt. All frozen source and evidence hashes match. These tests establish mechanism correctness within their fixtures, not construction speed or multi-owner correctness. The next available task is native policy wiring and a bounded paired proposal; the reference measurement remains first priority when upload review permits it. See [root review](dynamic-review/prepared-leaf-split-v1/root-review.json).

## Prepared split native wiring accepted

Root verifies the frozen two-file native integration and every evidence hash. Three valid native configurations preserve all 65 vector bits and exact query-distance bits; five incompatible configurations reject before output. Parent replacement and physical posting reorder reject stale preparations and preserve 64 current and old saved vectors. The paired specifications differ only in the feature flag. The proposal keeps 100K vectors, 32 workers, cadence 4,096, 64 GiB split scratch and 4 GiB output per arm. Local runtime packaging is next; no cloud execution or speedup is established. The reference measurement retains priority and its upload rejection remains unresolved. No new cloud compute or storage commitment is incurred. See dynamic-review/prepared-leaf-split-native-v1/root-review.json.

## Routing centroid investigation

The source comparison identifies routing work that the document-vector cache cannot remove: full-record navigation clones centroid and child arrays on cache hits, decodes complete routing records on misses, and fetches selected candidates again. Posting rows remain separate from these routing records. The optimized reference scores borrowed slices from shared packed child centroids. Retained node-miss timers combine multiple consumers and overlap, so they do not establish the cost attributable to navigation or a speedup. A 100-child, 768-dimensional packed centroid array occupies 307,200 bytes before metadata and exceeds the current node-record/cache limits. Any prototype therefore needs separately bounded versioned chunks and explicit centroid-update visibility. Keep reference and split measurements ahead of this candidate; defer implementation until their evidence is available. See dynamic-review/packed-centroid-readiness-v1.md. No source changes, tests or cloud spending result from this investigation.

## Prepared split runtime review

The frozen local runtime package passes root review: all 957 source files, the sole IDL link and packaged operator hashes match, and an independent rerun passes all 15 controller tests. The pair uses one newly compiled binary and changes only the prepared-split flag. Shared absolute deadlines and existing resource limits remain enforced. The controller requires 1,080 seconds remaining within a 1,200-second window; slow setup rejects admission rather than extending it. The package remains local and cannot replace or bypass the rejected reference upload. Reference measurement is still first priority; no new compute or storage commitment occurs. Evidence: dynamic-review/prepared-leaf-split-runtime-v1/root-review.json.

## Local work selected during upload block

The next available question is whether reference-like packed scoring preserves exact traversal results while avoiding per-child node materialization. The existing worker owns a test-only fixed-tree comparison using the actual current traversal as baseline. This advances the queued mechanism investigation without changing frozen benchmark packages or adopting an unmeasured production design. The fixture must separate logical fetch/copy counts from real provider I/O and account for bounded scoring chunks. Connected update/publication visibility remains a prerequisite for adoption. Cloud spending remains unchanged.

## Packed centroid fixture result and decision

The fixed 132-node, 768-dimensional fixture passes exact ordered ID and distance-bit parity for 32 queries across cold, repeat and eviction passes. Each scorer performs 4,192 distance evaluations per pass. Packed access reduces full-record requests from 4,672 to 480, but adds 544 chunk-file reads, records zero cache hits and slightly increases returned payload. All three internal nodes are expanded; this is a mechanism test, not selective-pruning or quality evidence. Root source review and evidence hashes pass. Keep production integration deferred: actual provider cost and connected centroid publication remain unproven, and the pending reference/split measurements have priority. No cloud spending is attributable to this local fixture. See dynamic-review/packed-centroid-fixture-v1/root-review.json.

## Matched reference completes with a construction gap

The optimized reference completes 100K vectors with 32 workers and one 100K checkpoint. Both precommit and reopened membership checks report 100,000 unique current rows and zero duplicates; the saved vector-bit digest matches dynamic epoch13. Displayed insertion is 0.049 seconds and balancing 13.85 seconds, versus dynamic long-cadence 4.273 and 50.635 seconds. Both finish with 76 leaves and 75 splits, but reassignment work differs. Reference navigation reports about 11.8K missing-node observations under its concurrent semantics; preserve this distinction from the dynamic complete-view contract. These are construction measurements, not query-quality evidence. The gap supports measuring the already prepared split change next, after full result/lifecycle review and fresh admission. Shutdown and final cost receipts remain pending. See dynamic-review/matched-reference-epoch3-assessment.json.

## Prepared split upload rejected before execution

Normal approval review rejects the prepared-split package despite the quoted standing and recent general upload authorizations. It requires direct authorization naming this package and destination. Zero bytes transfer; no build or science runs. The exact confirmation question is pending and the sole operator is closing the instance/window. The next useful local task reconciles reference and dynamic insertion timer boundaries and buffering, using the completed matched reference evidence. No source change or competing cloud operation is authorized by that investigation. See fleet/prepared-leaf-split-runtime-v1/epoch1/upload-rejection.json.

## Insertion boundaries and batching decision

The insertion comparison includes different work. Dynamic insertion includes 0.496 seconds of nested input rereading/decoding/hashing and 13 synchronous durable staging calls totaling 2.297 seconds. Reference insertion starts after loading resident vectors and defers durable publication to commit. Dynamic insertion also sends 1,568 small batches to the owner, whereas the reference joins and regroups worker results before appending to the initial root. Worker flush durations overlap and cannot be treated as exclusive elapsed time. Both initial insertions target a single root; packed child-centroid access cannot explain this initial gap. Promote bounded stage-level regrouping above additional vector lookup/cache work as the next construction candidate after the pending split comparison. Preserve durable handles, atomic membership and explicit additional buffer admission. Source investigation and proposal: dynamic-review/matched-insertion-gap-v1.md. No performance gain is yet measured.

## Bounded insertion regroup prototype selected locally

The completed timer analysis supports an isolated local implementation question: can workers join and regroup one already admitted staging chunk before an atomic destination append, eliminating repeated small destination transactions? The existing worker owns a default-off single-owner prototype and exact-order, membership, capacity, failure and saved-generation tests. Additional delta and overlap memory require explicit checked admission; durable handles and public phase visibility remain unchanged. This local work does not alter the frozen prepared-split runtime or bypass its pending upload approval. No new cloud spending is authorized or incurred by the local task.
