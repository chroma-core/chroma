# Two-machine construction and matched comparisons

**Current decision:** Lyon directs stopping larger comparisons and fixing the owner-local regression first. Epoch8 is interrupted during 2M balancing, with partial evidence preserved and both machines API-confirmed stopped. No further large test is scheduled. The current plan uses minimal reproductions and focused profiles to distinguish reservation-history scanning, repeated source compaction and navigation/allocation work. Static candidates are not measured latency attribution.

The experiment implements one evolving tree whose subtrees and postings are physically owned by two machines. It begins on one owner, preserves global navigation and neighbor eligibility, and moves existing branches only at shard-balancing boundaries. Passing isolated protocol tests is preparation, not evidence that the distributed engine works.

## Current gate: explain the initial-owner overhead

The valid standard 100K distributed run spends 14.84 seconds balancing versus 4.96 seconds for the reference, with roughly three times the process CPU work. The empty peer exchanges only 149 application bytes during that phase. This is an owner-local implementation or work-count problem to diagnose before larger tests. The earlier near-reference single-owner implementation remains an important comparison, but its different source must be identified explicitly.

The next diagnostic exercises the actual reservation manager with one fixed leaf and zero versus 64 completed attempts. It counts retained records visited for the same uncontended request and checks the construction lifetime that permits history to grow. Repeated source compaction and query/reply allocation remain alternative hypotheses. A mechanism result identifies avoidable work; only a relevant construction measurement can establish its contribution to the elapsed regression.

## Current sequence

1. Reproduce a specific overhead mechanism with the smallest fixed diagnostic that exercises it. Preserve the adverse result and record actual work counts.
2. Apply and independently review a consequential correction while preserving unique membership, replay protection, conflict ordering and navigation semantics.
3. Validate the same diagnostic and use a focused construction profile no larger than needed to reproduce the observed bottleneck. Separate useful work, copying, waiting and bookkeeping.
4. Record results, interpret them and reprioritize. The 3M comparison and refreshed distributed performance sequence remain deferred; they are not automatic next actions.

## Placement and workload conditions

Each final owner must contain approximately 30–70% of all vectors. Record per-cycle and final owner membership counts, handoff bytes/time, remote navigation, posting changes and worker activity. A final balanced save alone cannot establish that both owners performed construction work.

Report full-vector storage and caching separately from tree construction. The distributed input adapter reads a verified immutable corpus independently of ownership and currently gives each owner a bounded resident cache of that corpus. This duplicates cached bytes, not current postings. The reference harness additionally maintains and saves its writer's embedding store. Keep input verification/cache warming, tree construction, full-vector persistence, saved index publication and validation visible as separate costs; do not attribute their different storage paths to sharding or present omitted persistence as an algorithmic speedup.

The standard 100K run has one 100K insertion/balance interval. Its initial construction belongs to the initial owner; redistribution afterward exercises placement, saving and distributed query paths. Label that limit explicitly. The separate two-50K diagnostic must redistribute after its first interval and prove that the second interval performs useful construction work on both owners.

The 80% overload and 70% relief policy needs enough aggregate receiving headroom. For the two-50K diagnostic, use an explicitly recorded vector-budget ramp: 40K per owner after the first 50K interval, then 80K per owner for the final 100K. At 50K the whole root cannot fit within either destination's 28K receiving target, requiring selective refinement into existing child subtrees. This ramp changes placement capacity only; both arms use identical two-50K construction cadence and clustering settings. Actual memory headroom must separately cover ingestion and scratch space.

For 3M, the initial planned fixed vector budget is 2.16M per owner: overload above 1.728M, relief/receiving target 1.512M. The 2M phase should trigger redistribution before the final 1M insertion. Branch indivisibility and data skew mean arithmetic alone does not guarantee final occupancy; observed placement and useful work are acceptance gates. No hidden tuning sweep is authorized by this plan.

The [implementation comparison](IMPLEMENTATION-DIFFERENCES.md) records source-level differences that need attribution. It separates necessary distributed coordination from potential missing reference optimizations.

## Execution bounds and evidence

Use only sandbox account 079819354890 in us-east-1. Reuse approved worker i-011ff180f03584f40 and driver i-00268d1e1dfea0357, subject to fresh identity/resource checks and available capacity. Both now use r6i.16xlarge with 32 physical cores; the stopped-instance driver resize is recorded in the operations receipts. Any stopped-instance resize must preserve volumes and be recorded. Current source is /private/tmp/chroma-hierarchical-spann-sharding/single-shard. Leave the primary workspace untouched.

Before each cloud window, freeze source/configuration hashes, exact resource bindings, fresh rates, input identity and passing review. Establish independent local and remote absolute shutdown guards. Limit the first physical correctness execution to 10 minutes of workload and a 45-minute whole window; stop on invariant failure, unresolved outcome, lost progress, memory bound or deadline. A later performance window may be at most 90 minutes including build/setup, with 600 seconds per benchmark process and no automatic retry. Exact compute caps depend on verified rates. Preserve data, failed runs and source epochs; collect only small evidence to the Mac. Verify both machines stopped through the API and reconcile spending once.

Physical correctness and exact-version S3 restore on the other machine pass in window-epoch1. Both machines are API-confirmed stopped. Fixed performance comparisons are next; no throughput result is claimed from the correctness fixture.

## Gate after the first 100K attempt

The standard reference passes. The distributed run reaches a progress failure during balancing before placement or save. A 4,096-vector local fixture with branching 100, split threshold 128, merge threshold 32 and 32 workers reproduces the same failure. A deterministic endpoint test isolates an unrelated posting append invalidating a transfer with unchanged centroid and placement. Fix that dependency check while retaining structural snapshot revisions, and ensure the first unresolved job cancels outstanding waits promptly. Require negative tests for genuinely changed centroids and placement, exact memberships and codes, the broad-tree regression, and independent source review before a new cloud window. Then rerun physical correctness and the failed 100K arm before advancing to the remaining comparisons. Preserve the successful reference measurement and every failed attempt; no size or tuning sweep is needed.

## Reviewed repair admission

The transfer dependency repair passes 144/144 native tests and focused independent review. [The evidence](local-tests/POSTING-DEPENDENCY-FIX.json) records the deterministic failure, dense-tree reproduction, adverse endpoint changes, cancellation checks and exact source hashes. Epoch3 builds that source under fresh guards. Run physical correctness first, then the standard distributed 100K arm, the matched two-50K arms, and the 3M arms. Reuse the valid epoch2 standard reference result. Stop and preserve evidence on any invariant failure.

## Current merge regression

Prioritize the merge evacuation race exposed by epoch3. A balance job is unique for its source node, but other leaves can still transfer vectors into that source. Test this interleaving deterministically. Match the reference by preserving and rescheduling arrivals, and retire only after protected current-emptiness and structural-dependency checks. Then rerun the coherent suite, independent review, physical correctness, and the failed 100K arm. Retain the approved workload settings and all adverse evidence.

The focused review also checks the initial preferred-neighbor transfer. A row can leave its proposed source before endpoint protection, so a known pre-mutation rejection must trigger current-membership re-evaluation. Unknown or poisoned outcomes remain fatal. The next source freeze includes both merge interleavings and the protected empty-retirement check. The preliminary epoch4 archive remains retained but is not admitted.

## Current visibility gate

The coherent 149-test suite exposes a parent/child scoring-centroid mismatch in the repeated 32-worker two-owner fixture. Keep cloud execution stopped. Establish the failing parent context and test the shared-centroid publication ordering: a refresh must not skip its publication turn solely because an unprotected cached value matches its earlier observation. A controlled ordering test must distinguish a real publication race from other owned-parent update paths. Do not close the gate with a successful rerun alone. The existing protocol worker owns this narrow path and test, the engine worker remains the sole compiler, and the reviewer checks the combined result.


## Shared-parent replacement visibility gate

The repaired diagnostic captures a stale scoring centroid in a newly created shared parent whose owned child has moved to the second owner. Test the ordering where an owned structural update prepares its refresh, shared balancing replaces that parent, and the refresh observes the old parent missing from the current shared view. Both the completion call site and the refresh itself must follow current parent identity under compatible publication protection. Shared reparenting must not publish an obsolete scoring centroid merely because its owner-side parent update uses a fresh revision. Require a controlled failure before the fix, matching child-centroid assertions afterward, the coherent suite and independent review. Both machines remain stopped.


## Independent local-writer concurrency failure

The full native wrapper passes 153 of 154 tests after the distributed visibility repair. Its remaining failure is in the separate local HierarchicalSpannWriter concurrent parent/leaf split test: a leaf split reports no reachable parent. The writer source exactly matches the frozen epoch4 candidate source, and the test reproduces independently without constructing a distributed owner or shared runtime. It differs from the matched-reference checkout, so this finding does not establish a defect in the retained reference benchmark binary. Keep the failing log and isolated reproduction. This distributed milestone requires its applicable 153-test suite, compiled CLIs, independent review and physical correctness; it does not silently count the full wrapper as passing or expand into repairing the separate local writer.


## Reservation validation at internal branching

The distributed3M run stops at200K with125leaves and128nodes during its second balance. Transfer reservation retries report that the region is not granted. Test a queued request for unchanged leaves while another branch publishes a structural edit. Determine whether a global topology counter rejects compatible work, and preserve rejection when requested identity, placement or relevant ancestry actually changes. Require bundle atomicity, conflicting FIFO and cancellation tests. After a reviewed repair, rerun physical correctness and the three fixed distributed arms on one final source; reuse the unchanged reference results. The completed epoch5 candidate measurements remain valid for that source and are preserved separately. This is a source-change replacement, not a size or tuning sweep.
