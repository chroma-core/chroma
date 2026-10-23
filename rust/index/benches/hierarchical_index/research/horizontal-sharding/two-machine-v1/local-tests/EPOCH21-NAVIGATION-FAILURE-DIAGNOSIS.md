# Navigation across shared centroid publications

A centroid-only shared-tree update can exhaust construction navigation's retry budget even when every owned route remains valid. The controlled regression reproduces the exact epoch21 error by publishing one compatible view after each real navigation snapshot. The repair keeps one immutable shared scoring view for an attempt and accepts newer publications with the same routing topology. A change to structure or ownership still restarts the whole global beam.

## Physical evidence and its limit

The retained 3M run completes the 2M balance and redistributes existing branches across both physical owners. The final 1M add completes in 31.920034481 seconds. Its balance fails after 0.510487098 seconds with `split leaf 18446744073712431530: split self reassignment from 18446744073714361113: construction navigation exceeded its whole-attempt retry budget`. The evidence is `operations/window-epoch21/candidate-progress.json` and `operations/window-epoch21/3m-distributed/run-receipts.json` relative to the two-machine evidence folder. This is a failed construction attempt, not a 3M performance result.

The physical failure receipt does not preserve individual rejection reasons. The same generic error can follow a shared sequence change, a genuinely retired owner route or an incompatible internal/leaf image. The controlled result proves an unnecessary invalidation that produces the exact symptom; it cannot establish that every physical retry has this cause. The new terminal error retains the last reason, pinned/current publication sequences, query rows, attempts and elapsed time for the next bounded execution.

## Controlled mechanism

The test needs two routing levels: a shared root followed by an owned internal node with two leaves. It uses eight-dimensional vectors, two posting memberships and one query. A test-only hook runs immediately after the real construction caller captures its view; it uses the actual publication manager to publish a new centroid/revision while retaining every node identity, parent, child, type and placement. No timing or random scheduling is involved. This is the smallest structural condition needed to exercise a shared view followed by owner expansion; a corpus benchmark adds no information about this mechanism.

The unchanged caller retries eight times and returns the exact whole-attempt error in 0.00 seconds. The relevant source checks compare the latest global publication sequence before each local or remote level. That comparison rejects even an entirely owned remote request after an unrelated centroid publication. The red log is `epoch22-navigation-red-02.log`.

The leading explanation is global sequence invalidation of compatible routing. Its prediction is that a centroid-only publication causes a restart without any owner returning a stale route; the controlled test confirms that prediction. Actual retirement or placement change is a second explanation for the physical message and remains a required rejection case. Transport/phase failures have different terminal paths and do not explain this controlled symptom.

## Repair and safety boundary

Every publication prepares a cached identity for its routing topology alongside its packed centroid rows. The identity includes the root identity/type/dimension, all shared record keys, node identities and parents, ordered child identities/types, and ownership-unit identities and placements. It excludes centroid values, packed scoring values and broad revisions, which can change without changing a route. Preparation computes the digest once per published or installed view; navigation compares fixed-size identities rather than walking the shared tree at every level. The cache has no wire representation and does not affect semantic view equality. Receivers rebuild it before installing a view.

Each navigation attempt pins the complete immutable shared scoring view. All queries and owner groups use that same view and request sequence. A newer compatible centroid publication does not replace its scoring rows halfway through a beam. Owned internal records still come from their actual authority and must match the requested node identity and placement. The caller checks topology before expansion and before returning a complete result. A topology or ownership change restarts the entire attempt; an owned stale response still rejects the attempt. The global per-level candidate selection algorithm and reply identity/completeness validation are unchanged.

The retry bound remains eight complete attempts. Persistent structural churn still returns an error with its last rejection; the repair does not turn repeated incompatible changes into successful navigation or increase the cap. Posting transfer and structural installation retain their own current-membership and dependency revalidation.

## Validation

The corrected mechanism completes in one attempt. Seven focused navigation tests pass, including publication-time compatibility, wire reconstruction, owned retirement/relocation rejection, replacement-boundary retry, finite structural churn and a real loopback TCP owner. The TCP test keeps distinct node/vector IDs on the two owners and makes one global beam select leaves from both owners. The focused log is `epoch22-navigation-focused-02.log`.

The consolidated applicable suite passes 213 tests in 5.39 seconds with the same one known local-writer concurrency exclusion used by the baseline. It is not a claim that the excluded test passes. All three native command-line benchmarks compile in 18.41 seconds. Exact commands, source hashes, executable hashes, retained failed setup/assertion attempts and the unchanged staged-index hash are recorded in `NAVIGATION-SNAPSHOT-VALIDATION.json`. No cloud or larger vector benchmark runs as part of this diagnosis.

The runtime changes are in `distributed/engine.rs` and `distributed/construction.rs`; focused regressions are in `distributed/engine_endpoint_tests.rs`. The only save-file change initializes the new cache field in an existing test view literal. Paths are relative to `rust/index/benches/hierarchical_index` in the isolated single-shard checkout. These tests establish the mechanism and routing safety boundary, not a measured end-to-end speedup or complete 3M correctness result.
