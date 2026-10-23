# Transfer admission and progress review

The replacement epoch6 3M arm still fails at 200K with the generic transfer-region rejection. The queued-request repair remains supported by its controlled regression, but it does not establish the cause of this later failure. The inspected endpoint message hides whether rejection occurs on initial admission or after queueing. The new diagnostics retain the initial and final status, attempt, role, source and destination identities, captured/current topology sequences, and a specific normalization or ancestry rejection reason. These diagnostics improve the next failure’s evidence; they cannot reconstruct the missing ordering of the retained epoch6 failure.

## Controlled mechanisms

The initial-admission regression reproduces a separate global-sequence rejection in `transfer-capture-admission-red-02.log`. A transfer captures its request, a real split changes a disjoint subtree, then the endpoint submits the captured request. The initial state is Replan solely because captured sequence 0 differs from current sequence 1. The corrected actual endpoint reaches protection in `transfer-capture-admission-green-01.log` in 0.02 seconds. This exercises the interval before queue admission that the queued-request correction does not cover.

The repair adds a current-endpoint admission operation for posting transfers. It normalizes the requested live roots, checks their placement counters and captures their current ancestor paths atomically under the region manager’s bookkeeping mutex. It retains the fixed request body and same-attempt outcome for duplicate detection. The normal snapshot-based request operation retains its global sequence fence for structural callers. A posting transfer can bind current paths because it edits stable source/destination leaf identities and separately validates current node type, centroid dependency and membership under the granted region; it does not publish a structural edit against the captured ancestor snapshot. Queue dependency checks, whole-bundle exclusion and ordering remain unchanged.

The admission adverse matrix verifies that stale placement and retirement still return Replan, duplicate identical requests retain their result, changed duplicate bodies conflict, and structural snapshot callers still reject an outdated sequence. Existing endpoint regressions continue checking changed centroids, placements, internal-node replacements and source/destination membership failures without leaking resident reservations. The reviewed call-site delta uses current-endpoint admission only in the posting-transfer handler.

The reassignment regression identifies a second mechanism in `reassignment-progress-deadline-red-01.log`: a legitimate first transfer takes 5.1 seconds and succeeds, then one destination-centroid rejection exhausts a deadline measured from the operation’s start. The failure has completed one vector and retains one pending vector after one round and one rejection. The corrected case completes in `reassignment-progress-deadline-green-01.log` in 5.12 seconds.

The progress repair restarts the same five-second interval only after a nonempty transfer group completes all endpoint acknowledgements. Rejected, uncertain or failed groups never restart it. This deliberately permits more total elapsed time when the operation completes useful work; it is a consecutive no-progress allowance rather than the old total-runtime allowance. The 1,024-round cap and absolute workload deadline remain unchanged. The clock is checked between reassignment rounds; individual navigation or transfer operations retain their own bounds and can exceed this interval. A zero-progress regression repeatedly changes destination dependencies, verifies zero completed vectors and unchanged source membership, and requires failure within six seconds. The test may terminate through either the round cap or the no-progress interval.

Concurrent transfers can remove IDs from the source while this operation retries. The existing current-membership filter drops those IDs before each navigation, and returns success when none remain. Partial progress by another worker does not reset this operation’s clock unless this operation itself completes a transfer. That rule remains conservative without inventing successful transfers or duplicating membership.

## Structural coverage and admission limits

The branch-100 fixture uses the actual split threshold 2,048 and merge threshold 512. It begins with 100 leaves and one internal node, then reaches 101 leaves and three internal nodes with 52,737 vectors. Its retained `transfer-current-actual-threshold-green-01.log` observes two active jobs, one split and zero transfers. It verifies the structural threshold crossing only; it does not establish contended posting-transfer progress. The separate 4,096-vector, 32-worker fixture and controlled queued/admission regressions provide that targeted evidence. Neither fixture predicts 3M timing or proves the exact retained campaign failure’s cause.

The compiler owner’s `transfer-current-final-scoped-suite-01.log` records 161 passed, zero failed and one explicitly filtered local unsharded candidate regression in 5.16 seconds. The full unfiltered wrapper is not claimed to pass. This reviewer performs no Cargo, cloud execution or runtime source edits. Fresh reviewed Linux builds, physical correctness and a replacement large arm remain separate operator gates.

## Reviewed source identity

The reviewed SHA256 values are:

| Source | SHA256 |
| --- | --- |
| Region manager and its tests | `0d2570f56aa8bf94b5afd8c47f4da9f66adc9a6d1728eac1cdbd991c3386a5a8` |
| Owner transfer handler | `aa5a7396a7d4728d97529a355e4137e0448e45f700be9d0072523662ba5270f8` |
| Construction and reassignment | `63e28e1f251366329f605a820a8db1b6acf98a8b32c4454de5210841442509e4` |
| Controlled construction tests | `bd9f625d567d12dce1eef544edbee99b8659c2149ecba24bdb25fa85b177e78c` |

The reviewer finds no source blocker in the coherent admission, diagnostics and progress correction. Preserve failed campaign attempts and all controlled red receipts. The already completed 100K comparisons remain valid for their recorded source; a consistent final-source comparison requires the operator’s planned replacement arms.
