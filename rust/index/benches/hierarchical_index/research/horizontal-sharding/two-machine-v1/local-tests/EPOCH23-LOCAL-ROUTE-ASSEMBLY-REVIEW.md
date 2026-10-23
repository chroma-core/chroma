# Epoch23 local-route liveness and candidate assembly review

The corrected epoch23 source and fixed 3M candidate preparation have no remaining blocker within this review scope. Navigation reconstruction preserves exact reference ordering while reducing a measured ordering mechanism’s work. A separate liveness correction retains pending reassignment when repeated real structural changes exhaust navigation, without accepting a stable broken route as completed construction. Neither change establishes the next physical run’s latency or success in advance.

## Candidate ordering

Mixed-owner reconstruction places scored rows in current frontier-parent order and child-ordinal order before the unchanged reference-compatible score sort. Hash-map iteration does not determine row order. Counts derive from actual rows, candidate count retains the u32 bound, prefix arithmetic is checked, and ordinal ranges and occupied slots reject gaps, out-of-range values and duplicates. Allocation requests are fallible. A partial drain on failure aborts the traversal before selection.

All remote identity, completeness, ordinal, revision and finite-score checks remain before reconstruction. All-local ordering, exclusions, global beam/tau selection and mixed-depth leaf retention remain unchanged. The new temporary parent map, counts, offsets, row positions and optional-row slots add memory work; no peak-memory or latency reduction is established by source inspection.

The fixed 64-query TCP fixture compares complete pre-score-sort rows, selected IDs, scores and logical work with the retained ordering oracle, then compares the public path with the actual reference writer. It covers tied scores, mixed depths, tau/floor behavior and the existing NaN-tau behavior. Adverse old/new paths agree on reordering, replay, missing or partial replies, duplicate ordinals, overlapping parent sets, wrong levels and nonfinite scores. Its reported 118,228 wide ordering comparisons fall to zero and 236,456 parent lookups fall to 21,376. Those counts exclude unchanged remote validation and score sorting; they are not an end-to-end speedup estimate. All 15 focused navigation tests pass as recorded in `EPOCH23-CANDIDATE-ASSEMBLY.md`.

## Real route churn and retained work

Controlled protected installations reproduce eight whole-attempt retries with an unchanged shared sequence: navigation captures a parent image, an independent internal-node replacement retires its referenced child, and the next expansion sees that retired identity. Every newly published parent points to a live replacement. When edits stop, navigation succeeds without repairing any persistent edge. This demonstrates legitimate capture-before-retirement churn and whole-batch retry amplification. It does not prove that every stale-image event in physical epoch22 has that cause; the physical receipt lacks the required per-attempt ordering evidence.

Only an exhausted navigation-route error inside pending-ID reassignment can request conditional deferral. The coordinator checks and snapshots other currently active balance attempts under the queue lock, excludes the caller’s own attempt, and accepts bounded pending chunks before the original job acknowledges success. The original ingress body and producer context remain replay-bound; completion of a producer cannot erase an already accepted retry. If the producer finishes before conditional admission and no other structural attempt remains, admission rejects and the original navigation error stays fatal.

The dependencies are conservative evidence that structural work is still active, not proof that a named producer caused the captured route to retire. Reassignment-only activity cannot manufacture a new structural dependency, and queued retries do not keep their original producer active. Reciprocal producer snapshots therefore release after the producers return rather than waiting on themselves or on future retry jobs. The existing per-call navigation bound and whole-balance job/deadline bounds remain in force.

The actual post-install reassignment regression preserves the identical current IDs and exclusion, leaves no posting claim held, and prevents close/save from treating queued work as drained. Once the other producer finishes and edits stop, queued execution completes the membership obligation. This fixture exercises the protected installation plus the exact post-install reassignment stage; it does not run every later stage of a complete leaf split.

## Resolved retirement blocker and failure limits

Review identifies and resolves a missing deferred-merge continuation. Returning after a queued evacuation originally left nobody responsible for retiring the source after its last posting moved, because normal scheduling ignores empty leaves. Both scheduled and direct reassignment-job endpoints now resume current empty-source retirement only after a forced evacuation is truly drained. Deferred work leaves the source live. The actual merge regression defers its evacuation during real internal replacements, drains retained work and requires the source to disappear from resident and published routing state while all memberships remain exact.

Negative tests deliberately leave an obsolete edge in the current published parent. With no other structural producer, exhaustion remains fatal and current memberships stay intact. If that broken route is provisionally deferred behind an unrelated active balance job, it still fails after the producer ends. Replay tests preserve the original request and context, reject conflicting contexts, exclude self and preserve authority across dependencies. This distinguishes temporary churn from a permanent edge defect without silently skipping required vectors.

The separate one-query split neighbor-discovery step, initial merge-neighbor selection and insertion keep their existing fatal navigation behavior. This change does not catch every navigation error or guarantee liveness under sustained structural churn. The new `NavigationDeferredJobs` counter counts acknowledged bounded pending chunks; it is not completed work or time saved.

## Validation and source closure

The applicable suite reports 220 passed, zero failed and one filtered test in 5.47 seconds in `epoch23-suite-01.log`. Focused liveness receipts include the actual post-install green case, the deferred-merge retirement case and stable-broken/replay/dependency negatives. Red and fixture-failure attempts remain retained in the local-route logs; no failed attempt is presented as a passing experiment. The exclusion remains the known unrelated reference-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`; an unfiltered clean suite is not claimed. Native CLI builds and Linux artifact verification remain root-owned admission gates.

The frozen archive is `packages/source-epoch23.tar.gz`, 23,064,878 bytes, SHA256 `5344d8ad2480d8fe9e9b92f0b7a01b4c49ff17b743e8f8e3d956cb68addc32c1`, with source-manifest SHA256 `5e149d30e7d7c112691eeaeab93e3be011b9debdb7bf465bbf41b9637630d804`. Independent manifest comparison with complete epoch22b confirms exactly the six changes below and no added or removed files.

## Execution preparation

The epoch23 adapters retain one candidate-only 3M run against the valid epoch21 reference. Exact input, dimensions, 32 workers, cadence, thresholds, 2,160,000 owner capacity and zero first-owner override remain unchanged. The runner and build adapter have only namespace changes; provenance names navigation-candidate assembly. The epoch15 supervisor, 590-second job bound, 660-second remaining-window check, 1,800-second host window, independent shutdown guards and cleanup controls remain unchanged. There is no reference rerun, profiler, automatic retry or sweep.

All hashes in `operations/preparation-epoch23.json` match at review time, including the inherited epoch22 candidate template SHA256 `585a37464daecb185f4154cc709b8b9da751cad0c3082bc8d8aa03d0e6cbd169`; five preparation tests pass. Final configuration/source metadata, native and Linux build receipts, matching runtime hashes, fresh preflight/input checks and absolute guards remain root-operator admission work. The final interval must still demonstrate actual construction on both owners and complete fresh-reader validation. This reviewer performs no source edits, Cargo commands, cloud operations or Git writes.

## Reviewed source hashes

Paths are relative to `rust/index/benches/hierarchical_index/distributed` in the frozen archive.

| File | SHA256 |
| --- | --- |
| `construction.rs` | `ea43f32b0fc6333a9642a3895f4dc6ff9d732c62a5eb69adbd4f2e370cccc714` |
| `construction_tests.rs` | `5e5ec173671b563a3f1e0bb92c41b809ce281bf9034901d6e217ee778e5a9598` |
| `engine.rs` | `cfd4bcce3aab37478c29814a439b09bd8c85edce73b25611dc74485400bc11be` |
| `observability.rs` | `3b2dd2dff8052f44cdfc1cadff8aae732ba25b92a6b4b0c8c8e24f06c74b5751` |
| `routing.rs` | `fda48067d70d04f11dad29173e6304473b630ae02ac4777ad764473944c932bd` |
| `routing_tests.rs` | `eb445c667d6adb11e2c8657bdb82f2754c4c96381377c68d116e3a058c5ace14` |
