**V10 still has correctness blockers.** I verified all 126 frozen Python files against the manifest and verified the archive SHA-256 as `62bd2430e3308749e381af319c4fdc8a241c520f3c5825400a6458dd1b2001cf`. I used only local source and evidence; I made no edits and ran no tests.

1. **P1 — An observed controller failure can still permit publication.**  
   In [dynamic_twohost_control.py:196](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/dynamic_twohost_control.py:196), health checks reject a completed batch future, terminal workers, or exited tunnels. However, [guarded_host_batch.py:90](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/guarded_host_batch.py:90) records control failures internally and continues running while aborting and reconciling workers. Its polling interface reports `running` whenever the terminal file is absent, including during abort and lease-expiry cleanup.

   **Trigger:** A renewal or polling request fails after destination readiness. The batch thread records the failure, but abort delivery or terminal reconciliation is still pending. Subsequent health polling succeeds and reports `running`. The final health check inside the publication lock can therefore pass and commit a generation whose workers are being stopped.

   **Minimal fix:** Share a latched failure signal with publication, set immediately when the batch observes any failure and before abort requests. Check it in private preparation and inside the publication lock; propagate it to racing publisher processes. Worker health should also reject an expired lease, abort request, or expired deadline before terminal reconciliation finishes. The existing lease-fault scenario waits for batch completion before proceeding, so it does not expose this interval.

2. **P1 — Losing the agent process can leave its Rust children outside bounded cleanup.**  
   [resource_guard.py:51](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/resource_guard.py:51) refuses to signal surviving group members when the recorded group leader no longer exists. The guard reaps the agent before examining surviving children at [resource_guard.py:173](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/resource_guard.py:173). The host supervisor subsequently retries cleanup using the same leader identities.

   **Trigger:** The agent exits or is killed while a Rust build or query remains active. Its leader identity disappears before cleanup begins. Both cleanup layers reject the unanchored group; a lease expiration cannot restore the missing identity. The agent’s own build timeout also disappears with the agent.

   **Consequence:** Reporting correctly remains failed or unreconciled, but actual child termination is not guaranteed by the lease or deadline.

   **Minimal fix:** Retain verified child identities while the leader is alive and permit cleanup through a still-matching recorded member, including after leader exit. Keep the existing refusal to kill genuinely unidentified groups. Acceptance needs one leader-death case with a surviving child, followed by verified termination.

3. **P2 — A skipped reconciliation test has a definite callback-signature failure.**  
   The replacement publisher in [test_dynamic_real.py:243](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/test_dynamic_real.py:243) accepts four arguments. [dynamic_deployment.py:185](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/dynamic_deployment.py:185) now always supplies `health=health`.

   **Trigger/consequence:** Once the Rust-backed test is enabled and setup succeeds, it raises `TypeError` for the unexpected keyword before either intended publication. It never exercises the post-commit descendant reconciliation assertion.

   **Minimal fix:** Accept and forward the health keyword in the test wrapper. This is a concrete test defect, not evidence that reconciliation itself corrupts publication.

4. **P2 — The scenario journals publication, but not the private preparation it claims to classify.**  
   [dynamic_twohost_scenario.py:112](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/dynamic_twohost_scenario.py:112) rebuilds the topology and prepares remote readers before entering `run_transaction`; the transaction receives an already-prepared candidate at line 143. The lease fault hook also runs before that journal exists.

   **Trigger/consequence:** Forced controller termination during rebuilding or generation preparation leaves no transaction journal binding that attempt’s parent, operation, and private outputs. The previous progress checkpoint survives, while the final scenario report may never execute. This does not itself publish partial ownership, but it prevents the claimed complete failure classification.

   **Minimal fix:** Create the durable attempt record before preparation and execute preparation within its recorded lifecycle. Persist the prepared candidate before the fault hook or publication.

**V9 blocker disposition:** The driver disk-floor blocker is **closed in source**: [dynamic_twohost_control.py:256](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/dynamic_twohost_control.py:256) uses 8 GiB plus 2 GiB. The stale-placement backend blocker is **closed in the intended construction path**: readers check the complete deployment hash, and remote generation preparation checks physical instance and agent-run identity. Their real-runtime acceptance remains pending. The health-before-publication blocker is **partially repaired but remains open** because of finding 1.

The ownership and artifact checks otherwise appear sound within the stated contract. Replacement maps must exactly partition affected IDs; complete deployment validation rejects duplicate or missing dense-prefix IDs. Canonical values, normalization, maps, snapshots, routing models, and trusted build provenance are checked together. Publication serializes parent-hash comparison and fencing before durable pointer replacement. Old readers retain separate generation pins, and retirement rejects pinned or active generations. I found no additional concrete defect in the inspected nonce/public-key trust chain, private-token handling, or bounded regular-file transfer checks.

**Execution evidence remains separate from source correctness.** The preserved log reports 60 tests, with 22 skipped; the independent receipt reports 21 file/mock passes. Neither establishes V10 Linux/Rust or physical two-host acceptance. Full tiny mirrors, trusted workers, and dense IDs are intended limits. The documented nearest-upper-centroid append behavior remains a separate ordinary-insertion semantics gap at [dynamic_topology.py:157](/private/tmp/spann-crosshost-v10-review/tools/hierarchical_sharding/dynamic_topology.py:157), rather than a new split/merge ownership defect.

**The smallest next correctness experiment** is a deterministic local failure-window regression: finish readiness, latch a batch failure while terminal reconciliation remains pending, then attempt publication. Require an unchanged pointer and unchanged old-reader results. Follow that with the single surviving-child cleanup case before physical execution. Any repaired source should receive a new immutable epoch; the rejected V10 export remains blocked through the existing approval path.

For production integration, the existing compactor fleet must publish one complete, validated routing/ownership/snapshot generation through its authoritative commit mechanism. The existing query fleet must consume and pin that committed generation, with referenced artifacts retained for its lifetime. This requires neither a new live-reader migration service nor adoption of the standalone HTTP adapter.