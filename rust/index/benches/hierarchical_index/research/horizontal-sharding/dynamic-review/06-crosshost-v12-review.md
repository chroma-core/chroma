**V12 still has two P1 correctness blockers and one P2 journal gap.** The V12 manifest contains **128 files** and records the supplied archive SHA-256, `7a8c15fe13f6fd329e325ea9d99bd2da94bf20f757b1e360bd163cc7632ed40c`. I relied on the caller’s source hash verification. I read the prior review and receipt, both manifests, and both test logs. No edits, tests, agents, cloud operations, or uploads were performed.

1. **P1 — Racing publishers can miss a failure latched during their final health check.**  
   Frozen location: [dynamic_twohost_race.py:16](/private/tmp/spann-crosshost-v12-review/tools/hierarchical_sharding/dynamic_twohost_race.py:16).

   **Exact trigger:** A contender enters the health callback inside the publication lock and sees the failure event unset. While its worker polls are pending, the batch thread observes a control failure and sets the shared event. Both contender polls return `running`, which is possible while abort delivery remains pending. The callback returns without rechecking the event, and publication proceeds. The parent controller’s health callback checks the latch after polling; the child callback does not.

   **Minimal fix:** Recheck the shared failure event, failure marker, and parent stop event after the polls, before returning from child health.

   **Smallest gate:** Block the final child health poll, set the shared event, then release a `running` response. Require publication rejection and an unchanged pointer.

2. **P1 — A child created between the member scan and leader reap can escape bounded cleanup.**  
   Frozen location: [resource_guard.py:179](/private/tmp/spann-crosshost-v12-review/tools/hierarchical_sharding/resource_guard.py:179), particularly the subsequent `process.poll()` at line 183.

   **Exact trigger:** The guard finishes scanning the process group. The agent then starts a Rust child and exits before `process.poll()`, which reaps the leader. The child was absent from the saved member list. The subsequent survivor scan detects it but does not establish a verified anchor. Both cleanup layers therefore refuse to signal the group, leaving the child running.

   **Minimal fix:** Detect leader exit without reaping it, capture and persist a final member inventory while the unreaped leader still anchors the group, then reap. Preserve the refusal to trust an unanchored numeric process-group ID.

   **Smallest gate:** Exercise the actual guard with a synchronized fork-and-exit between its scan and exit check. Require child termination and successful journal-based recovery. The existing Linux test manually captures children before releasing the leader, so it does not expose this race.

3. **P2 — The placement contender still prepares private state without a durable attempt record.**  
   Frozen location: [dynamic_twohost_scenario.py:132](/private/tmp/spann-crosshost-v12-review/tools/hierarchical_sharding/dynamic_twohost_scenario.py:132).

   **Exact trigger:** The controller terminates during the `race-move-s2` upload, mirror, or remote generation preparation, before entering `race()`. No placement preparation journal binds that contender’s parent, private output, and replica reservation. The existing topology attempt describes the other contender; the child transaction journal is created later.

   **Minimal fix:** Create a placement `PreparationAttempt` before this transfer, reserve its replica before upload, and persist its prepared candidate before starting the race.

   **Smallest gate:** Inject failure immediately after upload acceptance and verify that the retained attempt identifies the original parent, placement operation, destination, and replica.

The journal ordering for the covered topology paths is sound: reservation persistence precedes the build RPC, and candidate persistence precedes the fault hook. A failed save stops those paths before their next side effect. The new journal regression, however, exercises successful saves rather than partial-write or directory-sync exceptions.

**Prior Astra05 dispositions:**

| Prior finding | Disposition |
|---|---|
| Observed failure permits publication | Partially repaired; remains open through finding 1. V12 correctly latches a failed terminal before a slow peer poll finishes. |
| Leader death leaves surviving children | Partially repaired; remains open through finding 2. Recorded-member anchoring and supervisor recovery are present. |
| Test callback rejects `health` | Closed in source: the wrapper accepts and forwards the keyword. Rust acceptance remains pending. |
| Preparation precedes durable journal | Closed for the topology operations and race-loser rebuild; incomplete for the placement contender in finding 3. |

The preserved logs report **22 passes and two skips for V11**, and **23 passes and two skips for V12**. Linux surviving-child acceptance and the physical two-host gate remain pending; those pending checks are separate from the source bugs above.

The smallest next gate is the deterministic child-publication latch regression, followed by the synchronized guard capture/reap case and placement-journal fault case. Preserve V10/V11/V12 and place any repairs in a new frozen version. No larger run or architectural expansion is needed.