# Reservation active-index and fixed 100K preparation review

The reservation correction removes retained completed attempts from active-conflict discovery while preserving their replay outcomes. Independent inspection of the epoch7 source comparison finds no remaining source blocker. It proves removal of avoidable bookkeeping work; it does not yet measure any end-to-end construction speedup or explain the full 3M slowdown.

## State and exclusion correctness

An ordered set now contains exactly the Granted attempt IDs. Queue pumping inserts an attempt only when its entire requested bundle receives a grant. Release changes its retained status to Released and removes its active ID under the same bookkeeping mutex before pumping waiting requests. Cancelling a Granted attempt still rejects the operation; cancelling a queued request or creating a cancellation tombstone never inserts an active ID. Requests rejected initially or replanned while queued also never enter the set. No path changes a Granted record directly to Replan or Cancelled.

Conflict discovery follows the ordered active IDs and retrieves the current roots from each retained record. Root replacement therefore changes the protected live frontier without changing active membership or leaving a stale copy of root coverage in the new index. The set retains the old attempt-order traversal among actual grants. The existing blocked-request ordering still prevents a later overlapping request from overtaking an earlier blocked bundle. Placement, current ancestry, root normalization and whole-bundle grants remain unchanged.

Terminal requests and outcomes remain in the replay map. Same-body delayed replay returns its recorded status, a changed body conflicts, duplicate release is harmless, and cancellation tombstones remain authoritative. Sealed reclamation now additionally checks the active set is empty before clearing records. The new tests independently compare the set with all records whose status is Granted after grant, cancellation, release, replay, replan, replacement-root publication and reclamation. Existing replacement tests exercise protection of new descendants until release.

## Evidence and limits

The actual-manager diagnostic compares identical live topology, request, placement and active work with zero versus 64 Released attempts retained. The red conflict loop visits one versus 65 records. The corrected loop visits zero versus zero while preserving old Released replay. The retained green timings point in the opposite direction from a simple speedup claim because first-use/allocation effects are uncontrolled; the work counts support the mechanism, not those isolated latency samples.

The retained protocol suite passes 26 tests. The applicable combined suite passes 164 tests in 5.15 seconds while explicitly filtering the already recorded local unsharded candidate regression. Neither result claims the unfiltered wrapper passes. Test-only counters are absent from production builds. The source correction adds an active set but retains logarithmic replay lookup costs, retained-history memory and work proportional to actual active contention. Manager replacements at placement boundaries constrain extrapolation across lifetimes.

## Fixed before/after execution preparation

The new generator accepts only `100k-before` and `100k-after`. Both derive the standard 100K construction settings from the checksum-bound existing workload plan, use the identical retained 100K corpus descriptor and vector path, and create new epoch9 output identities. The before arm uses the retained epoch7 binary/hash bindings. The after arm requires an explicit epoch9 executable-hash file and fails closed until it exists. It changes executable/reader identities and report provenance while preserving the workload. Seeds and scheduling remain uncontrolled, so different work counts must accompany any elapsed comparison.

The runner selects one arm explicitly and provides no retry, next-arm or profiler switch. It retains the original 600-second process supervisor, a 590-second absolute job deadline shared by both owners, and a 660-second remaining host interval and SSH timeout. The fresh window identity, closed-window rejection, independent remote guard validation, exclusive writer lock, targeted process-group cleanup and small-receipt collection remain required. It copies no credentials or corpus. The scripts authorize no 3M run. The retained admission log passes eight pure/mocked tests in 0.012 seconds.

Profiling is not implemented by these scripts. Before an execution intended to produce profile evidence, the operator must verify the available profiler and its bounded observation method separately. A passing spec validator does not establish profiler availability, actual installed shutdown guards, current instance identity or freshly built remote artifact hashes. The parent remains the sole operator; this reviewer runs no Cargo, AWS, SSH or Git commands and edits only this receipt.

## Reviewed identities

| File | SHA256 |
| --- | --- |
| `distributed/protocol/reservation.rs` | `e392b3d29bad92eade1445bdce2a535216de2eaca64d6fa17923dd045b84b63a` |
| `operations/make-owner-local-spec-epoch9.py` | `575769d46767182ac73b69d8cdb691dcf74df37201e7f37996111c45c50e23cc` |
| `operations/run-one-arm-epoch9.py` | `9774610576efa88894b56e82ff85ffdfe241ca6b31da12b4517f791c6123ea09` |
| Unchanged `operations/remote-job.py` | `f0c763749ed69c68f36cafcf852ccadd69dac626759e53e6cd3c48e193a82fbd` |

Source/native-build admission and fresh guarded-window verification remain the parent operator’s next gates. No execution or larger benchmark admission follows from this receipt alone.
