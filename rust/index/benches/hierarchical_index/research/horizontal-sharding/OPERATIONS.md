# Operational records

This is the entry point for permissions, spending and execution history. The [plan](CURRENT-PLAN.md) determines whether work may proceed. The single-owner 3M comparison and attribution are complete. The [two-machine campaign](two-machine-v1/PLAN.md) is authorized and remains in local implementation and correctness validation; no new cloud window is open. Its [final receipt](single-shard-3m-attribution-v1/operations/window-epoch2/final-receipt.json) records API-stopped worker and driver, restored profiling permissions, removed temporary access and preserved disks. Any subsequent execution requires a fresh admitted window and independent shutdown guards.

| Record | Purpose |
| --- | --- |
| [Upload authorization](UPLOAD-AUTHORIZATION.json) | Records standing upload permission and its scope. Ordinary tool approval controls still apply. |
| [Scale authorization](SCALE-AUTHORIZATION.json) | Records permission for tests of any size, superseding older numerical ceilings. |
| [Storage authorization](STORAGE-AUTHORIZATION.json) | Records permission for storage growth within the total budget. |
| [Instance reuse authorization](INSTANCE-REUSE-AUTHORIZATION.json) | Records permission to reuse the designated sandbox machine. |
| [Spending ledger](SPEND-LEDGER.json) | Records covered compute bounds and recurring storage commitments. It is not a whole-account bill. Count each receipt once. |
| [Execution journal](CONTINUATION-RUNS.md) | Preserves material outcomes, failures and operational decisions. |
| [Runtime and shutdown receipts](fleet/) | Preserve individual source/configuration bindings, deadlines, failed attempts and shutdown proof. |
| [Continuation state](CONTINUATION-STATE.json) | Provides machine-readable pointers; dated execution entries are historical unless the current plan explicitly activates them. |

The authorized cloud scope is AWS sandbox account 079819354890 in us-east-1, under the $1,000 cumulative new-research allowance. Use Cops-managed configuration and fresh identity checks. Lyon permits self-service `cops aws login`; ask for help only when interactive authentication requires it. Use one operator and independently enforced local and remote absolute shutdown deadlines for each new window. Never reuse or silently extend a closed window.

Preserve protected instance `i-0f0b5dcb6888ad43d`, excluded preparation instance `i-0d606301de7f8b4ae`, retained disks, source epochs, failed artifacts and staged work. The shelved preparation-attribution experiment remains excluded. Do not download corpora to the Mac, start a local Linux runtime or garbage-collect retained roots. Implementation belongs in `/private/tmp/chroma-hierarchical-spann-sharding`; leave `/Users/chroma/workspace/chroma` untouched. Use only `lyon-chroma <lyon@trychroma.com>` for Git actions and verify identity before writes.

The retained [reference closure](fleet/matched-reference-runtime-v1/epoch3/final-receipt.json), [cadence closure](fleet/100k-owner32-runtime-v1/epoch13/final-receipt.json) and [rejected split-upload closure](fleet/prepared-leaf-split-runtime-v1/epoch1/final-receipt.json) record stopped windows. They are observations at their recorded times. This consolidation performs no fresh AWS inventory and makes no new live-state claim.

Keep detailed approvals and lifecycle logs in place so existing provenance links remain valid. Add new operational outcomes to the journal and ledger, rather than duplicating them in the design or ranked research queue.
