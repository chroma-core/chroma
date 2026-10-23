**The fresh Astra-medium review finds two remaining correctness bugs. The three Astra03 findings close under the stated trusted, immutable local-worker contract; mutable-path races remain outside what the implementation prevents.**

All eleven manifest-listed files match both the frozen archive and canonical checkout. The archive SHA-256 is `ab31a2556d1c356b8e4d30c2c475fda04d558ec9aaa173b9a57dc97a3628ac29`. The retained Linux log records 29 passing tests, including 18 Rust-backed methods. Global instructions, the requested documents, frozen modules, and all frozen dynamic tests were read. No files were edited, tests or cloud commands run, or commits created.

**1. Medium: compaction publication still requires obsolete preparation paths.**

At [dynamic_deployment.py:126](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:126), the check that preserves existing local-ID order reads the candidate’s original map path. Earlier validation and readiness checks use its destination replicas.

The failure path is:

1. Build an appended tree.
2. Copy its checkpoints to fresh destination directories and construct owners for those sealed copies.
3. Make the preparation directories unavailable.
4. Publish the compaction.

Destination validation and readiness can succeed, but publication fails while opening the obsolete preparation map. Resolve the candidate through `resolved_tree(record)` for the prefix check.

This is a source-derived failure for a valid local-filesystem deployment. The new immediate-append-after-migration test covers unavailable **parent** checkpoints, not unavailable **candidate preparation** checkpoints.

**2. Medium: reconciliation can classify a visible pointer as published without establishing durability.**

The atomic writer replaces the pointer at [tree_manifest.py:219](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/tree_manifest.py:219), then synchronizes its directory. If that synchronization fails, reconciliation at [dynamic_deployment.py:188](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:188) can read the visible replacement and durably record `published_reconciliation_required` in a separate journal.

A power failure can then restore the previous pointer while preserving that journal classification. Reading the pointer establishes visibility, not persistence. Reconciliation should successfully synchronize the pointer namespace before establishing durable commitment, or retain `publication_state_unknown`.

The [retained test](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/test_dynamic_real.py:320) raises **after** the atomic writer completes, so it misses replacement followed by directory-sync failure. This finding concerns outcome classification; publication raises rather than acknowledging success.

**Disposition of the three Astra03 findings**

| Prior finding | V8 assessment |
|---|---|
| Corpus A snapshots can be admitted against corpus B. | **Closed under the trusted immutable-worker contract.** Publication compares retained actual construction inputs with canonical rows in mapped order, using metric-specific normalization. It checks producer admission, binary identity, successful execution evidence, snapshot identity, maps, and output inventory. |
| Fresh publication and journal ancestors lack durability. | **Closed.** Both paths synchronize their complete directory ancestry. Finding 2 concerns a later pointer-replacement failure. |
| Relative artifact paths pass publication but fail readers. | **Closed.** Relative model, canonical-corpus, and checkpoint paths are rejected, including when an external base is supplied. |

**The corpus guarantee requires actual immutability; hashes alone do not enforce it.** At [dynamic_runtime.py:118](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_runtime.py:118), inputs are copied into fresh paths and checked before and after execution. A temporary replacement can evade those checks: retain B before execution, let the writer consume A, then restore B before the final hash. The resulting provenance can describe B while the snapshot contains A. Binary paths have an analogous check-versus-execution gap.

That is a conditional failure path requiring another actor to mutate the supposedly private, immutable namespace. It is not evidence of an unconditional bypass within the stated trusted-worker contract, nor does the contract claim protection against fabricated evidence from a compromised authority. **If mutable-path races are part of the required guarantee, corpus correspondence is not fully closed.** Independent workers need enforced artifact ownership or immutable execution inputs. The retained tests do not cover these races.

The remaining reviewed behavior is supported within its local scope:

- The old-prefix migration path, repeated appends, HTTP parity, and successful IP/cosine appends have retained acceptance evidence.
- Non-axis cosine fixtures exercise normalization to FP32 and actual query results. They provide small-fixture evidence, not comprehensive numerical guarantees.
- The deployment-wide process lock and parent/fence checks remain sound for the shared local authority. Separate-process contention admits one winner.
- Old direct and HTTP readers retain their generations while callers keep the corresponding serving objects alive.
- Successful publication receipts and durable descendant ancestry remain valid reconciliation evidence. Finding 2 concerns an unsuccessful synchronization without a receipt.

**A real cross-host correctness demo remains blocked by destination-independent publication, verified transfer between independent filesystems, authenticated worker admission, supervised generation-specific endpoints, and an enforceable immutable-artifact contract.** It also needs failure evidence for interrupted transfers, worker restarts, and the pointer-sync gap above.

Distributed consensus remains separate future work. Local filesystem authority, full checkpoint copies, affected-owner rebuilds, dense IDs, and explicit retention of old generations are intentional limits. Physical crash recovery and measured capacity across overlapping builds and retained generations remain missing acceptance evidence. Newly added cross-host modules were excluded from this review.