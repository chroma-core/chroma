**I found two high-severity and two medium-severity bugs.** The deployment-wide publication fence is implemented correctly for competing publishers, but crash durability and failure reconciliation remain incomplete.

All seven canonical files match the milestone-v5 manifest. Their archived copies also match, and the archive SHA-256 matches its manifest. Every archived Python file present in the checkout matches the archive. I read the supplied instructions and the available global AGENTS.md. This review ran no tests, cloud commands, commits, or file edits.

**Remaining correctness findings**

1. **High: a topology change can publish a cosine routing model that rejects every query.**

   Rebuilding calculates each centroid as an arithmetic mean without handling cancellation: [dynamic_topology.py:83](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_topology.py:83), particularly line 91. Validation accepts any finite centroid, including zero: [tree_manifest.py:83](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/tree_manifest.py:83).

   **Failure path:** Start with two valid cosine owners containing opposite nonzero vectors, such as `[1,0]` and `[-1,0]`, and merge them. Their new centroid is `[0,0]`. The lower builder can accept both vectors, and validation/readiness do not query the upper routing model. After publication, every query evaluates that centroid and raises `cosine navigation requires nonzero query and centroid`: [subtree_transport.py:36](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/subtree_transport.py:36), [subtree_transport.py:261](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/subtree_transport.py:261). Exhaustive queries fail too.

   Choose a defined nonzero representative for this case, or reject the transaction before publication. Add metric-specific routing validation.

2. **High: a durable deployment pointer can reference a replica whose directory entries are not durable.**

   Transfer fsyncs individual files but never fsyncs the newly created destination directories or their parents: [dynamic_runtime.py:32](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_runtime.py:32). Publication durably replaces the deployment pointer independently: [dynamic_deployment.py:117](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:117).

   **Failure path:** Migrate into a fresh worker directory, finish readiness and publication, then lose power before the filesystem persists the destination directory entries. The committed pointer can survive while checkpoint files or directories disappear. Reopening then fails despite successful publication. File fsync alone does not guarantee persistence of the containing directory entries.

   Make the complete replica directory hierarchy durable before readiness/publication. Newly written topology models also use plain `write_bytes`, leaving a related durability gap: [dynamic_topology.py:94](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_topology.py:94).

3. **Medium: journal reconciliation can label an already published transaction “failed_unpublished.”**

   Exception handling checks only whether the transaction remains the current head: [dynamic_deployment.py:143](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:143) through line 151.

   **Failure path:** Transaction A publishes successfully. Before A finishes writing its journal, transaction B publishes using A as its parent. A’s journal write fails. A’s exception handler sees B at the head and writes `failed_unpublished`, although A committed and B depends on it. This produces an incorrect recovery record and could mislead later cleanup or retry logic.

   Preserve a successful publication receipt as definitive evidence. If publication itself raises after pointer replacement, reconcile against deployment ancestry or record uncertainty; a different current head does not prove nonpublication.

4. **Medium: the placement planner ignores the enforced 128-subtree worker limit.**

   Planning admits assignments using only storage, memory, and load: [dynamic_placement.py:31](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_placement.py:31). Deployment validation separately rejects workers with more than 128 subtrees: [dynamic_deployment.py:56](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:56).

   **Failure path:** Give 129 subtrees unit estimates for all three resources. Give worker A capacity 1,000,000 and worker B capacity 1,000 for each resource. The normalized-load planner assigns every subtree to A. Publication rejects that assignment, although assigning 128 to A and one to B is supported. Migration can copy all destinations before discovering this mismatch.

   Include subtree count in placement admission and test this heterogeneous-capacity case.

These are source-derived failure paths; I did not execute reproductions.

**Resolution of the prior findings**

| Prior finding | Assessment for milestone-v5 |
|---|---|
| **Critical: deployment-wide CAS and fencing.** | **Resolved for the stated local authority.** Parent deployment hash and increasing fence are checked under the same lock. Placement-only changes invalidate stale publishers. Preparation retains its original authorization. Journal reconciliation remains a separate bug above. |
| **High: identities must survive centroid renumbering.** | **Resolved in the dynamic contract.** Ownership revisions bind source, logical identity, and map hash. Boundary hashes bind centroid content independently of position. Unchanged lower snapshots can survive slot renumbering. |
| **High: complete transfer and lazy-vector availability.** | **Substantially resolved, with durability outstanding.** Full-directory inventories, sealed inventory hashes, all five block-reference names, snapshot-bound audits, and real reader probes are present. The source-offline reopening fixture exercises exact results from the destination. |
| **High: rebuilt vectors must retain their meaning.** | **Implemented and evidenced for the small L2 fixtures.** Rebuilds copy canonical FP32 bytes into fresh namespaces; coverage validation proves the dense global-ID set; append verifies the old corpus and local-map prefixes. Cosine/IP acceptance and an explicit normalization-policy binding remain incomplete. The cosine routing bug prevents general closure. |
| **Medium: pinned serving generations and global budgets.** | **Implemented and exercised.** Separate serving instances retain old readers. Topology tests compare direct/HTTP results and logical traces for the same new generation, with global beam and final candidate limits. These limits do not cap aggregate approximate scoring work. |

**Acceptance evidence still missing**

The retained log records **15 passing Linux tests, including nine Rust fixtures**, in 8.450 seconds. That is useful correctness evidence, with these gaps:

- The fixture corpus is 48 two-dimensional L2 vectors: [test_dynamic_real.py:24](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/test_dynamic_real.py:24). It does not cover cosine cancellation, cosine normalization, IP, or general FP32 edge cases.
- The competing-publisher test uses two threads and two placement transactions: [test_dynamic_real.py:114](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/test_dynamic_real.py:114). Separate-process contention and placement racing an already prepared topology/compaction transaction are not demonstrated.
- Failure fixtures do not exercise crash recovery, directory durability, pointer-replacement/fsync failures, or the journal interleaving above.
- The corruption fixture deletes an arbitrary nonmanifest file, rather than specifically identifying a lazy vector block: [test_dynamic_real.py:185](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/test_dynamic_real.py:185). Successful exhaustive reranking provides positive vector evidence; targeted lazy-block rejection remains unproven.
- Append acceptance covers a split followed by one direct-reader append. It does not demonstrate HTTP trace parity after append, repeated appends, or append/topology planning after migration with original source paths unavailable.
- The log does not independently show the binary hash verification; the manifest supplies that attestation. The recorded process RSS is not scale evidence.

**Implemented limits are separate from these bugs.** A local filesystem authority, complete directory copies, affected-subtree rebuilding, retained old generations, dense IDs, and greedy placement with declared estimates are intentional scope. Remote transfer supervision/authentication and distributed consensus remain unimplemented. Planner retained-resource accounting is present, but it depends on caller-supplied estimates; topology rebuild row/byte admission is a separate constraint.

Static defaults remain intact in the inspected shared validator. The v4→v5 archive comparison changes only dynamic implementation/tests; `tree_manifest.py` and archived static modules are byte-identical across those two milestones. The log also records three passing static manifest tests. This does not independently certify every historical benchmark artifact.