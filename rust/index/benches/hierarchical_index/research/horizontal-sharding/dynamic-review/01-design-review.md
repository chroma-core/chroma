**Make the complete deployment—not the tree hash—the unit of compare-and-swap and writer fencing.** A placement-only move preserves the tree hash, so a writer prepared before that move must still lose publication rights afterward. The staged immutable-snapshot approach is viable, but this distinction must be explicit before implementation.

I reviewed the current uncommitted source and the available `DYNAMIC-SHARDING.md`. There is no root `AGENTS.md` in this checkout; I followed the supplied global instructions. This was a read-only source review; I did not execute integration tests.

The findings below distinguish existing limitations from risks in the proposed extension.

1. **Critical: fence publication against placement changes as well as topology changes.**

   **Demonstrated limitation:** The existing publisher compares only `tree_sha256`; placement is validated and loaded separately. The lower writer’s lock protects one checkpoint directory, not ownership across workers. See [tree publication](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/tree_manifest.py:211), [placement validation](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/tree_manifest.py:136), and [lower writer locking](/private/tmp/chroma-hierarchical-spann-sharding/rust/index/benches/hierarchical_spann_shard.rs:254).

   **Required change:** The proposed deployment record should bind tree, placement, parent deployment hash, and fencing token. Validate the expected parent and token under the same lock that replaces the durable pointer. Every build transaction must retain its original parent and authorization; a stale worker must not “retry” by substituting the newest parent hash.

   **Future risk:** If any dynamic write path still publishes through the static tree pointer, an old owner can bypass the deployment fence. Keep static behavior intact, but give the dynamic runtime exactly one authoritative publication path. Private abandoned builds are acceptable; publishing them is not.

2. **High: separate reusable subtree identity from the complete routing-model hash.**

   **Demonstrated limitation:** Every subtree’s boundary hash includes the entire model hash and its positional centroid index. Changing one centroid—or renumbering slots after a merge—invalidates otherwise unchanged snapshots. The existing publisher also explicitly rejects topology changes and shrinking/reordered maps. See [boundary validation](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/tree_manifest.py:109) and [static publication restrictions](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/tree_manifest.py:225).

   **Required change:** Introduce a versioned dynamic validation contract. Bind the complete routing model at the tree level, while binding each lower snapshot to a stable subtree identity and an explicitly defined ownership revision. Avoid array position as persistent identity. Preserve unchanged snapshot bytes, map hashes, and lower generations.

   Define “unchanged boundary” carefully: if ownership means nearest-centroid assignment, changing one centroid can change neighboring regions too. Explicit reassignment also requires deciding whether the ownership map is authoritative and centroids merely guide search. State that rule before selecting affected subtrees.

   Do not rewrite old lower manifests merely to insert a new boundary hash.

3. **High: successful reader opening does not prove a complete transfer.**

   **Demonstrated limitation:** Root verification checks root kind/ID and declared metadata. Postings, vectors, and live-version entries are accessed lazily. A missing raw-vector block can therefore survive startup and fail during reranking. See [root verification](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/subtree_transport.py:231), [lazy reader opening](/private/tmp/chroma-hierarchical-spann-sharding/rust/index/benches/hierarchical_index/reader/persistance.rs:117), and [exact vector access](/private/tmp/chroma-hierarchical-spann-sharding/rust/index/benches/hierarchical_index/reader/remote.rs:150).

   **Required change:** Destination readiness must bind the destination, exact snapshot/map hashes, and complete transferred file inventory. Include the storage files behind all five persisted block references, especially scalar metadata containing live versions. Seal the destination before probing it. A full-directory copy is sufficient for this milestone; minimal reachable-file transfer is unnecessary.

   For tiny acceptance fixtures, traverse every live row and rerank every vector after transfer. The existing coverage audit deliberately does not read raw embeddings, so audit success alone is insufficient.

4. **High: ownership coverage does not prove that rebuilt vectors retain their meaning.**

   **Demonstrated limitation:** Manifest validation proves dense global-ID coverage and map/snapshot agreement, but does not prove that a rebuilt local row contains the correct vector. Existing resume interfaces preserve map prefixes and boundary hashes; they cannot implement removal or cross-owner reassignment. See [coverage validation](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/tree_manifest.py:83), [map preparation](/private/tmp/chroma-hierarchical-spann-sharding/rust/index/benches/sharding/id_map.rs:30), and [resume compatibility](/private/tmp/chroma-hierarchical-spann-sharding/rust/index/benches/hierarchical_spann_shard.rs:289).

   **Required change:** Rebuild affected owners in fresh directories with dense local IDs and explicit global-ID maps, as proposed. Bind canonical vector inputs to the transaction, including source digest, row interpretation, dimension, metric, and normalization policy. Require equality of the before/after global-ID sets, not merely equal counts.

   Publish each new snapshot’s complete block-reference tuple atomically with its map and routing references. Local posting-version numbers may restart in a fresh namespace; they must never be interpreted across snapshots. Reject empty outputs and capacity overflow before building, or define their representation explicitly.

5. **Medium: retain old serving bindings, and preserve the existing budget semantics.**

   **Demonstrated limitations:** A host serves one configured tree hash, and coordinators load tree and placement separately. Retaining files alone therefore does not ensure old readers remain queryable after replacing a server configuration. See [host request validation](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/subtree_server.py:29) and [coordinator construction](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/subtree_transport.py:188).

   **Required change:** Construct the dynamic coordinator from one pinned deployment. Keep generation-specific serving bindings alive; separate local processes/endpoints are sufficient.

   Preserve one coordinator-wide beam and one final candidate selection before reranking. The current implementation allows each subtree to contribute up to the candidate budget, then selects the global budget; it does **not** cap total approximate scoring work. See [candidate selection](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/subtree_transport.py:310). Test that behavior across changed subtree counts and mixed tree depths. Do not promise identical approximate results after rebuilding topology.

A small viable implementation decomposition is:

- **`dynamic_deployment.py` owns publication.** Define the immutable deployment schema, parent/token checks, readiness bindings, and durable CAS first.
- **`dynamic_placement.py` and `dynamic_runtime.py` own migration.** Plan bounded placement, copy sealed snapshots, probe real Rust readers, and retain old serving bindings.
- **`dynamic_topology.py` owns affected-subtree rebuilding.** Implement split, merge, and explicit reassignment using the same deployment transaction.
- **Dedicated dynamic tests own acceptance evidence.** Coordinate any necessary shared-interface changes; preserve static defaults and frozen sources/binaries.

The first acceptance milestone should move one subtree between two local worker directories/processes while retaining a third unchanged subtree. Then extend that fixture to split, merge, and reassignment:

- A real Rust reader must produce identical migration results and logical traversal selections before and after cutover.
- A reader pinned before each transaction must remain queryable throughout preparation, publication, and reopening.
- Truncated transfer, missing lazy vector data, failed reopen, and failed build must leave the published pointer unchanged.
- Two transactions prepared from the same deployment must yield exactly one winner—including a placement-only move racing an old-owner publication.
- Split, merge, and reassignment must preserve the exact global-ID set and canonical vector values. An unaffected snapshot must remain byte-identical.
- Exhaustive traversal with a candidate budget covering the fixture must match scalar exact references. Approximate tests should compare direct and transported traversal of the **same new generation**, verify global budgets and tie handling, and report topology-induced result changes separately.

These gates establish correctness without requiring production orchestration, in-place transfer, or migration-performance work.