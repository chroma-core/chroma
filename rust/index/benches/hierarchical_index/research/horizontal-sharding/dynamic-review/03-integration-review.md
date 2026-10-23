**Three correctness findings remain. The four concrete findings from review 02 are fixed in V7, although publication-directory durability has a separate gap.**

All nine canonical files match the manifest and their archived copies. The archive SHA-256 matches `0850357977a389d97e6897f0d403a024f68fa4b183ab36a501741b1b5cad6fa1`. I read the actual Linux log, both prior reviews, and `DYNAMIC-SHARDING.md`. The log records 23 passing tests, including 14 Rust-backed methods, and the binary hash. I performed no edits, tests, cloud operations, or commits.

**Remaining actionable findings**

1. **High: publication does not establish that stored vectors match the canonical corpus.**

   [dynamic_deployment.py:52](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:52) verifies the canonical file’s digest and size. Snapshot validation establishes ownership and snapshot identity, while [dynamic_runtime.py:76](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_runtime.py:76) checks readiness. Neither compares stored vector values with the corresponding canonical rows.

   **Failure path:** Build valid initial snapshots from corpus A, then supply a same-sized corpus B containing one changed vector to `make_record`. Initial publication accepts that combination. A subsequent split, merge, reassignment, or append affecting that vector rebuilds its owner from B. Existing vector values silently change, while unaffected snapshots retain A’s values.

   Establish canonical-content correspondence when admitting initial snapshots, either through verified build provenance or a content comparison. Apply the declared cosine normalization when comparing values. The current tests derive both inputs from the same arrays, so they cannot detect this mismatch. This leaves the vector-content requirement from review 01 unresolved.

2. **Medium: fresh publication and journal directory hierarchies are not fully durable.**

   [dynamic_deployment.py:85](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:85) creates publication directories, and [line 155](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:155) creates journal directories. The shared [atomic writer](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/tree_manifest.py:213) synchronizes the file and its immediate containing directory, but not newly created ancestors.

   **Failure path:** Publish into a fresh `/state/publications/tenant/current.json`, with artifacts stored elsewhere. Synchronizing `tenant` persists its contents but does not ensure that `publications` durably contains the `tenant` entry. A power failure can lose an acknowledged publication namespace. A separately located fresh journal directory has the same exposure.

   Synchronize the complete publication and journal directory ancestry. The replica/model durability helpers cover their own paths, so they do not close this separate gap.

3. **Medium: publication accepts relative artifact paths that readers cannot resolve.**

   [dynamic_deployment.py:86](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_deployment.py:86) validates model and canonical-corpus descriptors against the caller’s `base`, but publication does not persist that base. [dynamic_runtime.py:157](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/dynamic_runtime.py:157) subsequently validates against the reader’s working directory and loads the model the same way.

   **Failure path:** Supply relative model/corpus descriptors with a valid, non-working-directory `base`. Publication succeeds, but opening the resulting deployment immediately fails to locate those artifacts.

   Normalize paths before constructing the hashed record, or persist and consistently use the artifact base. Internal builders produce absolute descriptors, which explains why existing fixtures miss this exposed-API failure.

These are source-derived failure paths, not executed reproductions.

**Disposition of the four prior concrete findings**

| Prior finding | V7 assessment |
|---|---|
| Zero cosine routing | **Resolved.** Rebuilding rejects cancelling centroids, and dynamic tree validation rejects zero cosine centroids. The metric fixture exercises rejection before publication. |
| Replica-directory and model durability | **Resolved for those artifacts.** Files, child directories, and ancestors are synchronized before readiness/publication. The separate control-directory gap is finding 2. |
| Receipt/descendant journal reconciliation | **Resolved.** Successful receipts remain authoritative; ancestry inspection recognizes committed predecessors. Both interleavings have retained test evidence. |
| 128-slot planner admission | **Resolved.** Placement admission includes the subtree count and tests the heterogeneous 129-subtree case. |

Deployment-wide fencing remains sound for the stated local authority. Source-offline planning resolves maps and manifests through current replicas. Generation-specific endpoint probes and retained serving instances address the earlier serving-identity concern.

**Important acceptance evidence still missing**

- Successful **append immediately after migration with original checkpoint paths unavailable**, repeated appends, and HTTP parity after append. The source-offline fixture exercises a split; append acceptance exercises a separate post-split path.
- Canonical-corpus mismatch rejection, including normalization-aware content verification.
- Publication/journal namespace durability and failures around pointer replacement and directory synchronization. The retained synchronization tests establish ordering and failure propagation, not physical crash recovery.
- Successful IP/cosine appends and non-axis-aligned cosine vectors that exercise normalization rounding. Current metric evidence is small and narrower than the L2 transaction suite.
- Integrated resource admission across overlapping preparation and retained serving generations. Planner tests validate declared estimates; they do not establish measured resource guarantees.

Local filesystem authority, full copies, dense IDs, and affected-owner rebuilds are intentional scope. Cross-host transfer supervision/authentication and distributed consensus remain unimplemented. The retained tests establish small correctness evidence, not scale or physical crash proof; frozen source epochs remain separate.