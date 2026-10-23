# Place branches without changing the reference tree

This local correctness slice builds one ordinary tree and places its existing leaf branches after each completed writer batch. It starts with one leaf and one local owner. The reference insertion, split, merge, neighbor discovery, eligibility predicates, and reassignment methods remain unchanged. The ownership directory is separate from tree edges and node identities.

## The pieces

- The reference writer performs all construction using the existing global algorithm.
- A tree image records exact reachable edges, centroid bits, centroid codes, physical posting IDs, versions and code bytes. Its current memberships reject missing versions and duplicate active vector copies.
- Local owners hold branch-state handles. A directory gives every reachable leaf exactly one owner. Leaves form the smallest existing branch cover, so owners can hold multiple branches at different tree depths.
- A placement policy moves existing branches when a live-vector threshold is exceeded. It aims for a separate lower threshold and reports unresolved capacity when a branch cannot fit or the owner limit prevents a move.
- Saved readers continue using their original five root UUIDs. The local test reopens those roots and checks all old durable object bytes after a new writer generation is saved.

## What the implementation proves

Each placement call compares the complete reference image before and after handoff. A move transfers the existing local branch-state handle, with no reclustering, re-encoding, embedding copy, or tree mutation. The directory covers all reachable leaves exactly once and every current vector resolves through its leaf to exactly one local owner. Continued construction refreshes this cover after topology changes; surviving leaf IDs retain ownership, and newly created branches inherit the prior owner with the largest live-membership overlap, breaking ties by owner index.

The wrapper validates a complete new batch before calling reference insertion. It uses the ordinary add-then-balance schedule for each row. It admits finite vectors, unique new IDs, no replication, at most 4,096 rows per batch and 100,000 total live rows, dimensions at most 4,096, 100,000 reachable nodes and 128 local owners. These are experiment admission caps, not process-memory bounds.

## Execution and accounting limits

This slice is centralized reference construction with local branch placement simulation. It does not execute mutations on remote owners. Local owners hold actual branch-state payload handles, but the reference writer still holds authoritative mutable state. Moving a handle in one process is not a network transfer. The reported serialized branch size is a logical state-size proxy; it is not measured wire traffic, allocator usage, or full-vector traffic.

Each call reports one image’s reachable-node and physical-posting cardinalities, directory size, moved branch sizes and live owner loads. The fields named scanned_nodes and scanned_physical_rows are cardinalities, not total scan/read work. A batch captures its next image, and placement captures complete before and after images; each capture performs per-physical-row version lookups. Ownership inheritance and candidate sizing also rescan memberships. These costs are not fully instrumented in this slice. Snapshot construction copies posting/code metadata for every reachable node. It does not provide proportional construction or a bounded routing cache. Persistence uses the existing five-root writer and reader paths in the test; placement makes no storage reads or writes, and reuses the exact saved root references. No production immutable-storage enforcement, remote cache-warming measurement, GC or recovery protocol is implemented.

The saved-reader test preserves an independently opened immutable generation and its root UUIDs, checks exact rerank outputs before and after placement, reopens the original generation, continues construction, saves a new fork and verifies old object keys and hashes remain unchanged. An in-memory snapshot alone is not used as evidence for saved-reader isolation.

## Required distributed-construction integration

Global neighbor discovery currently runs inside the reference writer's `apply_npa_to_neighbors`: it navigates around the old center, selects neighboring leaves and scrubs each leaf before evaluating eligibility. The `apply_npa_to_fp_neighbor` and `apply_npa_to_quantized_neighbor` methods scan posting entries with the reference predicates. The `reassign` method loads the vector, navigates globally, selects destinations and performs versioned registration. These methods remain centralized in this slice.

A distributed next slice must resolve each selected neighbor through the directory, dispatch the unchanged eligibility scan to that owner, fetch durable vectors when the reference predicate requires them, navigate for eligible vectors globally, and coordinate old and new memberships with version validation. Cross-owner merge selection and replacement-parent updates also need explicit dispatch. Ownership labels alone do not satisfy these requirements. The local module therefore does not claim an implementation of distributed split, merge, reassignment, mutable-state transport or remote construction capacity.

The future quality comparison uses matched corpus, query and ground-truth inputs with the same metric and construction settings. Generic cohort sizing permits a roughly two-million-vector starting experiment; no dataset size, build duration or cloud launch is assumed here.

The saved tests prove old-object immutability and exact saved full-precision rerank outputs. They do not perform a persisted posting/current-membership audit of the new 80-row generation. Unique current membership is checked on the in-memory reference image; placement preserves that image exactly. These are distinct correctness claims.
