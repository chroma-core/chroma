# Hierarchical SPANN Index

Full-precision hierarchical centroid tree for SPANN posting list routing.
Vectors are stored in leaf nodes (posting lists). Internal nodes route
queries via beam search using f32 centroid distances. The tree grows
bottom-up: vectors are always added to leaf nodes, and splits propagate
upward when a parent exceeds the branching factor.

## Locking strategy

Three-tier concurrency design separating the common path (navigate + register)
from the rare path (structural modifications).

### Data structures

```
nodes:          DashMap<NodeId, TreeNode>   -- per-node shard locks
structure_lock: parking_lot::RwLock<()>     -- serializes split/merge only
balancing:      DashSet<NodeId>             -- prevents duplicate balance work
embeddings:     DashMap<u32, Arc<[f32]>>    -- per-vector shard locks
versions:       DashMap<u32, u32>           -- per-vector shard locks
root_id:        AtomicU32
next_node_id:   AtomicU32
```

### add() flow

```
Phase 1  embeddings.insert, versions.bump        no lock
Phase 2  navigate (beam search the tree)          per-node DashMap.get
Phase 3  register_in_leaf (append id+version)     per-leaf DashMap.get_mut
Phase 4  balance (if leaf exceeds threshold)      structure_lock.write()
```

Phases 1-3 are fully concurrent across threads. Only phase 4 serializes,
and only when a leaf actually needs splitting or merging (~1 per 2048 adds).

### Why not per-node mutexes / latch crabbing?

The tree is shallow (depth 2-4) and splits propagate upward. Classic
latch crabbing (lock parent, lock child, release parent if child is safe)
adds complexity for marginal benefit at this depth. A single structure_lock
serializes all structural modifications, which is correct and simple:

- Thread A splits leaf L1, updates parent P, cascades to split P if needed
- Thread B waits for A, then splits leaf L2, sees the updated parent_id

No orphaned nodes, no lost children. The DashSet<NodeId> balancing guard
prevents two threads from redundantly balancing the same cluster.

### Navigate during a concurrent split

A navigate running without any lock may observe a partially-updated tree:
a parent's children list references a node_id that was already removed by
a split. DashMap.get() returns None and the candidate is skipped. This is
benign -- beam search is probabilistic, and losing one candidate has
negligible recall impact. The `navigate_missing_nodes` stat tracks how
often this occurs.

### Register failure and retry

If all navigated leaf targets were removed by concurrent splits before
register_in_leaf could write to them, add() retries the full pipeline
(navigate + rng_select + register + balance) recursively. The
`add_register_fallbacks` stat tracks how often this happens. In a
degenerate case this could loop, but in practice it resolves on the
next attempt because the tree has stabilized.
