# Hierarchical SPANN Index

Full-precision hierarchical centroid tree for SPANN posting list routing.
Vectors are stored in leaf nodes (posting lists). Internal nodes route
queries via beam search using f32 centroid distances. The tree grows
bottom-up: vectors are always added to leaf nodes, and splits propagate
upward when a parent exceeds the branching factor.

## Concurrency model

Lock-free design using DashMap per-shard atomicity and retry on conflict.
No global lock -- split/merge and add run concurrently.

### Data structures

```
nodes:          DashMap<NodeId, TreeNode>   -- per-node shard locks
balancing:      DashSet<NodeId>            -- prevents duplicate balance work
embeddings:     DashMap<u32, Arc<[f32]>>   -- per-vector shard locks
versions:       DashMap<u32, u32>          -- per-vector shard locks
root_id:        AtomicU32
next_node_id:   AtomicU32
```

### add() flow

```
Phase 1  embeddings.insert, versions.bump        no lock
Phase 2  navigate (beam search the tree)          per-node DashMap.get
Phase 3  register_in_leaf (append id+version)     per-leaf DashMap.get_mut
Phase 4  balance (if leaf exceeds threshold)      balancing DashSet guard
```

All phases are fully concurrent across threads. No global serialization.

### split_leaf: atomic remove-first

split_leaf atomically extracts the leaf from the DashMap at the start
via `nodes.remove(&leaf_id)`. This has two effects:

1. The returned data is the complete, latest snapshot -- including any
   vector registered by a concurrent add() up to the instant of removal.
   No data is lost.

2. After the remove, any concurrent `register_in_leaf()` targeting this
   leaf sees `nodes.get_mut()` return None and fails immediately. The
   calling `add()` retries via re-navigate.

DashMap serializes `get_mut` and `remove` on the same shard key, so there
is no window where a registration can be lost: either it completes before
the remove (included in the extracted data) or fails after (triggers retry).

After the remove, split_leaf runs k-means, creates two new leaves, inserts
them, and updates the parent -- all without any global lock. NPA (nearest
posting assignment) also runs concurrently with other operations.

### merge_leaf: same remove-first pattern

merge_leaf atomically removes the source leaf first. If no suitable merge
target is found, or the target disappears concurrently, the leaf is
re-inserted to avoid data loss.

### Navigate during a concurrent split

A navigate may observe a partially-updated tree: a parent's children list
references a node_id that was already removed by a split. DashMap.get()
returns None and the candidate is skipped. This is benign -- beam search
is probabilistic, and losing one candidate has negligible recall impact.
The `navigate_missing_nodes` stat tracks how often this occurs.

### Register failure and retry

If all navigated leaf targets were removed by concurrent splits before
register_in_leaf could write to them, add() retries the full pipeline
(navigate + rng_select + register + balance) recursively. The
`add_register_fallbacks` stat tracks how often this happens.

reassign() (called from NPA during split) has the same retry mechanism
via reassign_inner(), which retries without re-incrementing the version.

### Preventing duplicate balance work

The `balancing` DashSet prevents two threads from concurrently splitting
or merging the same leaf. If a thread attempts to balance a cluster that
is already being balanced by another thread, it returns immediately.
