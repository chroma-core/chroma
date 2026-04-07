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

reassign() (called from NPA during split) has the same retry loop.

### Differences from quantized_spann.rs

reassign() takes a `from_cluster_id` parameter. After navigating and
running rng_select, if the selected clusters already include
from_cluster_id, the vector is already where it belongs and the
reassign is skipped. This avoids unnecessary work and reduces cascading.

quantized_spann.rs has the same from_cluster_id check but handles register
failure differently: it spawns a new single-vector cluster as a fallback.
The hierarchical index instead retries navigation until it finds a live
leaf, avoiding degenerate tiny clusters that immediately trigger merges.

### Preventing duplicate balance work

The `balancing` DashSet prevents two threads from concurrently splitting
or merging the same leaf. If a thread attempts to balance a cluster that
is already being balanced by another thread, it returns immediately.

## Search performance notes

### beam shape

There is likely an optimal number of nodes to visit at each level that
minimizes latency while preserving target recall. That optimum depends on
dataset dimensionality, dataset structure, tree size / row width, and other
factors. Today the benchmark approximates this with a mostly static per-level
tau schedule. In the future this policy could be made more expressive, using
some combination of per-level tau, row-width percentages, and static min/max
beam limits, potentially learned for each dataset during ingestion or
compaction.

### code changes

A likely future improvement is to replace the per-query dedup `HashMap<u32, f32>`
with dense scratch arrays keyed by vector id, for example:

- `seen_epoch: Vec<u32>`
- `best_dist: Vec<f32>`
- `touched_ids: Vec<u32>`

That would avoid hashing and pointer chasing during dedup, which should be
faster on benchmark datasets where ids are dense and bounded.

Another likely improvement is in split-time NPA for quantized codes. The
current path rebuilds a `HashMap<u32, Vec<u8>>` from vector id to old code,
which adds hashing, per-entry allocation, and pointer chasing to a hot loop.
A better layout would keep the old codes in one contiguous `Vec<u8>` buffer
and carry either the original slot index or final split labels through the
split pipeline, so NPA can recover code slices directly by position instead
of by hash lookup.
