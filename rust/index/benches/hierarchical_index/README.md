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

### leaf selection

Scanning posting lists is the dominant search cost (~50% of latency), so
choosing which leaves to scan matters a lot. The current approach ranks
leaves by `dist(q, centroid_i)`, but centroid distance is an imperfect proxy
for whether a leaf actually contains nearest neighbors. The gap between
achieved leaf-level recall ("L3 R@k") and optimal leaf-level recall
("opt R@k") quantifies how much accuracy is left on the table.

Candidate improvements, roughly ordered by expected impact-to-effort ratio:

1. **Radius-corrected scoring.** Store `max_radius = max_j ||v_j - c_i||`
   per leaf (one extra f32). Rank leaves by `dist(q, c_i) - max_radius_i`
   instead of raw centroid distance. By the triangle inequality this gives
   a lower bound on the closest vector in the leaf, so leaves whose spread
   reaches toward the query get promoted.

2. **Multi-representative navigation.** Store 2-3 representative points per
   leaf (e.g. k-means within the leaf, or centroid + two furthest-apart
   vectors). Score = `min_k dist(q, rep_k)`. This captures elongated or
   multi-modal clusters that a single centroid misrepresents.

3. **Directional variance correction.** Store the top-1 principal component
   direction `d_i` and its variance `sigma_i^2` per leaf (dim + 1 extra
   floats). Compute a corrected distance that accounts for how much of the
   `(q - c_i)` displacement aligns with the cluster's spread axis. Leaves
   whose elongation axis points toward the query get a scoring boost.

4. **Leaf reranking with codes.** After navigation produces candidate leaves,
   store a single 1-bit or 4-bit summary code per leaf (quantized centroid
   or a representative code). Use these codes for a cheap second rerank pass
   before committing to the expensive full posting list scan. This adds a
   lightweight intermediate stage between centroid ranking and full scan.

5. **Query-adaptive leaf count.** Instead of scanning a fixed number of
   leaves, dynamically decide how many to scan based on the distribution of
   candidate centroid distances. If there is a clear gap ("elbow") in the
   sorted distances, stop early; if distances are tightly packed, scan more.
   This avoids over-scanning easy queries and under-scanning hard ones.

6. **Half-space aware navigation.** Precompute decision boundaries between
   adjacent leaves as hyperplanes (the perpendicular bisector between pairs
   of nearby centroids). At query time, a dot product against each boundary
   reveals which side the query falls on, providing a geometric signal that
   pure distance-to-centroid misses -- especially for queries near the
   boundary between two clusters.
