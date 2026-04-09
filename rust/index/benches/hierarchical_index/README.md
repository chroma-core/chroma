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

   The diagnostics are very revealing. Here's what they show:
   The correction is far too large relative to the distance scale.
   The p90_radius values (~0.79 in L2) are nearly as large as the centroid distances themselves (sqrt(0.1)=0.316 typical, sqrt(0.8)=0.894 max in L2). The correction (sqrt(raw_dist) - 0.79).max(0)^2 is clamping most leaf distances to zero, destroying the ranking among close candidates. All nearby leaves become indistinguishable.
   Size has nothing to do with it -- correlation r=0.011, essentially zero.
   The diagnostic confirms broad harm -- 77 queries degraded, only 2 improved, median delta -2%, p10 delta -12%.
   The radius correction idea is sound geometrically but the magnitudes don't work for this dataset: clusters are too spread relative to how far apart they are. The per-leaf representative codes approach should work better because it directly estimates distance to actual vectors rather than trying to correct a proxy.

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

   The representative codes did not help. The results are slightly worse than the baseline across every metric:
   Metric	Baseline (no reps)	8 reps	Delta
   L2 R@100 (tau=0.30)	96.25%	96.16%	-0.09%
   L2 R@100 (tau=0.50)	96.53%	96.30%	-0.23%
   L2 R@100 (tau=0.70)	96.54%	96.30%	-0.24%
   lat_nav (tau=0.30)	2.3ms	5.5ms	+3.2ms (2.4x)
   Why it didn't work:
   fp navigation already computes exact centroid distances. The centroid is the mean of the cluster -- it's already the single best representative point in L2. Adding 8 noisy 1-bit code distances can't improve on an exact distance.
   The min() aggregation is biased by underestimates. 1-bit RaBitQ codes have significant quantization noise. Taking min(exact_centroid_dist, noisy_rep_1, ..., noisy_rep_8) means any single spurious underestimate contaminates the score. With 8 codes, the probability of at least one bad underestimate is high. This pulls irrelevant leaves into the beam, slightly degrading recall.
   The cost is substantial. Building a QuantizedQuery per parent node (which fp mode doesn't otherwise need) more than doubled navigation time from ~2.5ms to ~6ms.
   What this tells us about the path forward:
   The feature was designed for a scenario where centroid distance is a poor proxy (e.g., when centroids are quantized too) and there's a wide candidate beam to prune. In fp navigation with tight beams (~120 leaves), the centroid distance is already a near-perfect signal. The cases where representative codes could help -- leaves whose centroid is far but whose boundary is close -- are exactly the cases where 1-bit quantization noise dominates the signal.
   For this to work, you'd likely need either (a) higher-fidelity representative codes (4-bit), (b) a different aggregation than raw min (e.g., use reps only as a tiebreaker or for a second-pass reranking), or (c) quantized navigation mode where centroid distances are also noisy and the reps add genuine information.

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

## Tree geometry and replication

### High-dimensional cluster geometry

In 1024-dimensional space the index exhibits a geometry that differs sharply
from low-dimensional intuition:

- **Centroid-to-vector distances dominate centroid-to-centroid distances.**
  Typical vector-to-centroid distance is ~0.6 L2, while adjacent centroid
  pairs are only ~0.12 apart. This is expected: two adjacent centroids
  differ in a few dimensions, while a vector deviates from its centroid
  across all 1024 dimensions. In L2, the centroid-centroid axis contributes
  ~0.06 of the 0.6 total distance; the other 1023 orthogonal dimensions
  contribute the rest.

- **Cluster radii are large relative to centroid separation.** The p90
  cluster radius (~0.80) exceeds the nearest-neighbor inter-centroid
  distance (~0.12) by 6-7x. Clusters overlap extensively in high
  dimensions. This is why radius-corrected scoring fails: subtracting a
  radius comparable to the centroid distance itself destroys all ranking
  signal.

- **Centroids are closer to external points than individual vectors.**
  The centroid is the mean across 1024 dimensions; averaging cancels
  per-dimension deviations, making it systematically closer to any
  external point (like a query) than most individual vectors in the
  cluster. This is visible in the `score/gt_dist` diagnostic: for both
  selected and missed leaves, the ratio is < 1 (avg 0.87 for TP, 0.95
  for FN), meaning `dist(q, centroid) < dist(q, nearest_GT_vector)`.
  The centroid distance is an *optimistic* proxy -- individual vectors
  are farther from the query than the centroid suggests. For missed
  leaves this means the beam was correct to rank them low: both the
  centroid (0.83) and the GT vector (0.89) are genuinely far. The miss
  is not caused by a misleading centroid hiding a close vector.

### Search geometry: coverage vs capacity

For the search beam to find all GT vectors, every cluster containing a GT
vector must be reachable. Two conditions must hold simultaneously:

1. **Coverage**: `search_radius >= max d(q, c)` over all GT-containing
   centroids c. By triangle inequality, `d(q, c) <= d(q, v) + d(v, c)`,
   so a sufficient condition is `search_radius >= GT_radius + cluster_radius`
   where GT_radius = max dist to any GT vector and cluster_radius = max
   dist from any GT vector to its centroid.

2. **Capacity**: `beam_max >= number of clusters within search_radius`.
   Even with sufficient radius, the beam can only hold a finite number of
   clusters.

In practice, condition (1) is almost always satisfied without the
`+ cluster_radius` term. Since `score/gt_dist < 1` (centroids are closer
to the query than individual vectors), centroids of GT-containing clusters
typically fall *inside* the GT hypersphere, not outside it. The triangle
inequality bound is loose because the "centroid behind the vector" geometry
(centroid farther from query than the vector) is rare in high dimensions --
the averaging effect pulls centroids toward external points.

The **binding constraint is capacity**, not coverage. From the leaf miss
diagnostic, missed GT leaves have centroid-distance rank median ~237 while
beam size is ~120. These centroids are within the search radius but are
outnumbered by non-GT clusters at similar distances. Expanding the search
radius (increasing tau) doesn't help because it admits even more non-GT
clusters, keeping the GT clusters below the beam_max cutoff.

### Replication behavior (eps/RNG/max_replicas)

The `rng_select` function controls write-time replication. During `add()`,
a vector is navigated to its nearest leaves (write beam), and `rng_select`
picks which subset to register in. Three parameters govern this:

- `write_rng_epsilon`: fractional distance window. A candidate leaf at
  distance d is considered only if `|d - d_best| <= eps * d_best`. Small
  eps (0.01) restricts to nearly-equidistant leaves; large eps (10) admits
  all candidates.

- `write_rng_factor`: the RNG pruning threshold. A candidate is blocked if
  `d_candidate > dist(c_candidate, c_selected) * factor` for any
  already-selected centroid. With factor=1 (strict RNG), almost everything
  is blocked because vector-to-centroid distances (~0.6) vastly exceed
  inter-centroid distances (~0.12). Factor must be >= ~5 to allow any
  replication at all, at which point it barely filters.

- `max_replicas`: hard cap on copies per vector.

### Why boundary replication does not improve recall

Experiments with eps=0.01, max_replicas=4 (RNG disabled) show:

- 11.8% of vectors replicated, avg 2.15 copies.
- d2/d1 ratio median 1.025: replicated vectors are genuine Voronoi
  boundary vectors (the second-nearest centroid is only 2.5% farther).
- Inter-centroid distance between replica leaves: median 0.12. The replica
  clusters are immediate neighbors.
- Recall unchanged vs no-replication baseline (96.19% vs 96.29%).

This fails because the replicated vectors don't need help. They sit between
adjacent clusters that the read beam already covers together. The recall
misses are in a different population entirely:

- Missed GT leaves are at centroid-distance rank 200-600 (beam covers
  rank 1-128).
- Missed GT leaves have avg gt_count=1.2 (vs 5.2 for selected GT leaves).
  These are isolated GT vectors deep inside a far cluster.
- The centroid distance for missed leaves (~0.85) genuinely exceeds that
  of selected leaves (~0.70). The centroid ranking is not misleading;
  these clusters really are far from the query.
- The score/gt ratio for missed leaves is ~0.95, meaning the GT vector
  sits near the cluster's boundary closest to the query, but the cluster
  itself is too far to enter the beam.

### Why write-time replication cannot fix far misses

The write beam for vector v finds the 10-16 nearest clusters to v.
Replication can only place v in one of those clusters. But the recall
misses are vectors where the nearest cluster (to the vector) is far from
the query. For replication to help, the vector would need to be placed in
a cluster close to the query -- but that cluster isn't in the vector's
write beam (it's close to the query, not to the vector). The write beam
is centered on the vector; the read beam is centered on the query. No
epsilon or RNG tuning changes this fundamental mismatch.

### Promising directions

1. **Outward replication (wider write beam + anti-centroid selection).**
   The key observation: a missed GT vector v sits at the edge of its
   primary cluster c1, extending in direction d = v - c1. Queries that
   need v approach from that same direction. If we replicate v to a
   cluster whose centroid is "ahead" of v (in direction d), queries from
   that direction find v in their beam.

   The mechanism: compute the anti-centroid point a = 2v - c1 (reflection
   of c1 through v). Among the write beam candidates, pick the replica
   cluster closest to a. This selects the cluster most aligned with the
   direction the vector extends from its primary centroid, rather than the
   nearest adjacent cluster (which the beam already covers).

   This requires a wider write beam (write-beam-max=64 instead of 16) to
   provide enough candidate diversity. Navigation cost barely increases
   since distance computations already happen for all children of surviving
   parents; write-beam-max only controls the cutoff. Anti-centroid
   computation is O(D) per vector, and scoring k candidates against it is
   O(k*D) -- negligible.

   With eps-based selection (eps=0.01), the inter-centroid distance between
   replica leaves is ~0.12 (adjacent clusters, redundant). Outward
   replication should produce much larger inter-centroid distances, placing
   replicas in genuinely different regions of the query space.

2. **Query-time adaptive beam expansion.** Detect when the read beam is
   insufficient (e.g., score gap at the beam edge is small, or coverage
   of the leaf population is low) and expand dynamically for hard queries.
   This targets the problem directly without index bloat.

## stats

--leaf-miss-diagnostic

Avg beam size / total leaves
Missed GT vectors count (per query avg, total)
Missed leaf rank distribution (min, p10, p25, p50, p75, p90, max)
Recovery by beam expansion (+5, +10, +20, +50, +100 leaves)
Top 10 queries by missed GT count (with near-miss/far-miss breakdown)
Leaf Traits Comparison table (Sel+GT vs Sel+noGT vs Miss+GT):
score (centroid dist)
rank
leaf_size
p90_radius
gt_count
min_gt_dist
score/gt_dist
--geometry-diagnostic

cluster radius (beam med p90) -- per-query median p90_radius of beam leaves
search radius (d1*(1+tau)) -- tau-based beam cutoff threshold
beam radius (farthest sel.) -- actual farthest centroid in beam
GT radius (max gt dist) -- farthest GT vector from query
Tree stats (always printed)

Replication: avg replication, % w/ replica, replica count distribution
Replicated vector distance table:
d2/d1 (boundary proximity)
d_nearest (to closest centroid)
d_farthest (to farthest replica centroid)
inter-centroid dist (between replica leaves)
Index quality (always printed)

GT clusters (p100, p95, p90) -- how many clusters GT vectors spread across
