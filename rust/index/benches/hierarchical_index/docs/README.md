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

## Commit-time memory

`HierarchicalSpannWriter::commit` materialises the in-memory tree into four
forked blockfiles (scalar metadata, vector data, list data, posting lists).
The fork phase dominates the commit-time memory spike — empirically the
RSS delta during fork tracks the on-disk size of the previously committed
index almost exactly, while the subsequent flush phase is allocation-neutral
or even returns memory.

### Why fork is expensive

Each `set()` on an `ArrowOrderedBlockfileWriter` may push a completed Arrow
`Block` onto an internal `Vec<Block>` (`inner.completed_blocks`). Those
blocks are not freed until `flusher.flush()` writes them to storage. Any
block that gets touched during commit therefore lives in RAM until the very
end of the commit pipeline.

For a hierarchical SPANN index the per-node metadata (length, parent,
node_type, centroid, centroid_code, children) is itself O(live tree) — at
CP55 of a 113M run that was ~145K nodes worth of metadata blocks. The
posting list writer already skipped lazy shells (`!leaf.ids.is_empty()`
guard), but the metadata writers did not, so every commit re-touched every
metadata block of the entire live tree even though only a small fraction
of nodes were actually mutated since the previous commit.

### Option B (implemented): dirty-node commit

The writer now tracks three "dirty" sets, populated at every mutation site:

```
dirty_nodes:      DashSet<NodeId>  -- nodes inserted or in-place mutated
dirty_versions:   DashSet<u32>     -- vector ids whose version was bumped
dirty_embeddings: DashSet<u32>     -- vector ids whose embedding was added
```

`commit()` iterates `(dirty_nodes ∩ live) ∪ tombstones` for per-node
metadata, `dirty_versions` for the version prefix, and `dirty_embeddings`
for the embedding prefix. Lazy shells inherited from the forked parent
are skipped entirely; their on-disk blocks remain unmodified and are
inherited verbatim via copy-on-write.

Empirically the dirty-to-live ratio at CP55 was ~38K / ~145K ≈ 26%, so
per-commit allocations drop from O(live tree) to O(dirty tree) — roughly
4x less commit RAM in steady state, enough to fit a 113M build in ~500 GB
without OOM.

The `WriterMemoryUsage` struct exposes `dirty_nodes_count`,
`dirty_versions_count`, and `dirty_embeddings_count` so the bench can
print the dirty-set sizes alongside total in-memory state at every
checkpoint boundary, making it easy to spot a missed `mark_dirty` call
(dirty_nodes should be near-zero immediately after `open()` and grow with
adds/splits/merges between commits).

#### Correctness contract

Dirty-node commit is correct only if **every** mutation marks the affected
ids dirty before the next commit. Every site in `writer/mod.rs` that
mutates the in-memory tree calls one of `mark_node_dirty`,
`mark_version_dirty`, or `mark_embedding_dirty`:

- `nodes.insert(id, ...)` → `mark_node_dirty(id)`
- `register_in_leaf(id, ...)` on success → `mark_node_dirty(leaf_id)`
- `scrub(id)` when entries removed → `mark_node_dirty(cluster_id)`
- `replace_child(parent, ...)` → `mark_node_dirty(parent)` plus each new
  child whose `parent_id` and `centroid_code` get rewritten
- `remove_child_locked(parent, ...)` → `mark_node_dirty(parent)` plus the
  surviving "only_child" or new_root when applicable
- `create_root_above(...)` / `adopt_orphans(...)` → mark new root and
  re-parented children dirty
- `add()` / `reassign()` → `mark_embedding_dirty` and `mark_version_dirty`
  for the touched data id

When a node is tombstoned (`tombstones.insert(id)`), it is also removed
from `dirty_nodes` so we don't try to write a node that is being deleted
in the same commit. When a previously tombstoned id is resurrected
(merge_leaf bail-out re-inserts a leaf that was just tombstoned), the id
is removed from `tombstones` and re-marked dirty. `open()` initialises
all three dirty sets empty so a freshly reopened checkpoint commits
nothing if no mutations happen before the next commit.

### Option C (deferred): stream completed blocks during commit

The architectural fix is in `chroma_blockstore`: change
`swap_current_delta` to *immediately* upload the just-completed `Block`
to storage and drop it from RAM, replacing
`inner.completed_blocks: Vec<Block>` with a `Vec<Uuid>` (or similar)
tracking the in-flight uploads. Commit RAM would then be bounded by the
working set of one delta block (~1-2 MB) regardless of corpus size or
mutation rate, instead of being O(touched blocks).

#### Benefit relative to Option B

Option B caps commit RAM at O(dirty tree). That works well in steady
state where each commit only touches a fraction of the tree, but it has
two failure modes that Option C addresses:

1. **Initial bulk load**: the first commit of a fresh index writes every
   node from scratch, so dirty == live and Option B saves nothing. With
   Option C, that first commit fits in 1-2 MB of writer RAM regardless
   of how many nodes were inserted.

2. **Heavy-mutation checkpoints**: any rebalancing pass that touches a
   large fraction of the tree (e.g. branching factor change, tau retune,
   level-width retune) reverts to O(live tree) commit RAM. Option C is
   indifferent.

Option C also benefits production blockfile writers, not just the bench
— any caller that flushes a large dirty delta currently pays the same
"completed_blocks accumulates until flush" cost.

#### Why it isn't done yet

It is a real refactor of the writer/flusher contract:

- The flusher loses its in-memory `Vec<Block>`; downstream code that
  expected to access blocks between `commit()` and `flush()` (e.g. the
  cache pre-warm path) needs to be reworked.
- The `block_cache` insertion point shifts: inserting *before* upload
  caches an unflushed block that may never land on disk if upload fails;
  inserting *after* upload means cold reads right after commit.
- Error handling becomes more nuanced: a failed upload mid-commit needs
  to either retry, surface as a commit error, or be tracked for later
  retry — all of which currently work because everything is buffered.

For the bench, Option B is sufficient to fit 113M on a 495 GB box. Option
C is the right long-term fix for production and would also let the bench
scale to arbitrarily large corpora without further work.

## Reader-side block pinning

`HierarchicalSpannWriter` keeps two `BlockfileReader`s alive between
commits — `posting_list_reader` (used by `load(node_id)` for lazy
posting-list shells) and `vector_data_reader` (used by `load_raw(ids)`
for full-precision embeddings during NPA / split / reassign). Each
underlying `ArrowBlockfileReader` owns a per-reader pin set:

```
loaded_blocks: Arc<RwLock<HashMap<Uuid, Box<Block>>>>
```

This is **not** the foyer block cache (which lives in `BlockManager` and
is bounded by `--max-cache-bytes`). It is a separate, **unbounded,
never-evicting** map — every block ever fetched through this reader is
pinned in here for the reader's entire lifetime. The reason is the
unsafe `transmute::<&Block, &Block>` in `ArrowBlockfileReader::get_block`
that extends the borrow to the reader's `'me` lifetime so callers can
return `V<'me>` values that reference block-internal buffers without
copying. The safety invariant — "never remove the `Box<Block>` from the
HashMap, so the reference is always valid" — is what forces the cache
to be unbounded.

### Why this dominates RSS in the 100M+ regime

At dim=1024, lookups in `vector_data_reader` are scattered by vector id
across the *entire* embedding blockfile. With ~360 embeddings per block
on disk, the probability that a given block is hit at least once during
a CP that loads N embeddings is `1 - (1 - N / total)^360`. By
N ≈ 5M / total ≈ 100M that's effectively 100% per block — a single
checkpoint touches essentially every embedding block. Same story (worse,
actually) for `posting_list_reader`: navigate-driven loads cover ~all
leaf-postings blocks within a few CPs of a fresh reopen.

The result: `loaded_blocks` ends up pinning hundreds of GB by the end of
a CP, which is the bulk of the unaccounted RSS visible as
`(jemalloc.allocated - writer.memory_usage)` in the per-CP `Process mem`
line. Concretely, at CP206 of a 113M run:

```
balanced  RSS=359 GB anon, jemalloc.allocated=404 GB
writer.memory_usage=35 GB
gap = ~370 GB  ← all in loaded_blocks
```

Dropping the writer at the end of the CP releases everything (drops the
two readers → drops `loaded_blocks` → frees ~370 GB; jemalloc moves it
to `retained` and RSS recovers shortly after). That's why per-CP peak
RSS climbs across a writer's lifetime and falls back at the reopen
boundary, even with the foyer cache strictly bounded.

### Option 1 (implemented, bench-only): clear between CP phases

`HierarchicalSpannWriter::clear_reader_block_pins(&self)` calls
`BlockfileReader::clear_loaded_blocks()` on both readers, draining the
`loaded_blocks` HashMaps. The bench calls it once per CP after
`balance_index_parallel` returns and before `commit()`, gated by the
default-on `--clear-reader-block-pins` flag.

#### Safety contract

`ArrowBlockfileReader::clear_loaded_blocks(&self)` is sound only if **no
value previously returned by this reader is still borrowed**. The unsafe
`transmute` pretends a `&Block` rooted in the read guard lives for `'me`;
freeing the box invalidates that borrow.

The bench's call site satisfies this trivially: by the time we clear,
all `add()` and `balance_index_parallel()` work has returned and joined,
the writer is held by `&mut writer` on the main thread (no concurrent
borrowers), and the writer's own `load`/`load_raw` paths copy data via
`to_vec()` and drop the returned `V<'me>` before returning. There are no
outstanding `V` references on the stack at the clear point.

#### Per-CP probe

The bench prints a "Reader pins (balanced)" line right above the
existing "Writer mem" line. It captures pin stats *before* the clear so
the numbers reflect the actual peak paid for during add+balance, then
reports the post-clear RSS drop:

```
Reader pins (balanced): postings 5.2K blocks/14.1GB | vector_data 280K blocks/372.4GB | total 386.5GB | post-clear rss 38.4GB (-336.1GB, freed pl+vd=386.5GB)
```

The byte column undercounts the heap footprint by the `Box` header,
RecordBatch metadata, and validity bitmaps; expect the true footprint to
be ~1.3x larger than reported.

### Option 2 (deferred, upstream fix): bounded per-reader cache

The architectural fix is in `chroma_blockstore`: replace
`loaded_blocks: HashMap<Uuid, Box<Block>>` with a bounded LRU (or feed
lookups directly off the foyer block cache and stop double-pinning).
This requires eliminating the `transmute<&Block, &Block>` so callers
either receive an owned `Arc<Block>` guard whose lifetime they manage,
or copy out at the read API surface. Concretely:

- Change `get_block` to return `Result<Option<Arc<Block>>, GetError>`
  and have callers thread the `Arc` through to `V`'s materialisation.
- Stop maintaining a per-reader pin set entirely; let the foyer block
  cache be the single source of truth for hot-block residency. Cache
  misses re-fetch from storage on demand; with the foyer cache sized to
  the working set this is rare.
- Keep `clear_loaded_blocks` as an explicit `evict_all` on the foyer
  cache for callers (like the bench) that want to recover memory at
  known phase boundaries.

This is the right long-term fix for production too — any service that
keeps a `BlockfileReader` alive across many random-access reads will
otherwise pin a fraction of its blockfile that grows monotonically with
read coverage, completely independent of the foyer cap. Today the bench
hides this with periodic reopens; a long-lived production reader has no
such reset and will accumulate RSS until the process restarts.

#### Why it isn't done yet

The unsafe `transmute` is load-bearing for the entire ArrowReadable
value family — every `V<'me>` constructed from a block (e.g.
`QuantizedCluster<'static>`, `&'static [f32]`) carries borrows that
currently rely on the box living forever. Migrating to `Arc<Block>`
guards changes the read API signature, the `ArrowReadableValue` trait,
and every call site that synthesises a `V<'me>`. It is a wide refactor
but a mechanical one.

For the bench, Option 1 is sufficient to keep balanced-RSS bounded by
`writer.memory_usage + foyer_cap + jemalloc slack` (~50 GB at our
configs) and lets a 113M+ build fit comfortably on a 495 GB box.

### Recall-path corollary: `load_all_embeddings` + `load_all_postings`

The recall step (`HierarchicalSpannReader`) has the same two problems,
just with a different trigger:

- `load_all_postings()` copies every leaf posting into the reader's
  owned `nodes` DashMap **and** pins every posting block in the posting
  reader's `loaded_blocks` (doubled memory: ~30 GB owned + ~30 GB pins).
- `load_all_embeddings()` copies every f32 embedding into the reader's
  owned `embeddings` DashMap **and** pins every vector-data block in the
  vector-data reader's `loaded_blocks` (doubled memory: at 113M × dim=1024
  that is ~454 GB owned + ~454 GB pins, which will OOM any non-TB box).

Two fixes land in the bench, behind a single unified flag:

1. **Post-eager-load pin clear (unconditional).** Even on the eager
   path, after the reader finishes `load_all_postings` /
   `load_all_embeddings`, the bench calls
   `HierarchicalSpannReader::clear_loaded_blocks()`, which drops both
   readers' `loaded_blocks` HashMaps. The reader-owned copies are
   unaffected. Halves the recall-step RSS.

2. **Lazy recall path (`--lazy-recall`, default `true`).** A single
   flag that turns on the entire production-shaped recall path:

   - **Skip eager loads.** Both `load_all_postings` and
     `load_all_embeddings` are skipped at setup.
   - **Per-query lazy fetches with bounded async concurrency.** Each
     query calls `HierarchicalSpannReader::search_with_policy_lazy`,
     which uses `futures::stream::buffer_unordered` to keep up to
     `LAZY_RECALL_CONCURRENCY` (= 32) posting `load_node` calls in
     flight, then up to 32 vector-data `get` calls in flight for the
     rerank set. With ~32 rayon workers each driving one query, the
     system holds ~1k in-flight blockfile ops, which keeps the NVMe
     queue deep on the cold pass without changing query results.
     Embeddings land in a per-query local `HashMap<u32, Vec<f32>>`,
     never the shared `embeddings` DashMap.
   - **Cold/warm two-pass per row.** Each `(tau, rerank)` row runs
     twice: a `cold` pass with both `loaded_blocks` HashMaps and per-
     leaf posting data cleared first, then a `warm` pass that reuses
     what cold populated. The pair isolates the lazy fetch cost.
   - **Between-row clearing.** After every row's warm pass (and at the
     top of every cold pass) the bench clears both readers'
     `loaded_blocks` and the per-leaf posting data in `self.nodes`,
     so block pins stay bounded by one row's working set, not the
     union across all rows.

   `--lazy-recall=false` reverts to the legacy eager single-pass path
   (`load_all_postings` + conditional `load_all_embeddings` +
   `search_with_policy_sync`, no clearing). That path only fits in
   RAM with room to spare below ~50M vectors at dim=1024 on a 495 GB
   box with `max-cache-bytes=16 GiB`.

The lazy path is also the production-shaped one: it is what a long-
lived server would want. The eager path is a benchmark optimisation
useful only when the whole embedding set fits in RAM.

## Future improvements
- replication:
  - Experiment with internal node replication
  - Experiment with outward replication as a full replacement for the RNG rule
- Better clustering
  - Revisit our measure of optimal clustering `--compute-optimal-gt`. It looks broken currently. It would be very valuable to know how well our clustering compares to the optimal clustering.
  - Neighborhood aware split: split on the neighborhood of the centroid, not on size.
    - Or other forms of unbalanced clustering
- Leaf selection
  - try more scoring strategies from [Leaf selection](#leaf-selection) above
- Query performance
  - Find a way to always choose the optimal beam width for each level.
  - Tau values are dataset dependent. in high dimensional datasets vectors are closer together, so tau must be smaller for the same recall
  - centroids as residuals of parent or of center? Possible: compounding residual error through levels. would save us from quantizing data vector against each node as a query
- Build time
  - Thread scaling (esp of balancing)
  - Batch distance computations during split, merge, npa step (recreate the function from the paper)
  - 4 bit quantized NPA 
  - Deferred replication (not at insert time)
- Space efficiency
  - u8 versions, instead of u32
- Commit memory
  - Implement Option C in `chroma_blockstore` (stream completed blocks
    during `swap_current_delta` so commit RAM is bounded by one delta
    block instead of O(dirty tree)). See "Commit-time memory" above for
    the trade-offs and motivation.
- Reader memory
  - Implement Option 2 in `chroma_blockstore` (replace
    `ArrowBlockfileReader::loaded_blocks` with a bounded LRU and an
    `Arc<Block>` return contract on `get_block`, so per-reader RSS is
    bounded by the foyer cache cap regardless of read coverage). See
    "Reader-side block pinning" above for the diagnosis and the API
    surface that needs to change.