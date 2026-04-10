# Leaf reranking with codes

## Problem

Scanning posting lists is the dominant search cost (~50% of latency, ~80% of
I/O). Navigation selects leaves by centroid distance, which is an imperfect
proxy for whether a leaf actually contains nearest neighbors. The gap between
"L2 R@100" and "opt R@100" in the recall table shows that navigation
consistently picks suboptimal leaves.

With the production architecture (quantized_spann.rs), data lives in three
tiers ordered by access cost:

| Tier | Data | Location | Cost |
|------|------|----------|------|
| 1 | Node metadata: centroids, centroid codes, children, cluster lengths, p90_radius | Memory | Free |
| 2 | Per-vector quantized codes (posting lists) | Disk / S3, lazy `load(cluster_id)` | Medium |
| 3 | Per-vector f32 embeddings | Disk / S3, lazy `load_raw(&ids)` | High |

Today, navigation (Tier 1) directly determines which posting lists to fetch
(Tier 2). There is no intermediate filtering stage that uses in-memory data
to prune candidate leaves before committing to expensive I/O.

## Idea

Insert a cheap in-memory pruning stage between navigation and posting list
fetch. Store a small number of **representative codes** per leaf in the node
metadata (Tier 1). These are codes of actual data vectors in the leaf, not the
centroid code.

Current pipeline:

    navigate (Tier 1) -> pick N leaves -> fetch N posting lists (Tier 2) -> scan -> rerank (Tier 3)

Proposed pipeline:

    navigate (Tier 1) -> pick W leaves (wide) -> LEAF RERANK (Tier 1) -> pick N leaves (narrow) -> fetch N posting lists (Tier 2) -> scan -> rerank (Tier 3)

The leaf rerank stage uses only in-memory data to better discriminate which
leaves actually contain nearest neighbors. This decouples "how many leaves do
I consider" from "how many posting lists do I fetch."

## Why centroid distance is a bad proxy

For Euclidean distance, the actual distance to a vector v in leaf i is:

    ||q - v||^2 = ||q - c_i||^2 + ||v - c_i||^2 - 2<q - c_i, v - c_i>

Navigation uses only the first term. A leaf can have a mediocre centroid
distance but contain vectors very close to the query when:

- The leaf is large/spread (the ||v - c_i||^2 term is large for some vectors)
- The query-to-centroid direction aligns with vectors in the leaf (the cross
  term <q - c_i, v - c_i> is large)

Radius correction (p90_radius) addresses the first case. Representative codes
address both cases by directly estimating distance to actual vectors.

## Representative code selection

At split time (or any time a leaf is rebuilt), select R representative codes
from the leaf. Candidates for selection strategy:

1. **Closest to centroid**: the R vectors with smallest ||v - c||. These
   represent the "core" of the cluster and give a baseline distance estimate.
2. **K-means within leaf**: run mini k-means with R clusters on the leaf
   vectors and store the codes of the medoids. Better coverage of non-spherical
   clusters.
3. **Farthest-point sampling**: greedily pick the vector farthest from all
   previously selected representatives. Maximizes coverage of the leaf's
   extent.

Option 1 is simplest. Option 3 gives the best coverage and is likely the best
default for catching queries that are near the periphery of a cluster.

## Variant A: per-leaf QuantizedQuery (independent codes)

### How it works

Store R representative codes per leaf, quantized as residuals relative to the
leaf's own centroid (same coordinate system as the leaf's posting list codes).

At query time, after navigation produces W candidate leaves:

1. For each candidate leaf, compute a QuantizedQuery relative to that leaf's
   centroid:
   - r_q = q - c_i
   - qq = QuantizedQuery::new(r_q, padded_bytes, c_norm, c_dot_q, q_norm)
2. Score the R representative codes against this QuantizedQuery.
3. Take min distance as the leaf's "pilot score."
4. Sort candidate leaves by pilot score, keep top N.
5. Fetch only those N posting lists from disk.

### Cost

Compute per candidate leaf:
- QuantizedQuery construction: ~8us (residual subtraction + norms + bit packing)
- Code scoring: R x ~25ns per code distance

For W=512 candidates, R=8 representatives:
- QuantizedQuery: 512 x 8us = ~4ms
- Scoring: 512 x 8 x 25ns = ~100us
- Total: ~4.1ms

I/O: Zero (all data in Tier 1).

Storage per leaf: R x code_size bytes. For R=8, dim=1024, 1-bit codes:
8 x 128 = 1KB per leaf. With ~2300 leaves: ~2.3MB total.

### I/O savings

Pruning from W=512 to N=64: fetch 64 posting lists instead of 512.
At ~175KB per posting list (1400 vectors x 128 bytes/code):
64 x 175KB = 11MB vs 512 x 175KB = 90MB. ~8x I/O reduction.

### Tradeoff

The ~4ms compute overhead for QuantizedQuery construction is significant
relative to the current ~1.5ms navigation time. This is the dominant cost
and is proportional to W (the wide beam size). The benefit is only realized
when storage latency is high enough that the I/O savings dominate -- which
they will in production (disk/S3) but may not in the benchmark (everything
in memory).

## Variant B: parent-relative codes (reusing navigation QuantizedQuery)

### How it works

Store R representative codes per leaf, but quantized as residuals relative to
the **parent** centroid (the same coordinate system as the centroid codes used
during navigation at that level).

During navigation at the leaf level, the system already computes a
QuantizedQuery relative to each parent internal node:
- r_q = q - c_parent
- qq_parent = QuantizedQuery::new(r_q, ...)

This QuantizedQuery is used to score all children's centroid codes. The
representative codes are in the same coordinate system, so they can be scored
against the same QuantizedQuery with zero additional QuantizedQuery
construction.

At query time, during the leaf-level navigation step:

1. Navigation computes QuantizedQuery for each parent node (already happens).
2. After scoring children's centroid codes, also score each child leaf's R
   representative codes against the same QuantizedQuery.
3. Take min(centroid_code_dist, min(representative_code_dists)) as the leaf's
   combined score.
4. Beam selection uses this combined score instead of just the centroid score.

### Cost

Compute: only R extra code distance evaluations per candidate leaf.
For W=512 candidates, R=8: 512 x 8 x 25ns = ~100us total. Essentially free.

No additional QuantizedQuery construction -- the parent-relative QQ is already
computed during navigation.

I/O: Zero.

Storage per leaf: same as Variant A (R x code_size = 1KB per leaf for R=8,
dim=1024).

### Tradeoff

Lower precision than Variant A. The QuantizedQuery is computed relative to
the parent centroid, not the leaf centroid. The representative codes are
residuals relative to the parent centroid. This means the quantization
captures the vector's position relative to the parent, not relative to its
own cluster center.

For leaves that are close to their parent centroid, this is fine -- the
parent-relative residual is similar to the leaf-relative residual. For leaves
far from their parent, the quantization error grows because the residuals
are larger and less well-represented by 1-bit codes.

The benefit: ~40x lower compute overhead (100us vs 4ms) because it completely
avoids per-leaf QuantizedQuery construction. This makes it viable even in the
in-memory benchmark where I/O savings don't apply.

## Comparison

| | Variant A (per-leaf QQ) | Variant B (parent-relative) |
|---|---|---|
| Compute overhead | ~4ms for W=512 | ~100us for W=512 |
| QQ construction | W new QuantizedQueries | 0 (reuses navigation QQ) |
| Quantization precision | High (residual vs own centroid) | Lower (residual vs parent centroid) |
| Implementation complexity | Moderate (new stage after navigate) | Lower (extends existing navigate loop) |
| When it helps most | Production (disk/S3 storage) | Both benchmark and production |
| Code storage coordinate system | Leaf-relative residuals | Parent-relative residuals |

## Implementation plan

### Data structure changes (both variants)

Add to `LeafNode`:

    representative_codes: Vec<u8>  // R * code_size bytes, contiguous
    representative_count: u8       // R (typically 4-8)

### Write path changes (both variants)

In `split_leaf` and any leaf rebuild path, after computing the new leaf's
codes, select R representatives by farthest-point sampling and store their
codes in the leaf node.

### Read path changes

**Variant A**: Add a `leaf_rerank` step in `search_with_policy` between
navigate and scan. Takes the navigation output (wide beam), constructs
QuantizedQuery per leaf, scores representative codes, prunes to narrow beam.

**Variant B**: Modify `navigate_quantized` / `navigate_1bit` to also score
representative codes alongside centroid codes during the leaf-level step.
Use min(centroid_score, min(representative_scores)) as the child score.

### Benchmark changes

Add CLI flags:

    --leaf-rerank-reps R        # number of representative codes per leaf (0 = off)
    --leaf-rerank-wide W        # wide beam for Variant A (multiplier on beam_max)

Add columns or modify existing columns to track the leaf rerank stage
separately (pilot scores, timing, MB).

## Recommendation

Start with Variant B. It is simpler to implement (extends the existing
navigate loop rather than adding a new stage), has negligible compute
overhead, and will show immediately in the benchmark whether representative
codes improve leaf selection quality (the L2 R@100 vs opt R@100 gap).

If Variant B shows meaningful recall improvement, Variant A can be added later
as an option for production deployments where the higher-precision per-leaf
QuantizedQuery justifies its compute cost against the I/O savings.
