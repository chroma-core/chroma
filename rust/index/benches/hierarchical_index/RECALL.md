
# Recall

## Where we are today:
On 40M vectors, we are at 94% recall@100 with 40.7ms latency and 42.1MB of data scanned.

We can trade latency for recall by increasing the beam size, but it's expensive. I don't have empirical numbers on how the recall/latency trade off scales at this data size (40M), but I suspect it's not great/linear.

## Where we lose recall

All the possible places to lose recall are:
- navigating each level of the tree:
  - search width (nodes you consider at each level. ie which ones and how many)
  - beam width (nodes you select)
  - precision of the centroids and any reranking you do
- clustering quality (how well the centroids represent the data in the leaf)
- code precision during posting list scan
- reranking of the posting list codes

For all the recall we lose across this whole pipeline, the vast majority is lost at the leaf selection step. We navigate the internal layers of the tree at very high recall, we look at the right set of leaves, we select the right number of leaves, but we choose the wrong ones. This step drops us from ~100% recall to 95%. Then we scan the posting lists and, with a little bit of rerank, pick the best possible vectors from the leaves that were chosen, keeping us at 95%.

## How we can close the gap without increasing latency

Leaf selection, and thus recall, can be improved by either improving our leaf scoring approach (so that the right leaves are selected) or by replicating vectors to multiple leaves, so that it's harder to choose the wrong leaf.

### Leaf scoring 
I've tried two different leaf scoring approaches (outside of the normal centroid distance approach), neither of which have helped.
- Radius correction scoring: Account for the radius of the leaf to provide and upper and lower bound to the distance from the query to any vector in the leaf. This didn't work becuase the leaf radius is almost always the same size or larger than the distance from the query to the centroid of the leaf, resulting in all leaves being scored the same.
- Representative codes: Store a few representative codes per leaf, to provide information about the shape of the leaf cluster. This didn't work because the representative codes are quantized and introduce too much noise to be useful. I also suspect that most clusters are close to spherical, so the codes don't add information.

Not yet tried (details above):
- Directional variance correction.
- Leaf reranking with codes
- Half-space aware navigation

### Replication

We don't currently use replication, because I haven't yet found a configuration that increases recall at all. I have a guess about why this is, and confirming my guess would be a great step to getting replication working.

#### The RNG rule

To the best of my knowledge, the RNG rule (or pruning in general) is necessary for replication to be worth it. If you just replicate without pruning, the resulting bloat more than offsets the recall gains.

For the RNG rule to work, we need to be able to express "skip this candidate if it is too close to any of the already selected clusters". 

The paper expresses this as:
`skip the cluster ij for vector x if Dist(cij , x) > Dist(cij−1, cij )`

We do the same in quantized_spann and in this hierarchical index with an added multiplicative factor to scale the distance between the candidate and the already selected clusters, effectively the condition harder to satisfy as the factor increases.

```rust
if selected_centroids.iter().any(|sel| distance > self.dist(&centroid, sel) * self.config.write_rng_factor) {
    continue;
}
```

#### The problem with the RNG rule

To the best of my understanding the geometry of the wikipedia-en dataset (and other high-dimensional datasets?) makes the RNG rule ineffective at pruning candidates. Here's why:

- Empirically I have found that for a given query, the distance between the query and any candidate centroid is much larger than the distance between any two candidate centroids. (See table below)

```text
This table shows that dist_near and dist_far (the upper and lower bounds on the distance from a
replicated vector to the closest and farthest centroid candidate) are much larger than the distance 
between those two centroids.

Total entries: 4.61M (4.61M valid) | Unique vectors: 3.00M (0 orphaned) | Avg replication: 1.54x | % w/ replica: 35.5%
  Replicated vectors: 1.07M | Avg copies: 2.51 | Distribution: 2x=673927 3x=236419 4x=155825
  metric                              min      p25      p50      p75      max
  ------------------------------  -------  -------  -------  -------  -------
  dist_near (to closest cent.)     0.0723   0.5365   0.5778   0.6148   0.8115
  dist_far  (to farthest cent.)    0.2498   0.5657   0.6065   0.6447   1.2168
  inter-centroid dist              0.0000   0.0736   0.1112   0.1663   0.6881
```

- This means that the ratio between `distance` and `dist(&centroid, sel)` is always large.
- Without a the `write_rng_factor` multiplier, the RNG rule will always reject the candidate.
- Our goal is to make the this expression truthy for dist_near candidates and falsey for dist_far candidates.
- So let's just set the `write_rng_factor` to a value that bisects the candidates, right? 
- The problem is that, with the observed distances, write_rng_factor needs to be very precise. A little too small and you reject everything, a little too large and you reject nothing.
- For example, to bisect the p50 candidates in the table above, write_rng_factor needs to be 5.3251. If the value is too small, 5.196, you reject everything. If the value is too large, 5.454, you reject nothing.
- This is not practical to set manually.

```
Calculations for the p50 candidates:

0.1112 * write_rng_factor = (0.6065+0.5778) / 2
0.1112 * write_rng_factor = 0.59215
write_rng_factor = 0.59215 / 0.1112
write_rng_factor = 5.3251

too low (skips everything):
0.1112 * write_rng_factor = 0.5778
write_rng_factor = 0.5778 / 0.1112
write_rng_factor = 5.196

too high (skips nothing):
0.1112 * write_rng_factor = 0.6065
write_rng_factor = 0.6065 / 0.1112
write_rng_factor = 5.454
```

#### Theoretical/Geometric interpretation

So why is it that for a given query, the distance between the query and any candidate centroid is much larger than the distance between any two candidate centroids?

Opus says:
> Typical vector-to-centroid distance is ~0.6 L2, while adjacent centroid pairs are only ~0.12 apart. This is expected: two adjacent centroids differ in a few dimensions, while a vector deviates from its centroid across all 1024 dimensions. In L2, the centroid-centroid axis contributes ~0.06 of the 0.6 total distance; the other 1023 orthogonal dimensions contribute the rest.

