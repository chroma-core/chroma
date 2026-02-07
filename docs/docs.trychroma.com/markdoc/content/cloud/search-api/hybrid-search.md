---
id: hybrid-search
name: Hybrid Search with RRF
---

# Hybrid Search with RRF

Learn how to combine multiple ranking strategies using Reciprocal Rank Fusion (RRF). RRF is ideal for hybrid search scenarios where you want to merge results from different ranking methods (e.g., dense and sparse embeddings).

{% Banner type="tip" %}
**Prerequisites:** To use hybrid search with sparse embeddings, you must first configure a sparse vector index in your collection schema. See [Sparse Vector Search Setup](../schema/sparse-vector-search) for configuration instructions.
{% /Banner %}

## Understanding RRF

Reciprocal Rank Fusion combines multiple rankings by using rank positions rather than raw scores. This makes it effective for merging rankings with different score scales.

### RRF Formula

RRF combines rankings using the formula:

{% CenteredContent %}
{% Latex %} \\displaystyle \\text{score} = -\\sum_{i} \\frac{w_i}{k + r_i} {% /Latex %}
{% /CenteredContent %}

Where:
- {% Latex %} w_i {% /Latex %} = weight for ranking i (default: 1.0)
- {% Latex %} r_i {% /Latex %} = rank position from ranking i (0, 1, 2, ...)
- {% Latex %} k {% /Latex %} = smoothing parameter (default: 60)

The score is negative because Chroma uses ascending order (lower scores = better matches).

{% Banner type="tip" %}
**Important:** The legacy `query` API outputs *distances*, whereas RRF uses *scores*
{% /Banner %}


{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Example: How RRF calculates scores
# Document A: rank 0 in first Knn, rank 2 in second Knn
# Document B: rank 1 in first Knn, rank 0 in second Knn

# With equal weights (1.0, 1.0) and k=60:
# Document A score = -(1.0/(60+0) + 1.0/(60+2)) = -(0.0167 + 0.0161) = -0.0328
# Document B score = -(1.0/(60+1) + 1.0/(60+0)) = -(0.0164 + 0.0167) = -0.0331
# Document A ranks higher (smaller negative score)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Example: How RRF calculates scores
// Document A: rank 0 in first Knn, rank 2 in second Knn
// Document B: rank 1 in first Knn, rank 0 in second Knn

// With equal weights (1.0, 1.0) and k=60:
// Document A score = -(1.0/(60+0) + 1.0/(60+2)) = -(0.0167 + 0.0161) = -0.0328
// Document B score = -(1.0/(60+1) + 1.0/(60+0)) = -(0.0164 + 0.0167) = -0.0331
// Document A ranks higher (smaller negative score)
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Rrf Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ranks` | List[Rank] | Required | List of ranking expressions (must have `return_rank=True`) |
| `k` | int | `60` | Smoothing parameter - higher values reduce emphasis on top ranks |
| `weights` | List[float] or None | `None` | Weights for each ranking (defaults to 1.0 for each) |
| `normalize` | bool | `False` | If `True`, normalize weights to sum to 1.0 |

## RRF vs Linear Combination

| Approach | Use Case | Pros | Cons |
|----------|----------|------|------|
| **RRF** | Different score scales (e.g., dense + sparse) | Scale-agnostic, robust to outliers | Requires `return_rank=True` |
| **Linear Combination** | Same score scales | Simple, preserves distances | Sensitive to scale differences |

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# RRF - works well with different scales
rrf = Rrf([
    Knn(query="machine learning", return_rank=True),      # Dense embeddings
    Knn(query="machine learning", key="sparse_embedding", return_rank=True)  # Sparse embeddings
])

# Linear combination - better when scales are similar
linear = Knn(query="machine learning") * 0.7 + Knn(query="deep learning") * 0.3
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// RRF - works well with different scales
const rrf = Rrf({
  ranks: [
    Knn({ query: "machine learning", returnRank: true }),      // Dense embeddings
    Knn({ query: "machine learning", key: "sparse_embedding", returnRank: true })  // Sparse embeddings
  ]
});

// Linear combination - better when scales are similar
const linear = Knn({ query: "machine learning" }).multiply(0.7)
  .add(Knn({ query: "deep learning" }).multiply(0.3));
```
{% /Tab %}

{% /TabbedCodeBlock %}

## The return_rank Requirement

RRF requires rank positions (0, 1, 2...) not distance scores. Always set `return_rank=True` on all Knn expressions used in RRF.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# ✓ CORRECT - returns rank positions
rrf = Rrf([
    Knn(query="artificial intelligence", return_rank=True),  # Returns: 0, 1, 2, 3...
    Knn(query="artificial intelligence", key="sparse_embedding", return_rank=True)
])

# ✗ INCORRECT - returns distances
rrf = Rrf([
    Knn(query="artificial intelligence"),  # Returns: 0.23, 0.45, 0.67... (distances)
    Knn(query="artificial intelligence", key="sparse_embedding")
])
# This will produce incorrect results!
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// ✓ CORRECT - returns rank positions
const rrf1 = Rrf({
  ranks: [
    Knn({ query: "artificial intelligence", returnRank: true }),  // Returns: 0, 1, 2, 3...
    Knn({ query: "artificial intelligence", key: "sparse_embedding", returnRank: true })
  ]
});

// ✗ INCORRECT - returns distances
const rrf2 = Rrf({
  ranks: [
    Knn({ query: "artificial intelligence" }),  // Returns: 0.23, 0.45, 0.67... (distances)
    Knn({ query: "artificial intelligence", key: "sparse_embedding" })
  ]
});
// This will produce incorrect results!
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Weight Configuration

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Equal weights (default) - each ranking equally important
rrf = Rrf([
    Knn(query="neural networks", return_rank=True),
    Knn(query="neural networks", key="sparse_embedding", return_rank=True)
])  # Implicit weights: [1.0, 1.0]

# Custom weights - adjust relative importance
rrf = Rrf(
    ranks=[
        Knn(query="neural networks", return_rank=True),
        Knn(query="neural networks", key="sparse_embedding", return_rank=True)
    ],
    weights=[3.0, 1.0]  # Dense 3x more important than sparse
)

# Normalized weights - ensures weights sum to 1.0
rrf = Rrf(
    ranks=[
        Knn(query="neural networks", return_rank=True),
        Knn(query="neural networks", key="sparse_embedding", return_rank=True)
    ],
    weights=[75, 25],     # Will be normalized to [0.75, 0.25]
    normalize=True
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Equal weights (default) - each ranking equally important
const rrf1 = Rrf({
  ranks: [
    Knn({ query: "neural networks", returnRank: true }),
    Knn({ query: "neural networks", key: "sparse_embedding", returnRank: true })
  ]
});  // Implicit weights: [1.0, 1.0]

// Custom weights - adjust relative importance
const rrf2 = Rrf({
  ranks: [
    Knn({ query: "neural networks", returnRank: true }),
    Knn({ query: "neural networks", key: "sparse_embedding", returnRank: true })
  ],
  weights: [3.0, 1.0]  // Dense 3x more important than sparse
});

// Normalized weights - ensures weights sum to 1.0
const rrf3 = Rrf({
  ranks: [
    Knn({ query: "neural networks", returnRank: true }),
    Knn({ query: "neural networks", key: "sparse_embedding", returnRank: true })
  ],
  weights: [75, 25],     // Will be normalized to [0.75, 0.25]
  normalize: true
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

## The k Parameter

The `k` parameter controls how much emphasis is placed on top-ranked results:
- **Small k (e.g., 10)**: Heavy emphasis on top ranks
- **Default k (60)**: Balanced emphasis (standard in literature)
- **Large k (e.g., 100+)**: More uniform weighting across ranks

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Small k - top results heavily weighted
rrf = Rrf(ranks=[...], k=10)
# Rank 0 gets weight/(10+0) = weight/10
# Rank 10 gets weight/(10+10) = weight/20 (half as important)

# Default k - balanced
rrf = Rrf(ranks=[...], k=60)
# Rank 0 gets weight/(60+0) = weight/60
# Rank 10 gets weight/(60+10) = weight/70 (still significant)

# Large k - more uniform
rrf = Rrf(ranks=[...], k=200)
# Rank 0 gets weight/(200+0) = weight/200
# Rank 10 gets weight/(200+10) = weight/210 (almost equal importance)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Small k - top results heavily weighted
const rrf1 = Rrf({ ranks: [...], k: 10 });
// Rank 0 gets weight/(10+0) = weight/10
// Rank 10 gets weight/(10+10) = weight/20 (half as important)

// Default k - balanced
const rrf2 = Rrf({ ranks: [...], k: 60 });
// Rank 0 gets weight/(60+0) = weight/60
// Rank 10 gets weight/(60+10) = weight/70 (still significant)

// Large k - more uniform
const rrf3 = Rrf({ ranks: [...], k: 200 });
// Rank 0 gets weight/(200+0) = weight/200
// Rank 10 gets weight/(200+10) = weight/210 (almost equal importance)
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Common Use Case: Dense + Sparse

The most common RRF use case is combining dense semantic embeddings with sparse keyword embeddings.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn, Rrf

# Dense semantic embeddings
dense_rank = Knn(
    query="machine learning research",  # Text query for dense embeddings
    key="#embedding",          # Default embedding field
    return_rank=True,
    limit=200                  # Consider top 200 candidates
)

# Sparse keyword embeddings
sparse_rank = Knn(
    query="machine learning research",  # Text query for sparse embeddings
    key="sparse_embedding",    # Metadata field for sparse vectors
    return_rank=True,
    limit=200
)

# Combine with RRF
hybrid_rank = Rrf(
    ranks=[dense_rank, sparse_rank],
    weights=[0.7, 0.3],       # 70% semantic, 30% keyword
    k=60
)

# Use in search
search = (Search()
    .where(K("status") == "published")  # Optional filtering
    .rank(hybrid_rank)
    .limit(20)
    .select(K.DOCUMENT, K.SCORE, "title")
)

results = collection.search(search)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, Rrf } from 'chromadb';

// Dense semantic embeddings
const denseRank = Knn({
  query: "machine learning research",  // Text query for dense embeddings
  key: "#embedding",         // Default embedding field
  returnRank: true,
  limit: 200                 // Consider top 200 candidates
});

// Sparse keyword embeddings
const sparseRank = Knn({
  query: "machine learning research",  // Text query for sparse embeddings
  key: "sparse_embedding",   // Metadata field for sparse vectors
  returnRank: true,
  limit: 200
});

// Combine with RRF
const hybridRank = Rrf({
  ranks: [denseRank, sparseRank],
  weights: [0.7, 0.3],       // 70% semantic, 30% keyword
  k: 60
});

// Use in search
const search = new Search()
  .where(K("status").eq("published"))  // Optional filtering
  .rank(hybridRank)
  .limit(20)
  .select(K.DOCUMENT, K.SCORE, "title");

const results = await collection.search(search);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Edge Cases and Important Behavior

### Component Ranking Behavior
Each Knn component in RRF operates on the documents that pass the filter. The number of results from each component is the minimum of its `limit` parameter and the number of filtered documents. RRF handles varying result counts gracefully - documents from any ranking are scored.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Each Knn operates on filtered documents
# Results per Knn = min(limit, number of documents passing filter)
rrf = Rrf([
    Knn(query="quantum computing", return_rank=True, limit=100),
    Knn(query="quantum computing", key="sparse_embedding", return_rank=True, limit=100)
])
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Each Knn operates on filtered documents
// Results per Knn = min(limit, number of documents passing filter)
const rrf = Rrf({
  ranks: [
    Knn({ query: "quantum computing", returnRank: true, limit: 100 }),
    Knn({ query: "quantum computing", key: "sparse_embedding", returnRank: true, limit: 100 })
  ]
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Minimum Requirements
- At least one ranking expression is required
- All rankings must have `return_rank=True`
- Weights (if provided) must match the number of rankings

### Document Selection with RRF
Documents must appear in at least one component ranking to be scored. To include documents that don't appear in a specific Knn's results, set the `default` parameter on that Knn:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Without default: only documents in BOTH rankings are scored
rrf = Rrf([
    Knn(query="deep learning", return_rank=True, limit=100),
    Knn(query="deep learning", key="sparse_embedding", return_rank=True, limit=100)
])

# With default: documents in EITHER ranking can be scored
rrf = Rrf([
    Knn(query="deep learning", return_rank=True, limit=100, default=1000),
    Knn(query="deep learning", key="sparse_embedding", return_rank=True, limit=100, default=1000)
])
# Documents missing from one ranking get default rank of 1000
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Without default: only documents in BOTH rankings are scored
const rrf1 = Rrf({
  ranks: [
    Knn({ query: "deep learning", returnRank: true, limit: 100 }),
    Knn({ query: "deep learning", key: "sparse_embedding", returnRank: true, limit: 100 })
  ]
});

// With default: documents in EITHER ranking can be scored
const rrf2 = Rrf({
  ranks: [
    Knn({ query: "deep learning", returnRank: true, limit: 100, default: 1000 }),
    Knn({ query: "deep learning", key: "sparse_embedding", returnRank: true, limit: 100, default: 1000 })
  ]
});
// Documents missing from one ranking get default rank of 1000
```
{% /Tab %}

{% /TabbedCodeBlock %}

### RRF as a Convenience Wrapper
`Rrf` is a convenience class that constructs the underlying ranking expression. You can manually build the same expression if needed:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Using Rrf wrapper (recommended)
rrf = Rrf(
    ranks=[rank1, rank2],
    weights=[0.7, 0.3],
    k=60
)

# Manual construction (equivalent)
# RRF formula: -sum(weight_i / (k + rank_i))
manual_rrf = -0.7 / (60 + rank1) - 0.3 / (60 + rank2)

# Both produce the same ranking expression
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Using Rrf wrapper (recommended)
const rrf = Rrf({
  ranks: [rank1, rank2],
  weights: [0.7, 0.3],
  k: 60
});

// Manual construction (equivalent)
// RRF formula: -sum(weight_i / (k + rank_i))
const manualRrf = Val(-0.7).divide(Val(60).add(rank1))
  .subtract(Val(0.3).divide(Val(60).add(rank2)));

// Both produce the same ranking expression
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Complete Example

Here's a practical example showing RRF with filtering and result processing:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn, Rrf

# Create RRF ranking with text query
hybrid_rank = Rrf(
    ranks=[
        Knn(query="machine learning applications", return_rank=True, limit=300),
        Knn(query="machine learning applications", key="sparse_embedding", return_rank=True, limit=300)
    ],
    weights=[2.0, 1.0],  # Dense 2x more important
    k=60
)

# Build complete search
search = (Search()
    .where(
        (K("language") == "en") &
        (K("year") >= 2020)
    )
    .rank(hybrid_rank)
    .limit(10)
    .select(K.DOCUMENT, K.SCORE, "title", "year")
)

# Execute and process results
results = collection.search(search)
rows = results.rows()[0]  # Get first (and only) search results

for i, row in enumerate(rows, 1):
    print(f"{i}. {row['metadata']['title']} ({row['metadata']['year']})")
    print(f"   RRF Score: {row['score']:.4f}")
    print(f"   Preview: {row['document'][:100]}...")
    print()
```

{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, Rrf } from 'chromadb';

// Create RRF ranking with text query
const hybridRank = Rrf({
  ranks: [
    Knn({ query: "machine learning applications", returnRank: true, limit: 300 }),
    Knn({ query: "machine learning applications", key: "sparse_embedding", returnRank: true, limit: 300 })
  ],
  weights: [2.0, 1.0],  // Dense 2x more important
  k: 60
});

// Build complete search
const search = new Search()
  .where(
    K("language").eq("en")
      .and(K("year").gte(2020))
  )
  .rank(hybridRank)
  .limit(10)
  .select(K.DOCUMENT, K.SCORE, "title", "year");

// Execute and process results
const results = await collection.search(search);
const rows = results.rows()[0];  // Get first (and only) search results

for (const [i, row] of rows.entries()) {
  console.log(`${i+1}. ${row.metadata?.title} (${row.metadata?.year})`);
  console.log(`   RRF Score: ${row.score?.toFixed(4)}`);
  console.log(`   Preview: ${row.document?.substring(0, 100)}...`);
  console.log();
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

Example output:
```
1. Introduction to Neural Networks (2023)
   RRF Score: -0.0428
   Preview: Neural networks are computational models inspired by biological neural networks...

2. Deep Learning Fundamentals (2022)
   RRF Score: -0.0385
   Preview: This comprehensive guide covers the fundamental concepts of deep learning...
```

## Tips and Best Practices

- **Always use `return_rank=True`** for all Knn expressions in RRF
- **Set appropriate limits** on component Knn expressions (usually 100-500)
- **Consider the k parameter** - default of 60 works well for most cases
- **Test different weights** - start with equal weights, then tune based on results
- **Use `default` values in Knn** if you want documents from partial matches

## Next Steps

- Learn about [batch operations](./batch-operations) for running multiple RRF searches
- See [practical examples](./examples) of hybrid search in production
- Explore [ranking expressions](./ranking) for arithmetic combinations instead of RRF
