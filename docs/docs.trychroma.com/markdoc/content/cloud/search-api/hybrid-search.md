---
id: hybrid-search
name: Hybrid Search with RRF
---

# Hybrid Search with RRF

Learn how to combine multiple ranking strategies using Reciprocal Rank Fusion (RRF).

## Understanding RRF

Reciprocal Rank Fusion is a technique for combining multiple ranking strategies effectively.

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Rrf, Knn

# Basic RRF with equal weights
rrf = Rrf([
    Knn(query=dense_vector, return_rank=True),
    Knn(query=sparse_vector, key="sparse_embedding", return_rank=True)
])

# Weighted RRF
rrf = Rrf(
    ranks=[
        Knn(query=dense_vector, return_rank=True),
        Knn(query=sparse_vector, key="sparse_embedding", return_rank=True)
    ],
    weights=[2.0, 1.0],  # First ranking 2x more important
    k=60                 # Smoothing parameter
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## RRF Algorithm Explained

[TODO: Detailed explanation with formula]
```
RRF Score = -Σ(weight_i / (k + rank_i))
```
- Negative because Chroma uses ascending order (lower = better)
- rank_i is the position (0, 1, 2, ...)
- k is smoothing parameter (default: 60)
- weight_i is the importance of each ranking

[TODO: Visual diagram of RRF]

## Why RRF vs Linear Combination?

[TODO: Comparison and benefits]
- Rank-based vs score-based fusion
- Handles different score scales
- More robust to outliers
- Better for heterogeneous sources

## Complete Rrf Parameters

[TODO: Full parameter reference]
```python
Rrf(
    ranks,           # List[Rank] - ranking expressions
    k=60,           # Smoothing constant (higher = less emphasis on top ranks)
    weights=None,   # Optional weights for each rank
    normalize=False # Whether to normalize weights to sum to 1
)
```

### The return_rank Requirement

[TODO: Explain why return_rank=True is needed]
```python
# CORRECT - using rank positions
Rrf([
    Knn(query=v1, return_rank=True),  # Returns 0, 1, 2, ...
    Knn(query=v2, return_rank=True)
])

# INCORRECT - using distances
Rrf([
    Knn(query=v1),  # Returns distances - won't work correctly!
    Knn(query=v2)
])
```

## Weight Configuration Strategies

[TODO: Different weighting approaches]
```python
# Equal weights (default)
Rrf([rank1, rank2, rank3])  # Each weight = 1.0

# Custom weights (relative importance)
Rrf(
    ranks=[semantic, keyword, title],
    weights=[3.0, 2.0, 1.0]  # Semantic 3x, keyword 2x, title 1x
)

# Normalized weights (sum to 1)
Rrf(
    ranks=[semantic, keyword],
    weights=[0.7, 0.3],  # Must sum to 1.0
    normalize=True  # Enforces sum = 1
)

# Auto-normalization
Rrf(
    ranks=[semantic, keyword],
    weights=[70, 30],  # Will be normalized to [0.7, 0.3]
    normalize=True
)
```

## K Parameter Tuning

[TODO: How to choose k value]
```python
# Small k (e.g., 10) - Heavy emphasis on top ranks
Rrf(ranks, k=10)

# Default k (60) - Balanced
Rrf(ranks, k=60)

# Large k (e.g., 100+) - More uniform weighting
Rrf(ranks, k=100)
```

[TODO: Impact visualization/table]

## Combining Dense and Sparse Embeddings

[TODO: Complete example]
```python
# Dense semantic embeddings
dense_rank = Knn(
    query=dense_vector,
    key="#embedding",
    return_rank=True
)

# Sparse keyword embeddings (e.g., BM25)
sparse_rank = Knn(
    query=sparse_vector,
    key="sparse_embedding",
    return_rank=True
)

# Combine with RRF
hybrid = Rrf(
    ranks=[dense_rank, sparse_rank],
    weights=[0.6, 0.4],  # 60% semantic, 40% keyword
    k=60
)

search = Search().rank(hybrid).limit(20)
```

## Multi-Modal Search Examples

[TODO: 2+ modalities]
```python
# Text + Image search
text_rank = Knn(query=text_emb, return_rank=True)
image_rank = Knn(query=image_emb, key="image_embedding", return_rank=True)

# Text + Image + Audio
text_rank = Knn(query=text_emb, return_rank=True)
image_rank = Knn(query=image_emb, key="image_emb", return_rank=True)
audio_rank = Knn(query=audio_emb, key="audio_emb", return_rank=True)

multi_modal = Rrf(
    ranks=[text_rank, image_rank, audio_rank],
    weights=[0.5, 0.3, 0.2],
    k=60
)
```

## Common Hybrid Search Architectures

[TODO: Real-world patterns]
1. **Semantic + Keyword**
   - Dense embeddings for concepts
   - Sparse for exact matches

2. **Multiple Embedding Models**
   - Different models for different aspects
   - Ensemble approach

3. **Cross-lingual Search**
   - Language-specific embeddings
   - Multilingual embeddings

4. **Domain-specific Combinations**
   - General + specialized embeddings
   - Coarse + fine rankings

## Performance Benchmarks

[TODO: Add performance comparisons]
| Method | Recall@10 | Latency | Use Case |
|--------|-----------|---------|----------|
| Dense only | 0.85 | 10ms | Semantic |
| Sparse only | 0.75 | 5ms | Keyword |
| RRF Hybrid | 0.92 | 15ms | Best overall |

## Performance Optimization

[TODO: Tips for performance]
- Limit values for component Knn
- Pre-computing embeddings
- Caching strategies
- Batch processing

## Debugging RRF Results

[TODO: How to debug and tune]
- Inspecting individual rank components
- Visualizing score distributions
- A/B testing strategies
- Metrics for evaluation