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

## RRF Parameters

[Content to be added]

## Combining Dense and Sparse Embeddings

[Content to be added]

## Multi-Modal Search

[Content to be added]

## Weight Configuration

[Content to be added]

## Performance Considerations

[Content to be added]