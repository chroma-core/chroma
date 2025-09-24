---
id: ranking
name: Ranking and Scoring
---

# Ranking and Scoring

Learn how to use ranking expressions to score and order your search results.

## KNN (K-Nearest Neighbors)

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Knn

# Basic KNN search
Knn(query=[0.1, 0.2, 0.3])

# KNN with custom parameters
Knn(
    query=[0.1, 0.2, 0.3],
    key="#embedding",      # Field to search
    limit=128,            # Max results to consider
    return_rank=False     # Return distance vs rank
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Knn Class Complete Reference

[TODO: Complete parameter documentation]
```python
Knn(
    query,           # Vector: List[float] | SparseVector | np.ndarray
    key="#embedding", # Field to search against
    limit=128,       # Max neighbors to consider
    default=None,    # Default score for non-matches
    return_rank=False # Return rank position vs distance
)
```

### Dense Vectors

[TODO: Examples with different formats]
```python
# Python list
Knn(query=[0.1, 0.2, 0.3])

# NumPy array
import numpy as np
Knn(query=np.array([0.1, 0.2, 0.3]))

# Normalized vectors
from chromadb.api.types import normalize_embeddings
Knn(query=normalize_embeddings([[0.1, 0.2, 0.3]])[0])
```

### Sparse Vectors

[TODO: Sparse vector format and examples]
```python
sparse_vector = {
    "indices": [1, 5, 10, 50],
    "values": [0.5, 0.3, 0.8, 0.2]
}
Knn(query=sparse_vector, key="sparse_embedding")
```

### Custom Embedding Fields

[TODO: Searching non-default fields]
```python
# Search alternate embeddings
Knn(query=vector, key="title_embedding")
Knn(query=vector, key="summary_embedding")
```

## Arithmetic Operations

[TODO: All operators with precedence]
```python
# Addition
rank1 + rank2
Knn(query=v1) + Knn(query=v2)
Knn(query=v1) + 0.5  # Add constant

# Subtraction
rank1 - rank2
Knn(query=v1) - 0.1

# Multiplication
rank1 * rank2
Knn(query=v1) * 0.8  # Weight scaling

# Division
rank1 / rank2
Knn(query=v1) / 10.0  # Normalization

# Negation
-rank  # Equivalent to -1 * rank
```

[TODO: Operator precedence table]
[TODO: Parentheses for grouping]

## Mathematical Functions

[TODO: Each function with use cases]
```python
# Exponential - emphasize differences
rank.exp()
Knn(query=v).exp()

# Logarithm - compress range
rank.log()
(Knn(query=v) + 1).log()  # Add 1 to avoid log(0)

# Absolute value
abs(rank)
abs(Knn(query=v1) - Knn(query=v2))

# Maximum - upper bound
rank.max(1.0)  # Cap at 1.0
Knn(query=v).max(0.0)  # No negative scores

# Minimum - lower bound
rank.min(0.0)  # Floor at 0
Knn(query=v).min(1.0).max(0.0)  # Clamp to [0, 1]
```

## Val (Constant Values)

[TODO: When and why to use Val]
```python
# Creating constants
Val(0.5)
Val(1.0)

# Use in expressions
Knn(query=v) * 0.7 + Val(0.3)  # Val automatically created for 0.3

# Explicit Val for clarity
baseline_score = Val(0.5)
final_score = Knn(query=v) * 0.5 + baseline_score * 0.5
```

## Combining Multiple Knn Expressions

[TODO: Strategies for combination]
```python
# Linear combination (weighted average)
dense_score = Knn(query=dense_vec)
sparse_score = Knn(query=sparse_vec, key="sparse")
combined = dense_score * 0.7 + sparse_score * 0.3

# Product (AND-like behavior)
combined = dense_score * sparse_score

# Maximum (OR-like behavior)
combined = dense_score.max(sparse_score)

# Complex formulas
text_score = Knn(query=text_vec)
image_score = Knn(query=image_vec, key="image_emb")
metadata_boost = Val(0.1)
final = (text_score * 0.5 + image_score * 0.3) * (1 + metadata_boost)
```

## Score Interpretation

[TODO: Understanding score values]
- Lower scores = better matches (distance-based)
- Score range depends on distance metric
- Normalized vs unnormalized scores
- Converting to similarity (1 - distance)

## Performance Implications

[TODO: Performance of different strategies]
- Single Knn vs multiple Knn operations
- Limit parameter impact
- Complex expressions overhead
- Optimization tips

## Default Ranking Behavior

[TODO: What happens when rank=None]
- Natural order (insertion order)
- When to use no ranking
- Performance benefits

## Common Ranking Patterns

[TODO: Real-world examples]
```python
# Semantic search with boost
Knn(query=query_vec) * (1 + K("boost_factor"))

# Multi-modal search
text_rank = Knn(query=text_emb) * 0.6
image_rank = Knn(query=img_emb, key="image") * 0.4

# Re-ranking pattern
initial_rank = Knn(query=query_vec, limit=1000)
rerank = Knn(query=refined_vec, key="refined") 
final = initial_rank * 0.3 + rerank * 0.7
```