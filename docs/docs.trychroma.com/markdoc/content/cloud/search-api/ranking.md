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

## Arithmetic Operations

[Content to be added]

## Mathematical Functions

[Content to be added]

## Val (Constant Values)

[Content to be added]

## Combining Rank Expressions

[Content to be added]

## Score Normalization

[Content to be added]