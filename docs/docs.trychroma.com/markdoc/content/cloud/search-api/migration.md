---
id: migration
name: Migration Guide
---

# Migration Guide

Learn how to migrate from the legacy `query()` and `get()` methods to the new Search API.

## Comparison Overview

| Legacy Method | Search API Equivalent |
|--------------|----------------------|
| `collection.query()` | `collection.search()` |
| `query_texts` | `Knn(query=embedding)` |
| `where` | `.where()` method |
| `n_results` | `.limit()` method |
| `include` | `.select()` method |

## Simple Query Migration

{% Tabs %}

{% Tab label="python" %}
```python
# Legacy approach
results = collection.query(
    query_embeddings=[embedding],
    where={"category": "science"},
    n_results=10
)

# New Search API
from chromadb import Search, K, Knn

results = collection.search(
    Search()
    .where(K("category") == "science")
    .rank(Knn(query=embedding))
    .limit(10)
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Get Method Migration

[Content to be added]

## Feature Parity

[Content to be added]

## Advanced Migrations

[Content to be added]

## Timeline and Deprecation

[Content to be added]

## Getting Help

[Content to be added]