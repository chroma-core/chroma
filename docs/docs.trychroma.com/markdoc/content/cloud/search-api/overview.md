---
id: overview
name: Overview
---

# Search API Overview

The Search API is a powerful, flexible interface for hybrid search operations in Chroma Cloud, combining vector similarity search with metadata filtering and custom ranking expressions.

{% Note type="info" %}
The Search API is currently in beta and available exclusively for Chroma Cloud users.
{% /Note %}

## Quick Start

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Simple vector search with metadata filtering
result = collection.search(
    Search()
    .where(K("category") == "science")
    .rank(Knn(query=[0.1, 0.2, 0.3]))
    .limit(10)
    .select(K.DOCUMENT, K.SCORE)
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Key Features

[Content to be added]

## Why Use the Search API?

[Content to be added]

## What's Next?

- Learn about [Search Basics](./search-basics)
- Explore [Filtering with Where](./filtering)
- Understand [Ranking and Scoring](./ranking)