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

## What is the Search API?

[TODO: Explain the Search API philosophy - declarative, composable, type-safe]
[TODO: Add architecture overview - how it fits into Chroma's architecture]

## Key Benefits

[TODO: Create comparison with legacy methods]
[TODO: Performance improvements]
[TODO: Better type safety and IDE support]
[TODO: More flexible and powerful expressions]

## Feature Comparison

[TODO: Add table comparing Search API vs Legacy API]
| Feature | Legacy API | Search API |
|---------|-----------|------------|
| Vector Search | ✅ query() | ✅ Knn() |
| Metadata Filtering | ✅ where | ✅ Enhanced Where |
| ... | ... | ... |

## Availability

[TODO: Add availability matrix]
| Environment | Status | Notes |
|------------|--------|-------|
| Chroma Cloud | ✅ Beta | Full support |
| Local Chroma | 🚧 Coming Soon | Planned for v0.x |
| Client-Server | 🚧 Coming Soon | Planned for v0.x |

## Required Setup

[TODO: Add complete setup instructions]
```python
# Required imports
from chromadb import Search, K, Knn, Rrf
from chromadb.execution.expression.operator import Val, Limit, Select
```

## Complete Quick Start Example

[TODO: Add complete example with actual output]
```python
# Full example with output
result = collection.search(...)
# Output:
# SearchResult(
#   ids=[["id1", "id2", ...]],
#   documents=[["doc1", "doc2", ...]],
#   scores=[[0.1, 0.2, ...]]
# )
```

## Performance and Scalability

[TODO: Add performance benchmarks]
[TODO: Scalability notes - how many vectors, QPS, latency]
[TODO: Resource usage guidelines]

## Beta Disclaimer

[TODO: Add beta limitations]
[TODO: How to provide feedback]
[TODO: Link to GitHub issues or Discord]

## What's Next?

- **[Search Basics](./search-basics)** - Learn how to construct searches
- **[Filtering with Where](./filtering)** - Master metadata filtering
- **[Ranking and Scoring](./ranking)** - Understand ranking expressions
- **[Hybrid Search](./hybrid-search)** - Combine multiple strategies
- **[Examples](./examples)** - See real-world patterns