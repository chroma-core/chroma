---
id: search-basics
name: Search Basics
---

# Search Basics

Learn how to construct and use the Search class for querying your Chroma collections.

## The Search Class

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search

# Create an empty search
search = Search()

# Direct construction with parameters
search = Search(
    where={"status": "active"},
    rank={"$knn": {"query": [0.1, 0.2]}},
    limit=10,
    select=["#document", "#score"]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Builder Pattern

[Content to be added]

## Direct Construction

[Content to be added]

## Dictionary Format

[Content to be added]

## Serialization

[Content to be added]