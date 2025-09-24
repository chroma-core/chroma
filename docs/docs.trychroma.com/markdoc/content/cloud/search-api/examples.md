---
id: examples
name: Examples & Patterns
---

# Examples & Patterns

Real-world examples and common patterns for using the Search API effectively.

## Simple Vector Search

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, Knn

# Find similar documents
search = Search().rank(Knn(query=embedding_vector)).limit(5)
results = collection.search(search)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Filtered Vector Search

[Content to be added]

## Hybrid Search Example

[Content to be added]

## Multi-Stage Ranking

[Content to be added]

## Complex Real-World Scenario

[Content to be added]

## Common Patterns

[Content to be added]

## Anti-Patterns to Avoid

[Content to be added]