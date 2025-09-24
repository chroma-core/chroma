---
id: batch-operations
name: Batch Operations
---

# Batch Operations

Learn how to execute multiple searches efficiently in a single request.

## Running Multiple Searches

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Execute multiple searches in one call
searches = [
    # Search for recent articles
    (Search()
        .where((K("type") == "article") & (K("date") >= "2024-01-01"))
        .rank(Knn(query=query1))
        .limit(5)),
    
    # Search for papers by specific authors
    (Search()
        .where(K("author").is_in(["Smith", "Jones"]))
        .rank(Knn(query=query2))
        .limit(10)),
    
    # Pure filtering without ranking
    Search().where(K("status") == "featured").limit(20)
]

results = collection.search(searches)
# results.ids[0] contains IDs from first search
# results.ids[1] contains IDs from second search, etc.
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Understanding Batch Results

[Content to be added]

## Use Cases

[Content to be added]

## Performance Benefits

[Content to be added]

## Best Practices

[Content to be added]