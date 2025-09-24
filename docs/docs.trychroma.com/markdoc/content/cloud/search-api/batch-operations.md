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

## Batch Search Motivation

[TODO: Why use batch searches]
- Single round trip for multiple queries
- Better resource utilization
- Parallel execution on server
- Reduced latency for multiple searches
- Atomic operation for consistency

## Maximum Batch Size

[TODO: Document limits]
```python
# Maximum number of searches per batch
MAX_BATCH_SIZE = 100  # Example limit

# Handling large batches
def batch_search_chunked(searches, chunk_size=50):
    results = []
    for i in range(0, len(searches), chunk_size):
        chunk = searches[i:i + chunk_size]
        chunk_results = collection.search(chunk)
        results.append(chunk_results)
    return merge_results(results)
```

## Understanding Batch Results

[TODO: SearchResult structure for batches]
```python
# Single search result structure
result = collection.search(single_search)
result.ids[0]       # IDs from the single search
result.documents[0] # Documents from the single search

# Batch search result structure  
results = collection.search([search1, search2, search3])
results.ids[0]       # IDs from search1
results.ids[1]       # IDs from search2
results.ids[2]       # IDs from search3

# Accessing by search index
for i, search in enumerate(searches):
    search_ids = results.ids[i]
    search_docs = results.documents[i] if results.documents else None
    search_scores = results.scores[i] if results.scores else None
```

## SearchResult Column-Major Format

[TODO: Explain column-major format]
```python
SearchResult = {
    "ids": [
        ["id1", "id2", ...],  # Results from search 1
        ["id3", "id4", ...],  # Results from search 2
    ],
    "documents": [
        ["doc1", "doc2", ...],  # Documents from search 1
        ["doc3", "doc4", ...],  # Documents from search 2
    ],
    "scores": [
        [0.1, 0.2, ...],  # Scores from search 1
        [0.15, 0.25, ...],  # Scores from search 2
    ],
    # ... other fields
}
```

## Mixed Search Types in Batch

[TODO: Different search configurations]
```python
searches = [
    # Vector search with filtering
    Search()
        .where(K("type") == "article")
        .rank(Knn(query=vector1))
        .limit(10)
        .select(K.DOCUMENT, K.SCORE),
    
    # Pure metadata filtering
    Search()
        .where(K("status") == "featured")
        .limit(5)
        .select(K.DOCUMENT, "title"),
    
    # Hybrid search with RRF
    Search()
        .rank(Rrf([
            Knn(query=dense, return_rank=True),
            Knn(query=sparse, key="sparse", return_rank=True)
        ]))
        .limit(20)
        .select_all(),
    
    # Different limit and offset
    Search()
        .where(K("category") == "tech")
        .rank(Knn(query=vector2))
        .limit(15, offset=30)
]

results = collection.search(searches)
```

## Use Cases

[TODO: Real-world use cases]

### A/B Testing
```python
# Test different ranking strategies
searches = [
    Search().rank(Knn(query=q)),  # Control
    Search().rank(Knn(query=q) * 0.7 + Val(0.3)),  # Variant A
    Search().rank(Rrf([...])),  # Variant B
]
```

### Multi-Query Retrieval
```python
# Multiple queries for same document set
queries = [query1, query2, query3]
searches = [
    Search()
        .where(base_filter)
        .rank(Knn(query=q))
        .limit(10)
    for q in queries
]
```

### Faceted Search
```python
# Different facets in parallel
facets = ["category", "author", "year"]
searches = [
    Search()
        .where(K(facet) == value)
        .limit(5)
    for facet, value in facet_values
]
```

### Progressive Refinement
```python
# Increasingly specific searches
searches = [
    Search().where(K("type") == "doc").limit(100),
    Search().where((K("type") == "doc") & (K("year") >= 2020)).limit(50),
    Search().where((K("type") == "doc") & (K("year") >= 2020) & (K("score") > 0.8)).limit(10),
]
```

## Performance Benefits

[TODO: Performance comparison]
```python
# Sequential execution (slow)
results = []
for search in searches:
    result = collection.search(search)
    results.append(result)
# Time: N * single_search_time

# Batch execution (fast)
results = collection.search(searches)
# Time: ~single_search_time (with parallelization)
```

[TODO: Add performance table]
| Searches | Sequential | Batch | Speedup |
|----------|------------|-------|---------|
| 5 | 50ms | 15ms | 3.3x |
| 10 | 100ms | 20ms | 5x |
| 50 | 500ms | 60ms | 8.3x |

## Error Handling in Batches

[TODO: How errors are handled]
```python
# If one search fails
try:
    results = collection.search(searches)
except SearchError as e:
    # Handle partial failures
    failed_index = e.failed_index
    successful_results = e.partial_results
```

## Result Alignment and Processing

[TODO: Processing batch results]
```python
def process_batch_results(searches, results):
    """Align searches with their results"""
    aligned = []
    
    for i, search in enumerate(searches):
        search_result = {
            "search": search,
            "ids": results.ids[i] if i < len(results.ids) else [],
            "documents": results.documents[i] if results.documents and i < len(results.documents) else None,
            "scores": results.scores[i] if results.scores and i < len(results.scores) else None,
        }
        aligned.append(search_result)
    
    return aligned
```

## Best Practices

[TODO: Batch operation best practices]

1. **Batch Size**: Keep under 100 searches per batch
2. **Homogeneous Selects**: Use same select fields for easier processing
3. **Error Handling**: Always handle partial failures
4. **Memory Management**: Be mindful of result size with large batches
5. **Timeout Handling**: Set appropriate timeouts for large batches

```python
# Good practice: Consistent field selection
searches = [
    Search().select(K.DOCUMENT, K.SCORE).limit(10)
    for query in queries
]

# Avoid: Inconsistent selections make processing harder
searches = [
    Search().select(K.DOCUMENT),
    Search().select(K.SCORE),
    Search().select_all(),
]
```