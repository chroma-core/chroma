---
id: batch-operations
name: Batch Operations
---

# Batch Operations

Execute multiple searches in a single API call for better performance and easier comparison of results.

## Running Multiple Searches

Pass a list of Search objects to execute them in a single request. Each search operates independently and returns its own results.

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Execute multiple searches in one call
searches = [
    # Search 1: Recent articles
    (Search()
        .where((K("type") == "article") & (K("year") >= 2024))
        .rank(Knn(query=query_vector_1))
        .limit(5)
        .select(K.DOCUMENT, K.SCORE, "title")),
    
    # Search 2: Papers by specific authors
    (Search()
        .where(K("author").is_in(["Smith", "Jones"]))
        .rank(Knn(query=query_vector_2))
        .limit(10)
        .select(K.DOCUMENT, K.SCORE, "title", "author")),
    
    # Search 3: Featured content (no ranking)
    Search()
        .where(K("status") == "featured")
        .limit(20)
        .select("title", "date")
]

# Execute all searches in one request
results = collection.search(searches)

# Access results by index
first_search_ids = results.ids[0]     # IDs from Search 1
second_search_ids = results.ids[1]    # IDs from Search 2
third_search_ids = results.ids[2]     # IDs from Search 3
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Why Use Batch Operations

- **Single round trip** - All searches execute in one API call
- **Easy comparison** - Compare results from different queries or strategies
- **Parallel execution** - Server processes searches simultaneously

## Understanding Batch Results

Results from batch operations maintain the same order as your searches. Each search's results are accessed by its index.

{% Tabs %}

{% Tab label="python" %}
```python
# Single search returns single result set
result = collection.search(single_search)
ids = result.ids[0]  # Single list of IDs

# Batch search returns multiple result sets
results = collection.search([search1, search2, search3])
ids_1 = results.ids[0]    # IDs from search1
ids_2 = results.ids[1]    # IDs from search2
ids_3 = results.ids[2]    # IDs from search3

# Using rows() for easier processing
all_rows = results.rows()  # Returns list of lists
rows_1 = all_rows[0]      # Rows from search1
rows_2 = all_rows[1]      # Rows from search2
rows_3 = all_rows[2]      # Rows from search3

# Process each search's results
for search_index, rows in enumerate(all_rows):
    print(f"Results from search {search_index + 1}:")
    for row in rows:
        print(f"  - {row['id']}: {row.get('metadata', {}).get('title', 'N/A')}")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Result Structure

Each field in the SearchResult maintains a list where each index corresponds to a search:

- `results.ids[i]` - IDs from search at index i
- `results.documents[i]` - Documents from search at index i (if selected)
- `results.embeddings[i]` - Embeddings from search at index i (if selected)
- `results.metadatas[i]` - Metadata from search at index i (if selected)
- `results.scores[i]` - Scores from search at index i (if ranking was used)



## Common Use Cases

### Comparing Different Queries
Test multiple query variations to find the most relevant results.

{% Tabs %}

{% Tab label="python" %}
```python
# Compare different query embeddings
query_variations = [original_query, expanded_query, refined_query]

searches = [
    Search()
        .rank(Knn(query=q))
        .limit(10)
        .select(K.DOCUMENT, K.SCORE, "title")
    for q in query_variations
]

results = collection.search(searches)

# Compare top results from each variation
for i, query_name in enumerate(["Original", "Expanded", "Refined"]):
    print(f"{query_name} Query Top Result:")
    if results.scores[i]:
        print(f"  Score: {results.scores[i][0]:.3f}")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### A/B Testing Ranking Strategies
Compare different ranking approaches on the same query.

{% Tabs %}

{% Tab label="python" %}
```python
# Test different ranking strategies
searches = [
    # Strategy A: Pure KNN
    Search()
        .rank(Knn(query=query_vector))
        .limit(10)
        .select(K.SCORE, "title"),
    
    # Strategy B: Weighted KNN
    Search()
        .rank(Knn(query=query_vector) * 0.8 + 0.2)
        .limit(10)
        .select(K.SCORE, "title"),
    
    # Strategy C: Hybrid with RRF
    Search()
        .rank(Rrf([
            Knn(query=query_vector, return_rank=True),
            Knn(query=sparse_vector, key="sparse_embedding", return_rank=True)
        ]))
        .limit(10)
        .select(K.SCORE, "title")
]

results = collection.search(searches)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### Multiple Filters on Same Data
Apply different filters to explore different subsets of your data.

{% Tabs %}

{% Tab label="python" %}
```python
# Different category filters
categories = ["technology", "science", "business"]

searches = [
    Search()
        .where(K("category") == category)
        .rank(Knn(query=query_vector))
        .limit(5)
        .select("title", "category", K.SCORE)
    for category in categories
]

results = collection.search(searches)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Performance Benefits

Batch operations are significantly faster than running searches sequentially:

{% Tabs %}

{% Tab label="python" %}
```python
# ❌ Sequential execution (slow)
results = []
for search in searches:
    result = collection.search(search)  # Separate API call each time
    results.append(result)

# ✅ Batch execution (fast)
results = collection.search(searches)  # Single API call for all
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

Batch operations reduce network overhead and enable server-side parallelization, often providing 3-10x speedup depending on the number and complexity of searches.

## Edge Cases

### Empty Searches Array
Passing an empty list returns an empty result.

### Batch Size Limits
For Chroma Cloud users, batch operations may be subject to quota limits on the total number of searches per request.

### Mixed Field Selection
Different searches can select different fields - each search's results will contain only its requested fields.

{% Tabs %}

{% Tab label="python" %}
```python
searches = [
    Search().limit(5).select(K.DOCUMENT),       # Only documents
    Search().limit(5).select(K.SCORE, "title"), # Scores and title
    Search().limit(5).select_all()              # Everything
]

results = collection.search(searches)
# results.documents[0] will have values
# results.documents[1] will be None (not selected)
# results.documents[2] will have values
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Complete Example

Here's a practical example using batch operations to find and compare relevant documents across different categories:

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

def compare_category_relevance(collection, query_vector, categories):
    """Find top results in each category for the same query"""
    
    # Build searches for each category
    searches = [
        Search()
            .where(K("category") == cat)
            .rank(Knn(query=query_vector))
            .limit(3)
            .select(K.DOCUMENT, K.SCORE, "title", "category")
        for cat in categories
    ]
    
    # Execute batch search
    results = collection.search(searches)
    all_rows = results.rows()
    
    # Process and display results
    for cat_index, category in enumerate(categories):
        print(f"\nTop results in {category}:")
        rows = all_rows[cat_index]
        
        if not rows:
            print("  No results found")
            continue
            
        for i, row in enumerate(rows, 1):
            title = row.get('metadata', {}).get('title', 'Untitled')
            score = row.get('score', 0)
            preview = row.get('document', '')[:100]
            
            print(f"  {i}. {title}")
            print(f"     Score: {score:.3f}")
            print(f"     Preview: {preview}...")

# Usage
categories = ["technology", "science", "business", "health"]
query_vector = embedding_model.encode("artificial intelligence applications")

compare_category_relevance(collection, query_vector, categories)
```

Example output:
```
Top results in technology:
  1. AI in Software Development
     Score: 0.234
     Preview: The integration of artificial intelligence in modern software development has revolutionized...
  2. Machine Learning Frameworks
     Score: 0.312
     Preview: Popular frameworks for building AI applications include TensorFlow, PyTorch, and...

Top results in science:
  1. Neural Networks Research
     Score: 0.289
     Preview: Recent advances in neural network architectures have enabled breakthrough applications...
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Tips and Best Practices

- **Keep batch sizes reasonable** - Very large batches may hit quota limits
- **Use consistent field selection** when possible for easier result processing
- **Index alignment** - Results maintain the same order as input searches
- **Consider memory usage** - Large batches with `select_all()` can consume significant memory
- **Use `rows()` method** for easier result processing in batch operations

## Next Steps

- See [practical examples](./examples) of batch operations in production
- Learn about [performance optimization](./search-basics) for complex queries
- Explore [migration guide](./migration) for transitioning from legacy methods