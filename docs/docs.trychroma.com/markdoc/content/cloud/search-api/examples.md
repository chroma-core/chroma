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

## Simple Examples

### Pure Vector Search
[TODO: Complete example with output]
```python
from chromadb import Search, Knn

# Simple similarity search
search = Search().rank(Knn(query=embedding_vector)).limit(5)
results = collection.search(search)

# Access results
for i, doc_id in enumerate(results.ids[0]):
    print(f"{i+1}. {doc_id}: {results.scores[0][i]}")
```

### Metadata Filtering Only
[TODO: Filtering without ranking]
```python
# Get all published articles
search = Search().where(K("status") == "published").limit(100)

# Complex filter without ranking
search = Search().where(
    (K("category") == "tech") & 
    (K("year") >= 2020) & 
    (K("score") > 0.7)
)
```

### Combined Filtering and Ranking
[TODO: Most common pattern]
```python
search = (Search()
    .where(K("language") == "en")
    .rank(Knn(query=query_embedding))
    .limit(20)
    .select(K.DOCUMENT, K.SCORE, "title"))
```

## Real-World Scenarios

### Semantic Search with Date Filtering
[TODO: Time-aware search]
```python
from datetime import datetime, timedelta

# Search recent documents only
recent_date = (datetime.now() - timedelta(days=30)).isoformat()

search = (Search()
    .where(K("published_date") >= recent_date)
    .rank(Knn(query=query_embedding))
    .limit(10)
    .select(K.DOCUMENT, "published_date", "title"))
```

### Product Search with Price Ranges
[TODO: E-commerce example]
```python
# Product search with filters
search = (Search()
    .where(
        (K("category") == "electronics") &
        (K("price") >= 100) &
        (K("price") <= 500) &
        (K("in_stock") == True)
    )
    .rank(Knn(query=product_query_embedding))
    .limit(20)
    .select("name", "price", "description", K.SCORE))
```

### Document Search with Author and Topic
[TODO: Academic search example]
```python
# Academic paper search
search = (Search()
    .where(
        K("author").contains("Smith") &
        K("topics").is_in(["machine learning", "AI", "deep learning"])
    )
    .rank(Knn(query=research_query_embedding))
    .limit(50)
    .select("title", "abstract", "authors", "year", K.SCORE))
```

### Multi-Language Search
[TODO: Cross-lingual search]
```python
# Search across multiple languages
searches = [
    Search()
        .where(K("language") == lang)
        .rank(Knn(query=multilingual_embedding))
        .limit(10)
    for lang in ["en", "es", "fr", "de"]
]

results = collection.search(searches)
# Merge results from different languages
```

### Recommendation System Pattern
[TODO: Recommendations example]
```python
# Find similar items excluding already seen
seen_ids = ["id1", "id2", "id3"]

search = (Search()
    .where(
        K.ID.not_in(seen_ids) &
        (K("category") == user_preference_category)
    )
    .rank(Knn(query=user_embedding))
    .limit(10)
    .select("title", "description", "thumbnail_url"))
```

## Advanced Patterns

### Re-ranking Pattern
[TODO: Two-stage retrieval]
```python
# Stage 1: Broad retrieval
initial_search = (Search()
    .rank(Knn(query=initial_query, limit=1000))
    .limit(100))

initial_results = collection.search(initial_search)

# Stage 2: Re-rank with different query
rerank_search = (Search()
    .where(K.ID.is_in(initial_results.ids[0]))
    .rank(Knn(query=refined_query))
    .limit(10))
```

### Fallback Search Strategy
[TODO: Progressive relaxation]
```python
def search_with_fallback(query_embedding, filters):
    # Try strict search first
    strict_search = (Search()
        .where(filters)
        .rank(Knn(query=query_embedding))
        .limit(10))
    
    results = collection.search(strict_search)
    
    # If not enough results, relax filters
    if len(results.ids[0]) < 5:
        relaxed_search = (Search()
            .rank(Knn(query=query_embedding))
            .limit(10))
        results = collection.search(relaxed_search)
    
    return results
```

### Progressive Search Refinement
[TODO: Drill-down search]
```python
# Start broad, narrow down based on user interaction
class SearchSession:
    def __init__(self, base_query):
        self.base_query = base_query
        self.filters = []
    
    def refine(self, new_filter):
        self.filters.append(new_filter)
        
        # Combine all filters
        combined_filter = self.filters[0]
        for f in self.filters[1:]:
            combined_filter = combined_filter & f
        
        return (Search()
            .where(combined_filter)
            .rank(Knn(query=self.base_query))
            .limit(20))
```

### Faceted Search Implementation
[TODO: Facets for filtering]
```python
def get_facets_and_results(query_embedding, base_filter=None):
    # Main search
    main_search = Search()
    if base_filter:
        main_search = main_search.where(base_filter)
    main_search = main_search.rank(Knn(query=query_embedding)).limit(20)
    
    # Facet searches (counts for each category)
    facet_searches = []
    for facet_value in ["tech", "science", "business"]:
        facet_filter = K("category") == facet_value
        if base_filter:
            facet_filter = base_filter & facet_filter
        facet_search = Search().where(facet_filter).limit(0)  # Just count
        facet_searches.append(facet_search)
    
    # Execute all in one batch
    all_searches = [main_search] + facet_searches
    results = collection.search(all_searches)
    
    return {
        "results": results.ids[0],
        "facets": {
            "tech": len(results.ids[1]),
            "science": len(results.ids[2]),
            "business": len(results.ids[3]),
        }
    }
```

## Integration Examples

### With LangChain
[TODO: LangChain integration]
```python
from langchain.vectorstores import Chroma
from chromadb import Search, K, Knn

# Custom search with LangChain
def langchain_custom_search(query_text, metadata_filter):
    embedding = embedding_function(query_text)
    
    search = (Search()
        .where(metadata_filter)
        .rank(Knn(query=embedding))
        .limit(5)
        .select(K.DOCUMENT, K.METADATA))
    
    return collection.search(search)
```

### With LlamaIndex
[TODO: LlamaIndex integration]

### With FastAPI
[TODO: REST API example]
```python
from fastapi import FastAPI
from chromadb import Search, K, Knn

app = FastAPI()

@app.post("/search")
async def search_endpoint(
    query: str,
    filters: dict = None,
    limit: int = 10
):
    embedding = await get_embedding(query)
    
    search = Search().rank(Knn(query=embedding)).limit(limit)
    if filters:
        search = search.where(Where.from_dict(filters))
    
    results = collection.search(search)
    return {"results": results.rows()[0]}
```

### Async Patterns
[TODO: Async search examples]
```python
import asyncio
from chromadb import AsyncClient

async def parallel_searches(queries):
    async with AsyncClient() as client:
        collection = await client.get_collection("my_collection")
        
        searches = [
            Search().rank(Knn(query=embed(q))).limit(10)
            for q in queries
        ]
        
        results = await collection.search(searches)
        return results
```

## Common Patterns Summary

[TODO: Pattern reference table]
| Pattern | Use Case | Key Features |
|---------|----------|--------------|
| Filter + Rank | Most searches | Combine metadata and vector |
| Re-ranking | Quality improvement | Two-stage retrieval |
| Fallback | Guaranteed results | Progressive relaxation |
| Faceted | E-commerce, catalogs | Counts and filters |
| Batch | Multiple queries | Performance optimization |

## Anti-Patterns to Avoid

[TODO: Common mistakes]

### 1. Over-filtering
```python
# BAD: Too restrictive, might return nothing
search = Search().where(
    (K("field1") == "exact") &
    (K("field2") == "exact") &
    (K("field3") == "exact") &
    # ... many more conditions
)

# GOOD: Start broad, refine based on results
```

### 2. Inefficient Pagination
```python
# BAD: Large offset
Search().limit(10, offset=10000)  # Inefficient

# GOOD: Use filtering to reduce dataset first
```

### 3. Not Using Batch Operations
```python
# BAD: Sequential searches
for query in queries:
    collection.search(Search().rank(Knn(query=query)))

# GOOD: Batch execution
searches = [Search().rank(Knn(query=q)) for q in queries]
collection.search(searches)
```

### 4. Selecting Unnecessary Fields
```python
# BAD: Getting everything when you only need IDs
Search().select_all()  # Transfers lots of data

# GOOD: Select only what you need
Search().select(K.ID, K.SCORE)
```