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

## Complete Parameter Mapping

[TODO: Comprehensive mapping table]

### query() Method Mapping
| Legacy Parameter | Search API Equivalent | Notes |
|-----------------|----------------------|--------|
| `query_texts` | `Knn(query=embed(text))` | Requires embedding |
| `query_embeddings` | `Knn(query=embedding)` | Direct use |
| `n_results` | `.limit(n)` | Same behavior |
| `where` | `.where()` | Enhanced operators |
| `where_document` | `.where(K.DOCUMENT...)` | Unified filtering |
| `include` | `.select()` | More flexible |
| `ids` | `.where(K.ID.is_in())` | Filter by IDs |

### get() Method Mapping
| Legacy Parameter | Search API Equivalent | Notes |
|-----------------|----------------------|--------|
| `ids` | `.where(K.ID.is_in(ids))` | ID filtering |
| `where` | `.where()` | Same syntax |
| `where_document` | `.where(K.DOCUMENT...)` | Unified |
| `limit` | `.limit(limit)` | Same behavior |
| `offset` | `.limit(limit, offset=offset)` | Combined |
| `include` | `.select()` | More options |

### Include/Select Mapping
| Legacy Include | Search API Select | 
|---------------|------------------|
| `["embeddings"]` | `.select(K.EMBEDDING)` |
| `["documents"]` | `.select(K.DOCUMENT)` |
| `["metadatas"]` | `.select(K.METADATA)` |
| `["distances"]` | `.select(K.SCORE)` |
| `["embeddings", "documents"]` | `.select(K.EMBEDDING, K.DOCUMENT)` |

## Step-by-Step Migrations

### Simple Query Migration
[TODO: Basic query migration]
```python
# Legacy
results = collection.query(
    query_embeddings=[[0.1, 0.2, 0.3]],
    n_results=10
)

# Search API
from chromadb import Search, Knn

results = collection.search(
    Search()
    .rank(Knn(query=[0.1, 0.2, 0.3]))
    .limit(10)
)
```

### Query with All Parameters
[TODO: Complex query migration]
```python
# Legacy
results = collection.query(
    query_embeddings=[[0.1, 0.2, 0.3]],
    n_results=10,
    where={"category": "science"},
    where_document={"$contains": "quantum"},
    include=["documents", "metadatas", "distances"]
)

# Search API
results = collection.search(
    Search()
    .where(
        (K("category") == "science") & 
        (K.DOCUMENT.contains("quantum"))
    )
    .rank(Knn(query=[0.1, 0.2, 0.3]))
    .limit(10)
    .select(K.DOCUMENT, K.METADATA, K.SCORE)
)
```

### Get Method Migration
[TODO: Get migration examples]
```python
# Legacy - get by IDs
results = collection.get(
    ids=["id1", "id2", "id3"],
    include=["documents", "metadatas"]
)

# Search API
results = collection.search(
    Search()
    .where(K.ID.is_in(["id1", "id2", "id3"]))
    .select(K.DOCUMENT, K.METADATA)
)

# Legacy - get with filtering
results = collection.get(
    where={"status": "active"},
    limit=100,
    offset=50,
    include=["documents"]
)

# Search API
results = collection.search(
    Search()
    .where(K("status") == "active")
    .limit(100, offset=50)
    .select(K.DOCUMENT)
)
```

### Batch Query Migration
[TODO: Multiple queries migration]
```python
# Legacy - multiple separate calls
results1 = collection.query(query_embeddings=[emb1], n_results=10)
results2 = collection.query(query_embeddings=[emb2], n_results=10)
results3 = collection.query(query_embeddings=[emb3], n_results=10)

# Search API - single batch call
searches = [
    Search().rank(Knn(query=emb1)).limit(10),
    Search().rank(Knn(query=emb2)).limit(10),
    Search().rank(Knn(query=emb3)).limit(10),
]
results = collection.search(searches)
# Access as: results.ids[0], results.ids[1], results.ids[2]
```

## New Capabilities Not in Legacy API

[TODO: Features only available in Search API]

### 1. Advanced Filtering
```python
# Complex logical expressions
Search().where(
    ((K("a") == 1) & (K("b") == 2)) |
    ((K("c") == 3) & (K("d") == 4))
)

# Regex and contains on any field
Search().where(K("email").regex(r".*@company\.com"))
```

### 2. Custom Ranking Expressions
```python
# Weighted combination
Search().rank(Knn(query=v1) * 0.7 + Knn(query=v2) * 0.3)

# RRF for hybrid search
Search().rank(Rrf([...]))

# Mathematical transformations
Search().rank(Knn(query=v).exp())
```

### 3. Selective Field Return
```python
# Return specific metadata fields only
Search().select("field1", "field2")

# Mix predefined and custom fields
Search().select(K.SCORE, "custom_field")
```

### 4. Batch Operations
```python
# Execute multiple searches in one call
collection.search([search1, search2, search3])
```

## Breaking Changes and Workarounds

[TODO: What doesn't translate directly]

### 1. query_texts Parameter
```python
# Legacy - automatic embedding
collection.query(query_texts=["search text"])

# Search API - explicit embedding required
embedding = embedding_function(["search text"])[0]
collection.search(Search().rank(Knn(query=embedding)))

# Workaround: Create helper function
def query_with_text(collection, text, **kwargs):
    embedding = collection._embedding_function([text])[0]
    return collection.search(
        Search().rank(Knn(query=embedding)).limit(kwargs.get('n_results', 10))
    )
```

### 2. where_document Operator Differences
```python
# Legacy operators might differ
# Check documentation for exact mapping
```

### 3. Result Format Changes
```python
# Legacy: flat structure for single query
# Search API: always nested structure

# Adapter function
def adapt_search_result(search_result):
    """Convert Search API result to legacy format"""
    return {
        "ids": search_result.ids[0],
        "embeddings": search_result.embeddings[0] if search_result.embeddings else None,
        "documents": search_result.documents[0] if search_result.documents else None,
        "metadatas": search_result.metadatas[0] if search_result.metadatas else None,
        "distances": search_result.scores[0] if search_result.scores else None,
    }
```

## Gradual Migration Strategy

[TODO: How to migrate incrementally]

### Phase 1: Parallel Running
```python
class MigrationCollection:
    def __init__(self, collection):
        self.collection = collection
        self.use_new_api = False  # Feature flag
    
    def query(self, **kwargs):
        if self.use_new_api:
            return self._query_with_search_api(**kwargs)
        else:
            return self.collection.query(**kwargs)
    
    def _query_with_search_api(self, **kwargs):
        # Translation logic
        search = Search()
        if 'query_embeddings' in kwargs:
            search = search.rank(Knn(query=kwargs['query_embeddings'][0]))
        # ... more translation
        return self.collection.search(search)
```

### Phase 2: Testing Approach
```python
def compare_results(legacy_result, search_result):
    """Compare results from both APIs"""
    # Compare IDs, scores, etc.
    pass

# A/B testing
legacy_result = collection.query(...)
search_result = collection.search(...)
assert compare_results(legacy_result, search_result)
```

### Phase 3: Rollback Plan
```python
# Keep legacy code paths
if FEATURE_FLAGS.use_search_api:
    result = search_api_code()
else:
    result = legacy_api_code()
```

## Timeline and Deprecation

[TODO: Official timeline]

### Current Status
- **Search API**: Beta (Chroma Cloud only)
- **Legacy API**: Stable, supported
- **Recommendation**: Start experimenting with Search API

### Planned Timeline
| Milestone | Target Date | Status |
|-----------|------------|--------|
| Search API Beta | Current | ✅ Available |
| Search API GA | Q2 2024 | 🚧 Planned |
| Legacy API Deprecated | Q4 2024 | 📅 Planned |
| Legacy API Removed | Q2 2025 | 📅 Planned |

### Migration Checklist
- [ ] Inventory all query() and get() calls
- [ ] Identify complex queries needing careful migration
- [ ] Create migration helpers/adapters
- [ ] Test with parallel execution
- [ ] Gradually switch to Search API
- [ ] Remove legacy code

## Getting Help

[TODO: Support resources]

### Documentation
- [Search API Overview](./overview)
- [Examples](./examples)
- API Reference (coming soon)

### Community Support
- GitHub Issues: [github.com/chroma-core/chroma/issues](https://github.com/chroma-core/chroma/issues)
- Discord: [discord.gg/chroma](https://discord.gg/chroma)

### Migration Assistance
- Migration guide updates
- Code examples repository
- Community migration scripts

### Reporting Issues
When reporting Search API issues:
1. Include both legacy and Search API code
2. Provide sample data if possible
3. Specify Chroma version
4. Include error messages

### FAQs
[TODO: Common questions]
- Q: Can I use both APIs simultaneously?
- Q: Will my existing code break?
- Q: How do I handle the transition period?