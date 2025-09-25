---
id: migration
name: Migration Guide
---

# Migration Guide

Migrate from the legacy `query()` and `get()` methods to the new Search API.

{% Note type="info" %}
The Search API is currently available in Chroma Cloud (beta). This guide uses dictionary syntax for minimal migration effort.
{% /Note %}

## Parameter Mapping

### query() Parameters

| Legacy `query()` | Search API | Notes |
|------------------|------------|-------|
| `query_embeddings` | `rank={"$knn": {"query": ...}}` | Wrap in ranking expression |
| `query_texts` | Not yet supported | Text queries coming with collection schema |
| `query_images` | Not yet supported | Image queries coming with collection schema |
| `query_uris` | Not yet supported | URI queries coming with collection schema |
| `n_results` | `limit` | Direct mapping |
| `ids` | `where={"#id": {"$in": [...]}}` | Filter by IDs |
| `where` | `where` | Same syntax |
| `where_document` | `where={"#document": {...}}` | Use #document field |
| `include` | `select` | See field mapping below |

### get() Parameters

| Legacy `get()` | Search API | Notes |
|----------------|------------|-------|
| `ids` | `where={"#id": {"$in": [...]}}` | Filter by IDs |
| `where` | `where` | Same syntax |
| `where_document` | `where={"#document": {...}}` | Use #document field |
| `limit` | `limit` | Direct mapping |
| `offset` | `limit={"offset": ...}` | Part of limit dict |
| `include` | `select` | See field mapping below |

### Include/Select Field Mapping

| Legacy `include` | Search API `select` | Description |
|------------------|-------------------|-------------|
| `"ids"` | Always included | IDs are always returned |
| `"documents"` | `"#document"` | Document content |
| `"metadatas"` | `"#metadata"` | All metadata fields |
| `"embeddings"` | `"#embedding"` | Vector embeddings |
| `"distances"` | `"#score"` | Distance/score from query |
| `"uris"` | `"#uri"` | Document URIs |

## Examples

### Basic Similarity Search

{% TabbedCodeBlock %}
{% Tab label="python" %}
```python
# Legacy API
results = collection.query(
    query_embeddings=[[0.1, 0.2, 0.3]],
    n_results=10
)

# Search API
from chromadb import Search

results = collection.search(
    Search(
        rank={"$knn": {"query": [0.1, 0.2, 0.3]}},
        limit=10
    )
)
```
{% /Tab %}
{% Tab label="typescript" %}
```typescript
// TODO: TypeScript examples will be updated when the client is available
```
{% /Tab %}
{% /TabbedCodeBlock %}

### Document Filtering

{% TabbedCodeBlock %}
{% Tab label="python" %}
```python
# Legacy API
results = collection.query(
    query_embeddings=[[0.1, 0.2, 0.3]],
    n_results=5,
    where_document={"$contains": "quantum"}
)

# Search API
results = collection.search(
    Search(
        rank={"$knn": {"query": [0.1, 0.2, 0.3]}},
        where={"#document": {"$contains": "quantum"}},
        limit=5
    )
)
```
{% /Tab %}
{% Tab label="typescript" %}
```typescript
// TODO: TypeScript examples will be updated when the client is available
```
{% /Tab %}
{% /TabbedCodeBlock %}

### Combined Filters

{% TabbedCodeBlock %}
{% Tab label="python" %}
```python
# Legacy API
results = collection.query(
    query_embeddings=[[0.1, 0.2, 0.3]],
    n_results=10,
    where={"category": "science"},
    where_document={"$contains": "quantum"}
)

# Search API - combine filters with $and
results = collection.search(
    Search(
        where={"$and": [
            {"category": "science"},
            {"#document": {"$contains": "quantum"}}
        ]},
        rank={"$knn": {"query": [0.1, 0.2, 0.3]}},
        limit=10
    )
)
```
{% /Tab %}
{% Tab label="typescript" %}
```typescript
// TODO: TypeScript examples will be updated when the client is available
```
{% /Tab %}
{% /TabbedCodeBlock %}

### Get by IDs

{% TabbedCodeBlock %}
{% Tab label="python" %}
```python
# Legacy API
results = collection.get(
    ids=["id1", "id2", "id3"]
)

# Search API  
results = collection.search(
    Search(
        where={"#id": {"$in": ["id1", "id2", "id3"]}}
    )
)
```
{% /Tab %}
{% Tab label="typescript" %}
```typescript
// TODO: TypeScript examples will be updated when the client is available
```
{% /Tab %}
{% /TabbedCodeBlock %}

### Pagination

{% TabbedCodeBlock %}
{% Tab label="python" %}
```python
# Legacy API
results = collection.get(
    where={"status": "active"},
    limit=100,
    offset=50
)

# Search API
results = collection.search(
    Search(
        where={"status": "active"},
        limit={"limit": 100, "offset": 50}
    )
)
```
{% /Tab %}
{% Tab label="typescript" %}
```typescript
// TODO: TypeScript examples will be updated when the client is available
```
{% /Tab %}
{% /TabbedCodeBlock %}

## Key Differences

### Text Queries Not Yet Supported

The Search API currently requires embeddings. Text query support is coming when collection schema support is available.

```python
# Legacy - automatic embedding
collection.query(query_texts=["search text"])

# Search API - manual embedding (temporary)
embedding = embedding_function(["search text"])[0]
collection.search(Search(rank={"$knn": {"query": embedding}}))
```

### New Capabilities

- **Advanced filtering** - Complex logical expressions
- **Custom ranking** - Combine and transform ranking expressions
- **Hybrid search** - RRF for combining multiple strategies  
- **Selective fields** - Return only needed fields
- **Flexible batch operations** - Different parameters per search in batch

#### Flexible Batch Operations

The Search API allows different parameters for each search in a batch:

```python
# Legacy - same parameters for all queries
results = collection.query(
    query_embeddings=[emb1, emb2, emb3],
    n_results=10,
    where={"category": "science"}  # Same filter for all
)

# Search API - different parameters per search
searches = [
    Search(rank={"$knn": {"query": emb1}}, limit=10, where={"category": "science"}),
    Search(rank={"$knn": {"query": emb2}}, limit=5, where={"category": "tech"}),
    Search(rank={"$knn": {"query": emb3}}, limit=20)  # No filter
]
results = collection.search(searches)
```

## Migration Tips

- Start with simple queries before complex ones
- Test both APIs in parallel during migration  
- Use batch operations to reduce API calls
- Keep embeddings ready until text queries are supported

## Next Steps

- [Search Basics](./search-basics) - Core search concepts
- [Filtering](./filtering) - Advanced filtering options
- [Examples](./examples) - Practical search patterns