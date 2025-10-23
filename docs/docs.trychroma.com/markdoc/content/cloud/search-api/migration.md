---
id: migration
name: Migration Guide
---

# Migration Guide

Migrate from the legacy `query()` and `get()` methods to the new Search API.

{% Banner type="tip" %}
The `query()` and `get()` methods will continue to be supported, so migration to the Search API is optional.
{% /Banner %}



## Parameter Mapping

{% Banner type="info" %}
The Search API is available in Chroma Cloud. This guide uses dictionary syntax for minimal migration effort.
{% /Banner %}


### query() Parameters

| Legacy `query()` | Search API | Notes |
|------------------|------------|-------|
| `query_embeddings` | `rank={"$knn": {"query": ...}}` | Can use text or embeddings |
| `query_texts` | `rank={"$knn": {"query": "text"}}` | Text queries now supported |
| `query_images` | Not yet supported | Image queries coming in future release |
| `query_uris` | Not yet supported | URI queries coming in future release |
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

# Search API - with text query
from chromadb import Search

results = collection.search(
    Search(
        rank={"$knn": {"query": "machine learning"}},
        limit=10
    )
)
```
{% /Tab %}
{% Tab label="typescript" %}
```typescript
// Legacy API
const results = await collection.query({
  queryEmbeddings: [[0.1, 0.2, 0.3]],
  nResults: 10
});

// Search API - with text query
import { Search } from 'chromadb';

const results2 = await collection.search(
  new Search({
    rank: { $knn: { query: "machine learning" } },
    limit: 10
  })
);
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
        rank={"$knn": {"query": "quantum computing"}},
        where={"#document": {"$contains": "quantum"}},
        limit=5
    )
)
```
{% /Tab %}
{% Tab label="typescript" %}
```typescript
// Legacy API
const results = await collection.query({
  queryEmbeddings: [[0.1, 0.2, 0.3]],
  nResults: 5,
  whereDocument: { $contains: "quantum" }
});

// Search API
const results2 = await collection.search(
  new Search({
    rank: { $knn: { query: "quantum computing" } },
    where: { "#document": { $contains: "quantum" } },
    limit: 5
  })
);
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
        rank={"$knn": {"query": "quantum physics"}},
        limit=10
    )
)
```
{% /Tab %}
{% Tab label="typescript" %}
```typescript
// Legacy API
const results = await collection.query({
  queryEmbeddings: [[0.1, 0.2, 0.3]],
  nResults: 10,
  where: { category: "science" },
  whereDocument: { $contains: "quantum" }
});

// Search API - combine filters with $and
const results2 = await collection.search(
  new Search({
    where: {
      $and: [
        { category: "science" },
        { "#document": { $contains: "quantum" } }
      ]
    },
    rank: { $knn: { query: "quantum physics" } },
    limit: 10
  })
);
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
// Legacy API
const results = await collection.get({
  ids: ["id1", "id2", "id3"]
});

// Search API
const results2 = await collection.search(
  new Search({
    where: { "#id": { $in: ["id1", "id2", "id3"] } }
  })
);
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
// Legacy API
const results = await collection.get({
  where: { status: "active" },
  limit: 100,
  offset: 50
});

// Search API
const results2 = await collection.search(
  new Search({
    where: { status: "active" },
    limit: { limit: 100, offset: 50 }
  })
);
```
{% /Tab %}
{% /TabbedCodeBlock %}

## Key Differences

### Text Queries Now Supported

The Search API supports text queries directly - they are automatically converted to embeddings using the collection's configured embedding function.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Legacy API
collection.query(query_texts=["search text"])

# Search API - direct text query
collection.search(Search(rank={"$knn": {"query": "search text"}}))
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Legacy API
await collection.query({ queryTexts: ["search text"] });

// Search API - direct text query
await collection.search(
  new Search({ rank: { $knn: { query: "search text" } } })
);
```
{% /Tab %}

{% /TabbedCodeBlock %}

### New Capabilities

- **Advanced filtering** - Complex logical expressions
- **Custom ranking** - Combine and transform ranking expressions
- **Hybrid search** - RRF for combining multiple strategies
- **Selective fields** - Return only needed fields
- **Flexible batch operations** - Different parameters per search in batch

#### Flexible Batch Operations

The Search API allows different parameters for each search in a batch:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Legacy - same parameters for all queries
results = collection.query(
    query_embeddings=[emb1, emb2, emb3],
    n_results=10,
    where={"category": "science"}  # Same filter for all
)

# Search API - different parameters per search
searches = [
    Search(rank={"$knn": {"query": "machine learning"}}, limit=10, where={"category": "science"}),
    Search(rank={"$knn": {"query": "neural networks"}}, limit=5, where={"category": "tech"}),
    Search(rank={"$knn": {"query": "artificial intelligence"}}, limit=20)  # No filter
]
results = collection.search(searches)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Legacy - same parameters for all queries
const results = await collection.query({
  queryEmbeddings: [emb1, emb2, emb3],
  nResults: 10,
  where: { category: "science" }  // Same filter for all
});

// Search API - different parameters per search
const searches = [
  new Search({ rank: { $knn: { query: "machine learning" } }, limit: 10, where: { category: "science" } }),
  new Search({ rank: { $knn: { query: "neural networks" } }, limit: 5, where: { category: "tech" } }),
  new Search({ rank: { $knn: { query: "artificial intelligence" } }, limit: 20 })  // No filter
];
const results2 = await collection.search(searches);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Migration Tips

- Start with simple queries before complex ones
- Test both APIs in parallel during migration
- Use batch operations to reduce API calls
- Text queries are now supported - use them directly in the Search API

## Next Steps

- [Search Basics](./search-basics) - Core search concepts
- [Filtering](./filtering) - Advanced filtering options
- [Examples](./examples) - Practical search patterns