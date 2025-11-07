---
id: overview
name: Overview
---

# Search API Overview

The Search API is a powerful, flexible interface for hybrid search operations in Chroma Cloud, combining vector similarity search with metadata filtering and custom ranking expressions.

{% Banner type="tip" %}
**Search API is available in Chroma Cloud only.** Future support on single-node Chroma is planned.
{% /Banner %}

## What is the Search API?

The Search API provides a powerful, unified interface for all search operations in Chroma. Instead of using separate `query()` and `get()` methods with different parameters, the Search API offers:

- **Unified interface**: One consistent API replaces both `query()` and `get()` methods
- **Expression-based queries**: Use `K()` expressions for powerful filtering and field selection
- **Composable operations**: Chain methods to build complex queries naturally
- **Type safety**: Full type hints, IDE autocomplete, and clear error messages
- **Advanced capabilities**: Hybrid search with RRF, custom ranking expressions, and batch operations
- **Flexible result selection**: Choose exactly which fields to return, reducing payload size


## Quick Start

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Build the base search with filtering
search = (
    Search()
    .where(K("category") == "science")
    .limit(10)
    .select(K.DOCUMENT, K.SCORE)
)

# Option 1: Pass pre-computed embeddings directly
query_embedding = [0.25, -0.15, 0.33, ...]
result = collection.search(search.rank(Knn(query=query_embedding)))

# Option 2: Pass text query (embedding created using collection's schema configuration)
query_text = "What are the latest advances in quantum computing?"
result = collection.search(search.rank(Knn(query=query_text)))
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn } from 'chromadb';

// Build the base search with filtering
const search = new Search()
  .where(K("category").eq("science"))
  .limit(10)
  .select(K.DOCUMENT, K.SCORE);

// Option 1: Pass pre-computed embeddings directly
const queryEmbedding = [0.25, -0.15, 0.33, ...];
const result = await collection.search(search.rank(Knn({ query: queryEmbedding })));

// Option 2: Pass text query (embedding created using collection's schema configuration)
const queryText = "What are the latest advances in quantum computing?";
const result2 = await collection.search(search.rank(Knn({ query: queryText })));
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
When passing text to `Knn()`, the embedding is automatically created using the collection's schema configuration. By default, `Knn` uses the `#embedding` key, which corresponds to the default vector index. You can specify a different key with the `key` parameter (e.g., `Knn(query=query_text, key="my_custom_embedding")`). If the specified key doesn't have an embedding configuration in the collection schema, an error will be thrown.
{% /Note %}

## Feature Comparison

| Feature | `query()` | `get()` | `search()` |
|---------|-----------|---------|------------|
| Vector similarity search | ✅ | ❌ | ✅ |
| Filtering (metadata, document, ID) | ✅ | ✅ | ✅ |
| Custom ranking expressions | ❌ | ❌ | ✅ |
| Batch operations | ⚠️ Embedding only | ❌ | ✅ |
| Field selection | ⚠️ Coarse | ⚠️ Coarse | ✅ |
| Pagination | ❌ | ✅ | ✅ |
| Type safety | ⚠️ Partial | ⚠️ Partial | ✅ |

## Availability

The Search API is available for Chroma Cloud. Support for local Chroma deployments will be available in a future release.

## Required Setup

To use the Search API, you'll need to import the necessary components:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Optional: For advanced features
from chromadb import Rrf  # For hybrid search
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn } from 'chromadb';

// Optional: For advanced features
import { Rrf } from 'chromadb';  // For hybrid search
```
{% /Tab %}

{% /TabbedCodeBlock %}

Make sure you're connected to a Chroma Cloud instance, as the Search API is currently only available for cloud deployments.

## Complete Example

Here's a practical example searching for science articles:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
import chromadb
from chromadb import Search, K, Knn

# Connect to Chroma Cloud
client = chromadb.CloudClient(
    tenant="your-tenant",
    database="your-database",
    api_key="your-api-key"
)
collection = client.get_collection("articles")

# Build the base search query
search = (
    Search()
    .where((K("category") == "science") & (K("year") >= 2020))
    .limit(5)
    .select(K.DOCUMENT, K.SCORE, "title", "author")
)

# Option 1: Search with pre-computed embeddings
query_embedding = [0.12, -0.34, 0.56, ...]
result = collection.search(search.rank(Knn(query=query_embedding)))

# Option 2: Search with text query (embedding created automatically)
query_text = "recent quantum computing breakthroughs"
result = collection.search(search.rank(Knn(query=query_text)))

# Access results using the convenient rows() method
# Note: Results are ordered by score (ascending - lower is better)
# For KNN search, score represents distance
rows = result.rows()[0]  # Get first (and only) search results
for row in rows:
    print(f"ID: {row['id']}")
    print(f"Title: {row['metadata']['title']}")
    print(f"Distance: {row['score']:.3f}")
    print(f"Document: {row['document'][:100]}...")
    print("---")
```

{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { ChromaClient, Search, K, Knn } from 'chromadb';

// Connect to Chroma Cloud
const client = new ChromaClient({
  tenant: "your-tenant",
  database: "your-database",
  auth: { provider: "token", credentials: "your-api-key" }
});
const collection = await client.getCollection({ name: "articles" });

// Build the base search query
const search = new Search()
  .where(K("category").eq("science").and(K("year").gte(2020)))
  .limit(5)
  .select(K.DOCUMENT, K.SCORE, "title", "author");

// Option 1: Search with pre-computed embeddings
const queryEmbedding = [0.12, -0.34, 0.56, ...];
const result = await collection.search(search.rank(Knn({ query: queryEmbedding })));

// Option 2: Search with text query (embedding created automatically)
const queryText = "recent quantum computing breakthroughs";
result = await collection.search(search.rank(Knn({ query: queryText })));

// Access results using the convenient rows() method
// Note: Results are ordered by score (ascending - lower is better)
// For KNN search, score represents distance
const rows = result.rows()[0];  // Get first (and only) search results
for (const row of rows) {
  console.log(`ID: ${row.id}`);
  console.log(`Title: ${row.metadata?.title}`);
  console.log(`Distance: ${row.score?.toFixed(3)}`);
  console.log(`Document: ${row.document?.substring(0, 100)}...`);
  console.log("---");
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

Example output:
```
ID: doc_123
Title: Advances in Quantum Computing
Distance: 0.234
Document: Recent developments in quantum computing have shown promising results for...
---
ID: doc_456
Title: Machine Learning in Biology
Distance: 0.412
Document: The application of machine learning techniques to biological data has...
---
```

## Performance

The Search API provides the same performance as existing Chroma query endpoints, with the added benefit of more flexible query construction and batch operations that can reduce the number of round trips.

## Feedback

{% Note type="info" %}
Please report issues or feedback through the [Chroma GitHub repository](https://github.com/chroma-core/chroma/issues).
{% /Note %}

## What's Next?

- **[Search Basics](./search-basics)** - Learn how to construct searches
- **[Filtering with Where](./filtering)** - Master metadata filtering
- **[Ranking and Scoring](./ranking)** - Understand ranking expressions
- **[Hybrid Search](./hybrid-search)** - Combine multiple strategies
- **[Examples](./examples)** - See real-world patterns
