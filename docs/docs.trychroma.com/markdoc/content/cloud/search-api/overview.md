---
id: overview
name: Overview
---

# Search API Overview

The Search API is a powerful, flexible interface for hybrid search operations in Chroma Cloud, combining vector similarity search with metadata filtering and custom ranking expressions.

{% Note type="info" %}
The Search API is currently in beta and available exclusively for Chroma Cloud users.
{% /Note %}

## Quick Start

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Simple vector search with metadata filtering
query_embedding = [0.25, -0.15, 0.33, ...]  # Your query vector
# TODO: When collection schema is ready, you'll be able to pass text directly:
# .rank(Knn(query="What are the latest advances in quantum computing?"))

result = collection.search(
    Search()
    .where(K("category") == "science")
    .rank(Knn(query=query_embedding))
    .limit(10)
    .select(K.DOCUMENT, K.SCORE)
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn } from 'chromadb';

// Simple vector search with metadata filtering
const queryEmbedding = [0.25, -0.15, 0.33, ...];  // Your query vector
// TODO: When collection schema is ready, you'll be able to pass text directly:
// .rank(Knn({ query: "What are the latest advances in quantum computing?" }))

const result = await collection.search(
  new Search()
    .where(K("category").eq("science"))
    .rank(Knn({ query: queryEmbedding }))
    .limit(10)
    .select(K.DOCUMENT, K.SCORE)
);
```
{% /Tab %}

{% /Tabs %}

## What is the Search API?

The Search API provides a powerful, unified interface for all search operations in Chroma. Instead of using separate `query()` and `get()` methods with different parameters, the Search API offers:

- **Unified interface**: One consistent API replaces both `query()` and `get()` methods
- **Expression-based queries**: Use `K()` expressions for powerful filtering and field selection
- **Composable operations**: Chain methods to build complex queries naturally
- **Type safety**: Full type hints, IDE autocomplete, and clear error messages
- **Advanced capabilities**: Hybrid search with RRF, custom ranking expressions, and batch operations
- **Flexible result selection**: Choose exactly which fields to return, reducing payload size

## Feature Comparison

| Feature | `query()` | `get()` | `search()` |
|---------|-----------|---------|------------|
| Vector similarity search | ✅ | ❌ | ✅ |
| Filtering (metadata, document, ID) | ✅ | ✅ | ✅ |
| Custom ranking expressions | ❌ | ❌ | ✅ |
| Batch operations | ⚠️ Embedding only | ❌ | ✅ |
| Field selection | ⚠️ Coarse | ⚠️ Coarse | ✅ |
| Pagination | ✅ | ✅ | ✅ |
| Type safety | ⚠️ Partial | ⚠️ Partial | ✅ |

## Availability

The Search API is currently available in beta for Chroma Cloud. Support for local Chroma deployments will be available in a future release.

## Required Setup

To use the Search API, you'll need to import the necessary components:

{% Tabs %}

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

{% /Tabs %}

Make sure you're connected to a Chroma Cloud instance, as the Search API is currently only available for cloud deployments.

## Complete Example

Here's a practical example searching for science articles:

{% Tabs %}

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

# Search for science articles similar to a query
query_embedding = [0.12, -0.34, 0.56, ...]  # Your embedding vector
# TODO: When collection schema is ready, you'll be able to pass text directly:
# .rank(Knn(query="recent quantum computing breakthroughs"))

result = collection.search(
    Search()
    .where((K("category") == "science") & (K("year") >= 2020))
    .rank(Knn(query=query_embedding))
    .limit(5)
    .select(K.DOCUMENT, K.SCORE, "title", "author")
)

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

// Search for science articles similar to a query
const queryEmbedding = [0.12, -0.34, 0.56, ...];  // Your embedding vector
// TODO: When collection schema is ready, you'll be able to pass text directly:
// .rank(Knn({ query: "recent quantum computing breakthroughs" }))

const result = await collection.search(
  new Search()
    .where(K("category").eq("science").and(K("year").gte(2020)))
    .rank(Knn({ query: queryEmbedding }))
    .limit(5)
    .select(K.DOCUMENT, K.SCORE, "title", "author")
);

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
{% /Tab %}

{% /Tabs %}

## Performance

The Search API provides the same performance as existing Chroma query endpoints, with the added benefit of more flexible query construction and batch operations that can reduce the number of round trips.

## Beta Disclaimer

{% Note type="info" %}
The Search API is in beta. Features and syntax may change. Please report issues or feedback through the [Chroma GitHub repository](https://github.com/chroma-core/chroma/issues).
{% /Note %}

## What's Next?

- **[Search Basics](./search-basics)** - Learn how to construct searches
- **[Filtering with Where](./filtering)** - Master metadata filtering
- **[Ranking and Scoring](./ranking)** - Understand ranking expressions
- **[Hybrid Search](./hybrid-search)** - Combine multiple strategies
- **[Examples](./examples)** - See real-world patterns