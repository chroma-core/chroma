---
id: schema-overview
name: Overview
---

# Schema Overview

Schema enables fine-grained control over index configuration on collections. Control which indexes are created, optimize for your workload, and enable advanced capabilities like hybrid search.

## What is Schema?

Schema allows you to configure which indexes are created for different data types in your Chroma collections. You can enable or disable indexes globally or per-field, configure vector index parameters, and set up sparse vector indexes for keyword-based search.

## Why Use Schema?

- **Enable Hybrid Search**: Combine dense and sparse embeddings for better retrieval quality
- **Optimize Performance**: Disable unused indexes to speed up writes and reduce index build time
- **Fine-Tune Configuration**: Adjust vector index parameters for your workload

## Quick Start

Here's a simple example creating a collection with a custom schema:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
import chromadb
from chromadb import Schema, StringInvertedIndexConfig

# Connect to Chroma Cloud
client = chromadb.CloudClient(
    tenant="your-tenant",
    database="your-database",
    api_key="your-api-key"
)

# Create a schema and disable string indexing globally
schema = Schema()
schema.delete_index(config=StringInvertedIndexConfig())

# Create collection with the schema
collection = client.create_collection(
    name="my_collection",
    schema=schema
)

# Add data - string metadata won't be indexed
collection.add(
    ids=["id1", "id2"],
    documents=["Document 1", "Document 2"],
    metadatas=[
        {"category": "science", "year": 2024},
        {"category": "tech", "year": 2023}
    ]
)

# Querying on disabled index will raise an error
try:
    collection.query(
        query_texts=["query"],
        where={"category": "science"}  # Error: string index is disabled
    )
except Exception as e:
    print(f"Error: {e}")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { ChromaClient, Schema, StringInvertedIndexConfig } from 'chromadb';

// Connect to Chroma Cloud
const client = new ChromaClient({
  tenant: "your-tenant",
  database: "your-database",
  auth: { provider: "token", credentials: "your-api-key" }
});

// Create a schema and disable string indexing globally
const schema = new Schema();
schema.deleteIndex(new StringInvertedIndexConfig());

// Create collection with the schema
const collection = await client.createCollection({
  name: "my_collection",
  schema: schema
});

// Add data - string metadata won't be indexed
await collection.add({
  ids: ["id1", "id2"],
  documents: ["Document 1", "Document 2"],
  metadatas: [
    { category: "science", year: 2024 },
    { category: "tech", year: 2023 }
  ]
});

// Querying on disabled index will raise an error
try {
  await collection.query({
    queryTexts: ["query"],
    where: { category: "science" }  // Error: string index is disabled
  });
} catch (e) {
  console.log(`Error: ${e}`);
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Banner type="tip" %}
**Important:** Schema is only configurable in `create_collection`. We are working on supporting schema update via collection `modify`
{% /Banner %}

## Feature Highlights

- **Default Indexes**: Collections start with sensible defaults - inverted indexes for scalar types, vector index for embeddings, full text search index for documents
- **Global Configuration**: Set index defaults that apply to all metadata keys of a given type during collection creation
- **Per-Key Configuration**: Override defaults for specific metadata fields
- **Sparse Vector Support**: Enable sparse embeddings for hybrid search with BM25-style retrieval
- **Index Deletion**: Disable indexes you don't need to improve write performance
- **Dynamic Schema Evolution**: New metadata keys added during writes automatically inherit from global defaults

## Next Steps

- [Schema Basics](./schema-basics) - Learn the structure and how to use Schema
- [Sparse Vector Search Setup](./sparse-vector-search) - Configure sparse vectors and hybrid search
- [Index Configuration Reference](./index-reference) - Complete index type reference
