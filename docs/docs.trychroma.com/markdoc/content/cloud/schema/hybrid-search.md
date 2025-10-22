---
id: hybrid-search
name: Hybrid Search Setup
---

# Hybrid Search Setup

Hybrid search combines dense semantic embeddings with sparse keyword embeddings for better retrieval quality. This page shows how to configure Schema to enable hybrid search capabilities.

## What is Hybrid Search?

Hybrid search combines two complementary approaches:
- **Dense embeddings**: Capture semantic meaning (e.g., "car" matches "automobile")
- **Sparse embeddings**: Capture exact keyword matches (e.g., BM25-style retrieval)

By combining both with techniques like Reciprocal Rank Fusion (RRF), you often achieve better results than either approach alone.

## Configuration Steps

### Step 1: Create Schema with Sparse Vector Index

To enable hybrid search, add a sparse vector index to your schema. The `key` parameter is the metadata field name where sparse embeddings will be stored - you can name it whatever you want:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, SparseVectorIndexConfig

schema = Schema()

# Add sparse vector index for keyword-based search
# "sparse_embedding" is just a metadata key name - use any name you prefer
schema.create_index(
    config=SparseVectorIndexConfig(source_key="#document"),
    key="sparse_embedding"
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, SparseVectorIndexConfig } from 'chromadb';

const schema = new Schema();

// Add sparse vector index for keyword-based search
// "sparse_embedding" is just a metadata key name - use any name you prefer
schema.createIndex(
  new SparseVectorIndexConfig({ sourceKey: "#document" }),
  "sparse_embedding"
);
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
The `source_key` specifies which field to generate sparse embeddings from (typically `#document` for document text). The sparse embeddings are automatically generated and stored in the metadata field you specify as the `key`.
{% /Note %}

### Step 2: Create Collection with Schema

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
import chromadb

client = chromadb.CloudClient(
    tenant="your-tenant",
    database="your-database",
    api_key="your-api-key"
)

collection = client.create_collection(
    name="hybrid_search_collection",
    schema=schema
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { ChromaClient } from 'chromadb';

const client = new ChromaClient({
  tenant: "your-tenant",
  database: "your-database",
  auth: { provider: "token", credentials: "your-api-key" }
});

const collection = await client.createCollection({
  name: "hybrid_search_collection",
  schema: schema
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Step 3: Add Data

When you add documents, sparse embeddings are automatically generated from the source key:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.add(
    ids=["doc1", "doc2", "doc3"],
    documents=[
        "The quick brown fox jumps over the lazy dog",
        "A fast auburn fox leaps over a sleepy canine",
        "Machine learning is a subset of artificial intelligence"
    ],
    metadatas=[
        {"category": "animals"},
        {"category": "animals"},
        {"category": "technology"}
    ]
)

# Sparse embeddings for "sparse_embedding" are generated automatically
# from the documents (source_key="#document")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.add({
  ids: ["doc1", "doc2", "doc3"],
  documents: [
    "The quick brown fox jumps over the lazy dog",
    "A fast auburn fox leaps over a sleepy canine",
    "Machine learning is a subset of artificial intelligence"
  ],
  metadatas: [
    { category: "animals" },
    { category: "animals" },
    { category: "technology" }
  ]
});

// Sparse embeddings for "sparse_embedding" are generated automatically
// from the documents (source_key="#document")
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Querying with RRF

Once configured, use RRF (Reciprocal Rank Fusion) to combine dense and sparse search results:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn, Rrf

# Create RRF ranking combining dense and sparse embeddings
hybrid_rank = Rrf(
    ranks=[
        Knn(query="fox animal", return_rank=True),           # Dense semantic search
        Knn(query="fox animal", key="sparse_embedding", return_rank=True)  # Sparse keyword search
    ],
    weights=[0.7, 0.3],  # 70% semantic, 30% keyword
    k=60
)

# Build and execute search
search = (Search()
    .rank(hybrid_rank)
    .limit(10)
    .select(K.DOCUMENT, K.SCORE))

results = collection.search(search)

# Process results
for row in results.rows()[0]:
    print(f"Score: {row['score']:.3f} - {row['document']}")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, Rrf } from 'chromadb';

// Create RRF ranking combining dense and sparse embeddings
const hybridRank = Rrf({
  ranks: [
    Knn({ query: "fox animal", returnRank: true }),           // Dense semantic search
    Knn({ query: "fox animal", key: "sparse_embedding", returnRank: true })  // Sparse keyword search
  ],
  weights: [0.7, 0.3],  // 70% semantic, 30% keyword
  k: 60
});

// Build and execute search
const search = new Search()
  .rank(hybridRank)
  .limit(10)
  .select(K.DOCUMENT, K.SCORE);

const results = await collection.search(search);

// Process results
for (const row of results.rows()[0]) {
  console.log(`Score: ${row.score.toFixed(3)} - ${row.document}`);
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
For comprehensive details on RRF parameters, weight tuning, and advanced hybrid search strategies, see the [Search API Hybrid Search documentation](../search-api/hybrid-search).
{% /Note %}

## Next Steps

- **[Search API Hybrid Search with RRF](../search-api/hybrid-search)** - Learn RRF parameters, weight tuning, and advanced strategies
- [Index Configuration Reference](./index-reference) - Detailed parameters for all index types
- [Schema Basics](./schema-basics) - General Schema usage and patterns
