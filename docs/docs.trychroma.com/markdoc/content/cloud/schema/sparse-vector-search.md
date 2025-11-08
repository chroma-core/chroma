---
id: sparse-vector-search
name: Sparse Vector Search Setup
---

# Sparse Vector Search Setup

Learn how to configure and use sparse vectors for keyword-based search, and combine them with dense embeddings for powerful hybrid search capabilities.

## What are Sparse Vectors?

Sparse vectors are high-dimensional vectors with mostly zero values, designed for keyword-based retrieval. Unlike dense embeddings which capture semantic meaning, sparse vectors excel at:

- **Exact keyword matching**: Finding documents containing specific terms
- **Domain-specific terminology**: Better at matching technical terms, proper nouns, and rare words
- **Lexical retrieval**: BM25-style retrieval patterns

Sparse vectors use models like SPLADE that assign importance weights to specific tokens, making them complementary to dense semantic embeddings.

## Enabling Sparse Vector Index

To use sparse vectors, add a sparse vector index to your schema. The `key` parameter is the metadata field name where sparse embeddings will be stored - you can name it whatever you want:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, SparseVectorIndexConfig, K
from chromadb.utils.embedding_functions import ChromaCloudSpladeEmbeddingFunction

schema = Schema()

# Add sparse vector index for keyword-based search
# "sparse_embedding" is just a metadata key name - use any name you prefer
sparse_ef = ChromaCloudSpladeEmbeddingFunction()
schema.create_index(
    config=SparseVectorIndexConfig(
        source_key=K.DOCUMENT,
        embedding_function=sparse_ef
    ),
    key="sparse_embedding"
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, SparseVectorIndexConfig, K } from 'chromadb';
import { ChromaCloudSpladeEmbeddingFunction } from '@chroma-core/chroma-cloud-splade';

const schema = new Schema();

// Add sparse vector index for keyword-based search
// "sparse_embedding" is just a metadata key name - use any name you prefer
const sparseEf = new ChromaCloudSpladeEmbeddingFunction({
  apiKeyEnvVar: "CHROMA_API_KEY"
});
schema.createIndex(
  new SparseVectorIndexConfig({
    sourceKey: K.DOCUMENT,
    embeddingFunction: sparseEf
  }),
  "sparse_embedding"
);
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
The `source_key` specifies which field to generate sparse embeddings from (typically `K.DOCUMENT` for document text), and `embedding_function` specifies the function to generate the sparse embeddings. This example uses `ChromaCloudSpladeEmbeddingFunction`, but you can also use other sparse embedding functions like `HuggingFaceSparseEmbeddingFunction` or `FastembedSparseEmbeddingFunction`. The sparse embeddings are automatically generated and stored in the metadata field you specify as the `key`.
{% /Note %}

## Create Collection and Add Data

### Create Collection with Schema

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

### Add Data

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
# from the documents (source_key=K.DOCUMENT)
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
// from the documents (source_key=K.DOCUMENT)
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Using Sparse Vectors for Search

Once configured, you can search using sparse vectors alone or combine them with dense embeddings for hybrid search.

### Sparse Vector Search

Use sparse vectors for keyword-based retrieval:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Search using sparse embeddings only
sparse_rank = Knn(query="fox animal", key="sparse_embedding")

# Build and execute search
search = (Search()
    .rank(sparse_rank)
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
import { Search, K, Knn } from 'chromadb';

// Search using sparse embeddings only
const sparseRank = Knn({ query: "fox animal", key: "sparse_embedding" });

// Build and execute search
const search = new Search()
  .rank(sparseRank)
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

## Hybrid Search

Hybrid search combines dense semantic embeddings with sparse keyword embeddings for improved retrieval quality. By merging results from both approaches using Reciprocal Rank Fusion (RRF), you often achieve better results than either approach alone.

### Benefits of Hybrid Search

- **Semantic + Lexical**: Dense embeddings capture meaning while sparse vectors catch exact keywords
- **Improved recall**: Finds relevant documents that either semantic or keyword search might miss alone
- **Balanced results**: Combines the strengths of both retrieval methods

### Combining Dense and Sparse with RRF

Use RRF (Reciprocal Rank Fusion) to merge dense and sparse search results:

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
