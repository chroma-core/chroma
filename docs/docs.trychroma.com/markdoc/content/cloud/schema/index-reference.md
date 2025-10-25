---
id: index-reference
name: Index Configuration Reference
---

# Index Configuration Reference

Comprehensive reference for all index types and their configuration parameters.

## Index Types Overview

| Index Type | Value Type | Use Case | Parameters | Scope |
|------------|------------|----------|------------|-------|
| FtsIndexConfig | `string` | Full-text search on documents | None | Global only |
| StringInvertedIndexConfig | `string` | Exact/prefix matching on strings | None | Global or per-key |
| IntInvertedIndexConfig | `int_value` | Range/equality queries on integers | None | Global or per-key |
| FloatInvertedIndexConfig | `float_value` | Range/equality queries on floats | None | Global or per-key |
| BoolInvertedIndexConfig | `boolean` | Equality queries on booleans | None | Global or per-key |
| VectorIndexConfig | `float_list` | Semantic similarity search | space, hnsw, spann, embedding_function, source_key | Global only |
| SparseVectorIndexConfig | `sparse_vector` | Keyword-based search (BM25-style) | source_key, embedding_function, bm25 | Per-key only (one per collection) |

## Simple Index Configs

These index types have no configuration parameters - just enable or disable them.

### FtsIndexConfig

Full-text search index for document content.

**Use Case**: Enable full-text search with keyword matching, phrase queries, and ranking by relevance.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, FtsIndexConfig

schema = Schema()
# FTS is global only - cannot be applied to specific keys
schema.create_index(config=FtsIndexConfig())
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, FtsIndexConfig } from 'chromadb';

const schema = new Schema();
// FTS is global only - cannot be applied to specific keys
schema.createIndex(new FtsIndexConfig());
```
{% /Tab %}

{% /TabbedCodeBlock %}

### StringInvertedIndexConfig

Inverted index for exact and prefix string matching.

**Use Case**: Enable filtering on string metadata fields (e.g., `where({"category": "science"})`).

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, StringInvertedIndexConfig

schema = Schema()
# Enable globally or for specific keys
schema.create_index(config=StringInvertedIndexConfig())  # Global
schema.create_index(config=StringInvertedIndexConfig(), key="category")  # Specific key
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, StringInvertedIndexConfig } from 'chromadb';

const schema = new Schema();
// Enable globally or for specific keys
schema.createIndex(new StringInvertedIndexConfig());  // Global
schema.createIndex(new StringInvertedIndexConfig(), "category");  // Specific key
```
{% /Tab %}

{% /TabbedCodeBlock %}

### IntInvertedIndexConfig

Inverted index for integer filtering.

**Use Case**: Enable range and equality queries on integer metadata (e.g., `where({"year": {"$gte": 2020}})`).

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, IntInvertedIndexConfig

schema = Schema()
schema.create_index(config=IntInvertedIndexConfig())  # Global
schema.create_index(config=IntInvertedIndexConfig(), key="year")  # Specific key
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, IntInvertedIndexConfig } from 'chromadb';

const schema = new Schema();
schema.createIndex(new IntInvertedIndexConfig());  // Global
schema.createIndex(new IntInvertedIndexConfig(), "year");  // Specific key
```
{% /Tab %}

{% /TabbedCodeBlock %}

### FloatInvertedIndexConfig

Inverted index for float filtering.

**Use Case**: Enable range and equality queries on float metadata (e.g., `where({"price": {"$lt": 99.99}})`).

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, FloatInvertedIndexConfig

schema = Schema()
schema.create_index(config=FloatInvertedIndexConfig())  # Global
schema.create_index(config=FloatInvertedIndexConfig(), key="price")  # Specific key
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, FloatInvertedIndexConfig } from 'chromadb';

const schema = new Schema();
schema.createIndex(new FloatInvertedIndexConfig());  // Global
schema.createIndex(new FloatInvertedIndexConfig(), "price");  // Specific key
```
{% /Tab %}

{% /TabbedCodeBlock %}

### BoolInvertedIndexConfig

Inverted index for boolean filtering.

**Use Case**: Enable filtering on boolean metadata (e.g., `where({"published": True})`).

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, BoolInvertedIndexConfig

schema = Schema()
schema.create_index(config=BoolInvertedIndexConfig())  # Global
schema.create_index(config=BoolInvertedIndexConfig(), key="published")  // Specific key
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, BoolInvertedIndexConfig } from 'chromadb';

const schema = new Schema();
schema.createIndex(new BoolInvertedIndexConfig());  // Global
schema.createIndex(new BoolInvertedIndexConfig(), "published");  // Specific key
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Vector Index Configuration

Vector indexes enable semantic similarity search on dense embeddings. This is the most complex index type with several configurable parameters.

### Parameters

#### space

Distance function for measuring similarity between vectors:

| Distance | Parameter | Equation | Use Case |
|----------|-----------|----------|----------|
| Squared L2 | `l2` | {% Latex %} d = \\sum\\left(A_i-B_i\\right)^2 {% /Latex %} | Geometric distance - sensitive to magnitude |
| Inner product | `ip` | {% Latex %} d = 1.0 - \\sum\\left(A_i \\times B_i\\right) {% /Latex %} | Dot product - for normalized vectors |
| Cosine similarity | `cosine` | {% Latex %} d = 1.0 - \\frac{\\sum\\left(A_i \\times B_i\\right)}{\\sqrt{\\sum\\left(A_i^2\\right)} \\cdot \\sqrt{\\sum\\left(B_i^2\\right)}} {% /Latex %} | Angle between vectors - most common for text |

#### embedding_function

Optional embedding function to automatically generate embeddings. If not provided, you must supply embeddings manually when adding data.

#### source_key

The metadata field to source vectors from for auto-embedding. Default is `"#document"`.

#### hnsw

HNSW algorithm configuration (for single-node deployments). See HNSW Parameters below.

#### spann

SPANN algorithm configuration (for Chroma Cloud). Currently not user-configurable.

### HNSW Parameters

Configure HNSW index for single-node deployments:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ef_construction` | int | 100 | Candidate list size during index build. Higher = better quality, slower build |
| `max_neighbors` | int | 16 | Max connections per node. Higher = better recall, more memory |
| `ef_search` | int | 100 | Candidate list size during search. Higher = better recall, slower queries |
| `num_threads` | int | CPU count | Threads for index operations |
| `batch_size` | int | 100 | Vectors per batch |
| `sync_threshold` | int | 1000 | When to sync index to disk |
| `resize_factor` | float | 1.2 | Growth factor when resizing |

**Tuning Tips:**
- Increase `ef_construction` and `max_neighbors` for better recall at build time
- Increase `ef_search` for better recall at query time
- Balance recall vs speed based on your requirements

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, VectorIndexConfig, HnswIndexConfig

schema = Schema()

# Basic: just set distance metric
schema.create_index(config=VectorIndexConfig(space="cosine"))

# Advanced: tune HNSW parameters for better recall
schema.create_index(config=VectorIndexConfig(
    space="cosine",
    hnsw=HnswIndexConfig(
        ef_construction=200,
        max_neighbors=32,
        ef_search=200
    )
))
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, VectorIndexConfig } from 'chromadb';

const schema = new Schema();

// Basic: just set distance metric
schema.createIndex(new VectorIndexConfig({ space: "cosine" }));

// Advanced: tune HNSW parameters for better recall
schema.createIndex(new VectorIndexConfig({
  space: "cosine",
  hnsw: {
    ef_construction: 200,
    max_neighbors: 32,
    ef_search: 200
  }
}));
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Sparse Vector Index Configuration

Sparse vector indexes enable keyword-based search (BM25-style) that complements dense semantic search. Only one sparse vector index is allowed per collection.

### Parameters

#### source_key

**Required.** The field to generate sparse embeddings from. Typically `"#document"` for document text, but can be any metadata field containing text.

#### embedding_function

Optional sparse embedding function. If not provided, Chroma uses a default BM25-based sparse embedding function.

#### bm25

Optional BM25 configuration parameters (if using default embedding function).

### Use Cases

- **Hybrid search**: Combine with dense embeddings for better retrieval quality
- **Exact keyword matching**: Find documents containing specific terms
- **Domain-specific terminology**: Better at matching technical terms and proper nouns than dense embeddings

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, SparseVectorIndexConfig

schema = Schema()

# Basic: use default BM25 sparse embeddings from documents
schema.create_index(
    config=SparseVectorIndexConfig(source_key="#document"),
    key="sparse_embedding"
)

# Advanced: use custom source field
schema.create_index(
    config=SparseVectorIndexConfig(source_key="abstract"),
    key="abstract_sparse"
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, SparseVectorIndexConfig } from 'chromadb';

const schema = new Schema();

// Basic: use default BM25 sparse embeddings from documents
schema.createIndex(
  new SparseVectorIndexConfig({ sourceKey: "#document" }),
  "sparse_embedding"
);

// Advanced: use custom source field
schema.createIndex(
  new SparseVectorIndexConfig({ sourceKey: "abstract" }),
  "abstract_sparse"
);
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
For complete hybrid search setup and querying with RRF, see [Hybrid Search Setup](./hybrid-search).
{% /Note %}

## Next Steps

- Apply these configurations in [Schema Basics](./schema-basics)
- Set up [hybrid search](./hybrid-search) with sparse vectors
