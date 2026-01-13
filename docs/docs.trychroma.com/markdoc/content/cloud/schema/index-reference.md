---
id: index-reference
name: Index Configuration Reference
---

# Index Configuration Reference

Comprehensive reference for all index types and their configuration parameters.

## Index Types Overview

Schema recognizes six value types, each with associated index types. Without providing a Schema, collections use these built-in defaults:

| Config Class | Value Type | Default Behavior | Use Case |
|-------------|-----------|------------------|----------|
| `StringInvertedIndexConfig` | `string` | Enabled for all metadata | Filter on string values |
| `FtsIndexConfig` | `string` | Enabled for `K.DOCUMENT` only | Full-text search on documents |
| `VectorIndexConfig` | `float_list` | Enabled for `K.EMBEDDING` only | Similarity search on embeddings |
| `SparseVectorIndexConfig` | `sparse_vector` | Disabled (requires config) | Keyword-based search |
| `IntInvertedIndexConfig` | `int_value` | Enabled for all metadata | Filter on integer values |
| `FloatInvertedIndexConfig` | `float_value` | Enabled for all metadata | Filter on float values |
| `BoolInvertedIndexConfig` | `boolean` | Enabled for all metadata | Filter on boolean values |

## Simple Index Configs

These index types have no configuration parameters.

### FtsIndexConfig

**Use Case**: Full-text search and regular expression search on documents (e.g., `where(K.DOCUMENT.contains("search term"))`).

**Limitations**: Cannot be deleted. Applies to `K.DOCUMENT` only.

### StringInvertedIndexConfig

**Use Case**: Exact and prefix string matching on metadata fields (e.g., `where(K("category") == "science")`).

### IntInvertedIndexConfig

**Use Case**: Range and equality queries on integer metadata (e.g., `where(K("year") >= 2020)`).

### FloatInvertedIndexConfig

**Use Case**: Range and equality queries on float metadata (e.g., `where(K("price") < 99.99)`).

### BoolInvertedIndexConfig

**Use Case**: Filtering on boolean metadata (e.g., `where(K("published") == True)`).

## VectorIndexConfig

**Use Case**: Semantic similarity search on dense embeddings for finding conceptually similar content.

**Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `space` | string | No | Distance function: `l2` (geometric), `ip` (inner product), or `cosine` (angle-based, most common for text). Default: `l2` |
| `embedding_function` | EmbeddingFunction | No | Function to auto-generate embeddings from `K.DOCUMENT`. If not provided, supply embeddings manually |
| `source_key` | string | No | Reserved for future use. Currently always uses `K.DOCUMENT` |
| `hnsw` | HnswConfig | No | Advanced: HNSW algorithm tuning for single-node deployments |
| `spann` | SpannConfig | No | Advanced: SPANN algorithm tuning (clustering, probing) for Chroma Cloud |

**Limitations**: 
- Cannot be deleted
- Applies to `K.EMBEDDING` only

{% Banner type="tip" %}
**Advanced tuning:** HNSW and SPANN parameters control index build and search behavior. They are pre-optimized for most use cases. Only adjust if you have specific performance requirements and understand the tradeoffs between recall, speed, and resource usage. Incorrect tuning can degrade performance.
{% /Banner %}

## SparseVectorIndexConfig

**Use Case**: Keyword-based search for exact term matching, domain-specific terminology, and technical terms. Ideal for hybrid search when combined with dense embeddings.

**Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_key` | string | No | Field to generate sparse embeddings from. Typically `K.DOCUMENT`, but can be any text field |
| `embedding_function` | SparseEmbeddingFunction | No | Sparse embedding function (e.g., `ChromaCloudSpladeEmbeddingFunction`, `HuggingFaceSparseEmbeddingFunction`, `Bm25EmbeddingFunction`) |
| `bm25` | boolean | No | Set to `true` when using `Bm25EmbeddingFunction` to enable inverse document frequency (IDF) scaling for queries. Not applicable for SPLADE |

**Limitations**:
- Must specify a metadata key name (per-key configuration required)
- Only one sparse vector index allowed per collection
- Cannot be deleted once created

{% Note type="info" %}
For complete sparse vector search setup and querying examples, see [Sparse Vector Search Setup](./sparse-vector-search).
{% /Note %}

## Next Steps

- Apply these configurations in [Schema Basics](./schema-basics)
- Set up [sparse vector search](./sparse-vector-search) with sparse vectors and hybrid search
