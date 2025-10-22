---
id: index-reference
name: Index Configuration Reference
---

# Index Configuration Reference

Comprehensive reference for all index types and their configuration parameters.

## Index Types Overview

[Comprehensive table with all index types]

| Index Type | Value Type | Use Case | Parameters | Global vs Key-Specific |
|------------|------------|----------|------------|------------------------|
| FtsIndexConfig | `string` | Full-text search | None | Global only |
| StringInvertedIndexConfig | `string` | String filtering | None | Both |
| VectorIndexConfig | `float_list` | Dense embeddings | Multiple | Global only |
| SparseVectorIndexConfig | `sparse_vector` | Sparse embeddings | Multiple | Key-specific only |
| IntInvertedIndexConfig | `int_value` | Integer filtering | None | Both |
| FloatInvertedIndexConfig | `float_value` | Float filtering | None | Both |
| BoolInvertedIndexConfig | `boolean` | Boolean filtering | None | Both |

## Simple Index Configs

### FtsIndexConfig

[Full-text search configuration]

**Use Case**: [When to use]

**Parameters**: None

**Examples**:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# FTS index examples
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// FTS index examples
```
{% /Tab %}

{% /TabbedCodeBlock %}

### StringInvertedIndexConfig

[String inverted index configuration]

**Use Case**: [When to use]

**Parameters**: None

**Examples**:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# String inverted index examples
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// String inverted index examples
```
{% /Tab %}

{% /TabbedCodeBlock %}

### IntInvertedIndexConfig

[Integer inverted index configuration]

**Use Case**: [When to use]

**Parameters**: None

**Examples**:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Int inverted index examples
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Int inverted index examples
```
{% /Tab %}

{% /TabbedCodeBlock %}

### FloatInvertedIndexConfig

[Float inverted index configuration]

**Use Case**: [When to use]

**Parameters**: None

**Examples**:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Float inverted index examples
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Float inverted index examples
```
{% /Tab %}

{% /TabbedCodeBlock %}

### BoolInvertedIndexConfig

[Boolean inverted index configuration]

**Use Case**: [When to use]

**Parameters**: None

**Examples**:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Bool inverted index examples
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Bool inverted index examples
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Vector Index Configuration

[Detailed explanation of VectorIndexConfig]

### Parameters

#### space

[Description with distance formulas]

| Distance | Parameter | Equation | Intuition |
|----------|-----------|----------|-----------|
| Squared L2 | `l2` | {% Latex %} d = \\sum\\left(A_i-B_i\\right)^2 {% /Latex %} | ... |
| Inner product | `ip` | {% Latex %} d = 1.0 - \\sum\\left(A_i \\times B_i\\right) {% /Latex %} | ... |
| Cosine similarity | `cosine` | {% Latex %} d = 1.0 - \\frac{\\sum\\left(A_i \\times B_i\\right)}{\\sqrt{\\sum\\left(A_i^2\\right)} \\cdot \\sqrt{\\sum\\left(B_i^2\\right)}} {% /Latex %} | ... |

#### embedding_function

[Description]

#### source_key

[Description]

#### hnsw

[Description and link to HNSW parameters section]

#### spann

[Description and link to SPANN parameters section]

### HNSW Parameters

[Detailed table of HNSW parameters]

| Parameter | Type | Default | Description | Tuning Guidance |
|-----------|------|---------|-------------|-----------------|
| `ef_construction` | int | 100 | ... | ... |
| `max_neighbors` | int | 16 | ... | ... |
| `ef_search` | int | 100 | ... | ... |
| `num_threads` | int | CPU count | ... | ... |
| `batch_size` | int | 100 | ... | ... |
| `sync_threshold` | int | 1000 | ... | ... |
| `resize_factor` | float | 1.2 | ... | ... |

### SPANN Parameters

[Detailed table of SPANN parameters if configurable]

### Tuning Guide

[Similar to configure.md - recall vs performance tradeoffs]

#### Recall vs Performance

[Explanation]

#### Example Scenarios

[Examples with different configurations]

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Vector index configuration examples
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Vector index configuration examples
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Sparse Vector Index Configuration

[Detailed explanation of SparseVectorIndexConfig]

### Parameters

#### embedding_function

[Description - required for auto-generation]

#### source_key

[Description - where to source text from]

#### bm25

[Description - BM25 configuration if applicable]

### Use Cases

[When to use sparse vector indexes]

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Sparse vector index configuration examples
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Sparse vector index configuration examples
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Integration with Hybrid Search

[How sparse vector indexes enable hybrid search]

{% Note type="info" %}
For complete hybrid search setup and usage, see [Hybrid Search Setup](./hybrid-search).
{% /Note %}

## Next Steps

- Apply these configurations in [Schema Basics](./schema-basics)
- Set up [hybrid search](./hybrid-search) with sparse vectors
