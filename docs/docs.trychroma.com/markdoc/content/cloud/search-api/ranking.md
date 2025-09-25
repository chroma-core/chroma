---
id: ranking
name: Ranking and Scoring
---

# Ranking and Scoring

Learn how to use ranking expressions to score and order your search results. In Chroma, lower scores indicate better matches (distance-based scoring).

## How Ranking Works

A ranking expression determines which documents are scored and how they're ordered:

### Expression Evaluation Process

1. **No ranking (`rank=None`)**: Documents are returned in index order (typically insertion order)

2. **With ranking expression**: 
   - Must contain at least one `Knn` expression
   - Documents must appear in at least one `Knn`'s top-k results to be considered
   - Documents must also appear in ALL `Knn` results where `default=None`
   - Documents missing from a `Knn` with a `default` value get that default score
   - Each `Knn` considers its top `limit` candidates (default: 128)
   - Documents are sorted by score (ascending - lower scores first)
   - Final results based on `Search.limit()`

### Document Selection and Scoring

{% Tabs %}

{% Tab label="python" %}
```python
# Example 1: Single Knn - scores top 128 documents
rank = Knn(query=vector, limit=128)
# Only the 128 nearest documents get scored

# Example 2: Multiple Knn with default=None
rank = Knn(query=v1, limit=100) + Knn(query=v2, limit=100, key="sparse_embedding")
# Both Knn have default=None (the default)
# Documents must appear in BOTH top-100 lists to be scored
# Documents in only one list are excluded

# Example 3: Mixed default values
rank = Knn(query=v1, limit=100) * 0.5 + Knn(query=v2, limit=50, default=1000.0) * 0.5
# v1 has default=None, v2 has default=1000.0
# Documents in v1's top-100 but not in v2's top-50:
#   - Get v1's distance * 0.5 + 1000.0 * 0.5 (v2's default)
# Documents in v2's top-50 but not in v1's top-100:
#   - Excluded (must appear in all Knn where default=None)
# Documents in both lists:
#   - Get v1's distance * 0.5 + v2's distance * 0.5
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

{% Note type="warning" %}
When combining multiple `Knn` expressions, documents must appear in at least one `Knn`'s results AND must appear in every `Knn` where `default=None`. To avoid excluding documents, set `default` values on your `Knn` expressions.
{% /Note %}

## The Knn Class

The `Knn` class performs K-nearest neighbor search to find similar vectors. It's the primary way to add vector similarity scoring to your searches.

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Knn

# Basic vector search on default embedding field
Knn(query=[0.1, 0.2, 0.3])

# TODO: When collection schema is supported, you'll be able to pass text directly:
# Knn(query="What is machine learning?")

# Search with custom parameters
Knn(
    query=[0.1, 0.2, 0.3],
    key="#embedding",      # Field to search (default: "#embedding")
    limit=128,            # Max candidates to consider (default: 128)
    return_rank=False     # Return rank position vs distance (default: False)
)

# Search custom embedding field in metadata
Knn(query=sparse_vector, key="sparse_embedding")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Knn Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | List[float], SparseVector, or np.ndarray | Required | The query vector to search with |
| `key` | str | `"#embedding"` | Field to search - `"#embedding"` for dense embeddings, or a metadata field name for sparse embeddings |
| `limit` | int | `128` | Maximum number of candidates to consider |
| `default` | float or None | `None` | Score for documents not in KNN results |
| `return_rank` | bool | `False` | If `True`, return rank position (0, 1, 2...) instead of distance |

{% Note type="info" %}
`"#embedding"` (or `K.EMBEDDING`) refers to the default embedding field where Chroma stores dense embeddings. Sparse embeddings must be stored in metadata under a consistent key.
{% /Note %}

## Query Vector Formats

### Dense Vectors

{% Tabs %}

{% Tab label="python" %}
```python
# Python list (most common)
Knn(query=[0.1, 0.2, 0.3, 0.4])

# NumPy array
import numpy as np
embedding = np.array([0.1, 0.2, 0.3, 0.4])
Knn(query=embedding)

# Pre-normalized vectors (if your embeddings are already normalized)
from chromadb.api.types import normalize_embeddings
normalized = normalize_embeddings([[0.1, 0.2, 0.3, 0.4]])[0]
Knn(query=normalized)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### Sparse Vectors

{% Tabs %}

{% Tab label="python" %}
```python
# Sparse vector format: dictionary with indices and values
sparse_vector = {
    "indices": [1, 5, 10, 50],  # Non-zero indices
    "values": [0.5, 0.3, 0.8, 0.2]  # Corresponding values
}

# Search using sparse vector (must specify the metadata field)
Knn(query=sparse_vector, key="sparse_embedding")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### Embedding Fields

Chroma currently supports:
1. **Dense embeddings** - Stored in the default embedding field (`"#embedding"` or `K.EMBEDDING`) 
2. **Sparse embeddings** - Can be stored in metadata under a consistent key

{% Tabs %}

{% Tab label="python" %}
```python
# Dense embeddings - use the default embedding field
Knn(query=dense_vector)                    # Implicitly uses key="#embedding"
Knn(query=dense_vector, key="#embedding")  # Explicit
Knn(query=dense_vector, key=K.EMBEDDING)   # Using constant (same as "#embedding")

# Sparse embeddings - store in metadata under a consistent key
# The sparse vector should be stored under the same metadata key across all documents
sparse_vector = {
    "indices": [1, 5, 10, 50],
    "values": [0.5, 0.3, 0.8, 0.2]
}
Knn(query=sparse_vector, key="sparse_embedding")  # Search sparse embeddings in metadata

# NOT SUPPORTED: Dense embeddings in metadata
# Knn(query=dense_vector, key="some_metadata_field")  # ✗ Not supported

# TODO: When collection schema is supported:
# - You'll be able to store multiple dense embeddings
# - You'll be able to declare metadata fields as embedding fields
# - This will enable optimized indexing for additional embeddings
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

{% Note type="warning" %}
Currently, dense embeddings can only be stored in the default embedding field (`#embedding`). Only sparse vector embeddings can be stored in metadata, and they must be stored consistently under the same key across all documents.
{% /Note %}

## Arithmetic Operations

**Supported operators:**
- `+` - Addition
- `-` - Subtraction
- `*` - Multiplication
- `/` - Division
- `-` (unary) - Negation

Combine ranking expressions using arithmetic operators. Operator precedence follows Python's standard rules.

{% Tabs %}

{% Tab label="python" %}
```python
# Weighted combination of two embeddings
text_score = Knn(query=text_vector)
image_score = Knn(query=image_vector, key="image_embedding")
combined = text_score * 0.7 + image_score * 0.3

# Scaling scores
normalized = Knn(query=vector) / 100.0

# Adding baseline score
with_baseline = Knn(query=vector) + 0.5

# Complex expressions (use parentheses for clarity)
final_score = (Knn(query=v1) * 0.5 + Knn(query=v2) * 0.3) / 1.8
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

{% Note type="info" %}
Numbers in expressions are automatically converted to `Val` constants. For example, `Knn(query=v) * 0.5` is equivalent to `Knn(query=v) * Val(0.5)`.
{% /Note %}

## Mathematical Functions

**Supported functions:**
- `exp()` - Exponential (e^x)
- `log()` - Natural logarithm
- `abs()` - Absolute value
- `min()` - Minimum of two values
- `max()` - Maximum of two values

{% Tabs %}

{% Tab label="python" %}
```python
# Exponential - amplifies differences between scores
score = Knn(query=vector).exp()

# Logarithm - compresses score range
# Add constant to avoid log(0)
compressed = (Knn(query=vector) + 1).log()

# Absolute value - useful for difference calculations
diff = abs(Knn(query=v1) - Knn(query=v2))

# Clamping scores to a range
score = Knn(query=vector)
clamped = score.min(0.0).max(1.0)  # Clamp to [0, 1]

# Ensuring non-negative scores
positive_only = Knn(query=vector).min(0.0)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Val for Constant Values

The `Val` class represents constant values in ranking expressions. Numbers are automatically converted to `Val`, but you can use it explicitly for clarity.

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Val

# Automatic conversion (these are equivalent)
score1 = Knn(query=vector) * 0.5
score2 = Knn(query=vector) * Val(0.5)

# Explicit Val for named constants
baseline = Val(0.1)
boost_factor = Val(2.0)
final_score = (Knn(query=vector) + baseline) * boost_factor

# Using Val in complex expressions
threshold = Val(0.8)
penalty = Val(0.5)
adjusted = Knn(query=vector).max(threshold) - penalty
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Combining Ranking Expressions

You can combine multiple Knn searches using arithmetic operations for custom scoring strategies.

{% Tabs %}

{% Tab label="python" %}
```python
# Linear combination - weighted average of different embeddings
text_score = Knn(query=text_vector)
title_score = Knn(query=title_vector, key="title_embedding")
combined = text_score * 0.8 + title_score * 0.2

# Multi-modal search - image and text
image_score = Knn(query=image_vector, key="image_embedding")
text_score = Knn(query=text_vector)
multi_modal = image_score * 0.4 + text_score * 0.6

# Boosting with metadata
base_score = Knn(query=vector)
# Note: K("boost") would need to be part of select() to use in ranking
final_score = base_score * (1 + Val(0.1))  # Fixed 10% boost
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

{% Note type="info" %}
For advanced hybrid search combining multiple ranking strategies, consider using [RRF (Reciprocal Rank Fusion)](./hybrid-search) which is specifically designed for this purpose.
{% /Note %}

## Dictionary Syntax

You can also construct ranking expressions using dictionary syntax. This is useful when building ranking expressions programmatically.

**Supported dictionary operators:**
- `$knn` - K-nearest neighbor search
- `$val` - Constant value
- `$sum` - Addition of multiple ranks
- `$sub` - Subtraction (left - right)
- `$mul` - Multiplication of multiple ranks
- `$div` - Division (left / right)
- `$abs` - Absolute value
- `$exp` - Exponential
- `$log` - Natural logarithm
- `$max` - Maximum of multiple ranks
- `$min` - Minimum of multiple ranks

{% Tabs %}

{% Tab label="python" %}
```python
# Knn as dictionary
rank_dict = {
    "$knn": {
        "query": [0.1, 0.2, 0.3],
        "key": "#embedding",  # Optional, defaults to "#embedding"
        "limit": 128,         # Optional, defaults to 128
        "return_rank": False  # Optional, defaults to False
    }
}

# Val as dictionary
const_dict = {"$val": 0.5}

# Arithmetic operations
sum_dict = {
    "$sum": [
        {"$knn": {"query": [0.1, 0.2, 0.3]}},
        {"$val": 0.5}
    ]
}  # Same as Knn(query=[0.1, 0.2, 0.3]) + 0.5

mul_dict = {
    "$mul": [
        {"$knn": {"query": [0.1, 0.2, 0.3]}},
        {"$val": 0.8}
    ]
}  # Same as Knn(query=[0.1, 0.2, 0.3]) * 0.8

# Complex expression
weighted_combo = {
    "$sum": [
        {"$mul": [
            {"$knn": {"query": text_vector}},
            {"$val": 0.7}
        ]},
        {"$mul": [
            {"$knn": {"query": image_vector, "key": "image_embedding"}},
            {"$val": 0.3}
        ]}
    ]
}  # Same as Knn(query=text_vector) * 0.7 + Knn(query=image_vector, key="image_embedding") * 0.3

# Use in Search
search = Search(rank=rank_dict)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Understanding Scores

- **Lower scores = better matches** - Chroma uses distance-based scoring
- **Score range** - Depends on your embedding model and distance metric
- **No ranking** - When `rank=None`, results are returned in natural storage order
- **Distance vs similarity** - Scores represent distance; for similarity, use `1 - score` (for normalized embeddings)

## Edge Cases and Important Behavior

### Default Ranking
When no ranking is specified (`rank=None`), results are returned in index order (typically insertion order). This is useful when you only need filtering without scoring.

{% Tabs %}

{% Tab label="python" %}
```python
# No ranking - results in index order
search = Search().where(K("status") == "active").limit(10)
# Score for each document is simply its index position
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### Combining Knn Expressions with default=None
Documents must appear in at least one `Knn`'s results to be candidates, AND must appear in ALL `Knn` results where `default=None`.

{% Tabs %}

{% Tab label="python" %}
```python
# Problem: Restrictive filtering with default=None
rank = Knn(query=v1, limit=100) * 0.7 + Knn(query=v2, limit=100) * 0.3
# Both have default=None
# Only documents in BOTH top-100 lists get scored

# Solution: Set default values for more inclusive results
rank = (
    Knn(query=v1, limit=100, default=10.0) * 0.7 + 
    Knn(query=v2, limit=100, default=10.0) * 0.3
)
# Now documents in either top-100 list can be scored
# Documents get default score (10.0) for Knn where they don't appear
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### Vector Dimension Mismatch
Query vectors must match the dimension of the indexed embeddings. Mismatched dimensions will result in an error.

{% Tabs %}

{% Tab label="python" %}
```python
# If your embeddings are 384-dimensional
Knn(query=[0.1, 0.2, 0.3])  # ✗ Error - only 3 dimensions
Knn(query=[0.1] * 384)      # ✓ Correct - 384 dimensions
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### The return_rank Parameter
Set `return_rank=True` when using Knn with RRF to get rank positions (0, 1, 2...) instead of distances.

{% Tabs %}

{% Tab label="python" %}
```python
# For regular scoring - use distances
Knn(query=vector)  # Returns: 0.23, 0.45, 0.67...

# For RRF - use rank positions
Knn(query=vector, return_rank=True)  # Returns: 0, 1, 2...
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### The limit Parameter
The `limit` parameter in Knn controls how many candidates are considered, not the final result count. Use `Search.limit()` to control the number of results returned.

{% Tabs %}

{% Tab label="python" %}
```python
# Knn.limit - candidates to consider for scoring
rank = Knn(query=vector, limit=1000)  # Score top 1000 candidates

# Search.limit - results to return
search = Search().rank(rank).limit(10)  # Return top 10 results
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Complete Example

Here's a practical example combining different ranking features:

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn, Val

# Complex ranking with filtering and mathematical functions
search = (Search()
    .where(
        (K("status") == "published") &
        (K("category").is_in(["tech", "science"]))
    )
    .rank(
        # Combine two embeddings with weights
        (
            Knn(query=content_vector) * 0.7 +
            Knn(query=title_vector, key="title_embedding") * 0.3
        ).exp()  # Amplify score differences
        .min(0.0)  # Ensure non-negative
    )
    .limit(20)
    .select(K.DOCUMENT, K.SCORE, "title", "category")
)

results = collection.search(search)

# Process results using rows() for cleaner access
rows = results.rows()[0]  # Get first (and only) search results
for i, row in enumerate(rows):
    print(f"{i+1}. {row['metadata']['title']}")
    print(f"   Score: {row['score']:.3f}")
    print(f"   Category: {row['metadata']['category']}")
    print(f"   Preview: {row['document'][:100]}...")
    print()
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Tips and Best Practices

- **Normalize your vectors** - Ensure consistent scoring by normalizing query vectors
- **Use appropriate limit values** - Higher limits in Knn mean more accurate but slower results
- **Set return_rank=True for RRF** - Essential when using Reciprocal Rank Fusion
- **Test score ranges** - Understand your model's typical score ranges for better thresholding
- **Combine strategies wisely** - Linear combinations work well for similar score ranges

## Next Steps

- Learn about [hybrid search with RRF](./hybrid-search) for advanced ranking strategies
- See [practical examples](./examples) of ranking in real-world scenarios
- Explore [batch operations](./batch-operations) for multiple searches