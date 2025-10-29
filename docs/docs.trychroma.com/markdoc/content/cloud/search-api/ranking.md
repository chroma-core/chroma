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
   - Each `Knn` considers its top `limit` candidates (default: 16)
   - Documents are sorted by score (ascending - lower scores first)
   - Final results based on `Search.limit()`

### Document Selection and Scoring

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Example 1: Single Knn - scores top 16 documents
rank = Knn(query="machine learning research")
# Only the 16 nearest documents get scored (default limit)

# Example 2: Multiple Knn with default=None
rank = Knn(query="research papers", limit=100) + Knn(query="academic publications", limit=100, key="sparse_embedding")
# Both Knn have default=None (the default)
# Documents must appear in BOTH top-100 lists to be scored
# Documents in only one list are excluded

# Example 3: Mixed default values
rank = Knn(query="AI research", limit=100) * 0.5 + Knn(query="scientific papers", limit=50, default=1000.0, key="sparse_embedding") * 0.5
# First Knn has default=None, second has default=1000.0
# Documents in first top-100 but not in second top-50:
#   - Get first distance * 0.5 + 1000.0 * 0.5 (second's default)
# Documents in second top-50 but not in first top-100:
#   - Excluded (must appear in all Knn where default=None)
# Documents in both lists:
#   - Get first distance * 0.5 + second distance * 0.5
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Example 1: Single Knn - scores top 16 documents
const rank1 = Knn({ query: "machine learning research" });
// Only the 16 nearest documents get scored (default limit)

// Example 2: Multiple Knn with default undefined
const rank2 = Knn({ query: "research papers", limit: 100 })
  .add(Knn({ query: "academic publications", limit: 100, key: "sparse_embedding" }));
// Both Knn have default undefined (the default)
// Documents must appear in BOTH top-100 lists to be scored
// Documents in only one list are excluded

// Example 3: Mixed default values
const rank3 = Knn({ query: "AI research", limit: 100 }).multiply(0.5)
  .add(Knn({ query: "scientific papers", limit: 50, default: 1000.0, key: "sparse_embedding" }).multiply(0.5));
// First Knn has default undefined, second has default 1000.0
// Documents in first top-100 but not in second top-50:
//   - Get first distance * 0.5 + 1000.0 * 0.5 (second's default)
// Documents in second top-50 but not in first top-100:
//   - Excluded (must appear in all Knn where default is undefined)
// Documents in both lists:
//   - Get first distance * 0.5 + second distance * 0.5
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="warning" %}
When combining multiple `Knn` expressions, documents must appear in at least one `Knn`'s results AND must appear in every `Knn` where `default=None`. To avoid excluding documents, set `default` values on your `Knn` expressions.
{% /Note %}

## The Knn Class

The `Knn` class performs K-nearest neighbor search to find similar vectors. It's the primary way to add vector similarity scoring to your searches.

{% Banner type="tip" %}
**Sparse embeddings:** To search custom sparse embedding fields, you must first configure a sparse vector index in your collection schema. See [Sparse Vector Search Setup](../schema/sparse-vector-search) for configuration instructions.
{% /Banner %}

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Knn

# Basic search on default embedding field
Knn(query="What is machine learning?")

# Search with custom parameters
Knn(
    query="What is machine learning?",
    key="#embedding",      # Field to search (default: "#embedding")
    limit=100,            # Max candidates to consider (default: 16)
    return_rank=False     # Return rank position vs distance (default: False)
)

# Search custom sparse embedding field in metadata
Knn(query="machine learning", key="sparse_embedding")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Knn } from 'chromadb';

// Basic search on default embedding field
Knn({ query: "What is machine learning?" });

// Search with custom parameters
Knn({
  query: "What is machine learning?",
  key: "#embedding",      // Field to search (default: "#embedding")
  limit: 100,            // Max candidates to consider (default: 16)
  returnRank: false      // Return rank position vs distance (default: false)
});

// Search custom sparse embedding field in metadata
Knn({ query: "machine learning", key: "sparse_embedding" });
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Knn Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | str, List[float], SparseVector, or np.ndarray | Required | The query text or vector to search with |
| `key` | str | `"#embedding"` | Field to search - `"#embedding"` for dense embeddings, or a metadata field name for sparse embeddings |
| `limit` | int | `16` | Maximum number of candidates to consider |
| `default` | float or None | `None` | Score for documents not in KNN results |
| `return_rank` | bool | `False` | If `True`, return rank position (0, 1, 2...) instead of distance |

{% Note type="info" %}
`"#embedding"` (or `K.EMBEDDING`) refers to the default embedding field where Chroma stores dense embeddings. Sparse embeddings must be stored in metadata under a consistent key.
{% /Note %}

## Query Formats

### Text Queries

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Text query (most common - auto-embedded using collection schema)
Knn(query="machine learning applications")

# Text is automatically converted to embeddings using the collection's
# configured embedding function
Knn(query="What are the latest advances in quantum computing?")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Text query (most common - auto-embedded using collection schema)
Knn({ query: "machine learning applications" });

// Text is automatically converted to embeddings using the collection's
// configured embedding function
Knn({ query: "What are the latest advances in quantum computing?" });
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Dense Vectors

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Python list
Knn(query=[0.1, 0.2, 0.3, 0.4])

# NumPy array
import numpy as np
embedding = np.array([0.1, 0.2, 0.3, 0.4])
Knn(query=embedding)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Array
Knn({ query: [0.1, 0.2, 0.3, 0.4] });

// Float32Array or other typed arrays
const embedding = new Float32Array([0.1, 0.2, 0.3, 0.4]);
Knn({ query: embedding });
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Sparse Vectors

{% TabbedCodeBlock %}

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
// Sparse vector format: object with indices and values
const sparseVector = {
  indices: [1, 5, 10, 50],         // Non-zero indices
  values: [0.5, 0.3, 0.8, 0.2]     // Corresponding values
};

// Search using sparse vector (must specify the metadata field)
Knn({ query: sparseVector, key: "sparse_embedding" });
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Embedding Fields

Chroma currently supports:
1. **Dense embeddings** - Stored in the default embedding field (`"#embedding"` or `K.EMBEDDING`) 
2. **Sparse embeddings** - Can be stored in metadata under a consistent key

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Text or dense embeddings - use the default embedding field
Knn(query="machine learning")              # Implicitly uses key="#embedding"
Knn(query="machine learning", key="#embedding")  # Explicit
Knn(query="machine learning", key=K.EMBEDDING)   # Using constant (same as "#embedding")

# Sparse embeddings - store in metadata under a consistent key
# The sparse vector should be stored under the same metadata key across all documents
Knn(query="machine learning", key="sparse_embedding")  # Search sparse embeddings in metadata

# NOT SUPPORTED: Dense embeddings in metadata
# Knn(query=[0.1, 0.2], key="some_metadata_field")  # ✗ Not supported
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Text or dense embeddings - use the default embedding field
Knn({ query: "machine learning" });              // Implicitly uses key "#embedding"
Knn({ query: "machine learning", key: "#embedding" });  // Explicit
Knn({ query: "machine learning", key: K.EMBEDDING });   // Using constant (same as "#embedding")

// Sparse embeddings - store in metadata under a consistent key
// The sparse vector should be stored under the same metadata key across all documents
Knn({ query: "machine learning", key: "sparse_embedding" });  // Search sparse embeddings in metadata

// NOT SUPPORTED: Dense embeddings in metadata
// Knn({ query: [0.1, 0.2], key: "some_metadata_field" })  // ✗ Not supported
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="warning" %}
Currently, dense embeddings can only be stored in the default embedding field (`#embedding`). Only sparse vector embeddings can be stored in metadata, and they must be stored consistently under the same key across all documents. Additionally, only one sparse vector index is allowed per collection in metadata.
{% /Note %}

{% Note type="info" %}
Support for multiple dense embedding fields and multiple sparse vector indices is coming in a future release. This will allow you to store and query multiple embeddings per document, with optimized indexing for each field.
{% /Note %}

## Arithmetic Operations

**Supported operators:**
- `+` - Addition
- `-` - Subtraction
- `*` - Multiplication
- `/` - Division
- `-` (unary) - Negation

Combine ranking expressions using arithmetic operators. Operator precedence follows Python's standard rules.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Weighted combination of two searches
text_score = Knn(query="machine learning research")
sparse_q = {"indices": [1, 5, 10], "values": [0.5, 0.3, 0.8]}
sparse_score = Knn(query=sparse_q, key="sparse_embedding")
combined = text_score * 0.7 + sparse_score * 0.3

# Scaling scores
normalized = Knn(query="quantum computing") / 100.0

# Adding baseline score
with_baseline = Knn(query="artificial intelligence") + 0.5

# Complex expressions (use parentheses for clarity)
final_score = (Knn(query="deep learning") * 0.5 + Knn(query="neural networks") * 0.3) / 1.8
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Weighted combination of two searches
const textScore = Knn({ query: "machine learning research" });
const sparseQ = { indices: [1, 5, 10], values: [0.5, 0.3, 0.8] };
const sparseScore = Knn({ query: sparseQ, key: "sparse_embedding" });
const combined = textScore.multiply(0.7).add(sparseScore.multiply(0.3));

// Scaling scores
const normalized = Knn({ query: "quantum computing" }).divide(100.0);

// Adding baseline score
const withBaseline = Knn({ query: "artificial intelligence" }).add(0.5);

// Complex expressions (use chaining for clarity)
const finalScore = Knn({ query: "deep learning" }).multiply(0.5)
  .add(Knn({ query: "neural networks" }).multiply(0.3))
  .divide(1.8);
```
{% /Tab %}

{% /TabbedCodeBlock %}

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

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Exponential - amplifies differences between scores
score = Knn(query="machine learning").exp()

# Logarithm - compresses score range
# Add constant to avoid log(0)
compressed = (Knn(query="deep learning") + 1).log()

# Absolute value - useful for difference calculations
diff = abs(Knn(query="neural networks") - Knn(query="machine learning"))

# Clamping scores to a range
score = Knn(query="artificial intelligence")
clamped = score.min(0.0).max(1.0)  # Clamp to [0, 1]

# Ensuring non-negative scores
positive_only = Knn(query="quantum computing").min(0.0)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Exponential - amplifies differences between scores
const score = Knn({ query: "machine learning" }).exp();

// Logarithm - compresses score range
// Add constant to avoid log(0)
const compressed = Knn({ query: "deep learning" }).add(1).log();

// Absolute value - useful for difference calculations
const diff = Knn({ query: "neural networks" }).subtract(Knn({ query: "machine learning" })).abs();

// Clamping scores to a range
const score2 = Knn({ query: "artificial intelligence" });
const clamped = score2.min(0.0).max(1.0);  // Clamp to [0, 1]

// Ensuring non-negative scores
const positiveOnly = Knn({ query: "quantum computing" }).min(0.0);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Val for Constant Values

The `Val` class represents constant values in ranking expressions. Numbers are automatically converted to `Val`, but you can use it explicitly for clarity.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Val

# Automatic conversion (these are equivalent)
score1 = Knn(query="machine learning") * 0.5
score2 = Knn(query="machine learning") * Val(0.5)

# Explicit Val for named constants
baseline = Val(0.1)
boost_factor = Val(2.0)
final_score = (Knn(query="artificial intelligence") + baseline) * boost_factor

# Using Val in complex expressions
threshold = Val(0.8)
penalty = Val(0.5)
adjusted = Knn(query="deep learning").max(threshold) - penalty
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Val, Knn } from 'chromadb';

// Automatic conversion (these are equivalent)
const score1 = Knn({ query: "machine learning" }).multiply(0.5);
const score2 = Knn({ query: "machine learning" }).multiply(Val(0.5));

// Explicit Val for named constants
const baseline = Val(0.1);
const boostFactor = Val(2.0);
const finalScore = Knn({ query: "artificial intelligence" }).add(baseline).multiply(boostFactor);

// Using Val in complex expressions
const threshold = Val(0.8);
const penalty = Val(0.5);
const adjusted = Knn({ query: "deep learning" }).max(threshold).subtract(penalty);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Combining Ranking Expressions

You can combine multiple Knn searches using arithmetic operations for custom scoring strategies.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Linear combination - weighted average of different searches
dense_score = Knn(query="machine learning applications")
sparse_score = Knn(query="machine learning applications", key="sparse_embedding")
combined = dense_score * 0.8 + sparse_score * 0.2

# Multi-query search - combining different perspectives
general_score = Knn(query="artificial intelligence overview")
specific_score = Knn(query="neural network architectures")
multi_query = general_score * 0.4 + specific_score * 0.6

# Boosting with constant
base_score = Knn(query="quantum computing")
# Note: K("boost") would need to be part of select() to use in ranking
final_score = base_score * (1 + Val(0.1))  # Fixed 10% boost
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Linear combination - weighted average of different searches
const denseScore = Knn({ query: "machine learning applications" });
const sparseScore = Knn({ query: "machine learning applications", key: "sparse_embedding" });
const combined = denseScore.multiply(0.8).add(sparseScore.multiply(0.2));

// Multi-query search - combining different perspectives
const generalScore = Knn({ query: "artificial intelligence overview" });
const specificScore = Knn({ query: "neural network architectures" });
const multiQuery = generalScore.multiply(0.4).add(specificScore.multiply(0.6));

// Boosting with constant
const baseScore = Knn({ query: "quantum computing" });
// Note: K("boost") would need to be part of select() to use in ranking
const finalScore = baseScore.multiply(Val(1).add(Val(0.1)));  // Fixed 10% boost
```
{% /Tab %}

{% /TabbedCodeBlock %}

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

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Knn as dictionary
rank_dict = {
    "$knn": {
        "query": "machine learning research",
        "key": "#embedding",  # Optional, defaults to "#embedding"
        "limit": 100,         # Optional, defaults to 16
        "return_rank": False  # Optional, defaults to False
    }
}

# Val as dictionary
const_dict = {"$val": 0.5}

# Arithmetic operations
sum_dict = {
    "$sum": [
        {"$knn": {"query": "deep learning"}},
        {"$val": 0.5}
    ]
}  # Same as Knn(query="deep learning") + 0.5

mul_dict = {
    "$mul": [
        {"$knn": {"query": "neural networks"}},
        {"$val": 0.8}
    ]
}  # Same as Knn(query="neural networks") * 0.8

# Complex expression
weighted_combo = {
    "$sum": [
        {"$mul": [
            {"$knn": {"query": "machine learning"}},
            {"$val": 0.7}
        ]},
        {"$mul": [
            {"$knn": {"query": "machine learning", "key": "sparse_embedding"}},
            {"$val": 0.3}
        ]}
    ]
}  # Same as Knn(query="machine learning") * 0.7 + Knn(query="machine learning", key="sparse_embedding") * 0.3

# Use in Search
search = Search(rank=rank_dict)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Knn as dictionary
const rankDict = {
  $knn: {
    query: "machine learning research",
    key: "#embedding",  // Optional, defaults to "#embedding"
    limit: 100,         // Optional, defaults to 16
    return_rank: false  // Optional, defaults to false
  }
};

// Val as dictionary
const constDict = { $val: 0.5 };

// Arithmetic operations
const sumDict = {
  $sum: [
    { $knn: { query: "deep learning" } },
    { $val: 0.5 }
  ]
};  // Same as Knn({ query: "deep learning" }).add(0.5)

const mulDict = {
  $mul: [
    { $knn: { query: "neural networks" } },
    { $val: 0.8 }
  ]
};  // Same as Knn({ query: "neural networks" }).multiply(0.8)

// Complex expression
const weightedCombo = {
  $sum: [
    {
      $mul: [
        { $knn: { query: "machine learning" } },
        { $val: 0.7 }
      ]
    },
    {
      $mul: [
        { $knn: { query: "machine learning", key: "sparse_embedding" } },
        { $val: 0.3 }
      ]
    }
  ]
};  // Same as Knn({ query: "machine learning" }).multiply(0.7).add(Knn({ query: "machine learning", key: "sparse_embedding" }).multiply(0.3))

// Use in Search
const search = new Search({ rank: rankDict });
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Understanding Scores

- **Lower scores = better matches** - Chroma uses distance-based scoring
- **Score range** - Depends on your embedding model and distance metric
- **No ranking** - When `rank=None`, results are returned in natural storage order
- **Distance vs similarity** - Scores represent distance; for similarity, use `1 - score` (for normalized embeddings)

## Edge Cases and Important Behavior

### Default Ranking
When no ranking is specified (`rank=None`), results are returned in index order (typically insertion order). This is useful when you only need filtering without scoring.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# No ranking - results in index order
search = Search().where(K("status") == "active").limit(10)
# Score for each document is simply its index position
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// No ranking - results in index order
const search = new Search().where(K("status").eq("active")).limit(10);
// Score for each document is simply its index position
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Combining Knn Expressions with default=None
Documents must appear in at least one `Knn`'s results to be candidates, AND must appear in ALL `Knn` results where `default=None`.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Problem: Restrictive filtering with default=None
rank = Knn(query="machine learning", limit=100) * 0.7 + Knn(query="deep learning", limit=100) * 0.3
# Both have default=None
# Only documents in BOTH top-100 lists get scored

# Solution: Set default values for more inclusive results
rank = (
    Knn(query="machine learning", limit=100, default=10.0) * 0.7 + 
    Knn(query="deep learning", limit=100, default=10.0) * 0.3
)
# Now documents in either top-100 list can be scored
# Documents get default score (10.0) for Knn where they don't appear
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Problem: Restrictive filtering with default undefined
const rank1 = Knn({ query: "machine learning", limit: 100 }).multiply(0.7)
  .add(Knn({ query: "deep learning", limit: 100 }).multiply(0.3));
// Both have default undefined
// Only documents in BOTH top-100 lists get scored

// Solution: Set default values for more inclusive results
const rank2 = Knn({ query: "machine learning", limit: 100, default: 10.0 }).multiply(0.7)
  .add(Knn({ query: "deep learning", limit: 100, default: 10.0 }).multiply(0.3));
// Now documents in either top-100 list can be scored
// Documents get default score (10.0) for Knn where they don't appear
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Vector Dimension Mismatch
Query vectors must match the dimension of the indexed embeddings. Mismatched dimensions will result in an error.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# If your embeddings are 384-dimensional
Knn(query=[0.1, 0.2, 0.3])  # ✗ Error - only 3 dimensions
Knn(query=[0.1] * 384)      # ✓ Correct - 384 dimensions
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// If your embeddings are 384-dimensional
Knn({ query: [0.1, 0.2, 0.3] });         // ✗ Error - only 3 dimensions
Knn({ query: Array(384).fill(0.1) });   // ✓ Correct - 384 dimensions
```
{% /Tab %}

{% /TabbedCodeBlock %}

### The return_rank Parameter
Set `return_rank=True` when using Knn with RRF to get rank positions (0, 1, 2...) instead of distances.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# For regular scoring - use distances
Knn(query="machine learning")  # Returns: 0.23, 0.45, 0.67...

# For RRF - use rank positions
Knn(query="machine learning", return_rank=True)  # Returns: 0, 1, 2...
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// For regular scoring - use distances
Knn({ query: "machine learning" });  // Returns: 0.23, 0.45, 0.67...

// For RRF - use rank positions
Knn({ query: "machine learning", returnRank: true });  // Returns: 0, 1, 2...
```
{% /Tab %}

{% /TabbedCodeBlock %}

### The limit Parameter
The `limit` parameter in Knn controls how many candidates are considered, not the final result count. Use `Search.limit()` to control the number of results returned.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Knn.limit - candidates to consider for scoring
rank = Knn(query="artificial intelligence", limit=1000)  # Score top 1000 candidates

# Search.limit - results to return
search = Search().rank(rank).limit(10)  # Return top 10 results
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Knn.limit - candidates to consider for scoring
const rank = Knn({ query: "artificial intelligence", limit: 1000 });  // Score top 1000 candidates

// Search.limit - results to return
const search = new Search().rank(rank).limit(10);  // Return top 10 results
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Complete Example

Here's a practical example combining different ranking features:

{% TabbedCodeBlock %}

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
        # Combine two queries with weights
        (
            Knn(query="latest AI research developments") * 0.7 +
            Knn(query="artificial intelligence breakthroughs") * 0.3
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
import { Search, K, Knn, Val } from 'chromadb';

// Complex ranking with filtering and mathematical functions
const search = new Search()
  .where(
    K("status").eq("published")
      .and(K("category").isIn(["tech", "science"]))
  )
  .rank(
    // Combine two queries with weights
    Knn({ query: "latest AI research developments" }).multiply(0.7)
      .add(Knn({ query: "artificial intelligence breakthroughs" }).multiply(0.3))
      .exp()  // Amplify score differences
      .min(0.0)  // Ensure non-negative
  )
  .limit(20)
  .select(K.DOCUMENT, K.SCORE, "title", "category");

const results = await collection.search(search);

// Process results using rows() for cleaner access
const rows = results.rows()[0];  // Get first (and only) search results
for (const [i, row] of rows.entries()) {
  console.log(`${i+1}. ${row.metadata?.title}`);
  console.log(`   Score: ${row.score?.toFixed(3)}`);
  console.log(`   Category: ${row.metadata?.category}`);
  console.log(`   Preview: ${row.document?.substring(0, 100)}...`);
  console.log();
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

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