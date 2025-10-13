---
id: search-basics
name: Search Basics
---

# Search Basics

Learn how to construct and use the Search class for querying your Chroma collections.

This page covers the basics of Search construction. For detailed usage of specific components, see:
- [Filtering with Where](./filtering) - Complex filter expressions with `K()` and `.where()`
- [Ranking and Scoring](./ranking) - Using `Knn` and `.rank()` for vector search
- [Pagination and Selection](./pagination-selection) - Field selection with `.select()` and pagination with `.limit()`

## The Search Class

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search

# Create an empty search
search = Search()

# Direct construction with parameters
search = Search(
    where={"status": "active"},
    rank={"$knn": {"query": [0.1, 0.2]}},
    limit=10,
    select=["#document", "#score"]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search } from 'chromadb';

// Create an empty search
const search = new Search();

// Direct construction with parameters
const search2 = new Search({
  where: { status: "active" },
  rank: { $knn: { query: [0.1, 0.2] } },
  limit: 10,
  select: ["#document", "#score"]
});
```
{% /Tab %}

{% /Tabs %}

## Constructor Parameters

The Search class accepts four optional parameters:

- **where**: Filter expressions to narrow down results
  - Types: `Where` expression, `dict`, or `None`
  - Default: `None` (no filtering)
  
- **rank**: Ranking expressions to score and order results  
  - Types: `Rank` expression, `dict`, or `None`
  - Default: `None` (no ranking, natural order)
  
- **limit**: Pagination control
  - Types: `Limit` object, `dict`, `int`, or `None`
  - Default: `None` (no limit)
  
- **select**: Fields to include in results
  - Types: `Select` object, `dict`, `list`, `set`, or `None`
  - Default: `None` (returns IDs only)
  - Available fields: `#id`, `#document`, `#embedding`, `#metadata`, `#score`, or any custom metadata field
  - See [field selection](./pagination-selection#field-selection) for details

## Builder Pattern

The Search class provides a fluent interface with method chaining. Each method returns a new Search instance, making queries immutable and safe to reuse.

For detailed usage of each builder method, see the respective sections:
- `.where()` - See [Filter expressions](./filtering)
- `.rank()` - See [Ranking and scoring](./ranking)  
- `.limit()` - See [Pagination](./pagination-selection#pagination)
- `.select()` and `.select_all()` - See [Field selection](./pagination-selection#field-selection)

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Basic method chaining
search = (Search()
    .where(K("status") == "published")
    .rank(Knn(query=[0.1, 0.2, 0.3]))
    .limit(10)
    .select(K.DOCUMENT, K.SCORE))

# Each method returns a new instance
base_search = Search().where(K("category") == "science")
search_v1 = base_search.limit(5)  # New instance
search_v2 = base_search.limit(10) # Different instance

# Progressive building
search = Search()
search = search.where(K("status") == "active")
search = search.rank(Knn(query=embedding))
search = search.limit(20)
search = search.select(K.DOCUMENT, K.METADATA)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn } from 'chromadb';

// Basic method chaining
const search = new Search()
  .where(K("status").eq("published"))
  .rank(Knn({ query: [0.1, 0.2, 0.3] }))
  .limit(10)
  .select(K.DOCUMENT, K.SCORE);

// Each method returns a new instance
const baseSearch = new Search().where(K("category").eq("science"));
const searchV1 = baseSearch.limit(5);  // New instance
const searchV2 = baseSearch.limit(10); // Different instance

// Progressive building
let search2 = new Search();
search2 = search2.where(K("status").eq("active"));
search2 = search2.rank(Knn({ query: embedding }));
search2 = search2.limit(20);
search2 = search2.select(K.DOCUMENT, K.METADATA);
```
{% /Tab %}

{% /Tabs %}

**Benefits of immutability:**
- Base queries can be reused safely
- No unexpected side effects from modifications
- Easy to create query variations

## Direct Construction

You can create Search objects directly with various parameter types:

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn
from chromadb.execution.expression.operator import Limit, Select

# With expression objects
search = Search(
    where=K("status") == "active",
    rank=Knn(query=[0.1, 0.2, 0.3]),
    limit=Limit(limit=10, offset=0),
    select=Select(keys={K.DOCUMENT, K.SCORE})
)

# With dictionaries (MongoDB-style)
search = Search(
    where={"status": "active"},
    rank={"$knn": {"query": [0.1, 0.2, 0.3]}},
    limit={"limit": 10, "offset": 0},
    select={"keys": ["#document", "#score"]}
)

# Mixed types
search = Search(
    where=K("category") == "science",           # Expression
    rank={"$knn": {"query": embedding}},        # Dictionary
    limit=10,                                   # Integer
    select=[K.DOCUMENT, K.SCORE, "author"]      # List
)

# Minimal search (IDs only)
search = Search()

# Just filtering
search = Search(where=K("status") == "published")

# Just ranking
search = Search(rank=Knn(query=embedding))
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, Limit, Select } from 'chromadb';

// With expression objects
const search1 = new Search({
  where: K("status").eq("active"),
  rank: Knn({ query: [0.1, 0.2, 0.3] }),
  limit: new Limit({ limit: 10, offset: 0 }),
  select: new Select([K.DOCUMENT, K.SCORE])
});

// With dictionaries (MongoDB-style)
const search2 = new Search({
  where: { status: "active" },
  rank: { $knn: { query: [0.1, 0.2, 0.3] } },
  limit: { limit: 10, offset: 0 },
  select: { keys: ["#document", "#score"] }
});

// Mixed types
const search3 = new Search({
  where: K("category").eq("science"),      // Expression
  rank: { $knn: { query: embedding } },    // Dictionary
  limit: 10,                               // Number
  select: [K.DOCUMENT, K.SCORE, "author"]  // Array
});

// Minimal search (IDs only)
const search4 = new Search();

// Just filtering
const search5 = new Search({ where: K("status").eq("published") });

// Just ranking
const search6 = new Search({ rank: Knn({ query: embedding }) });
```
{% /Tab %}

{% /Tabs %}

## Dictionary Format Specification

When using dictionaries to construct Search objects, follow this format. For complete operator schemas:
- [Where dictionary operators](./filtering#dictionary-format) - `$eq`, `$gt`, `$in`, etc.
- [Rank dictionary operators](./ranking#dictionary-format) - `$knn` and ranking expressions

{% Tabs %}

{% Tab label="python" %}
```python
# Where dictionary (MongoDB-style operators)
# Note: Each dict can only have one field or one logical operator

# Simple equality
where_dict = {"status": "active"}

# Comparison operator
where_dict = {"score": {"$gt": 0.5}}

# Logical AND combination
where_dict = {
    "$and": [
        {"status": "active"},
        {"category": "science"},
        {"year": {"$gte": 2020}}
    ]
}

# Logical OR combination  
where_dict = {
    "$or": [
        {"category": "science"},
        {"category": "technology"}
    ]
}

# Rank dictionary
rank_dict = {
    "$knn": {
        "query": [0.1, 0.2, 0.3],         # Query vector
        "key": "#embedding",              # Optional: field to search
        "limit": 128                      # Optional: max candidates
    }
}

# Limit dictionary
limit_dict = {
    "limit": 10,                          # Number of results
    "offset": 20                          # Skip first N results
}

# Select dictionary
# Keys can be predefined fields (with # prefix) or custom metadata fields
select_dict = {
    "keys": [
        "#id",          # Document ID (always returned)
        "#document",    # Document content
        "#embedding",   # Embedding vectors
        "#metadata",    # All metadata (includes all custom fields)
        "#score",       # Search score (when ranking is used)
    ]
}

# Or select specific metadata fields only (without #metadata)
select_dict = {
    "keys": [
        "#document",
        "#score",
        "title",        # Specific metadata field
        "author"        # Specific metadata field
    ]
}
# Note: Using #metadata returns ALL metadata fields, so no need to list individual fields
# For more details on field selection, see: ./pagination-selection#field-selection

# Complete search with dictionaries
search = Search(
    where=where_dict,
    rank=rank_dict,
    limit=limit_dict,
    select=select_dict
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Where dictionary (MongoDB-style operators)
// Note: Each dict can only have one field or one logical operator

// Simple equality
let whereDict = { status: "active" };

// Comparison operator
whereDict = { score: { $gt: 0.5 } };

// Logical AND combination
whereDict = {
  $and: [
    { status: "active" },
    { category: "science" },
    { year: { $gte: 2020 } }
  ]
};

// Logical OR combination  
whereDict = {
  $or: [
    { category: "science" },
    { category: "technology" }
  ]
};

// Rank dictionary
const rankDict = {
  $knn: {
    query: [0.1, 0.2, 0.3],         // Query vector
    key: "#embedding",              // Optional: field to search
    limit: 128                      // Optional: max candidates
  }
};

// Limit dictionary
const limitDict = {
  limit: 10,                        // Number of results
  offset: 20                        // Skip first N results
};

// Select dictionary
// Keys can be predefined fields (with # prefix) or custom metadata fields
let selectDict = {
  keys: [
    "#id",          // Document ID (always returned)
    "#document",    // Document content
    "#embedding",   // Embedding vectors
    "#metadata",    // All metadata (includes all custom fields)
    "#score",       // Search score (when ranking is used)
  ]
};

// Or select specific metadata fields only (without #metadata)
selectDict = {
  keys: [
    "#document",
    "#score",
    "title",        // Specific metadata field
    "author"        // Specific metadata field
  ]
};
// Note: Using #metadata returns ALL metadata fields, so no need to list individual fields
// For more details on field selection, see: ./pagination-selection#field-selection

// Complete search with dictionaries
const search = new Search({
  where: whereDict,
  rank: rankDict,
  limit: limitDict,
  select: selectDict
});
```
{% /Tab %}

{% /Tabs %}

## Empty Search Behavior

An empty Search object has specific default behaviors:

{% Tabs %}

{% Tab label="python" %}
```python
# Empty search
search = Search()

# Equivalent to:
# - where: None (returns all documents)
# - rank: None (natural storage order)
# - limit: None (no limit on results)
# - select: None (returns IDs only)

result = collection.search(search)
# Result contains only IDs, no documents/embeddings/metadata/scores

# Add selection to get more fields
search = Search().select(K.DOCUMENT, K.METADATA)
result = collection.search(search)
# Now includes documents and metadata
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Empty search
const search = new Search();

// Equivalent to:
// - where: undefined (returns all documents)
// - rank: undefined (natural storage order)
// - limit: undefined (no limit on results)
// - select: empty (returns IDs only)

const result = await collection.search(search);
// Result contains only IDs, no documents/embeddings/metadata/scores

// Add selection to get more fields
const search2 = new Search().select(K.DOCUMENT, K.METADATA);
const result2 = await collection.search(search2);
// Now includes documents and metadata
```
{% /Tab %}

{% /Tabs %}

{% Note type="info" %}
Empty searches are useful for retrieving all document IDs or when you only need to apply field selection.
{% /Note %}

## Common Initialization Patterns

Here are common patterns for building Search queries:

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Pattern 1: Filter-first approach (narrow down, then rank)
def search_recent_science(query_vector):
    return (Search()
        .where((K("category") == "science") & (K("year") >= 2023))
        .rank(Knn(query=query_vector))
        .limit(10)
        .select(K.DOCUMENT, K.SCORE))

# Pattern 2: Rank-first approach (score all, filter high-quality)
def search_high_quality(query_vector, min_quality=0.8):
    return (Search()
        .rank(Knn(query=query_vector))
        .where(K("quality_score") >= min_quality)
        .limit(5)
        .select_all())

# Pattern 3: Conditional building
def build_search(query_vector=None, category=None, limit=10):
    search = Search()
    
    # Add filtering if category specified
    if category:
        search = search.where(K("category") == category)
    
    # Add ranking if query vector provided
    if query_vector is not None:
        search = search.rank(Knn(query=query_vector))
        # TODO: When collection schema is ready:
        # search = search.rank(Knn(query="text query"))
    
    # Always limit results
    search = search.limit(limit)
    
    # Select common fields
    search = search.select(K.DOCUMENT, K.METADATA)
    
    return search

# Pattern 4: Base query with variations
base_search = Search().where(K("status") == "published")

# Create variations
recent_search = base_search.where(K("year") == "2025").limit(20)
popular_search = base_search.where(K("views") > 1000).limit(10)
featured_search = base_search.where(K("featured") == True).limit(5)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn } from 'chromadb';

// Pattern 1: Filter-first approach (narrow down, then rank)
function searchRecentScience(queryVector: number[]) {
  return new Search()
    .where(K("category").eq("science").and(K("year").gte(2023)))
    .rank(Knn({ query: queryVector }))
    .limit(10)
    .select(K.DOCUMENT, K.SCORE);
}

// Pattern 2: Rank-first approach (score all, filter high-quality)
function searchHighQuality(queryVector: number[], minQuality = 0.8) {
  return new Search()
    .rank(Knn({ query: queryVector }))
    .where(K("quality_score").gte(minQuality))
    .limit(5)
    .selectAll();
}

// Pattern 3: Conditional building
function buildSearch(queryVector?: number[], category?: string, limit = 10) {
  let search = new Search();
  
  // Add filtering if category specified
  if (category) {
    search = search.where(K("category").eq(category));
  }
  
  // Add ranking if query vector provided
  if (queryVector !== undefined) {
    search = search.rank(Knn({ query: queryVector }));
    // TODO: When collection schema is ready:
    // search = search.rank(Knn({ query: "text query" }))
  }
  
  // Always limit results
  search = search.limit(limit);
  
  // Select common fields
  search = search.select(K.DOCUMENT, K.METADATA);
  
  return search;
}

// Pattern 4: Base query with variations
const baseSearch = new Search().where(K("status").eq("published"));

// Create variations
const recentSearch = baseSearch.where(K("year").eq("2025")).limit(20);
const popularSearch = baseSearch.where(K("views").gt(1000)).limit(10);
const featuredSearch = baseSearch.where(K("featured").eq(true)).limit(5);
```
{% /Tab %}

{% /Tabs %}



## Next Steps

- Learn about [filtering with Where expressions](./filtering)
- Explore [ranking and scoring](./ranking) options
- Understand [pagination and field selection](./pagination-selection)
