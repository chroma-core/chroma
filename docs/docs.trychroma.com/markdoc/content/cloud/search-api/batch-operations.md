---
id: batch-operations
name: Batch Operations
---

# Batch Operations

Execute multiple searches in a single API call for better performance and easier comparison of results.

## Running Multiple Searches

Pass a list of Search objects to execute them in a single request. Each search operates independently and returns its own results.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Execute multiple searches in one call
searches = [
    # Search 1: Recent articles
    (Search()
        .where((K("type") == "article") & (K("year") >= 2024))
        .rank(Knn(query="machine learning applications"))
        .limit(5)
        .select(K.DOCUMENT, K.SCORE, "title")),
    
    # Search 2: Papers by specific authors
    (Search()
        .where(K("author").is_in(["Smith", "Jones"]))
        .rank(Knn(query="neural network research"))
        .limit(10)
        .select(K.DOCUMENT, K.SCORE, "title", "author")),
    
    # Search 3: Featured content (no ranking)
    Search()
        .where(K("status") == "featured")
        .limit(20)
        .select("title", "date")
]

# Execute all searches in one request
results = collection.search(searches)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn } from 'chromadb';

// Execute multiple searches in one call
const searches = [
  // Search 1: Recent articles
  new Search()
    .where(K("type").eq("article").and(K("year").gte(2024)))
    .rank(Knn({ query: "machine learning applications" }))
    .limit(5)
    .select(K.DOCUMENT, K.SCORE, "title"),
  
  // Search 2: Papers by specific authors
  new Search()
    .where(K("author").isIn(["Smith", "Jones"]))
    .rank(Knn({ query: "neural network research" }))
    .limit(10)
    .select(K.DOCUMENT, K.SCORE, "title", "author"),
  
  // Search 3: Featured content (no ranking)
  new Search()
    .where(K("status").eq("featured"))
    .limit(20)
    .select("title", "date")
];

// Execute all searches in one request
const results = await collection.search(searches);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Why Use Batch Operations

- **Single round trip** - All searches execute in one API call
- **Easy comparison** - Compare results from different queries or strategies
- **Parallel execution** - Server processes searches simultaneously

## Understanding Batch Results

Results from batch operations maintain the same order as your searches. Each search's results are accessed by its index.

### Result Structure

Each field in the SearchResult maintains a list where each index corresponds to a search:

- `results.ids[i]` - IDs from search at index i
- `results.documents[i]` - Documents from search at index i (if selected)
- `results.embeddings[i]` - Embeddings from search at index i (if selected)
- `results.metadatas[i]` - Metadata from search at index i (if selected)
- `results.scores[i]` - Scores from search at index i (if ranking was used)

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Batch search returns multiple result sets
results = collection.search([search1, search2, search3])

# Access results by index
ids_1 = results.ids[0]    # IDs from search1
ids_2 = results.ids[1]    # IDs from search2
ids_3 = results.ids[2]    # IDs from search3

# Using rows() for easier processing
all_rows = results.rows()  # Returns list of lists
rows_1 = all_rows[0]      # Rows from search1
rows_2 = all_rows[1]      # Rows from search2
rows_3 = all_rows[2]      # Rows from search3

# Process each search's results
for search_index, rows in enumerate(all_rows):
    print(f"Results from search {search_index + 1}:")
    for row in rows:
        print(f"  - {row['id']}: {row.get('metadata', {}).get('title', 'N/A')}")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Batch search returns multiple result sets
const results = await collection.search([search1, search2, search3]);

// Access results by index
const ids1 = results.ids[0];    // IDs from search1
const ids2 = results.ids[1];    // IDs from search2
const ids3 = results.ids[2];    // IDs from search3

// Using rows() for easier processing
const allRows = results.rows();  // Returns list of lists
const rows1 = allRows[0];       // Rows from search1
const rows2 = allRows[1];       // Rows from search2
const rows3 = allRows[2];       // Rows from search3

// Process each search's results
for (const [searchIndex, rows] of allRows.entries()) {
  console.log(`Results from search ${searchIndex + 1}:`);
  for (const row of rows) {
    console.log(`  - ${row.id}: ${row.metadata?.title ?? 'N/A'}`);
  }
}
```
{% /Tab %}

{% /TabbedCodeBlock %}



## Common Use Cases

### Comparing Different Queries
Test multiple query variations to find the most relevant results.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Compare different query variations
query_variations = [
    "machine learning",
    "machine learning algorithms and applications", 
    "modern machine learning techniques"
]

searches = [
    Search()
        .rank(Knn(query=q))
        .limit(10)
        .select(K.DOCUMENT, K.SCORE, "title")
    for q in query_variations
]

results = collection.search(searches)

# Compare top results from each variation
for i, query_name in enumerate(["Original", "Expanded", "Refined"]):
    print(f"{query_name} Query Top Result:")
    if results.scores[i]:
        print(f"  Score: {results.scores[i][0]:.3f}")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Compare different query variations
const queryVariations = [
  "machine learning",
  "machine learning algorithms and applications",
  "modern machine learning techniques"
];

const searches = queryVariations.map(q =>
  new Search()
    .rank(Knn({ query: q }))
    .limit(10)
    .select(K.DOCUMENT, K.SCORE, "title")
);

const results = await collection.search(searches);

// Compare top results from each variation
["Original", "Expanded", "Refined"].forEach((queryName, i) => {
  console.log(`${queryName} Query Top Result:`);
  if (results.scores[i] && results.scores[i].length > 0) {
    console.log(`  Score: ${results.scores[i][0].toFixed(3)}`);
  }
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

### A/B Testing Ranking Strategies
Compare different ranking approaches on the same query.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Test different ranking strategies
searches = [
    # Strategy A: Pure KNN
    Search()
        .rank(Knn(query="artificial intelligence"))
        .limit(10)
        .select(K.SCORE, "title"),
    
    # Strategy B: Weighted KNN
    Search()
        .rank(Knn(query="artificial intelligence") * 0.8 + 0.2)
        .limit(10)
        .select(K.SCORE, "title"),
    
    # Strategy C: Hybrid with RRF
    Search()
        .rank(Rrf([
            Knn(query="artificial intelligence", return_rank=True),
            Knn(query="artificial intelligence", key="sparse_embedding", return_rank=True)
        ]))
        .limit(10)
        .select(K.SCORE, "title")
]

results = collection.search(searches)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Test different ranking strategies
const searches = [
  // Strategy A: Pure KNN
  new Search()
    .rank(Knn({ query: "artificial intelligence" }))
    .limit(10)
    .select(K.SCORE, "title"),
  
  // Strategy B: Weighted KNN
  new Search()
    .rank(Knn({ query: "artificial intelligence" }).multiply(0.8).add(0.2))
    .limit(10)
    .select(K.SCORE, "title"),
  
  // Strategy C: Hybrid with RRF
  new Search()
    .rank(Rrf({
      ranks: [
        Knn({ query: "artificial intelligence", returnRank: true }),
        Knn({ query: "artificial intelligence", key: "sparse_embedding", returnRank: true })
      ]
    }))
    .limit(10)
    .select(K.SCORE, "title")
];

const results = await collection.search(searches);
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Multiple Filters on Same Data
Apply different filters to explore different subsets of your data.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Different category filters
categories = ["technology", "science", "business"]

searches = [
    Search()
        .where(K("category") == category)
        .rank(Knn(query="artificial intelligence"))
        .limit(5)
        .select("title", "category", K.SCORE)
    for category in categories
]

results = collection.search(searches)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Different category filters
const categories = ["technology", "science", "business"];

const searches = categories.map(category =>
  new Search()
    .where(K("category").eq(category))
    .rank(Knn({ query: "artificial intelligence" }))
    .limit(5)
    .select("title", "category", K.SCORE)
);

const results = await collection.search(searches);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Performance Benefits

Batch operations are significantly faster than running searches sequentially:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# ❌ Sequential execution (slow)
results = []
for search in searches:
    result = collection.search(search)  # Separate API call each time
    results.append(result)

# ✅ Batch execution (fast)
results = collection.search(searches)  # Single API call for all
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// ❌ Sequential execution (slow)
const results = [];
for (const search of searches) {
  const result = await collection.search(search);  // Separate API call each time
  results.push(result);
}

// ✅ Batch execution (fast)
const results2 = await collection.search(searches);  // Single API call for all
```
{% /Tab %}

{% /TabbedCodeBlock %}

Batch operations reduce network overhead and enable server-side parallelization, often providing 3-10x speedup depending on the number and complexity of searches.

## Edge Cases

### Empty Searches Array
Passing an empty list returns an empty result.

### Batch Size Limits
For Chroma Cloud users, batch operations may be subject to quota limits on the total number of searches per request.

### Mixed Field Selection
Different searches can select different fields - each search's results will contain only its requested fields.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
searches = [
    Search().limit(5).select(K.DOCUMENT),       # Only documents
    Search().limit(5).select(K.SCORE, "title"), # Scores and title
    Search().limit(5).select_all()              # Everything
]

results = collection.search(searches)
# results.documents[0] will have values
# results.documents[1] will be None (not selected)
# results.documents[2] will have values
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
const searches = [
  new Search().limit(5).select(K.DOCUMENT),       // Only documents
  new Search().limit(5).select(K.SCORE, "title"), // Scores and title
  new Search().limit(5).selectAll()               // Everything
];

const results = await collection.search(searches);
// results.documents[0] will have values
// results.documents[1] will be null (not selected)
// results.documents[2] will have values
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Complete Example

Here's a practical example using batch operations to find and compare relevant documents across different categories:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

def compare_category_relevance(collection, query_text, categories):
    """Find top results in each category for the same query"""
    
    # Build searches for each category
    searches = [
        Search()
            .where(K("category") == cat)
            .rank(Knn(query=query_text))
            .limit(3)
            .select(K.DOCUMENT, K.SCORE, "title", "category")
        for cat in categories
    ]
    
    # Execute batch search
    results = collection.search(searches)
    all_rows = results.rows()
    
    # Process and display results
    for cat_index, category in enumerate(categories):
        print(f"\nTop results in {category}:")
        rows = all_rows[cat_index]
        
        if not rows:
            print("  No results found")
            continue
            
        for i, row in enumerate(rows, 1):
            title = row.get('metadata', {}).get('title', 'Untitled')
            score = row.get('score', 0)
            preview = row.get('document', '')[:100]
            
            print(f"  {i}. {title}")
            print(f"     Score: {score:.3f}")
            print(f"     Preview: {preview}...")

# Usage
categories = ["technology", "science", "business", "health"]
query_text = "artificial intelligence applications"

compare_category_relevance(collection, query_text, categories)
```

{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, type Collection } from 'chromadb';

async function compareCategoryRelevance(
  collection: Collection,
  queryText: string,
  categories: string[]
) {
  // Find top results in each category for the same query
  
  // Build searches for each category
  const searches = categories.map(cat =>
    new Search()
      .where(K("category").eq(cat))
      .rank(Knn({ query: queryText }))
      .limit(3)
      .select(K.DOCUMENT, K.SCORE, "title", "category")
  );
  
  // Execute batch search
  const results = await collection.search(searches);
  const allRows = results.rows();
  
  // Process and display results
  for (const [catIndex, category] of categories.entries()) {
    console.log(`\nTop results in ${category}:`);
    const rows = allRows[catIndex];
    
    if (!rows || rows.length === 0) {
      console.log("  No results found");
      continue;
    }
        
    for (const [i, row] of rows.entries()) {
      const title = row.metadata?.title ?? 'Untitled';
      const score = row.score ?? 0;
      const preview = row.document?.substring(0, 100) ?? '';
      
      console.log(`  ${i+1}. ${title}`);
      console.log(`     Score: ${score.toFixed(3)}`);
      console.log(`     Preview: ${preview}...`);
    }
  }
}

// Usage
const categories = ["technology", "science", "business", "health"];
const queryText = "artificial intelligence applications";

await compareCategoryRelevance(collection, queryText, categories);
```
{% /Tab %}

{% /TabbedCodeBlock %}

Example output:
```
Top results in technology:
  1. AI in Software Development
     Score: 0.234
     Preview: The integration of artificial intelligence in modern software development has revolutionized...
  2. Machine Learning Frameworks
     Score: 0.312
     Preview: Popular frameworks for building AI applications include TensorFlow, PyTorch, and...

Top results in science:
  1. Neural Networks Research
     Score: 0.289
     Preview: Recent advances in neural network architectures have enabled breakthrough applications...
```

## Tips and Best Practices

- **Keep batch sizes reasonable** - Very large batches may hit quota limits
- **Use consistent field selection** when possible for easier result processing
- **Index alignment** - Results maintain the same order as input searches
- **Consider memory usage** - Large batches with `select_all()` can consume significant memory
- **Use `rows()` method** for easier result processing in batch operations

## Next Steps

- See [practical examples](./examples) of batch operations in production
- Learn about [performance optimization](./search-basics) for complex queries
- Explore [migration guide](./migration) for transitioning from legacy methods
