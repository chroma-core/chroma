---
id: pagination-selection
name: Pagination & Selection
---

# Pagination & Field Selection

Control how many results to return and which fields to include in your search results.

## Pagination with Limit

Use `limit()` to control how many results to return and `offset` to skip results for pagination.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search

# Limit results
search = Search().limit(10)  # Return top 10 results

# Pagination with offset
search = Search().limit(10, offset=20)  # Skip first 20, return next 10

# No limit - returns all matching results
search = Search()  # Be careful with large collections!
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search } from 'chromadb';

// Limit results
const search1 = new Search().limit(10);  // Return top 10 results

// Pagination with offset
const search2 = new Search().limit(10, 20);  // Skip first 20, return next 10

// No limit - returns all matching results
const search3 = new Search();  // Be careful with large collections!
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Limit Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int or None | `None` | Maximum results to return (`None` = no limit) |
| `offset` | int | `0` | Number of results to skip (for pagination) |

{% Note type="info" %}
For Chroma Cloud users: The actual number of results returned will be capped by your quota limits, regardless of the `limit` value specified. This applies even when no limit is set.
{% /Note %}

## Pagination Patterns

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Page through results (0-indexed)
page_size = 10

# Page 0: Results 1-10
page_0 = Search().limit(page_size, offset=0)

# Page 1: Results 11-20  
page_1 = Search().limit(page_size, offset=10)

# Page 2: Results 21-30
page_2 = Search().limit(page_size, offset=20)

# General formula
def get_page(page_number, page_size=10):
    return Search().limit(page_size, offset=page_number * page_size)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Page through results (0-indexed)
const pageSize = 10;

// Page 0: Results 1-10
const page0 = new Search().limit(pageSize, 0);

// Page 1: Results 11-20  
const page1 = new Search().limit(pageSize, 10);

// Page 2: Results 21-30
const page2 = new Search().limit(pageSize, 20);

// General formula
function getPage(pageNumber: number, pageSize = 10) {
  return new Search().limit(pageSize, pageNumber * pageSize);
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
Pagination uses 0-based indexing. The first page is page 0, not page 1.
{% /Note %}

## Field Selection with Select

Control which fields are returned in your results to optimize data transfer and processing.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K

# Default - returns IDs only
search = Search()

# Select specific fields
search = Search().select(K.DOCUMENT, K.SCORE)

# Select metadata fields
search = Search().select("title", "author", "date")

# Mix predefined and metadata fields
search = Search().select(K.DOCUMENT, K.SCORE, "title", "author")

# Select all available fields
search = Search().select_all()
# Returns: IDs, documents, embeddings, metadata, scores
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K } from 'chromadb';

// Default - returns IDs only
const search1 = new Search();

// Select specific fields
const search2 = new Search().select(K.DOCUMENT, K.SCORE);

// Select metadata fields
const search3 = new Search().select("title", "author", "date");

// Mix predefined and metadata fields
const search4 = new Search().select(K.DOCUMENT, K.SCORE, "title", "author");

// Select all available fields
const search5 = new Search().selectAll();
// Returns: IDs, documents, embeddings, metadata, scores
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Selectable Fields

| Field | Internal Key | Usage | Description |
|-------|--------------|-------|-------------|
| IDs | `#id` | Always included | Document IDs are always returned |
| `K.DOCUMENT` | `#document` | `.select(K.DOCUMENT)` | Full document text |
| `K.EMBEDDING` | `#embedding` | `.select(K.EMBEDDING)` | Vector embeddings |
| `K.METADATA` | `#metadata` | `.select(K.METADATA)` | All metadata fields as a dict |
| `K.SCORE` | `#score` | `.select(K.SCORE)` | Search scores (when ranking is used) |
| `"field_name"` | (user-defined) | `.select("title", "author")` | Specific metadata fields |

{% Note type="info" %}
**Field constants:** `K.*` constants (e.g., `K.DOCUMENT`, `K.EMBEDDING`, `K.ID`) correspond to internal keys with `#` prefix (e.g., `#document`, `#embedding`, `#id`). Use the `K.*` constants in queries. Internal keys like `#document` and `#embedding` are used in schema configuration, while `#metadata` and `#score` are query-only fields not used in schema.

When selecting specific metadata fields (e.g., "title"), they appear directly in the metadata dict. Using `K.METADATA` returns ALL metadata fields at once.
{% /Note %}

## Performance Considerations

Selecting fewer fields improves performance by reducing data transfer:

- **Minimal**: IDs only (default) - fastest queries
- **Moderate**: Add scores and specific metadata fields
- **Heavy**: Including documents and embeddings - larger payloads
- **Maximum**: `select_all()` - returns everything

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Fast - minimal data
search = Search().limit(100)  # IDs only

# Moderate - just what you need
search = Search().limit(100).select(K.SCORE, "title", "date")

# Slower - large fields
search = Search().limit(100).select(K.DOCUMENT, K.EMBEDDING)

# Slowest - everything
search = Search().limit(100).select_all()
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Fast - minimal data
const search1 = new Search().limit(100);  // IDs only

// Moderate - just what you need
const search2 = new Search().limit(100).select(K.SCORE, "title", "date");

// Slower - large fields
const search3 = new Search().limit(100).select(K.DOCUMENT, K.EMBEDDING);

// Slowest - everything
const search4 = new Search().limit(100).selectAll();
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Edge Cases

### No Limit Specified
Without a limit, the search attempts to return all matching results, but will be capped by quota limits in Chroma Cloud.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Attempts to return ALL matching documents
search = Search().where(K("status") == "active")  # No limit()
# Chroma Cloud: Results capped by quota
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Attempts to return ALL matching documents
const search = new Search().where(K("status").eq("active"));  // No limit()
// Chroma Cloud: Results capped by quota
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Empty Results
When no documents match, results will have empty lists/arrays.

### Non-existent Fields
Selecting non-existent metadata fields simply omits them from the results - they won't appear in the metadata dict.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# If "non_existent_field" doesn't exist
search = Search().select("title", "non_existent_field")

# Result metadata will only contain "title" if it exists
# "non_existent_field" will not appear in the metadata dict at all
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// If "non_existent_field" doesn't exist
const search = new Search().select("title", "non_existent_field");

// Result metadata will only contain "title" if it exists
// "non_existent_field" will not appear in the metadata object at all
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Complete Example

Here's a practical example combining pagination with field selection:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Paginated search with field selection
def search_with_pagination(collection, query_text, page_size=20):
    current_page = 0
    
    while True:
        search = (Search()
            .where(K("status") == "published")
            .rank(Knn(query=query_text))
            .limit(page_size, offset=current_page * page_size)
            .select(K.DOCUMENT, K.SCORE, "title", "author", "date")
        )
        
        results = collection.search(search)
        rows = results.rows()[0]  # Get first (and only) search results
        
        if not rows:  # No more results
            break
            
        print(f"\n--- Page {current_page + 1} ---")
        for i, row in enumerate(rows, 1):
            print(f"{i}. {row['metadata']['title']} by {row['metadata']['author']}")
            print(f"   Score: {row['score']:.3f}, Date: {row['metadata']['date']}")
            print(f"   Preview: {row['document'][:100]}...")
        
        # Check if we want to continue
        user_input = input("\nPress Enter for next page, or 'q' to quit: ")
        if user_input.lower() == 'q':
            break
            
        current_page += 1
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, type Collection } from 'chromadb';
import * as readline from 'readline';

// Paginated search with field selection
async function searchWithPagination(
  collection: Collection, 
  queryText: string, 
  pageSize = 20
) {
  let currentPage = 0;
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  while (true) {
    const search = new Search()
      .where(K("status").eq("published"))
      .rank(Knn({ query: queryText }))
      .limit(pageSize, currentPage * pageSize)
      .select(K.DOCUMENT, K.SCORE, "title", "author", "date");
    
    const results = await collection.search(search);
    const rows = results.rows()[0];  // Get first (and only) search results
    
    if (!rows || rows.length === 0) {  // No more results
      break;
    }
        
    console.log(`\n--- Page ${currentPage + 1} ---`);
    for (const [i, row] of rows.entries()) {
      console.log(`${i+1}. ${row.metadata?.title} by ${row.metadata?.author}`);
      console.log(`   Score: ${row.score?.toFixed(3)}, Date: ${row.metadata?.date}`);
      console.log(`   Preview: ${row.document?.substring(0, 100)}...`);
    }
    
    // Check if we want to continue
    const userInput = await new Promise<string>(resolve => {
      rl.question("\nPress Enter for next page, or 'q' to quit: ", resolve);
    });
    
    if (userInput.toLowerCase() === 'q') {
      break;
    }
        
    currentPage += 1;
  }
  
  rl.close();
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Tips and Best Practices

- **Select only what you need** - Reduces network transfer and memory usage
- **Use appropriate page sizes** - 10-50 for UI, 100-500 for batch processing
- **Consider bandwidth** - Avoid selecting embeddings unless necessary
- **IDs are always included** - No need to explicitly select them
- **Use `select_all()` sparingly** - Only when you truly need all fields

## Next Steps

- Learn about [batch operations](./batch-operations) for running multiple searches
- See [practical examples](./examples) of pagination in production
- Explore [search basics](./search-basics) for building complete queries
