---
id: pagination-selection
name: Pagination & Selection
---

# Pagination & Field Selection

Control how many results to return and which fields to include in your search results.

## Pagination with Limit

Use `limit()` to control how many results to return and `offset` to skip results for pagination.

{% Tabs %}

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
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Limit Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int or None | `None` | Maximum results to return (`None` = no limit) |
| `offset` | int | `0` | Number of results to skip (for pagination) |

{% Note type="info" %}
For Chroma Cloud users: The actual number of results returned will be capped by your quota limits, regardless of the `limit` value specified. This applies even when no limit is set.
{% /Note %}

## Pagination Patterns

{% Tabs %}

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
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

{% Note type="info" %}
Pagination uses 0-based indexing. The first page is page 0, not page 1.
{% /Note %}

## Field Selection with Select

Control which fields are returned in your results to optimize data transfer and processing.

{% Tabs %}

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
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Selectable Fields

| Field | Usage | Description |
|-------|-------|-------------|
| IDs | Always included | Document IDs are always returned |
| `K.DOCUMENT` | `.select(K.DOCUMENT)` | Full document text |
| `K.EMBEDDING` | `.select(K.EMBEDDING)` | Vector embeddings |
| `K.METADATA` | `.select(K.METADATA)` | All metadata fields as a dict |
| `K.SCORE` | `.select(K.SCORE)` | Search scores (when ranking is used) |
| `"field_name"` | `.select("title", "author")` | Specific metadata fields |

{% Note type="info" %}
When selecting specific metadata fields (e.g., "title"), they appear directly in the metadata dict. Using `K.METADATA` returns ALL metadata fields at once.
{% /Note %}

## Performance Considerations

Selecting fewer fields improves performance by reducing data transfer:

- **Minimal**: IDs only (default) - fastest queries
- **Moderate**: Add scores and specific metadata fields
- **Heavy**: Including documents and embeddings - larger payloads
- **Maximum**: `select_all()` - returns everything

{% Tabs %}

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
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Edge Cases

### No Limit Specified
Without a limit, the search attempts to return all matching results, but will be capped by quota limits in Chroma Cloud.

{% Tabs %}

{% Tab label="python" %}
```python
# Attempts to return ALL matching documents
search = Search().where(K("status") == "active")  # No limit()
# Chroma Cloud: Results capped by quota
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

### Empty Results
When no documents match, results will have empty lists/arrays.

### Non-existent Fields
Selecting non-existent metadata fields simply omits them from the results - they won't appear in the metadata dict.

{% Tabs %}

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
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Complete Example

Here's a practical example combining pagination with field selection:

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Paginated search with field selection
def search_with_pagination(collection, query_vector, page_size=20):
    current_page = 0
    
    while True:
        search = (Search()
            .where(K("status") == "published")
            .rank(Knn(query=query_vector))
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
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

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