---
id: pagination-selection
name: Pagination & Selection
---

# Pagination & Field Selection

Learn how to control pagination and select which fields to return in search results.

## Pagination with Limit

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search

# Simple limit
search = Search().limit(10)

# Limit with offset for pagination
search = Search().limit(10, offset=20)  # Skip first 20 results

# Using Limit object directly
from chromadb.execution.expression.operator import Limit
search = Search(limit=Limit(limit=10, offset=20))
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Field Selection with Select

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import Search, K

# Select specific fields
search = Search().select(K.DOCUMENT, K.SCORE, "custom_field")

# Select all predefined fields
search = Search().select_all()  # Returns document, embedding, metadata, score
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Limit Class Detailed Reference

[TODO: Complete Limit documentation]
```python
Limit(
    limit=None,  # Max results (None = no limit)
    offset=0    # Skip first N results
)
```

### Offset Calculation for Pagination

[TODO: Pagination patterns]
```python
# Page 1: First 10 results
Search().limit(10, offset=0)

# Page 2: Results 11-20
Search().limit(10, offset=10)

# Page 3: Results 21-30
Search().limit(10, offset=20)

# General formula
page_size = 10
page_number = 3  # 0-indexed
Search().limit(page_size, offset=page_number * page_size)
```

### Page Size Optimization

[TODO: Guidelines for choosing page size]
- Small (10-20): Interactive UIs
- Medium (50-100): Batch processing
- Large (500+): Export operations
- Trade-offs: Memory vs round trips

## Select Class Detailed Reference

[TODO: Complete Select documentation]
```python
Select(keys=set())  # Set of fields to return
```

### Predefined Field Constants

[TODO: All constants with descriptions]
| Constant | String Value | Returns |
|----------|-------------|---------|
| K.ID | "#id" | Document IDs (always included) |
| K.DOCUMENT | "#document" | Full document text |
| K.EMBEDDING | "#embedding" | Vector embeddings |
| K.METADATA | "#metadata" | All metadata fields |
| K.SCORE | "#score" | Search scores |

### Selecting Specific Fields

[TODO: Examples of field selection]
```python
# Select only documents and scores
Search().select(K.DOCUMENT, K.SCORE)

# Select specific metadata fields
Search().select("title", "author", "date")

# Mix predefined and custom fields
Search().select(K.DOCUMENT, K.SCORE, "custom_field")

# Select all predefined fields
Search().select_all()
# Equivalent to:
Search().select(K.DOCUMENT, K.EMBEDDING, K.METADATA, K.SCORE)
```

## Custom Metadata Field Selection

[TODO: How to select metadata]
```python
# Select specific metadata fields only
search = Search().select("field1", "field2", "field3")

# Combine with predefined fields
search = Search().select(K.DOCUMENT, "custom1", "custom2")

# Dynamic field selection
fields_to_select = ["title", "author"]
if include_dates:
    fields_to_select.append("published_date")
search = Search().select(*fields_to_select)
```

## Performance Impact of Field Selection

[TODO: Performance considerations]
```python
# Minimal data transfer (fastest)
Search().select()  # Only IDs

# Moderate data transfer
Search().select(K.SCORE, "title")

# Heavy data transfer (slowest)
Search().select_all()  # Everything
```

[TODO: Add performance table]
| Selection | Data Size | Network Transfer | Use Case |
|-----------|-----------|------------------|----------|
| IDs only | Minimal | ~1KB/1000 | Existence check |
| + Scores | Small | ~10KB/1000 | Ranking only |
| + Documents | Large | ~1MB/1000 | Full results |
| + Embeddings | Very Large | ~10MB/1000 | Re-processing |

## Memory Considerations

[TODO: Memory usage patterns]
```python
# Memory-efficient pagination
def paginate_results(search_base, total_limit=10000):
    page_size = 100
    offset = 0
    all_results = []
    
    while offset < total_limit:
        page = search_base.limit(page_size, offset=offset)
        results = collection.search(page)
        
        if not results.ids[0]:  # No more results
            break
            
        all_results.extend(results.ids[0])
        offset += page_size
    
    return all_results
```

## Result Size Estimation

[TODO: How to estimate result size]
```python
# Estimate before fetching
def estimate_result_size(num_results, fields):
    size_per_result = {
        K.ID: 50,  # bytes
        K.DOCUMENT: 1000,  # Average doc size
        K.EMBEDDING: 4 * 768,  # 768-dim float32
        K.METADATA: 200,  # Average metadata
        K.SCORE: 8,  # float64
    }
    
    total = sum(size_per_result.get(f, 100) for f in fields)
    return total * num_results
```

## Cursor-Based Pagination Patterns

[TODO: If/when cursor support is added]
```python
# Future cursor-based pagination
# search = Search().limit(10).after(cursor="...")
```

## select_all() vs Selective Retrieval

[TODO: When to use each approach]
```python
# Use select_all() when:
# - Need complete data for analysis
# - Exporting/backing up data
# - Don't know fields in advance

# Use selective retrieval when:
# - Displaying in UI (only needed fields)
# - Network bandwidth is limited
# - Processing large result sets
```

## Common Patterns

[TODO: Real-world patterns]
```python
# Pattern 1: Progressive loading
# First load IDs and scores, then load documents on demand

# Pattern 2: Field projection for UI
# Select only fields shown in the interface

# Pattern 3: Batch export
# Large limit with specific fields for data export
```