---
id: search-basics
name: Search Basics
---

# Search Basics

Learn how to construct and use the Search class for querying your Chroma collections.

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
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Constructor Parameters

[TODO: Add detailed parameter documentation]
- **where**: Filter expressions (Where | Dict | None)
- **rank**: Ranking expressions (Rank | Dict | None)  
- **limit**: Pagination (Limit | Dict | int | None)
- **select**: Field selection (Select | Dict | List | Set | None)

## Builder Pattern

[TODO: Add complete builder pattern examples]
```python
# Method chaining example
search = (Search()
    .where(...)
    .rank(...)
    .limit(...)
    .select(...))
```

[TODO: Show how each method returns new Search instance]
[TODO: Explain immutability benefits]

## Direct Construction

[TODO: Show all parameter type variations]
```python
# With expression objects
Search(where=Where(...), rank=Rank(...), limit=Limit(...), select=Select(...))

# With dictionaries  
Search(where={...}, rank={...}, limit={...}, select={...})

# Mixed types
Search(where=K("field") == "value", rank={"$knn": {...}}, limit=10, select=["#document"])
```

## Dictionary Format Specification

[TODO: Complete dictionary format spec]
```python
{
    "filter": {...},  # Where expression dict
    "rank": {...},    # Rank expression dict
    "limit": {...},   # Limit dict
    "select": {...}   # Select dict
}
```

## Serialization

[TODO: to_dict() examples]
[TODO: from_dict() reconstruction]
[TODO: JSON serialization for storage/transmission]
[TODO: Use cases for serialization]

## Empty Search Behavior

[TODO: Document defaults]
- where: None (no filtering)
- rank: None (no ranking, default order)
- limit: No limit
- select: Empty (return only IDs)

## Common Initialization Patterns

[TODO: Add patterns]
```python
# Pattern 1: Start with filter, add ranking
# Pattern 2: Start with ranking, add filter
# Pattern 3: Build progressively based on conditions
```

## Error Handling

[TODO: Invalid parameter errors]
[TODO: Type errors]
[TODO: Validation errors]

## Type Hints and IDE Support

[TODO: Show type hints]
[TODO: IDE autocomplete tips]
[TODO: Type checking with mypy]