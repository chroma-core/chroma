---
id: filtering
name: Filtering with Where
---

# Filtering with Where

Learn how to filter search results using Where expressions and the Key/K class.

## The Key/K Class

{% Tabs %}

{% Tab label="python" %}
```python
from chromadb import K

# K is an alias for Key - use either interchangeably
from chromadb import Key

# Simple equality
K("status") == "active"

# Comparison operators
K("score") > 0.5
K("year") >= 2020
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// TypeScript implementation coming soon
```
{% /Tab %}

{% /Tabs %}

## Key/K Class Complete Reference

[TODO: Add complete reference table]
| Constant | Value | Description |
|----------|-------|-------------|
| K.ID | "#id" | Document ID |
| K.DOCUMENT | "#document" | Document content |
| K.EMBEDDING | "#embedding" | Embedding vector |
| K.METADATA | "#metadata" | All metadata |
| K.SCORE | "#score" | Search score |

## Comparison Operators

[TODO: Add all operators with examples and edge cases]
```python
# Equality
K("status") == "active"  # Exact match
K("count") == 5          # Numeric equality

# Inequality
K("status") != "draft"   # Not equal

# Greater/Less than
K("score") > 0.5
K("score") >= 0.5
K("score") < 1.0
K("score") <= 1.0
```

[TODO: Type handling for different data types]
[TODO: NULL/None handling]
[TODO: Edge cases and gotchas]

## Collection Operators

[TODO: Complete examples for each operator]
```python
# is_in - check if value in list
K("category").is_in(["tech", "science"])

# not_in - check if value not in list
K("status").not_in(["draft", "deleted"])

# contains - substring search
K.DOCUMENT.contains("machine learning")

# not_contains
K.DOCUMENT.not_contains("deprecated")

# regex - pattern matching
K("email").regex(r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$")

# not_regex
K("phone").not_regex(r"^\d{3}-\d{3}-\d{4}$")
```

[TODO: Performance implications of each operator]
[TODO: Case sensitivity notes]

## Logical Operators

[TODO: Precedence rules]
[TODO: Complex nested examples]
```python
# AND operator (&)
(K("status") == "published") & (K("year") >= 2020)

# OR operator (|)
(K("category") == "tech") | (K("category") == "science")

# Complex nesting
((K("status") == "published") & (K("featured") == True)) | (K("priority") > 5)
```

[TODO: Parentheses and precedence]
[TODO: Common mistakes to avoid]

## MongoDB-style Syntax Complete Reference

[TODO: All operators with examples]
```python
# Comparison operators
{"field": {"$eq": "value"}}
{"field": {"$ne": "value"}}
{"field": {"$gt": 10}}
{"field": {"$gte": 10}}
{"field": {"$lt": 100}}
{"field": {"$lte": 100}}

# Collection operators
{"field": {"$in": ["val1", "val2"]}}
{"field": {"$nin": ["val1", "val2"]}}
{"field": {"$contains": "text"}}
{"field": {"$not_contains": "text"}}
{"field": {"$regex": "^pattern"}}
{"field": {"$not_regex": "pattern"}}

# Logical operators
{"$and": [{"field1": "val1"}, {"field2": "val2"}]}
{"$or": [{"field1": "val1"}, {"field2": "val2"}]}
```

## Filtering by Special Fields

[TODO: ID filtering patterns]
```python
# Filter by specific IDs
K.ID.is_in(["id1", "id2", "id3"])

# Exclude specific IDs
K.ID.not_in(["id4", "id5"])

# Document content filtering
K.DOCUMENT.contains("search term")

# Score filtering (in re-ranking scenarios)
K.SCORE > 0.8
```

## Metadata Field Type Handling

[TODO: How different types are handled]
- Strings: exact match, contains, regex
- Numbers: comparison operators
- Booleans: equality only
- Arrays: element matching
- Nested objects: dot notation (if supported)

## Filter Optimization

[TODO: Performance tips]
- Index usage
- Filter selectivity
- Operator performance comparison
- Query planning

## Common Filtering Patterns

[TODO: Real-world patterns]
```python
# Date range filtering
(K("created_at") >= "2024-01-01") & (K("created_at") < "2024-02-01")

# Multi-value matching
K("tags").is_in(["ai", "ml", "nlp"])

# Null checking
K("optional_field") != None

# Complex business logic
((K("status") == "active") & (K("score") > 0.7)) | (K("featured") == True)
```

## Anti-Patterns to Avoid

[TODO: Common mistakes]
- Over-filtering reducing recall
- Inefficient operator combinations
- Type mismatches
- Case sensitivity issues