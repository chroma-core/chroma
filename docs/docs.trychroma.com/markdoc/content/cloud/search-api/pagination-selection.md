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

## Predefined Fields

[Content to be added]

## Custom Metadata Fields

[Content to be added]

## Performance Optimization

[Content to be added]