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

## Comparison Operators

[Content to be added]

## Collection Operators

[Content to be added]

## Logical Operators

[Content to be added]

## MongoDB-style Syntax

[Content to be added]

## Filtering by IDs

[Content to be added]

## Complex Filter Examples

[Content to be added]