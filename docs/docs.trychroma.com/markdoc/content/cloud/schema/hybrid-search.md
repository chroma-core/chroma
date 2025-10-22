---
id: hybrid-search
name: Hybrid Search Setup
---

# Hybrid Search Setup

Learn how to configure Schema for hybrid search, combining dense semantic embeddings with sparse keyword embeddings.

## What is Hybrid Search?

[Brief explanation: combining dense + sparse embeddings for better retrieval]

## Prerequisites

### Sparse Embedding Function

[What you need to have ready]

### Understanding Vector vs Sparse Vector Indexes

[Brief clarification of the difference]

## Complete Configuration Example

### Step 1: Set Up Sparse Embedding Function

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Setting up sparse embedding function
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Setting up sparse embedding function
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Step 2: Create Schema with Sparse Vector Index

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Creating Schema with sparse vector index
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Creating Schema with sparse vector index
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Step 3: Create Collection with Schema

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Creating collection with Schema
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Creating collection with Schema
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Complete Example

[Full end-to-end code example]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Complete hybrid search setup
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Complete hybrid search setup
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Auto-Embedding Behavior

### How It Works

[Explanation of automatic sparse embedding generation during add/upsert]

### Source Key Concept

[Where sparse embeddings are sourced from]

### What Happens When Source Data is Missing

[Error handling and edge cases]

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Auto-embedding in action
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Auto-embedding in action
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Querying with RRF

[Brief example of using RRF for hybrid search]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Basic RRF query example
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Basic RRF query example
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
For comprehensive details on querying with RRF and hybrid search strategies, see the [Search API Hybrid Search documentation](../search-api/hybrid-search).
{% /Note %}

## Best Practices

### Choosing Appropriate Source Keys

[Guidance on selecting source_key values]

### Setting Up Embedding Functions

[Tips for embedding function configuration]

### Performance Considerations

[What to consider for performance]

### When to Use Hybrid Search

[Decision guide for hybrid search usage]

## Complete End-to-End Example

[Real-world example with setup, data addition, and querying]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Full working example
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Full working example
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Next Steps

- Learn about [Search API hybrid search with RRF](../search-api/hybrid-search) for advanced querying
- Review [rules and constraints](./rules-constraints) for sparse vector indexes
- Explore the [index configuration reference](./index-reference) for detailed parameters
