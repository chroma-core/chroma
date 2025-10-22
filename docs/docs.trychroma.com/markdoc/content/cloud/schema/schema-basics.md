---
id: schema-basics
name: Schema Basics
---

# Schema Basics

Learn how to create and use Schema to configure indexes on your Chroma collections.

## Schema Structure

[Explanation of defaults vs keys]

### Defaults

[What defaults are and how they work]

### Keys

[What key-specific overrides are and how they work]

### How They Work Together

[Precedence and interaction between defaults and keys]

## Value Types Overview

[Table showing 6 value types, their index types, and default enabled status]

| Value Type | Index Types | Default Enabled |
|-----------|-------------|-----------------|
| `string` | FTS Index, String Inverted Index | ... |
| `float_list` | Vector Index | ... |
| `sparse_vector` | Sparse Vector Index | ... |
| `int_value` | Int Inverted Index | ... |
| `float_value` | Float Inverted Index | ... |
| `boolean` | Bool Inverted Index | ... |

## Special Keys

### #document

[Explanation of #document key and its default configuration]

### #embedding

[Explanation of #embedding key and its default configuration]

### Why These Keys Exist

[Purpose and usage of special keys]

## Default Behavior

[What you get without any Schema configuration]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Default behavior example
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Default behavior example
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Creating and Using Schema

### Basic Schema Creation

[How to create a Schema object]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Basic creation
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Basic creation
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Using Schema with Collections

[How to pass Schema to create_collection and get_or_create_collection]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Using with create_collection
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Using with create_collection
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Schema Persistence

[How Schema is persisted and retrieved]

## Creating Indexes

### The create_index() Method

[Syntax and parameters]

### Creating Global Indexes

[Examples of creating indexes that apply to all keys]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Global index creation
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Global index creation
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Creating Key-Specific Indexes

[Examples of creating indexes for specific metadata fields]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Key-specific index creation
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Key-specific index creation
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Method Chaining

[Examples of chaining multiple create_index calls]

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Method chaining
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Method chaining
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Disabling Indexes

### The delete_index() Method

[Syntax and what can be disabled]

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Disabling indexes
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Disabling indexes
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Next Steps

- Learn about [rules and constraints](./rules-constraints) for Schema configuration
- Set up [hybrid search](./hybrid-search) with sparse vectors
- Browse the complete [index configuration reference](./index-reference)
