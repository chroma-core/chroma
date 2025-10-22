---
id: rules-constraints
name: Rules & Constraints
---

# Rules & Constraints

Important rules and limitations to understand when configuring Schema.

## Special Key Restrictions

[Explanation that #document and #embedding cannot be manually configured]

### Why These Restrictions Exist

[Reasoning behind the restrictions]

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# ✗ INCORRECT - Cannot modify special keys
# ✓ CORRECT - How to work with special keys
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// ✗ INCORRECT - Cannot modify special keys
// ✓ CORRECT - How to work with special keys
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Vector Index Rules

[Can ONLY be configured globally, not per-key]

### Key Points

- Always enabled on `#embedding`
- Disabled by default elsewhere
- Cannot be created on specific keys

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# ✓ CORRECT - Global vector index configuration
# ✗ INCORRECT - Attempting per-key configuration
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// ✓ CORRECT - Global vector index configuration
// ✗ INCORRECT - Attempting per-key configuration
```
{% /Tab %}

{% /TabbedCodeBlock %}

## FTS Index Rules

[Can ONLY be configured globally, not per-key]

### Key Points

- Always enabled on `#document`
- Disabled by default elsewhere
- Cannot be created on specific keys

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# ✓ CORRECT - Global FTS index configuration
# ✗ INCORRECT - Attempting per-key configuration
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// ✓ CORRECT - Global FTS index configuration
// ✗ INCORRECT - Attempting per-key configuration
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Sparse Vector Index Rules

[MUST specify a key when creating, only ONE per collection]

### Key Points

- Must be created on a specific key
- Cannot be created globally
- Only one sparse vector index per collection
- Used primarily for hybrid search

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# ✓ CORRECT - Creating sparse vector index on a key
# ✗ INCORRECT - Attempting global or multiple sparse indexes
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// ✓ CORRECT - Creating sparse vector index on a key
// ✗ INCORRECT - Attempting global or multiple sparse indexes
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Deletion Restrictions

[What can and cannot be deleted]

### Currently Unsupported Deletions

- Vector indexes
- FTS indexes
- Sparse vector indexes

### What Can Be Deleted

[List of index types that can be disabled]

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# What can be deleted
# What cannot be deleted
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// What can be deleted
// What cannot be deleted
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Common Patterns That Work Well

### Pattern 1: Enable All Indexes for a Key

[Quick example]

### Pattern 2: Disable Unused Indexes Globally

[Quick example]

### Pattern 3: Selective Metadata Field Indexing

[Quick example]

## Summary Table

| Operation | Vector Index | FTS Index | Sparse Vector Index | Other Indexes |
|-----------|--------------|-----------|---------------------|---------------|
| Create globally | ✓ | ✓ | ✗ | ✓ |
| Create on specific key | ✗ | ✗ | ✓ (required) | ✓ |
| Delete | ✗ | ✗ | ✗ | ✓ |
| Multiple per collection | N/A | N/A | ✗ (one only) | ✓ |

## Next Steps

- Set up [hybrid search](./hybrid-search) with proper sparse vector configuration
- Review the [index configuration reference](./index-reference) for detailed parameters
- Go back to [Schema basics](./schema-basics) for usage examples
