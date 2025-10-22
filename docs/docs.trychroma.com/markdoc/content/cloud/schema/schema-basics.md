---
id: schema-basics
name: Schema Basics
---

# Schema Basics

Learn how to create and use Schema to configure indexes on your Chroma collections.

## Schema Structure

A Schema has two main components that work together to control indexing behavior:

### Defaults

Defaults define index configuration for **all keys** of a given data type. When you add metadata to your collection, Chroma looks at the value type (string, int, float, etc.) and applies the default index configuration for that type.

For example, if you disable string inverted indexes globally, no string metadata fields will be indexed unless you create a key-specific override.

### Keys

Keys define index configuration for **specific metadata fields**. These override the defaults for individual fields, giving you fine-grained control.

For example, you might disable string indexing globally but enable it specifically for a "category" field that you frequently filter on.

### How They Work Together

When determining whether to index a field, Chroma follows this precedence:

1. **Key-specific configuration** (if exists) - highest priority
2. **Default configuration** (for that value type) - fallback
3. **Built-in defaults** (if no Schema provided) - final fallback

This means you can set broad defaults and then override them for specific fields as needed.

## Value Types and Default Behavior

Schema recognizes six value types, each with associated index types. Without providing a Schema, collections use these built-in defaults:

| Value Type | Index Types | Default Enabled | Use Case |
|-----------|-------------|-----------------|----------|
| `string` | String Inverted Index | ✓ (all metadata) | Filter on string values |
| `string` | FTS Index | ✓ (`#document` only) | Full-text search on documents |
| `float_list` | Vector Index | ✓ (`#embedding` only) | Similarity search on embeddings |
| `sparse_vector` | Sparse Vector Index | ✗ (requires config) | Keyword-based search |
| `int_value` | Int Inverted Index | ✓ (all metadata) | Filter on integer values |
| `float_value` | Float Inverted Index | ✓ (all metadata) | Filter on float values |
| `boolean` | Bool Inverted Index | ✓ (all metadata) | Filter on boolean values |

### Special Keys

Chroma uses two reserved key names:

**`#document`** stores document text content with FTS enabled and String Inverted Index disabled. This allows full-text search while avoiding redundant indexing.

**`#embedding`** stores dense vector embeddings with Vector Index enabled, sourcing from `#document`. This enables semantic similarity search.

{% Note type="info" %}
Currently, you cannot manually configure these special keys - their configuration is managed automatically. This restriction may be relaxed in future versions.
{% /Note %}

### Example: Using Defaults

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Without Schema - uses defaults from table above
collection = client.create_collection(name="my_collection")

collection.add(
    ids=["id1"],
    documents=["Some text"],
    metadatas=[{
        "category": "science",  # String inverted index
        "year": 2024,           # Int inverted index
        "score": 0.95,          # Float inverted index
        "published": True       # Bool inverted index
    }]
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Without Schema - uses defaults from table above
const collection = await client.createCollection({ name: "my_collection" });

await collection.add({
  ids: ["id1"],
  documents: ["Some text"],
  metadatas: [{
    category: "science",  // String inverted index
    year: 2024,           // Int inverted index
    score: 0.95,          // Float inverted index
    published: true       // Bool inverted index
  }]
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Creating and Using Schema

### Basic Schema Creation

Create a Schema object to customize index configuration:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema

# Create an empty schema (starts with defaults)
schema = Schema()

# The schema is now ready to be configured
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema } from 'chromadb';

// Create an empty schema (starts with defaults)
const schema = new Schema();

// The schema is now ready to be configured
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Using Schema with Collections

Pass the schema to `create_collection()` or `get_or_create_collection()`:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema

schema = Schema()
# Configure schema here (see Creating Indexes below)

# Create collection with schema
collection = client.create_collection(
    name="my_collection",
    schema=schema
)

# Or use get_or_create_collection
collection = client.get_or_create_collection(
    name="my_collection",
    schema=schema
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema } from 'chromadb';

const schema = new Schema();
// Configure schema here (see Creating Indexes below)

// Create collection with schema
const collection = await client.createCollection({
  name: "my_collection",
  schema: schema
});

// Or use getOrCreateCollection
const collection = await client.getOrCreateCollection({
  name: "my_collection",
  schema: schema
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Schema Persistence

Schema configuration is automatically saved with the collection. When you retrieve a collection with `get_collection()` or `get_or_create_collection()`, the schema is loaded automatically. You don't need to provide the schema again.

## Creating Indexes

### The create_index() Method

Use `create_index()` to enable or configure indexes. The method takes:
- `config`: An index configuration object (or `None` to enable all indexes for a key)
- `key`: Optional - specify a metadata field name for key-specific configuration

The method returns the Schema object, enabling method chaining.

### Creating Global Indexes

Create indexes that apply to all keys of a given type:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, IntInvertedIndexConfig, FloatInvertedIndexConfig

schema = Schema()

# Enable int inverted index globally (already enabled by default)
schema.create_index(config=IntInvertedIndexConfig())

# Enable float inverted index globally (already enabled by default)
schema.create_index(config=FloatInvertedIndexConfig())
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, IntInvertedIndexConfig, FloatInvertedIndexConfig } from 'chromadb';

const schema = new Schema();

// Enable int inverted index globally (already enabled by default)
schema.createIndex(new IntInvertedIndexConfig());

// Enable float inverted index globally (already enabled by default)
schema.createIndex(new FloatInvertedIndexConfig());
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Creating Key-Specific Indexes

Override defaults for specific metadata fields:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, StringInvertedIndexConfig

schema = Schema()

# Enable string indexing only for specific fields
schema.create_index(config=StringInvertedIndexConfig(), key="category")
schema.create_index(config=StringInvertedIndexConfig(), key="author")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, StringInvertedIndexConfig } from 'chromadb';

const schema = new Schema();

// Enable string indexing only for specific fields
schema.createIndex(new StringInvertedIndexConfig(), "category");
schema.createIndex(new StringInvertedIndexConfig(), "author");
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Disabling Indexes

### The delete_index() Method

Use `delete_index()` to disable indexes. Like `create_index()`, it takes:
- `config`: An index configuration object (or `None` to disable all indexes for a key)
- `key`: Optional - specify a metadata field name for key-specific configuration

Returns the Schema object for method chaining.

{% Note type="info" %}
Not all indexes can be deleted. Vector, FTS, and Sparse Vector indexes currently cannot be disabled.
{% /Note %}

### Examples

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, StringInvertedIndexConfig, IntInvertedIndexConfig

schema = Schema()

# Disable string inverted index globally
schema.delete_index(config=StringInvertedIndexConfig())

# Disable int inverted index for a specific key
schema.delete_index(config=IntInvertedIndexConfig(), key="unimportant_count")

# Disable all indexes for a specific key
schema.delete_index(key="temporary_field")

collection = client.create_collection(name="optimized", schema=schema)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, StringInvertedIndexConfig, IntInvertedIndexConfig } from 'chromadb';

const schema = new Schema();

// Disable string inverted index globally
schema.deleteIndex(new StringInvertedIndexConfig());

// Disable int inverted index for a specific key
schema.deleteIndex(new IntInvertedIndexConfig(), "unimportant_count");

// Disable all indexes for a specific key
schema.deleteIndex(undefined, "temporary_field");

const collection = await client.createCollection({ name: "optimized", schema });
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Method Chaining

Both `create_index()` and `delete_index()` return the Schema object, enabling fluent method chaining:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, StringInvertedIndexConfig, IntInvertedIndexConfig

schema = (Schema()
    .delete_index(config=StringInvertedIndexConfig())  # Disable globally
    .create_index(config=StringInvertedIndexConfig(), key="category")  # Enable for category
    .create_index(config=StringInvertedIndexConfig(), key="tags")  # Enable for tags
    .delete_index(config=IntInvertedIndexConfig()))  # Disable int indexing

collection = client.create_collection(name="optimized", schema=schema)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, StringInvertedIndexConfig, IntInvertedIndexConfig } from 'chromadb';

const schema = new Schema()
  .deleteIndex(new StringInvertedIndexConfig())  // Disable globally
  .createIndex(new StringInvertedIndexConfig(), "category")  // Enable for category
  .createIndex(new StringInvertedIndexConfig(), "tags")  // Enable for tags
  .deleteIndex(new IntInvertedIndexConfig());  // Disable int indexing

const collection = await client.createCollection({ name: "optimized", schema });
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Next Steps

- Set up [hybrid search](./hybrid-search) with sparse vectors
- Browse the complete [index configuration reference](./index-reference)
