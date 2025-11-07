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

## Default Index Behavior

Without providing a Schema, collections use built-in defaults for indexing. For a complete overview of all value types, index types, and their defaults, see the [Index Configuration Reference](./index-reference#index-types-overview).

### Special Keys

Chroma uses two reserved key names:

**`K.DOCUMENT`** (`#document`) stores document text content with FTS enabled and String Inverted Index disabled. This allows full-text search while avoiding redundant indexing.

**`K.EMBEDDING`** (`#embedding`) stores dense vector embeddings with Vector Index enabled, sourcing from `K.DOCUMENT`. This enables semantic similarity search.

{% Note type="info" %}
Use `K.DOCUMENT` and `K.EMBEDDING` in your code (they correspond to internal keys `#document` and `#embedding`). These special keys are automatically configured and cannot be manually modified. See the [Search API field reference](../search-api/pagination-selection#available-fields) for more details.
{% /Note %}

### Example: Using Defaults

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Without Schema - uses defaults from table above
collection = client.create_collection(name="my_collection")

collection.add(
    ids=["id1"],
    documents=["Some text"],    # FTS index
    embeddings=[[1.0, 2.0]],    # Vector index
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

## Creating Schema Objects

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

## Creating Indexes

### The create_index() Method

Use `create_index()` to enable or configure indexes. The method takes:
- `config`: An index configuration object (or `None` to enable all indexes for a key)
- `key`: Optional - specify a metadata field name for key-specific configuration

The method returns the Schema object, enabling method chaining.

### Creating Global Indexes

Create indexes that apply globally. This example shows configuring the vector index with custom settings:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, VectorIndexConfig
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

schema = Schema()

# Configure vector index with custom embedding function
embedding_function = OpenAIEmbeddingFunction(
    api_key="your-api-key",
    model_name="text-embedding-3-small"
)

schema.create_index(config=VectorIndexConfig(
    space="cosine",
    embedding_function=embedding_function
))
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, VectorIndexConfig } from 'chromadb';
import { OpenAIEmbeddingFunction } from 'chromadb';

const schema = new Schema();

// Configure vector index with custom embedding function
const embeddingFunction = new OpenAIEmbeddingFunction({
  apiKey: "your-api-key",
  model: "text-embedding-3-small"
});

schema.createIndex(new VectorIndexConfig({
  space: "cosine",
  embeddingFunction: embeddingFunction
}));
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Creating Key-Specific Indexes

Configure indexes for specific metadata fields. This example shows configuring the sparse vector index with custom settings:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Schema, SparseVectorIndexConfig, K
from chromadb.utils.embedding_functions import ChromaCloudSpladeEmbeddingFunction

schema = Schema()

# Add sparse vector index for a specific key (required for hybrid search)
sparse_ef = ChromaCloudSpladeEmbeddingFunction()
schema.create_index(
    config=SparseVectorIndexConfig(
        source_key=K.DOCUMENT,
        embedding_function=sparse_ef
    ),
    key="sparse_embedding"
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Schema, SparseVectorIndexConfig, K } from 'chromadb';
import { ChromaCloudSpladeEmbeddingFunction } from '@chroma-core/chroma-cloud-splade';

const schema = new Schema();

// Add sparse vector index for a specific key (required for hybrid search)
const sparseEf = new ChromaCloudSpladeEmbeddingFunction({
  apiKeyEnvVar: "CHROMA_API_KEY"
});
schema.createIndex(
  new SparseVectorIndexConfig({
    sourceKey: K.DOCUMENT,
    embeddingFunction: sparseEf
  }),
  "sparse_embedding"
);
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
This example uses `ChromaCloudSpladeEmbeddingFunction`, but you can use other sparse embedding functions like `HuggingFaceSparseEmbeddingFunction` or `FastembedSparseEmbeddingFunction` depending on your needs.
{% /Note %}

## Disabling Indexes

### The delete_index() Method

Use `delete_index()` to disable indexes. Like `create_index()`, it takes:
- `config`: An index configuration object (or `None` to disable all indexes for a key)
- `key`: Optional - specify a metadata field name for key-specific configuration

Returns the Schema object for method chaining.

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
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Banner type="tip" %}
**Note:** Not all indexes can be deleted. Vector and FTS indexes currently cannot be disabled
{% /Banner %}

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
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Using Schema with Collections

Pass the configured schema to `create_collection()` or `get_or_create_collection()`:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
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

## Next Steps

- Set up [sparse vector search](./sparse-vector-search) with sparse vectors
- Browse the complete [index configuration reference](./index-reference)
