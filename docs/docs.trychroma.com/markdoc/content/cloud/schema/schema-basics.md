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
Use `K.DOCUMENT` and `K.EMBEDDING` in your code (they correspond to internal keys `#document` and `#embedding`). In Go, use the constants `chroma.DocumentKey` and `chroma.EmbeddingKey`. These special keys are automatically configured and cannot be manually modified. See the [Search API field reference](../search-api/pagination-selection#available-fields) for more details.
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

{% Tab label="go" %}
```go
// Without Schema - uses defaults from table above
collection, err := client.CreateCollection(ctx, "my_collection")
if err != nil {
    log.Fatal(err)
}

_, err = collection.Add(ctx,
    chroma.WithIDs("id1"),
    chroma.WithDocuments("Some text"),     // FTS index
    chroma.WithEmbeddings([]float32{1.0, 2.0}), // Vector index
    chroma.WithMetadatas(
        chroma.NewDocumentMetadata().
            SetString("category", "science"). // String inverted index
            SetInt("year", 2024).             // Int inverted index
            SetFloat("score", 0.95).          // Float inverted index
            SetBool("published", true),       // Bool inverted index
    ),
)
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

{% Tab label="go" %}
```go
import chroma "github.com/chroma-core/chroma/clients/go"

// Create an empty schema (starts with defaults)
schema, err := chroma.NewSchema()
if err != nil {
    log.Fatal(err)
}

// Or create a schema with L2 vector index preset
schemaWithDefaults, err := chroma.NewSchemaWithDefaults()
if err != nil {
    log.Fatal(err)
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
**Go Pattern:** The Go client uses a functional options pattern. Schema configuration is declared at creation time via `NewSchema(options...)` rather than via method chaining.
{% /Note %}

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
import { OpenAIEmbeddingFunction } from '@chroma-core/openai';

const schema = new Schema();

// Configure vector index with custom embedding function
const embeddingFunction = new OpenAIEmbeddingFunction({
  apiKey: "your-api-key",
  modelName: "text-embedding-3-small"
});

schema.createIndex(new VectorIndexConfig({
  space: "cosine",
  embeddingFunction: embeddingFunction
}));
```
{% /Tab %}

{% Tab label="go" %}
```go
import (
    chroma "github.com/chroma-core/chroma/clients/go"
    "github.com/chroma-core/chroma/clients/go/pkg/embeddings/openai"
)

// Create embedding function
ef, err := openai.NewEmbeddingFunction(
    openai.WithAPIKey("your-api-key"),
    openai.WithModel("text-embedding-3-small"),
)
if err != nil {
    log.Fatal(err)
}

// Create schema with vector index configuration
schema, err := chroma.NewSchema(
    chroma.WithDefaultVectorIndex(chroma.NewVectorIndexConfig(
        chroma.WithSpace(chroma.SpaceCosine),
        chroma.WithVectorEmbeddingFunction(ef),
    )),
)
if err != nil {
    log.Fatal(err)
}
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

{% Tab label="go" %}
```go
import (
    chroma "github.com/chroma-core/chroma/clients/go"
    "github.com/chroma-core/chroma/clients/go/pkg/embeddings/chromacloudsplade"
)

// Create sparse embedding function
sparseEF, err := chromacloudsplade.NewEmbeddingFunction(
    chromacloudsplade.WithAPIKeyFromEnvVar("CHROMA_API_KEY"),
)
if err != nil {
    log.Fatal(err)
}

// Create schema with sparse vector index for a specific key
schema, err := chroma.NewSchema(
    chroma.WithSparseVectorIndex("sparse_embedding",
        chroma.NewSparseVectorIndexConfig(
            chroma.WithSparseSourceKey(chroma.DocumentKey),
            chroma.WithSparseEmbeddingFunction(sparseEF),
        ),
    ),
)
if err != nil {
    log.Fatal(err)
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
This example uses `ChromaCloudSpladeEmbeddingFunction`, but you can use other sparse embedding functions like `HuggingFaceSparseEmbeddingFunction` or `FastembedSparseEmbeddingFunction` depending on your needs. In Go, use `chromacloudsplade.NewEmbeddingFunction()` or `bm25.NewEmbeddingFunction()`.
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

{% Tab label="go" %}
```go
import chroma "github.com/chroma-core/chroma/clients/go"

// Create schema with various indexes disabled
schema, err := chroma.NewSchema(
    // Disable string inverted index globally
    chroma.DisableDefaultStringIndex(),

    // Disable int inverted index for a specific key
    chroma.DisableIntIndex("unimportant_count"),

    // Disable string index for a specific key
    chroma.DisableStringIndex("temporary_field"),
)
if err != nil {
    log.Fatal(err)
}
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

{% Tab label="go" %}
```go
import chroma "github.com/chroma-core/chroma/clients/go"

// Go uses functional options instead of method chaining
// All configuration is provided at schema creation time
schema, err := chroma.NewSchema(
    chroma.DisableDefaultStringIndex(),      // Disable globally
    chroma.WithStringIndex("category"),      // Enable for category
    chroma.WithStringIndex("tags"),          // Enable for tags
    chroma.DisableDefaultIntIndex(),         // Disable int indexing globally
)
if err != nil {
    log.Fatal(err)
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
**Go Pattern:** The Go client uses functional options instead of method chaining. All schema configuration must be provided at creation time via `NewSchema(options...)`.
{% /Note %}

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

{% Tab label="go" %}
```go
// Create collection with schema
collection, err := client.CreateCollection(ctx, "my_collection",
    chroma.WithSchemaCreate(schema),
)
if err != nil {
    log.Fatal(err)
}

// Or use GetOrCreateCollection
collection, err = client.GetOrCreateCollection(ctx, "my_collection",
    chroma.WithSchemaCreate(schema),
)
if err != nil {
    log.Fatal(err)
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Schema Persistence

Schema configuration is automatically saved with the collection. When you retrieve a collection with `get_collection()` or `get_or_create_collection()`, the schema is loaded automatically. You don't need to provide the schema again.

## Next Steps

- Set up [sparse vector search](./sparse-vector-search) with sparse vectors
- Browse the complete [index configuration reference](./index-reference)
