# Managing Chroma Collections

Chroma lets you manage collections of embeddings, using the **collection** primitive. Collections are the fundamental unit of storage and querying in Chroma.

## Creating Collections

Chroma collections are created with a name. Collection names are used in the url, so there are a few restrictions on them:

- The length of the name must be between 3 and 512 characters.
- The name must start and end with a lowercase letter or a digit, and it can contain dots, dashes, and underscores in between.
- The name must not contain two consecutive dots.
- The name must not be a valid IP address.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection = client.create_collection(name="my_collection")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
const collection = await client.createCollection({
    name: "my_collection",
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

Note that collection names must be **unique** inside a Chroma database. If you try to create a collection with a name of an existing one, you will see an exception.

### Embedding Functions

When you add documents to a collection, Chroma will embed them for you by using the collection's **embedding function**. Chroma will use [sentence transformer](https://www.sbert.net/index.html) embedding function as a default.

Chroma also offers various embedding function, which you can provide upon creating a collection. For example, you can create a collection using the `OpenAIEmbeddingFunction`:

{% Tabs %}

{% Tab label="python" %}

Install the `openai` package:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="pip" %}
```terminal
pip install openai
```
{% /Tab %}

{% Tab label="poetry" %}
```terminal
poetry add openai
```
{% /Tab %}

{% Tab label="uv" %}
```terminal
uv pip install openai
```
{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Create your collection with the `OpenAIEmbeddingFunction`:

```python
import os
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

collection = client.create_collection(
    name="my_collection",
    embedding_function=OpenAIEmbeddingFunction(
        api_key=os.getenv("OPENAI_API_KEY"),
        model_name="text-embedding-3-small"
    )
)
```

Instead of having Chroma embed documents, you can also provide embeddings directly when [adding data](./add-data) to a collection. In this case, your collection will not have an embedding function set, and you will be responsible for providing embeddings directly when adding data and querying.

```python
collection = client.create_collection(
    name="my_collection",
    embedding_function=None
)
```

{% /Tab %}

{% Tab label="typescript" %}

Install the `@chroma-core/openai` package to get access to the `OpenAIEmbeddingFunction`:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="npm" %}
```terminal
npm install @chroma-core/openai
```
{% /Tab %}

{% Tab label="pnpm" %}
```terminal
pnpm add @chroma-core/openai
```
{% /Tab %}

{% Tab label="yarn" %}
```terminal
yarn add @chroma-core/openai
```
{% /Tab %}

{% Tab label="bun" %}
```terminal
bun add @chroma-core/openai
```
{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Create your collection with the `OpenAIEmbeddingFunction`:

```typescript
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";

const collection = await client.createCollection({
    name: "my_collection",
    embeddingFunction: new OpenAIEmbeddingFunction({
        apiKey: process.env.OPENAI_API_KEY,
        modelName: "text-embedding-3-small"
    })
});
```

Instead of having Chroma embed documents, you can also provide embeddings directly when [adding data](./add-data) to a collection. In this case, your collection will not have an embedding function set, and you will be responsible for providing embeddings directly when adding data and querying.

```typescript
const collection = await client.createCollection({
    name: "my_collection",
    embeddingFunction: null
})
```

{% /Tab %}

{% /Tabs %}

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection = client.create_collection(
    name="my_collection",
    embedding_function=None
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
let collection = await client.createCollection({
    name: "my_collection",
    embeddingFunction: emb_fn,
    metadata: {
        description: "my first Chroma collection",
        created: (new Date()).toString()
    }
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Collection Metadata

When creating collections, you can pass the optional `metadata` argument to add a mapping of metadata key-value pairs to your collections. This can be useful for adding general about the collection like creation time, description of the data stored in the collection, and more.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from datetime import datetime

collection = client.create_collection(
    name="my_collection",
    embedding_function=emb_fn,
    metadata={
        "description": "my first Chroma collection",
        "created": str(datetime.now())
    }
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
let collection = await client.createCollection({
    name: "my_collection",
    embeddingFunction: emb_fn,
    metadata: {
        description: "my first Chroma collection",
        created: (new Date()).toString()
    }
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Getting Collections

{% Tabs %}

{% Tab label="python" %}
There are several ways to get a collection after it was created.

The `get_collection` function will get a collection from Chroma by name. It returns a `Collection` object with `name`, `metadata`, `configuration`, and `embedding_function`.

```python
collection = client.get_collection(name="my-collection")
```

The `get_or_create_collection` function behaves similarly, but will create the collection if it doesn't exist. You can pass to it the same arguments `create_collection` expects, and the client will ignore them if the collection already exists.

```python
collection = client.get_or_create_collection(
    name="my-collection",
    metadata={"description": "..."}
)
```

The `list_collections` function returns the collections you have in your Chroma database. The collections will be ordered by creation time from oldest to newest.

```python
collections = client.list_collections()
```

By default, `list_collections` returns up to 100 collections. If you have more than 100 collections, or need to get only a subset of your collections, you can use the `limit` and `offset` arguments:

```python
first_collections_batch = client.list_collections(limit=100) # get the first 100 collections
second_collections_batch = client.list_collections(limit=100, offset=100) # get the next 100 collections
collections_subset = client.list_collections(limit=20, offset=50) # get 20 collections starting from the 50th
```

Current versions of Chroma store the embedding function you used to create a collection on the server, so the client can resolve it for you on subsequent "get" operations. If you are running an older version of the Chroma client or server (<1.1.13), you will need to provide the same embedding function you used to create a collection when using `get_collection`:

```python
collection = client.get_collection(
    name='my-collection',
    embedding_function=ef
)
```

{% /Tab %}

{% Tab label="typescript" %}
There are several ways to get a collection after it was created.

The `getCollection` function will get a collection from Chroma by name. It returns a collection object with `name`, `metadata`, `configuration`, and `embeddingFunction`. If you did not provide an embedding function to `createCollection`, you can provide it to `getCollection`.

```typescript
const collection = await client.getCollection({ name: 'my-collection '})
```

The `getOrCreate` function behaves similarly, but will create the collection if it doesn't exist. You can pass to it the same arguments `createCollection` expects, and the client will ignore them if the collection already exists.

```typescript
const collection = await client.getOrCreateCollection({
    name: 'my-collection',
    metadata: { 'description': '...' }
});
```

If you need to get multiple collections at once, you can use `getCollections()`:

```typescript
const [col1, col2] = client.getCollections(["col1", "col2"]);
```

The `listCollections` function returns all the collections you have in your Chroma database. The collections will be ordered by creation time from oldest to newest.

```typescript
const collections = await client.listCollections()
```

By default, `listCollections` returns up to 100 collections. If you have more than 100 collections, or need to get only a subset of your collections, you can use the `limit` and `offset` arguments:

```typescript
const firstCollectionsBatch = await client.listCollections({ limit: 100 }) // get the first 100 collections
const secondCollectionsBatch = await client.listCollections({ limit: 100, offset: 100 }) // get the next 100 collections
const collectionsSubset = await client.listCollections({ limit: 20, offset: 50 }) // get 20 collections starting from the 50th
```

Current versions of Chroma store the embedding function you used to create a collection on the server, so the client can resolve it for you on subsequent "get" operations. If you are running an older version of the Chroma JS/TS client (<3.04) or server (<1.1.13), you will need to provide the same embedding function you used to create a collection when using `getCollection` and `getCollections`:

```typescript
const collection = await client.getCollection({
    name: 'my-collection',
    embeddingFunction: ef
})

const [col1, col2] = client.getCollections([
    { name: 'col1', embeddingFunction: openaiEF },
    { name: 'col2', embeddingFunction: defaultEF },
]);
```

{% /Tab %}

{% /Tabs %}

## Modifying Collections

After a collection is created, you can modify its name, metadata and elements of its [index configuration](./configure) with the `modify` method:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.modify(
   name="new-name",
   metadata={"description": "new description"}
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.modify({
    name: "new-name",
    metadata: {description: "new description"}
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Deleting Collections

You can delete a collection by name. This action will delete a collection, all of its embeddings, and associated documents and records' metadata.

{% Banner type="warn" %}
Deleting collections is destructive and not reversible
{% /Banner %}

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
client.delete_collection(name="my-collection")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await client.deleteCollection({ name: "my-collection" });
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Convenience Methods

Collections also offer a few useful convenience methods:
* `count` - returns the number of records in the collection.
* `peek` - returns the first 10 records in the collection.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.count()
collection.peek()
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.count();
await collection.peek();
```
{% /Tab %}

{% /TabbedCodeBlock %}
