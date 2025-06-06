# Create, Get, and Delete Chroma Collections

Chroma lets you manage collections of embeddings, using the `collection` primitive.

Chroma uses collection names in the url, so there are a few restrictions on naming them:

- The length of the name must be between 3 and 63 characters.
- The name must start and end with a lowercase letter or a digit, and it can contain dots, dashes, and underscores in between.
- The name must not contain two consecutive dots.
- The name must not be a valid IP address.

{% Tabs %}

{% Tab label="python" %}
Chroma collections are created with a name and an optional embedding function.

```python
collection = client.create_collection(name="my_collection", embedding_function=emb_fn)
```

The embedding function takes text as input and embeds it. If no embedding function is supplied, Chroma will use [sentence transformer](https://www.sbert.net/index.html) as a default. You can learn more about [embedding functions](../embeddings/embedding-functions), and how to create your own.

When creating collections, you can pass the optional `metadata` argument to add a mapping of metadata key-value pairs to your collections. This can be useful for adding general about the collection like creation time, description of the data stored in the collection, and more.

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

The Chroma client allows you to get and delete existing collections by their name. It also offers a `get or create` method to get a collection if it exists, or create it otherwise.

```python
collection = client.get_collection(name="test") # Get a collection object from an existing collection, by name. Will raise an exception if it's not found.
collection = client.get_or_create_collection(name="test") # Get a collection object from an existing collection, by name. If it doesn't exist, create it.
client.delete_collection(name="my_collection") # Delete a collection and all associated embeddings, documents, and metadata. ⚠️ This is destructive and not reversible
```

Collections have a few useful convenience methods.

* `peek()` - returns a list of the first 10 items in the collection.
* `count()` - returns the number of items in the collection.
* `modify()` - rename the collection

{% /Tab %}

{% Tab label="Typescript" %}

Chroma collections are created with a name and an optional embedding function.

```typescript
let collection = await client.createCollection({
    name: "my_collection",
    embeddingFunction: emb_fn,
});
```

The embedding function takes text as input and embeds it. Different embedding functions are available on npm under the `@chroma-core` organization. For example, if you want to use the `OpenAIEmbeddingFunction`, install `@chroma-core/openai`:

```typescript
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";

let collection = await client.createCollection({
    name: "my_collection",
    embeddingFunction: new OpenAIEmbeddingFunction(),
});
```

If no embedding function is supplied, Chroma will use [sentence transformer](https://www.sbert.net/index.html) as a default. Make sure the `@chroma-core/default-embed` package is installed. 

You can learn more about [embedding functions](../embeddings/embedding-functions), and how to create your own.

When creating collections, you can pass the optional `metadata` argument to add a mapping of metadata key-value pairs to your collections. This can be useful for adding general about the collection like creation time, description of the data stored in the collection, and more.

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

You can also provide `getCollections` the embedding function for each collection:

```typescript
const [col1, col2] = client.getCollections([
    { name: 'col1', embeddingFunction: openaiEF },
    { name: 'col2', embeddingFunction: defaultEF },
])
```

You can also delete collections by name using `deleteCollection`:

```typescript
await client.deleteCollection({ name: 'my-collection '});
```

Collections have a few useful convenience methods.

```typescript
await collection.peek();
await collection.count();
await collection.modify({ name: "new_name" })
```

{% /Tab %}

{% /Tabs %}
