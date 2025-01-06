# Create, Get, and Delete Chroma Collections

Chroma lets you manage collections of embeddings, using the `collection` primitive.

Chroma uses collection names in the url, so there are a few restrictions on naming them:

- The length of the name must be between 3 and 63 characters.
- The name must start and end with a lowercase letter or a digit, and it can contain dots, dashes, and underscores in between.
- The name must not contain two consecutive dots.
- The name must not be a valid IP address.

Chroma collections are created with a name and an optional embedding function.

{% Banner type="note" %}
If you supply an embedding function, you must supply it every time you get the collection.
{% /Banner %}

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection = client.create_collection(name="my_collection", embedding_function=emb_fn)
collection = client.get_collection(name="my_collection", embedding_function=emb_fn)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
let collection = await client.createCollection({
    name: "my_collection",
    embeddingFunction: emb_fn,
});

collection = await client.getCollection({
    name: "my_collection",
    embeddingFunction: emb_fn,
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

The embedding function takes text as input and embeds it. If no embedding function is supplied, Chroma will use [sentence transformer](https://www.sbert.net/index.html) as a default. You can learn more about [embedding functions](../embeddings/embedding-functions), and how to create your own.

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

The collection metadata is also used to configure the embedding space of a collection. Learn more about it in [Configuring Chroma Collections](./configure).

The Chroma client allows you to get and delete existing collections by their name. It also offers a `get or create` method to get a collection if it exists, or create it otherwise.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection = client.get_collection(name="test") # Get a collection object from an existing collection, by name. Will raise an exception if it's not found.
collection = client.get_or_create_collection(name="test") # Get a collection object from an existing collection, by name. If it doesn't exist, create it.
client.delete_collection(name="my_collection") # Delete a collection and all associated embeddings, documents, and metadata. ⚠️ This is destructive and not reversible
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
const collection = await client.getCollection({ name: "test" }); // Get a collection object from an existing collection, by name. Will raise an exception of it's not found.
collection = await client.getOrCreateCollection({ name: "test" }); // Get a collection object from an existing collection, by name. If it doesn't exist, create it.
await client.deleteCollection(collection); // Delete a collection and all associated embeddings, documents, and metadata. ⚠️ This is destructive and not reversible
```
{% /Tab %}

{% /TabbedCodeBlock %}

Collections have a few useful convenience methods.

* `peek()` - returns a list of the first 10 items in the collection.
* `count()` - returns the number of items in the collection.
* `modify()` - rename the collection

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection.peek() 
collection.count() 
collection.modify(name="new_name")
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
await collection.peek();
await collection.count();
await collection.modify({ name: "new_name" })
```
{% /Tab %}

{% /TabbedCodeBlock %}