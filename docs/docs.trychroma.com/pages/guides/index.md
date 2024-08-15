---
title: "🧪 Usage Guide"
---

{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

---

## Initiating a persistent Chroma client

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
import chromadb
```

You can configure Chroma to save and load the database from your local machine. Data will be persisted automatically and loaded on start (if it exists).

```python
client = chromadb.PersistentClient(path="/path/to/save/to")
```

The `path` is where Chroma will store its database files on disk, and load them on start.

{% /tab %}
{% tab label="Javascript" %}

```js
// CJS
const { ChromaClient } = require("chromadb");

// ESM
import { ChromaClient } from "chromadb";
```

{% note type="note" title="Connecting to the backend" %}
To connect with the JS client, you must connect to a backend running Chroma. See [Running Chroma in client-server mode](#running-chroma-in-client-server-mode) for how to do this.
{% /note %}

```js
const client = new ChromaClient();
```

{% /tab %}

{% /tabs %}

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

The client object has a few useful convenience methods.

```python
client.heartbeat() # returns a nanosecond heartbeat. Useful for making sure the client remains connected.
client.reset() # Empties and completely resets the database. ⚠️ This is destructive and not reversible.
```

{% /tab %}
{% tab label="Javascript" %}

The client object has a few useful convenience methods.

```javascript
await client.reset() # Empties and completely resets the database. ⚠️ This is destructive and not reversible.
```

{% /tab %}

{% /tabs %}

## Running Chroma in client-server mode

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Chroma can also be configured to run in client/server mode. In this mode, the Chroma client connects to a Chroma server running in a separate process.

To start the Chroma server, run the following command:

```bash
chroma run --path /db_path
```

Then use the Chroma HTTP client to connect to the server:

```python
import chromadb
chroma_client = chromadb.HttpClient(host='localhost', port=8000)
```

That's it! Chroma's API will run in `client-server` mode with just this change.

---

Chroma also provides an async HTTP client. The behaviors and method signatures are identical to the synchronous client, but all methods that would block are now async. To use it, call `AsyncHttpClient` instead:

```python
import asyncio
import chromadb

async def main():
    client = await chromadb.AsyncHttpClient()
    collection = await client.create_collection(name="my_collection")

    await collection.add(
        documents=["hello world"],
        ids=["id1"]
    )

asyncio.run(main())
```

<!-- #### Run Chroma inside your application

To run the Chroma docker from inside your application code, create a docker-compose file or add to the existing one you have.

1. Download [`docker-compose.server.example.yml`](https://github.com/chroma-core/chroma/blob/main/docker-compose.server.example.yml) file and [`config`](https://github.com/chroma-core/chroma/tree/main/config) folder along with both the files inside from [GitHub Repo](https://github.com/chroma-core/chroma)
2. Rename `docker-compose.server.example.yml` to `docker-compose.yml`
3. Install docker on your local machine. [`Docker Engine`](https://docs.docker.com/engine/install/) or [`Docker Desktop`](https://docs.docker.com/desktop/install/)
4. Install docker compose [`Docker Compose`](https://docs.docker.com/compose/install/)

Use following command to manage Dockerized Chroma:
- __Command to Start Chroma__: `docker-compose up -d`
- __Command to Stop Chroma__: `docker-compose down`
- __Command to Stop Chroma and delete volumes__
This is distructive command. With this command volumes created earlier will be deleted along with data stored.: `docker-compose down -v` -->

#### Using the Python HTTP-only client

If you are running Chroma in client-server mode, you may not need the full Chroma library. Instead, you can use the lightweight client-only library.
In this case, you can install the `chromadb-client` package. This package is a lightweight HTTP client for the server with a minimal dependency footprint.

```python
pip install chromadb-client
```

```python
import chromadb
# Example setup of the client to connect to your chroma server
client = chromadb.HttpClient(host='localhost', port=8000)

# Or for async usage:
async def main():
    client = await chromadb.AsyncHttpClient(host='localhost', port=8000)
```

Note that the `chromadb-client` package is a subset of the full Chroma library and does not include all the dependencies. If you want to use the full Chroma library, you can install the `chromadb` package instead.
Most importantly, there is no default embedding function. If you add() documents without embeddings, you must have manually specified an embedding function and installed the dependencies for it.

{% /tab %}
{% tab label="Javascript" %}

To run Chroma in client server mode, first install the chroma library and CLI via pypi:

```bash
pip install chromadb
```

Then start the Chroma server:

```bash
chroma run --path /db_path
```

The JS client then talks to the chroma server backend.

```js
// CJS
const { ChromaClient } = require("chromadb");

// ESM
import { ChromaClient } from "chromadb";

const client = new ChromaClient();
```

You can also run the Chroma server in a docker container, or deployed to a cloud provider. See the [deployment docs](./deployment.md) for more information.

{% /tab %}

{% /tabs %}

## Using collections

Chroma lets you manage collections of embeddings, using the `collection` primitive.

### Creating, inspecting, and deleting Collections

Chroma uses collection names in the url, so there are a few restrictions on naming them:

- The length of the name must be between 3 and 63 characters.
- The name must start and end with a lowercase letter or a digit, and it can contain dots, dashes, and underscores in between.
- The name must not contain two consecutive dots.
- The name must not be a valid IP address.

Chroma collections are created with a name and an optional embedding function. If you supply an embedding function, you must supply it every time you get the collection.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection = client.create_collection(name="my_collection", embedding_function=emb_fn)
collection = client.get_collection(name="my_collection", embedding_function=emb_fn)
```

{% note type="caution" %}
If you later wish to `get_collection`, you MUST do so with the embedding function you supplied while creating the collection
{% /note %}

The embedding function takes text as input, and performs tokenization and embedding. If no embedding function is supplied, Chroma will use [sentence transformer](https://www.sbert.net/index.html) as a default.

{% /tab %}
{% tab label="Javascript" %}

```js
// CJS
const { ChromaClient } = require("chromadb");

// ESM
import { ChromaClient } from "chromadb";
```

The JS client talks to a chroma server backend. This can run on your local computer or be easily deployed to AWS.

```js
let collection = await client.createCollection({
  name: "my_collection",
  embeddingFunction: emb_fn,
});
let collection2 = await client.getCollection({
  name: "my_collection",
  embeddingFunction: emb_fn,
});
```

{% note type="caution" %}
If you later wish to `getCollection`, you MUST do so with the embedding function you supplied while creating the collection
{% /note %}

The embedding function takes text as input, and performs tokenization and embedding.

{% /tab %}

{% /tabs %}

You can learn more about [🧬 embedding functions](./guides/embeddings), and how to create your own.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Existing collections can be retrieved by name with `.get_collection`, and deleted with `.delete_collection`. You can also use `.get_or_create_collection` to get a collection if it exists, or create it if it doesn't.

```python
collection = client.get_collection(name="test") # Get a collection object from an existing collection, by name. Will raise an exception if it's not found.
collection = client.get_or_create_collection(name="test") # Get a collection object from an existing collection, by name. If it doesn't exist, create it.
client.delete_collection(name="my_collection") # Delete a collection and all associated embeddings, documents, and metadata. ⚠️ This is destructive and not reversible
```

{% /tab %}
{% tab label="Javascript" %}

Existing collections can be retrieved by name with `.getCollection`, and deleted with `.deleteCollection`.

```javascript
const collection = await client.getCollection({ name: "test" }); // Get a collection object from an existing collection, by name. Will raise an exception of it's not found.
collection = await client.getOrCreateCollection({ name: "test" }); // Get a collection object from an existing collection, by name. If it doesn't exist, create it.
await client.deleteCollection(collection); // Delete a collection and all associated embeddings, documents, and metadata. ⚠️ This is destructive and not reversible
```

{% /tab %}

{% /tabs %}

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Collections have a few useful convenience methods.

```python
collection.peek() # returns a list of the first 10 items in the collection
collection.count() # returns the number of items in the collection
collection.modify(name="new_name") # Rename the collection
```

{% /tab %}
{% tab label="Javascript" %}

There are a few useful convenience methods for working with Collections.

```javascript
await client.peekRecords(collection); // returns a list of the first 10 items in the collection
await client.countRecords(collection); // returns the number of items in the collection
```

{% /tab %}

{% /tabs %}

### Changing the distance function

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

`create_collection` also takes an optional `metadata` argument which can be used to customize the distance method of the embedding space by setting the value of `hnsw:space`.

```python
 collection = client.create_collection(
        name="collection_name",
        metadata={"hnsw:space": "cosine"} # l2 is the default
    )
```

{% /tab %}
{% tab label="Javascript" %}

`createCollection` also takes an optional `metadata` argument which can be used to customize the distance method of the embedding space by setting the value of `hnsw:space`

```js
let collection = client.createCollection({
  name: "collection_name",
  metadata: { "hnsw:space": "cosine" },
});
```

{% /tab %}

{% /tabs %}

Valid options for `hnsw:space` are "l2", "ip, "or "cosine". The **default** is "l2" which is the squared L2 norm.

{% special_table %}
{% /special_table %}

| Distance          | parameter |                                                                                                                                                            Equation |
| ----------------- | :-------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| Squared L2        |   `l2`    |                                                                                                 {% math latexText="d = \\sum\\left(A_i-B_i\\right)^2" %}{% /math %} |
| Inner product     |   `ip`    |                                                                                    {% math latexText="d = 1.0 - \\sum\\left(A_i \\times B_i\\right) " %}{% /math %} |
| Cosine similarity | `cosine`  | {% math latexText="d = 1.0 - \\frac{\\sum\\left(A_i \\times B_i\\right)}{\\sqrt{\\sum\\left(A_i^2\\right)} \\cdot \\sqrt{\\sum\\left(B_i^2\\right)}}" %}{% /math %} |

### Adding data to a Collection

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

When ingesting data for the first time, we recommend using the `.bulk_upsert()` method as it's optimized for large amounts of data (it also displays a progress bar by default):

```python
collection.bulk_upsert(
    documents=["lorem ipsum...", "doc2", "doc3", ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    ids=["id1", "id2", "id3", ...]
)
```

You can also add data to Chroma with `.add()`:

```python
collection.add(
    documents=["lorem ipsum...", "doc2", "doc3", ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    ids=["id1", "id2", "id3", ...]
)
```

Note that `.bulk_upsert()` is not atomic: if it fails partway through some of your data will have been added or updated. You should use `.add()` or `.upsert()` if you require atomicity for your application.

{% /tab %}
{% tab label="Javascript" %}

When ingesting data for the first time, we recommend using the `.bulkUpsertRecords()` method as it's optimized for large amounts of data (it also displays a progress bar by default):

```javascript
await client.bulkUpsertRecords(collection, {
    ids: ["id1", "id2", "id3", ...],
    metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    documents: ["lorem ipsum...", "doc2", "doc3", ...],
})
```

You can also add data to Chroma with `.addRecords()`:

```javascript
await client.addRecords(collection, {
    ids: ["id1", "id2", "id3", ...],
    metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    documents: ["lorem ipsum...", "doc2", "doc3", ...],
})
```

Note that `.bulkUpsertRecords()` is not atomic: if it fails partway through some of your data will have been added or updated. You should use `.addRecords()` or `.upsertRecords()` if you require atomicity for your application.

{% /tab %}

{% /tabs %}

If Chroma is passed a list of `documents`, it will automatically tokenize and embed them with the collection's embedding function (the default will be used if none was supplied at collection creation). Chroma will also store the `documents` themselves. If the documents are too large to embed using the chosen embedding function, an exception will be raised.

Each document must have a unique associated `id`. Trying to `.add` the same ID twice will result in only the initial value being stored. An optional list of `metadata` dictionaries can be supplied for each document, to store additional information and enable filtering.

Alternatively, you can supply a list of document-associated `embeddings` directly, and Chroma will store the associated documents without embedding them itself.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection.add(
    documents=["doc1", "doc2", "doc3", ...],
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    ids=["id1", "id2", "id3", ...]
)
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
await client.addRecords(collection, {
    ids: ["id1", "id2", "id3", ...],
    embeddings: [[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    documents: ["lorem ipsum...", "doc2", "doc3", ...],
})

```

{% /tab %}

{% /tabs %}

If the supplied `embeddings` are not the same dimension as the collection, an exception will be raised.

You can also store documents elsewhere, and just supply a list of `embeddings` and `metadata` to Chroma. You can use the `ids` to associate the embeddings with your documents stored elsewhere.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection.add(
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    ids=["id1", "id2", "id3", ...]
)
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
await client.addRecords(collection, {
    ids: ["id1", "id2", "id3", ...],
    embeddings: [[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
})
```

{% /tab %}

{% /tabs %}

### Querying a Collection

You can query by a set of `query_embeddings`.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Chroma collections can be queried in a variety of ways, using the `.query` method.

```python
collection.query(
    query_embeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    n_results=10,
    where={"metadata_field": "is_equal_to_this"},
    where_document={"$contains":"search_string"}
)
```

{% /tab %}
{% tab label="Javascript" %}

Chroma collections can be queried in a variety of ways, using the `.queryRecords` method.

```javascript
const result = await client.queryRecords(collection, {
    queryEmbeddings: [[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    nResults: 10,
    where: {"metadata_field": "is_equal_to_this"},
})
// input order
// queryEmbeddings - optional, exactly one of queryEmbeddings and queryTexts must be provided
// queryTexts - optional
// n_results - required
// where - optional
```

{% /tab %}

{% /tabs %}

The query will return the `n_results` closest matches to each `query_embedding`, in order.
An optional `where` filter dictionary can be supplied to filter by the `metadata` associated with each document.
Additionally, an optional `where_document` filter dictionary can be supplied to filter by contents of the document.

If the supplied `query_embeddings` are not the same dimension as the collection, an exception will be raised.

You can also query by a set of `query_texts`. Chroma will first embed each `query_text` with the collection's embedding function, and then perform the query with the generated embedding.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection.query(
    query_texts=["doc10", "thus spake zarathustra", ...],
    n_results=10,
    where={"metadata_field": "is_equal_to_this"},
    where_document={"$contains":"search_string"}
)
```

You can also retrieve items from a collection by `id` using `.get`.

```python
collection.get(
	ids=["id1", "id2", "id3", ...],
	where={"style": "style1"}
)
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
await client.queryRecords(collection, {
    nResults: 10, // n_results
    where: {"metadata_field": "is_equal_to_this"}, // where
    queryTexts: ["doc10", "thus spake zarathustra", ...], // query_text
})
```

You can also retrieve records from a collection by `id` using `.getRecords`.

```javascript
await client.getRecords(collection, {
	ids: ["id1", "id2", "id3", ...], //ids
	where: {"style": "style1"} // where
})
```

{% /tab %}

{% /tabs %}

`.get` also supports the `where` and `where_document` filters. If no `ids` are supplied, it will return all items in the collection that match the `where` and `where_document` filters.

##### Choosing which data is returned

When using get or query you can use the include parameter to specify which data you want returned - any of `embeddings`, `documents`, `metadatas`, and for query, `distances`. By default, Chroma will return the `documents`, `metadatas` and in the case of query, the `distances` of the results. `embeddings` are excluded by default for performance and the `ids` are always returned. You can specify which of these you want returned by passing an array of included field names to the includes parameter of the query or get method.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
# Only get documents and ids
collection.get(
    include=["documents"]
)

collection.query(
    query_embeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    include=["documents"]
)
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
# Only get documents and ids
client.getRecords(collection,
    {include=["documents"]}
)

client.getRecords(collection, {
    queryEmbeddings=[[11.1, 12.1, 13.1],[1.1, 2.3, 3.2], ...],
    include=["documents"]
})
```

{% /tab %}

{% /tabs %}

### Using Where filters

Chroma supports filtering queries by `metadata` and `document` contents. The `where` filter is used to filter by `metadata`, and the `where_document` filter is used to filter by `document` contents.

##### Filtering by metadata

In order to filter on metadata, you must supply a `where` filter dictionary to the query. The dictionary must have the following structure:

```python
{
    "metadata_field": {
        <Operator>: <Value>
    }
}
```

Filtering metadata supports the following operators:

- `$eq` - equal to (string, int, float)
- `$ne` - not equal to (string, int, float)
- `$gt` - greater than (int, float)
- `$gte` - greater than or equal to (int, float)
- `$lt` - less than (int, float)
- `$lte` - less than or equal to (int, float)

Using the $eq operator is equivalent to using the `where` filter.

```python
{
    "metadata_field": "search_string"
}

# is equivalent to

{
    "metadata_field": {
        "$eq": "search_string"
    }
}

```

{% note type="note" %}
Where filters only search embeddings where the key exists. If you search `collection.get(where={"version": {"$ne": 1}})`. Metadata that does not have the key `version` will not be returned.
{% /note %}

##### Filtering by document contents

In order to filter on document contents, you must supply a `where_document` filter dictionary to the query. We support two filtering keys: `$contains` and `$not_contains`. The dictionary must have the following structure:

```python
# Filtering for a search_string
{
    "$contains": "search_string"
}
```

```python
# Filtering for not contains
{
    "$not_contains": "search_string"
}
```

##### Using logical operators

You can also use the logical operators `$and` and `$or` to combine multiple filters.

An `$and` operator will return results that match all of the filters in the list.

```python
{
    "$and": [
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        },
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        }
    ]
}
```

An `$or` operator will return results that match any of the filters in the list.

```python
{
    "$or": [
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        },
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        }
    ]
}
```

##### Using inclusion operators (`$in` and `$nin`)

The following inclusion operators are supported:

- `$in` - a value is in predefined list (string, int, float, bool)
- `$nin` - a value is not in predefined list (string, int, float, bool)

An `$in` operator will return results where the metadata attribute is part of a provided list:

```json
{
  "metadata_field": {
    "$in": ["value1", "value2", "value3"]
  }
}
```

An `$nin` operator will return results where the metadata attribute is not part of a provided list:

```json
{
  "metadata_field": {
    "$nin": ["value1", "value2", "value3"]
  }
}
```

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

{% note type="note" title="Practical examples" %}
For additional examples and a demo how to use the inclusion operators, please see provided notebook [here](https://github.com/chroma-core/chroma/blob/main/examples/basic_functionality/in_not_in_filtering.ipynb)
{% /note %}

{% /tab %}
{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

### Updating data in a collection

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Any property of records in a collection can be updated using `.update`.

```python
collection.update(
    ids=["id1", "id2", "id3", ...],
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    documents=["doc1", "doc2", "doc3", ...],
)
```

{% /tab %}
{% tab label="Javascript" %}

Any property of records in a collection can be updated using `.updateRecords`.

```javascript
client.updateRecords(
    collection,
    {
      ids: ["id1", "id2", "id3", ...],
      embeddings: [[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
      metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
      documents: ["doc1", "doc2", "doc3", ...],
    },
)
```

{% /tab %}

{% /tabs %}

If an `id` is not found in the collection, an error will be logged and the update will be ignored. If `documents` are supplied without corresponding `embeddings`, the embeddings will be recomputed with the collection's embedding function.

If the supplied `embeddings` are not the same dimension as the collection, an exception will be raised.

Chroma also supports an `upsert` operation, which updates existing items, or adds them if they don't yet exist.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection.upsert(
    ids=["id1", "id2", "id3", ...],
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    documents=["doc1", "doc2", "doc3", ...],
)
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
await client.upsertRecords(collection, {
  ids: ["id1", "id2", "id3"],
  embeddings: [
    [1.1, 2.3, 3.2],
    [4.5, 6.9, 4.4],
    [1.1, 2.3, 3.2],
  ],
  metadatas: [
    { chapter: "3", verse: "16" },
    { chapter: "3", verse: "5" },
    { chapter: "29", verse: "11" },
  ],
  documents: ["doc1", "doc2", "doc3"],
});
```

{% /tab %}

{% /tabs %}

If an `id` is not present in the collection, the corresponding items will be created as per `add`. Items with existing `id`s will be updated as per `update`.

### Deleting data from a collection

Chroma supports deleting items from a collection by `id` using `.delete`. The embeddings, documents, and metadata associated with each item will be deleted.
⚠️ Naturally, this is a destructive operation, and cannot be undone.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection.delete(
    ids=["id1", "id2", "id3",...],
	where={"chapter": "20"}
)
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
await client.deleteRecords(collection, {
    ids: ["id1", "id2", "id3",...], //ids
	where: {"chapter": "20"} //where
})
```

{% /tab %}

{% /tabs %}

`.delete` also supports the `where` filter. If no `ids` are supplied, it will delete all items in the collection that match the `where` filter.
