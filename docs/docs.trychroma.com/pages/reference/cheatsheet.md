---
title: "📖 API Cheatsheet"
---

# 📖 API Cheatsheet

{% note type="note" %}
This is a quick cheatsheet of the API. For full API docs, refer to the JS and Python docs in the sidebar.
{% /note %}

---

{% tabs group="code-lang" hideContent=true %}
{% tab label="Python" %}
{% /tab %}
{% tab label="Javascript" %}
{% /tab %}
{% /tabs %}

---

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

## Initialize client - Python

### In-memory chroma

```python
import chromadb
client = chromadb.Client()
```

### In-memory chroma with saving/loading to disk

In this mode, Chroma will persist data between sessions. On load - it will load up the data in the directory you specify. And as you add data - it will save to that directory.

```python
import chromadb
client = chromadb.PersistentClient(path="/path/to/data")
```

### Run chroma just as a client to talk to a backend service

You can run Chroma a standalone Chroma server using the Chroma command line. Run `chroma run --path /db_path` to run a server.

Then update your API initialization and then use the API the same way as before.

```python
import chromadb
chroma_client = chromadb.HttpClient(host="localhost", port=8000)
```

## Methods on Client

### Methods related to Collections

{% note type="note" title="Collection naming" %}
Collections are similar to AWS s3 buckets in their naming requirements because they are used in URLs in the REST API. Here's the [full list](/usage-guide#creating-inspecting-and-deleting-collections).
{% /note %}

```python
# list all collections
client.list_collections()

# make a new collection
collection = client.create_collection("testname")

# get an existing collection
collection = client.get_collection("testname")

# get a collection or create if it doesn't exist already
collection = client.get_or_create_collection("testname")

# delete a collection
client.delete_collection("testname")
```

### Utility methods

```python
# resets entire database - this *cant* be undone!
client.reset()

# returns timestamp to check if service is up
client.heartbeat()
```

## Methods on Collection

```python
# change the name or metadata on a collection
collection.modify(name="testname2")

# get the number of items in a collection
collection.count()

# add new items to a collection
# either one at a time
collection.add(
    metadatas={"uri": "img9.png", "style": "style1"},
    documents="doc1000101",
)
# or many, up to 100k+!
collection.add(
    embeddings=[[1.5, 2.9, 3.4], [9.8, 2.3, 2.9]],
    metadatas=[{"style": "style1"}, {"style": "style2"}]
)
collection.add(
    documents=["doc1000101", "doc288822"],
    metadatas=[{"style": "style1"}, {"style": "style2"}]
)

# update items in a collection
collection.update()

# upsert items. new items will be added, existing items will be updated.
collection.upsert(
    ids=["id1", "id2", "id3", ...],
    embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], ...],
    metadatas=[{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}, ...],
    documents=["doc1", "doc2", "doc3", ...],
)

# get items from a collection
collection.get()

# convenience, get first 5 items from a collection
collection.peek()

# do nearest neighbor search to find similar embeddings or documents, supports filtering
collection.query(
    query_embeddings=[[1.1, 2.3, 3.2], [5.1, 4.3, 2.2]],
    n_results=2,
    where={"style": "style2"}
)

# delete items
collection.delete()
```

{% /tab %}
{% tab label="Javascript" %}

### Run the backend

Run `chroma run --path /db_path` to run the Chroma backend as a standalone server on your local computer.

## Initialize client - JS

```javascript
// CJS
const { ChromaClient } = require("chromadb");

// ESM
import { ChromaClient } from 'chromadb'

const client = new ChromaClient();
```

## Methods on Client

### Methods related to Collections

{% note type="note" title="Collection naming" %}
Collections are similar to AWS s3 buckets in their naming requirements because they are used in URLs in the REST API. Here's the [full list](/usage-guide#creating-inspecting-and-deleting-collections).
{% /note %}

```javascript
// list all collections
await client.listCollections();

// make a new collection
const collection = await client.createCollection({ name: "testname" });

// get an existing collection
const collection = await client.getCollection({ name: "testname" });

// delete a collection
await client.deleteCollection({ name: "testname" });
```

### Utility methods

```javascript
// resets entire database - this *cant* be undone!
await client.reset();
```

## Methods on Collection

```javascript
// get the number of items in a collection
await collection.count()

// add new items to a collection
// either one at a time
await collection.add({
    embeddings: [1.5, 2.9, 3.4],
    metadatas: {"source": "my_source"},
    documents: "This is a document",
})
// or many, up to 100k+!
await collection.add({
    embeddings: [[1.5, 2.9, 3.4], [9.8, 2.3, 2.9]],
    metadatas: [{"style": "style1"}, {"style": "style2"}],
    documents: ["This is a document", 'that is a document']
})
// including just documents
await collection.add({
    metadatas: [{"style": "style1"}, {"style": "style2"}],
    documents: ["doc1000101", "doc288822"],
})
// or use upsert, so records will be updated if they already exist
// (instead of throwing an error)
await collection.upsert({
    ids: "id1",
    embeddings: [1.5, 2.9, 3.4],
    metadatas: {"source": "my_source"},
    documents: "This is a document",
})

// get items from a collection
await collection.get()

// convenience, get first 5 items from a collection
await collection.peek()

// do nearest neighbor search to find similar embeddings or documents, supports filtering
await collection.query({
    queryEmbeddings: [[1.1, 2.3, 3.2], [5.1, 4.3, 2.2]],
    nResults: 2,
    where: {"style": "style2"}
})

// delete items
await collection.delete()

```

{% /tab %}
{% /tabs %}
