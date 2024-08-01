---
title: "🔑 Getting Started"
---





{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

Chroma is an AI-native open-source vector database. It comes with everything you need to build on your local machine and run in a production environment.

Chroma is known for it's ease-of-use and easy-to-learn API. Here's how it works:


### 1. Install

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```bash
pip install chromadb # [!code $]
```

{% /tab %}
{% tab label="Javascript" %}

{% codetabs customHeader="sh" %}
{% codetab label="yarn" %}
```bash {% codetab=true %}
yarn install chromadb chromadb-default-embed # [!code $]
```
{% /codetab %}
{% codetab label="npm" %}
```bash {% codetab=true %}
npm install --save chromadb chromadb-default-embed # [!code $]
```
{% /codetab %}
{% codetab label="pnpm" %}
```bash {% codetab=true %}
pnpm install chromadb chromadb-default-embed # [!code $]
```
{% /codetab %}
{% /codetabs %}

Install chroma via `pypi` to easily run the backend server. (Docker also available)

```bash
pip install chromadb # [!code $]
```

{% /tab %}
{% /tabs %}

### 2. Create a Chroma Client



{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

A Chroma client is how you connect to a Chroma database. In Python, a `chromadb.PersistentClient()` will save and load data to a folder on your computer that you specify. You can also easily run Chroma [as a server]() and connect to it via `chromadb.HttpClient()`.

```python
import chromadb
chroma_client = chromadb.PersistentClient(path="./chroma")
```

{% /tab %}
{% tab label="Javascript" %}

First, run Chroma through the CLI or Docker.

{% codetabs customHeader="sh" %}
{% codetab label="CLI" %}
```bash {% codetab=true %}
chroma run --path ./getting-started # [!code $]
```
{% /codetab %}
{% codetab label="Docker" %}
```bash {% codetab=true %}
docker pull chromadb/chroma # [!code $]
docker run -p 8000:8000 chromadb/chroma # [!code $]
```
{% /codetab %}
{% /codetabs %}

A Chroma client is how you connect to a Chroma database.

{% codetabs customHeader="js" %}
{% codetab label="ESM" %}
```js {% codetab=true %}
import { ChromaClient } from 'chromadb'
const client = new ChromaClient();
```
{% /codetab %}
{% codetab label="CJS" %}
```js {% codetab=true %}
const { ChromaClient } = require("chromadb");
const client = new ChromaClient();
```
{% /codetab %}
{% /codetabs %}

{% /tab %}

{% /tabs %}

### 3. Create a collection

Collections are where you store your documents, embeddings, and associated metadata. You can create a collection with a name:

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection = chroma_client.create_collection(name="my_collection")
```

{% /tab %}
{% tab label="Javascript" %}


```js
const collection = await client.createCollection({
    name: "myCollection",
});
```

{% /tab %}

{% /tabs %}

### 4. Add records to the collection

A record is made up of:
- some `data` (commonly a document, which consists of a chunk of text)
- an associated `embedding` (Chroma generates this for you by default)
- a developer-defined `ID`
- and optional `metadata`

An `embedding` is a large vector (array of floats) that represents the "meaning" of the document. When you add records to the collection, by default Chroma will automatically create embeddings for these documents. If you don’t wish to use Chroma’s built-in default embedding model, you can always [use your own]().

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection.add(
    documents=[
        "Here are some facts about pineapple",
        "Here are some facts about oranges",
        "Here are some facts about surfing"
    ],
    ids=["id1", "id2", "id3"]
)
```

{% /tab %}
{% tab label="Javascript" %}

```js
await collection.add({
    documents: [
        "Here are some facts about pineapple",
        "Here are some facts about oranges",
        "Here are some facts about surfing"
    ],
    ids: ["id1", "id2", "id3"],
});
```

{% /tab %}

{% /tabs %}

### 5. Query the collection

You can query the collection by passing an array of query texts. Chroma will create embeddings for each of these texts and then find the records that are closest in embedding space. In other words, it will find the documents with the closest "meaning" to the query text.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
results = collection.query(
    query_texts=["What should I make for a Hawaiian-themed dinner?"], # Chroma will embed this for you
    n_results=1 # how many results to return
)
print(results)
```

{% /tab %}
{% tab label="Javascript" %}

```js
const results = await collection.query({
    queryTexts: ["What should I make for a Hawaiian-themed dinner?"], // Chroma will embed this for you
    nResults: 1 // how many results to return
});

console.log(results)
```

{% /tab %}

{% /tabs %}

### 6. See Results

A query about `What should I make for a Hawaiin themed dinner?` returns the document about pineapples.

This makes sense if you consider that conceptually pineapples are most closely related to tropical places like Hawaii, and that the fact that it is edible is conceptually related to a question about food.

```js
{
  'documents': [[
      'Here are some facts about pineapple',
  ]],
  'ids': [['id1']],
  'distances': [[1.0404009819030762]],
  'uris': None,
  'data': None,
  'metadatas': [[None]],
  'embeddings': None,
}
```

The JSON has an array of ararys because you can pass multiple query texts and get multiple results.

### 7. Add Metadata

You can also add metadata to your records. This might be data that you would want to search over later (such as keywords), or data that you will need when you retrieve the original documents (such as the URL that the document originally came from).

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection.add(
        documents=[
            "Here are some facts about pineapple",
            "Here are some facts about oranges",
            "Here are some facts about surfing",
        ],
        ids=["id1", "id2", "id3"],
        metadatas=[
            {"source": "/pinapple_facts.html", "updated": 20240401},
            {"source": "/orange_tidbits.html", "updated": 20240501},
            {"source": "/surfing_stuff.html", "updated": 20231211},
        ],
    )

```

{% /tab %}
{% tab label="Javascript" %}

```js
await collection.add({
  documents: [
    "Here are some facts about pineapple",
    "Here are some facts about oranges",
    "Here are some facts about surfing",
  ],
  ids: ["id1", "id2", "id3"],
  metadatas: [
    { source: "/pinapple_facts.html", updated: 20240401 },
    { source: "/orange_tidbits.html", updated: 20240501 },
    { source: "/surfing_stuff.html", updated: 20231211 },
  ],
});

```

{% /tab %}
{% /tabs %}

### 8. More ways to search your Collection

Use `metadata search` and its operators to search over metadata fields. Use the `.get` API if you want to search metadata.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}
```python
# metadata search
results = collection.get(where={"updated": {"$gte": 20240101}})
```
{% /tab %}
{% tab label="Javascript" %}
```js
// metadata search
const recentlyUpdated = await collection.get({
    where: { updated: { $gte: 20240101 } },
});
```
{% /tab %}
{% /tabs %}

Use `document search` to do full-text search. Use the `.get` API if you want to search metadata.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}
```python
# document search
results = collection.get(where_document={"$contains": "surfing"})
```
{% /tab %}
{% tab label="Javascript" %}
```js
// metadata search
const surfingDocs = await collection.get({
    whereDocument: { $contains: "surfing" },
  });

```
{% /tab %}
{% /tabs %}

Most often, you will use 2 or 3 approaches together to find the most relevant information.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}
```python
# vector, metadata, and document search
results = collection.query(
    query_texts=["Tell me about Hawaiian things."],
    n_results=1,
    where={"updated": {"$gte": 20221211}},
    where_document={"$contains": "surfing"},
)
```
{% /tab %}
{% tab label="Javascript" %}
```js
// vector, metadata, and document search
const results = await collection.query({
    queryTexts: ["Tell me about Hawaiian things."], // Chroma will embed this for you
    nResults: 1, // how many results to return,
    where: { updated: { $gte: 20221211 } },
    whereDocument: { $contains: "surfing" },
});

```
{% /tab %}
{% /tabs %}


## 📚 Next steps

<!-- - Check out [💡 Examples](/examples) of what you can build with Chroma -->
- Read the [🧪 Usage Guide](/guides) to learn more about the API
- Learn how to [☁️ Deploy Chroma](/deployment) to a server
- Join Chroma's [Discord Community](https://discord.com/invite/MMeYNTmh3x) to ask questions and get help
- Follow Chroma on [Twitter (@trychroma)](https://twitter.com/trychroma) for updates

{% hint style="info" %}
