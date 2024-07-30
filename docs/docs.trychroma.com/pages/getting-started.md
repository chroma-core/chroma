---
title: "🔑 Getting Started"
---

{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

Chroma is an AI-native open-source vector database. It comes with everything you need to get started built in, and runs on your machine. A [hosted version](https://airtable.com/shrOAiDUtS2ILy5vZ) is coming soon!

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

```python
import chromadb
chroma_client = chromadb.Client()
```

{% /tab %}
{% tab label="Javascript" %}

Run the Chroma backend:

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

Then create a client which connects to it:

{% codetabs customHeader="js" %}
{% codetab label="ESM" %}

```js {% codetab=true %}
import { ChromaClient } from "chromadb";
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

Collections are where you'll store your embeddings, documents, and any additional metadata. You can create a collection with a name:

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection = chroma_client.create_collection(name="my_collection")
```

{% /tab %}
{% tab label="Javascript" %}

```js
const collection = await client.createCollection({
  name: "my_collection",
});
```

{% /tab %}

{% /tabs %}

### 4. Add some text documents to the collection

Chroma will store your text and handle embedding and indexing automatically. You can also customize the embedding model.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
collection.add(
    documents=[
        "This is a document about pineapple",
        "This is a document about oranges"
    ],
    ids=["id1", "id2"]
)
```

{% /tab %}
{% tab label="Javascript" %}

```js
await client.addRecords(collection, {
  documents: [
    "This is a document about pineapple",
    "This is a document about oranges",
  ],
  ids: ["id1", "id2"],
});
```

{% /tab %}

{% /tabs %}

### 5. Query the collection

You can query the collection with a list of query texts, and Chroma will return the `n` most similar results. It's that easy!

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
results = collection.query(
    query_texts=["This is a query document about hawaii"], # Chroma will embed this for you
    n_results=2 # how many results to return
)
print(results)
```

{% /tab %}
{% tab label="Javascript" %}

```js
const results = await client.queryRecords(collection, {
  queryTexts: "This is a query document about hawaii", // Chroma will embed this for you
  nResults: 2, // how many results to return
});

console.log(results);
```

{% /tab %}

{% /tabs %}

### 6. Inspect Results

From the above query - you can see that our query about `hawaii` is the semantically most similar to the document about `pineapple`. This, intuitively, makes sense!

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
{
  'documents': [[
      'This is a document about pineapple',
      'This is a document about oranges'
  ]],
  'ids': [['id1', 'id2']],
  'distances': [[1.0404009819030762, 1.243080496788025]],
  'uris': None,
  'data': None,
  'metadatas': [[None, None]],
  'embeddings': None,
}
```

{% /tab %}
{% tab label="Javascript" %}

```js
{
  'documents': [[
      'This is a document about pineapple',
      'This is a document about oranges'
  ]],
  'ids': [['id1', 'id2']],
  'distances': [[1.0404009819030762, 1.243080496788025]],
  'uris': null,
  'data': null,
  'metadatas': [[null, null]],
  'embeddings': null,
}
```

{% /tab %}

{% /tabs %}

### 7. Try it out yourself

For example - what if we tried querying with `"This is a document about florida"`?

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```py
import chromadb
chroma_client = chromadb.Client()

# switch `create_collection` to `get_or_create_collection` to avoid creating a new collection every time
collection = chroma_client.get_or_create_collection(name="my_collection")

# switch `add` to `upsert` to avoid adding the same documents every time
collection.upsert(
    documents=[
        "This is a document about pineapple",
        "This is a document about oranges"
    ],
    ids=["id1", "id2"]
)

results = collection.query(
    query_texts=["This is a query document about florida"], # Chroma will embed this for you
    n_results=2 # how many results to return
)

print(results)
```

{% /tab %}
{% tab label="Javascript" %}

```js
import { ChromaClient } from "chromadb";
const client = new ChromaClient();

// switch `createCollection` to `getOrCreateCollection` to avoid creating a new collection every time
const collection = await client.getOrCreateCollection({
  name: "my_collection",
});

// switch `addRecords` to `upsertRecords` to avoid adding the same documents every time
await client.upsertRecords(collection, {
  documents: [
    "This is a document about pineapple",
    "This is a document about oranges",
  ],
  ids: ["id1", "id2"],
});

const results = await client.queryRecords(collection, {
  queryTexts: "This is a query document about florida", // Chroma will embed this for you
  nResults: 2, // how many results to return
});

console.log(results);
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
