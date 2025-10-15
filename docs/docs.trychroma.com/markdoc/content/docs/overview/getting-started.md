---
id: getting-started
name: Getting Started
---

# Getting Started

Chroma is an AI-native open-source vector database. It comes with everything you need to get started built-in, and runs on your machine.

{% Tabs %}

{% Tab label="python" %}
{% Video link="https://www.youtube.com/embed/yvsmkx-Jaj0" title="Getting Started Video" / %}
{% /Tab %}

{% Tab label="typescript" %}
{% Video link="https://www.youtube.com/embed/I1Xr1okBREc" title="Getting Started Video" / %}
{% /Tab %}

{% /Tabs %}

{% Banner type="tip" %}

For production, Chroma offers [Chroma Cloud](https://trychroma.com/signup?utm_source=docs-getting-started) - a fast, scalable, and serverless database-as-a-service. Get started in 30 seconds - $5 in free credits included.

{% /Banner %}

### 1. Install

{% Tabs %}

{% Tab label="python" %}

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="pip" %}

```terminal
pip install chromadb
```

{% /Tab %}

{% Tab label="poetry" %}

```terminal
poetry add chromadb
```

{% /Tab %}

{% Tab label="uv" %}

```terminal
uv pip install chromadb
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

{% /Tab %}

{% Tab label="typescript" %}

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="npm" %}

```terminal
npm install chromadb @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="pnpm" %}

```terminal
pnpm add chromadb @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="yarn" %}

```terminal
yarn add chromadb @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="bun" %}

```terminal
bun add chromadb @chroma-core/default-embed
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

{% /Tab %}

{% /Tabs %}

### 2. Create a Chroma Client

{% Tabs %}

{% Tab label="python" %}

```python
import chromadb
chroma_client = chromadb.Client()
```

{% /Tab %}
{% Tab label="typescript" %}

Run the Chroma backend:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="CLI" %}

```terminal
chroma run --path ./getting-started
```

{% /Tab %}

{% Tab label="Docker" %}

```terminal
docker pull chromadb/chroma
docker run -p 8000:8000 chromadb/chroma
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Then create a client which connects to it:

{% TabbedUseCaseCodeBlock language="typescript" %}

{% Tab label="ESM" %}

```typescript
import { ChromaClient } from "chromadb";
const client = new ChromaClient();
```

{% /Tab %}

{% Tab label="CJS" %}

```typescript
const { ChromaClient } = require("chromadb");
const client = new ChromaClient();
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

{% /Tab %}

{% /Tabs %}

### 3. Create a collection

Collections are where you'll store your embeddings, documents, and any additional metadata. Collections index your embeddings and documents, and enable efficient retrieval and filtering. You can create a collection with a name:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection = chroma_client.create_collection(name="my_collection")
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

### 4. Add some text documents to the collection

Chroma will store your text and handle embedding and indexing automatically. You can also customize the embedding model. You must provide unique string IDs for your documents.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
collection.add(
    ids=["id1", "id2"],
    documents=[
        "This is a document about pineapple",
        "This is a document about oranges"
    ]
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
await collection.add({
  ids: ["id1", "id2"],
  documents: [
    "This is a document about pineapple",
    "This is a document about oranges",
  ],
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

### 5. Query the collection

You can query the collection with a list of query texts, and Chroma will return the `n` most similar results. It's that easy!

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
results = collection.query(
    query_texts=["This is a query document about hawaii"], # Chroma will embed this for you
    n_results=2 # how many results to return
)
print(results)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
const results = await collection.query({
  queryTexts: "This is a query document about hawaii", // Chroma will embed this for you
  nResults: 2, // how many results to return
});

console.log(results);
```

{% /Tab %}

{% /TabbedCodeBlock %}

If `n_results` is not provided, Chroma will return 10 results by default. Here we only added 2 documents, so we set `n_results=2`.

### 6. Inspect Results

From the above - you can see that our query about `hawaii` is semantically most similar to the document about `pineapple`.

{% TabbedCodeBlock %}

{% Tab label="python" %}

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

{% /Tab %}

{% Tab label="typescript" %}

```typescript
{
    documents: [
        [
            'This is a document about pineapple',
            'This is a document about oranges'
        ]
    ],
    ids: [
        ['id1', 'id2']
    ],
    distances: [[1.0404009819030762, 1.243080496788025]],
    uris: null,
    data: null,
    metadatas: [[null, null]],
    embeddings: null
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

### 7. Try it out yourself

What if we tried querying with `"This is a document about florida"`? Here is a full example.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
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

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { ChromaClient } from "chromadb";
const client = new ChromaClient();

// switch `createCollection` to `getOrCreateCollection` to avoid creating a new collection every time
const collection = await client.getOrCreateCollection({
  name: "my_collection",
});

// switch `addRecords` to `upsertRecords` to avoid adding the same documents every time
await collection.upsert({
  documents: [
    "This is a document about pineapple",
    "This is a document about oranges",
  ],
  ids: ["id1", "id2"],
});

const results = await collection.query({
  queryTexts: ["This is a query document about florida"], // Chroma will embed this for you
  nResults: 2, // how many results to return
});

console.log(results);
```

{% /Tab %}

{% /TabbedCodeBlock %}

{% Tabs %}

{% Tab label="python" %}

## Next steps

In this guide we used Chroma's [ephemeral client](../run-chroma/ephemeral-client) for simplicity. It starts a Chroma server in-memory, so any data you ingest will be lost when your program terminates. You can use the [persistent client](../run-chroma/persistent-client) or run Chroma in [client-server mode](../run-chroma/client-server) if you need data persistence.

- Learn how to [Deploy Chroma](../../guides/deploy/client-server-mode) to a server
- Join Chroma's [Discord Community](https://discord.com/invite/MMeYNTmh3x) to ask questions and get help
- Follow Chroma on [X (@trychroma)](https://twitter.com/trychroma) for updates

{% /Tab %}

{% Tab label="typescript" %}
## Next steps

- We offer [first class support](/docs/embeddings/embedding-functions) for various embedding providers via our embedding function interface. Each embedding function ships in its own npm package.
- Learn how to [Deploy Chroma](../../guides/deploy/client-server-mode) to a server
- Join Chroma's [Discord Community](https://discord.com/invite/MMeYNTmh3x) to ask questions and get help
- Follow Chroma on [X (@trychroma)](https://twitter.com/trychroma) for updates

{% /Tab %}

{% /Tabs %}