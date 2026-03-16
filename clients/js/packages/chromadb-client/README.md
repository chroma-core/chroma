# ChromaDB Client

Chroma is the open-source embedding database. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

**Note:** JS client version 3._ is only compatible with chromadb v1.0.6 and newer or Chroma Cloud. For prior version compatiblity, please use JS client version 2._.

**This package provides embedding libraries as peer dependencies**, allowing you to manage your own versions of embedding libraries and keep your dependency tree lean by not bundling dependencies you don't use. For a thick client with bundled embedding functions, install `chromadb`.

## Features

- ✅ Complete TypeScript support
- ✅ Embedding libraries as peer dependencies for a smaller package size
- ✅ Works in both Node.js and browser environments
- ✅ Only install the embedding libraries you need

## Installation

```bash
# npm
npm install chromadb-client

# pnpm
pnpm add chromadb-client

# yarn
yarn add chromadb-client
```

You'll need to install any required embedding libraries separately. For example:

```bash
# For OpenAI embeddings
npm install chromadb-client openai

# For default embeddings
npm install chromadb-client chromadb-default-embed

# For Cohere embeddings
npm install chromadb-client cohere-ai
```

## Getting Started

Chroma needs to be running in order for this client to talk to it. Please see the [Usage Guide](https://docs.trychroma.com/guides) to learn how to quickly stand this up.

```js
import { ChromaClient } from "chromadb-client";

// Initialize the client
const chroma = new ChromaClient({ path: "http://localhost:8000" });

// Create a collection
const collection = await chroma.createCollection({ name: "my-collection" });

// Add documents to the collection
await collection.add({
  ids: ["id1", "id2"],
  embeddings: [
    [1.1, 2.3, 3.2],
    [4.5, 6.9, 4.4],
  ],
  metadatas: [{ source: "doc1" }, { source: "doc2" }],
  documents: ["Document 1 content", "Document 2 content"],
});

// Query the collection
const results = await collection.query({
  queryEmbeddings: [1.1, 2.3, 3.2],
  nResults: 2,
});
```

## Using Embedding Functions

Make sure to install the necessary peer dependencies before using embedding functions:

```js
// First install: npm install chromadb-client openai
import { ChromaClient, OpenAIEmbeddingFunction } from "chromadb-client";

const embedder = new OpenAIEmbeddingFunction({
  openai_api_key: "your-api-key",
  model_name: "text-embedding-ada-002",
});

const chroma = new ChromaClient({ path: "http://localhost:8000" });
const collection = await chroma.createCollection({
  name: "my-collection",
  embeddingFunction: embedder,
});

// Now you can add documents without providing embeddings
await collection.add({
  ids: ["id1"],
  documents: ["Document content"],
});

// And query with text
const results = await collection.query({
  queryTexts: ["similar document"],
  nResults: 2,
});
```

## Available Embedding Functions

This package supports multiple embedding providers as peer dependencies:

- OpenAI (`openai`)
- Cohere (`cohere-ai`)
- Default embeddings (`chromadb-default-embed`)
- Google Generative AI (`@google/generative-ai`)
- Xenova Transformers (`@xenova/transformers`)
- Voyage AI (`voyageai`)
- Ollama (`ollama`)

## Why choose chromadb-client?

- **Smaller package size**: Only install the dependencies you need
- **Flexible dependency versions**: Manage your own versions of embedding libraries
- **Less bloat**: Ideal for production environments where you only use specific embedding providers
- **Same functionality**: Provides identical features as the main `chromadb` package

## Additional Resources

- [📖 Documentation](https://docs.trychroma.com/)
- [💬 Community Discord](https://discord.gg/MMeYNTmh3x)
- [🏠 Homepage](https://www.trychroma.com/)
- [GitHub Repository](https://github.com/chroma-core/chroma)

## License

Apache 2.0
