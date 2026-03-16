# ChromaDB JavaScript Client

Chroma is the open-source embedding database. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

**This package includes all embedding libraries as bundled dependencies**, providing a simple installation experience without worrying about dependency management. For a thin client, install `chromadb-client`

## Features

- ✅ Complete TypeScript support
- ✅ All embedding libraries included as bundled dependencies
- ✅ Works in both Node.js and browser environments
- ✅ Simple installation with no peer dependency requirements

## Installation

```bash
# npm
npm install chromadb

# pnpm
pnpm add chromadb

# yarn
yarn add chromadb
```

## Getting Started

Chroma needs to be running in order for this client to talk to it. Please see the [Usage Guide](https://docs.trychroma.com/guides) to learn how to quickly stand this up.

```js
import { ChromaClient } from "chromadb";

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

This package includes all embedding libraries as bundled dependencies, so you can use them directly:

```js
import { ChromaClient, OpenAIEmbeddingFunction } from "chromadb";

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

## Additional Resources

- [📖 Documentation](https://docs.trychroma.com/)
- [💬 Community Discord](https://discord.gg/MMeYNTmh3x)
- [🏠 Homepage](https://www.trychroma.com/)
- [GitHub Repository](https://github.com/chroma-core/chroma)

## License

Apache 2.0
