## chromadb

Chroma is the open-source embedding database. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

This package gives you a JS/TS interface to talk to a backend Chroma DB over REST.

**Note:** JS client version 3._ is only compatible with chromadb v1.0.6 and newer or Chroma Cloud. For prior version compatiblity, please use JS client version 2._.

[Learn more about Chroma](https://github.com/chroma-core/chroma)

- [💬 Community Discord](https://discord.gg/MMeYNTmh3x)
- [📖 Documentation](https://docs.trychroma.com/)
- [💡 Colab Example](https://colab.research.google.com/drive/1QEzFyqnoFxq7LUGyP1vzR4iLt9PpCDXv?usp=sharing)
- [🏠 Homepage](https://www.trychroma.com/)

## Package Options

There are two packages available for using ChromaDB in your JavaScript/TypeScript projects:

1. **chromadb**: Includes all embedding libraries as bundled dependencies.

   - Use this if you want a simple installation without worrying about dependency management.
   - Install with: `npm install chromadb` or `pnpm add chromadb`

2. **chromadb-client**: Provides embedding libraries as peer dependencies.
   - Use this if you want to manage your own versions of embedding libraries, or embed outside of Chroma.
   - Keeps your dependency tree lean by not bundling dependencies you don't use.
   - Install with: `npm install chromadb-client` or `pnpm add chromadb-client`
   - You'll need to install any required embedding libraries separately, e.g., `npm install chromadb-client chromadb-default-embed`

Both packages provide identical functionality, differing only in how dependencies are managed.

## Getting started

Chroma needs to be running in order for this client to talk to it. Please see the [🧪 Usage Guide](https://docs.trychroma.com/guides) to learn how to quickly stand this up.

## Small example

```js
import { ChromaClient } from "chromadb"; // or "chromadb-client"
const chroma = new ChromaClient({ path: "http://localhost:8000" });
const collection = await chroma.createCollection({ name: "test-from-js" });
for (let i = 0; i < 20; i++) {
  await collection.add({
    ids: ["test-id-" + i.toString()],
    embeddings: [1, 2, 3, 4, 5],
    documents: ["test"],
  });
}
const queryData = await collection.query({
  queryEmbeddings: [1, 2, 3, 4, 5],
  queryTexts: ["test"],
});
```

## Local development

[View the Development Readme](./DEVELOP.md)

## License

Apache 2.0
