---
title: 🦜️🔗 Langchain
---

## Langchain - Python

- [LangChain + Chroma](https://blog.langchain.dev/langchain-chroma/) on the LangChain blog
- [Harrison's `chroma-langchain` demo repo](https://github.com/hwchase17/chroma-langchain)
  - [question answering over documents](https://github.com/hwchase17/chroma-langchain/blob/master/qa.ipynb) - ([Replit version](https://replit.com/@swyx/LangChainChromaStarter#main.py))
  - [to use Chroma as a persistent database](https://github.com/hwchase17/chroma-langchain/blob/master/persistent-qa.ipynb)
- Tutorials
  - [Chroma and LangChain tutorial](https://github.com/grumpyp/chroma-langchain-tutorial) - The demo showcases how to pull data from the English Wikipedia using their API. The project also demonstrates how to vectorize data in chunks and get embeddings using OpenAI embeddings model.
  - [Create a Voice-based ChatGPT Clone That Can Search on the Internet and local files](https://betterprogramming.pub/how-to-create-a-voice-based-chatgpt-clone-that-can-search-on-the-internet-24d7f570ea8)
- [LangChain's Chroma Documentation](https://python.langchain.com/docs/integrations/vectorstores/chroma)


## Langchain - JS

Here is an [example in LangChainJS](https://github.com/langchain-ai/langchainjs/blob/main/examples/src/indexes/vector_stores/chroma/fromDocs.ts)

```typescript
import { Chroma } from "@langchain/community/vectorstores/chroma";
import { OpenAIEmbeddings } from "@langchain/openai";
import { TextLoader } from "langchain/document_loaders/fs/text";

// Create docs with a loader
const loader = new TextLoader("src/document_loaders/example_data/example.txt");
const docs = await loader.load();

// Create vector store and index the docs
const vectorStore = await Chroma.fromDocuments(docs, new OpenAIEmbeddings(), {
  collectionName: "a-test-collection",
  url: "http://localhost:8000", // Optional, will default to this value
  collectionMetadata: {
    "hnsw:space": "cosine",
  }, // Optional, can be used to specify the distance method of the embedding space https://docs.trychroma.com/guides#changing-the-distance-function
});

// Search for the most similar document
const response = await vectorStore.similaritySearch("hello", 1);

console.log(response);

```

- [LangChainJS Chroma Documentation](https://js.langchain.com/docs/modules/indexes/vector_stores/integrations/chroma)
