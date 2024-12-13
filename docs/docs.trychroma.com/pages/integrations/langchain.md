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

- [LangChainJS Chroma Documentation](https://js.langchain.com/docs/modules/indexes/vector_stores/integrations/chroma)

### Embedding Functions

Chroma JS client offers an adapter that allows Chroma and LangChain JS users to reuse embedding functions.
The adapter works both ways, allowing Chroma Embedding Functions to be used to embed data in LangChain and vice versa.

**Using Chroma Embeddings in LangChain:**

```js {% codetab=true %}
import {OllamaEmbeddingFunction, LangChainEmbeddingFunction} from "chromadb";
import {Chroma} from "@langchain/community/vectorstores/chroma";

const embedding = await LangChainEmbeddingFunction.create({
  chromaEmbeddingFunction: new OllamaEmbeddingFunction({
    url:"http://localhost:11434/api/embeddings",
    model: "chroma/all-minilm-l6-v2-f32",
  }),
});

// use as Chroma EF
const embeddings = chromaEmbedding.generate(["document1", "document2"])

// use as LangChain EF
const chroma = new Chroma(chromaEmbedding);
const document1 = {
  pageContent: "The powerhouse of the cell is the mitochondria",
  metadata: {},
};
const res = await chroma.addDocuments([document1], { ids: ["1"] });
```

**Using Chroma Embeddings in LangChain:**

```js {% codetab=true %}
import {OllamaEmbeddingFunction, LangChainEmbeddingFunction} from "chromadb";
import {Chroma} from "@langchain/community/vectorstores/chroma";
import {OllamaEmbeddings} from "@langchain/ollama";

const embedding = await LangChainEmbeddingFunction.create({
  langchainEmbeddings: new importedModule.OllamaEmbeddings({
    baseUrl: "http://localhost:11434",
    model: "chroma/all-minilm-l6-v2-f32",
  }),
});

// use as Chroma EF
const embeddings = chromaEmbedding.generate(["document1", "document2"]);

// use as LangChain EF
const chroma = new Chroma(chromaEmbedding);
const document1 = {
  pageContent: "The powerhouse of the cell is the mitochondria",
  metadata: {},
};
await chroma.addDocuments([document1], { ids: ["1"] });
const retriever = await vectorStore.asRetriever();
const retrieverResults = await retriever.invoke("Tell me about the mitochondria");
```
