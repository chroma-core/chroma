## chromadb

Chroma is the open-source embedding database. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

This package gives you a JS/TS interface to talk to a backend Chroma DB over REST.

[Learn more about Chroma](https://github.com/chroma-core/chroma)

- [💬 Community Discord](https://discord.gg/MMeYNTmh3x)
- [📖 Documentation](https://docs.trychroma.com/)
- [💡 Colab Example](https://colab.research.google.com/drive/1QEzFyqnoFxq7LUGyP1vzR4iLt9PpCDXv?usp=sharing)
- [🏠 Homepage](https://www.trychroma.com/)

## Getting started

Chroma needs to be running in order for this client to talk to it. Please see the [🧪 Usage Guide](https://docs.trychroma.com/usage-guide) to learn how to quickly stand this up.

## Small example

```js
import { ChromaClient } from "chromadb";
const chroma = new ChromaClient({ path: "http://localhost:8000" });
const collection = await chroma.createCollection({ name: "test-from-js" });
for (let i = 0; i < 20; i++) {
  await collection.add({
    ids: ["test-id-" + i.toString()],
    embeddings, [1, 2, 3, 4, 5],
    documents: ["test"],
  });
}
const queryData = await collection.query({
  queryEmbeddings: [1, 2, 3, 4, 5],
  queryTexts: ["test"],
});
```

### How to Use Embedding Functions

---

#### OpenAI Embedding Function

To use OpenAI's GPT-based embeddings, import and set it up like this:

```javascript
import { OpenAIEmbeddingFunction } from "chromadb/openai";

const openAI = new OpenAIEmbeddingFunction({ apiKey: "Your-API-Key" });

const embeddings = await openAI.generate(["text1", "text2"]);
```
[Learn more about OpenAI](https://github.com/openai/openai-node)

---

#### Cohere Embedding Function

To use Cohere's embedding functions, import and instantiate the class.

```javascript
import { CohereEmbeddingFunction } from "chromadb/cohere";

const cohere = new CohereEmbeddingFunction({ apiKey: "Your-API-Key" });

const embeddings = await cohere.generate(["text1", "text2"]);
```
[Learn more about Cohere](https://github.com/cohere-ai/cohere-node)

---

#### WebAI Embedding Function

To utilize WebAI for generating embeddings, import and initialize it as shown below:

```javascript
import { WebAIEmbeddingFunction } from "chromadb/webai";

const webAI = new WebAIEmbeddingFunction("text", false, false);

const embeddings = await webAI.generate(["Hello", "World"]);
```

**Note**: Additional setup may be needed. You might have to install certain peer dependencies. Check out the `package.json` for details: [WebAI package.json](https://github.com/visheratin/web-ai/blob/main/package.json).

[Learn more about WebAI](https://github.com/visheratin/web-ai)

---

#### Transformers Embedding Function

To use Hugging Face Transformers, you can easily import it and initialize it with optional parameters.

```javascript
import { TransformersEmbeddingFunction } from "chromadb/transformers";

const transformers = new TransformersEmbeddingFunction({
  model: "Your-Model-Name",
  revision: "main",
  quantized: false,
  progress_callback: null,
});

const embeddings = await transformers.generate(["text1", "text2"]);
```
[Learn more about Transformers](https://github.com/xenova/transformers.js)

## Local development

[View the Development Readme](./DEVELOP.md)

## License

Apache 2.0
