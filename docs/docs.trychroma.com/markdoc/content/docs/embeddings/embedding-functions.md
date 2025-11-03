---
id: embedding-functions
name: Embedding Functions
---

# Embedding Functions

Embeddings are the way to represent any kind of data, making them the perfect fit for working with all kinds of AI-powered tools and algorithms. They can represent text, images, and soon audio and video. Chroma collections index embeddings to enable efficient similarity search on the data they represent. There are many options for creating embeddings, whether locally using an installed library, or by calling an API.

Chroma provides lightweight wrappers around popular embedding providers, making it easy to use them in your apps. You can set an embedding function when you [create](../collections/manage-collections) a Chroma collection, to be automatically used when adding and querying data, or you can call them directly yourself.

|                                                                                          | Python | Typescript |
| ---------------------------------------------------------------------------------------- | ------ | ---------- |
| [Cloudflare Workers AI](../../integrations/embedding-models/cloudflare-workers-ai)       | ✓      | ✓          |
| [Cohere](../../integrations/embedding-models/cohere)                                     | ✓      | ✓          |
| [Google Generative AI](../../integrations/embedding-models/google-gemini)                | ✓      | ✓          |
| [Hugging Face](../../integrations/embedding-models/hugging-face)                         | ✓      | -          |
| [Hugging Face Embedding Server](../../integrations/embedding-models/hugging-face-server) | ✓      | ✓          |
| [Instructor](../../integrations/embedding-models/instructor)                             | ✓      | -          |
| [Jina AI](../../integrations/embedding-models/jina-ai)                                   | ✓      | ✓          |
| [Mistral](../../integrations/embedding-models/mistral)                                   | ✓      | ✓          |
| [Morph](../../integrations/embedding-models/morph)                                       | ✓      | ✓          |
| [OpenAI](../../integrations/embedding-models/openai)                                     | ✓      | ✓          |
| [Together AI](../../integrations/embedding-models/together-ai)                           | ✓      | ✓          |


For TypeScript users, Chroma provides packages for a number of embedding model providers. The Chromadb python package ships will all embedding functions included.

| Provider                    | Embedding Function Package                    
| ----------                  | ------------------------- 
| All (installs all packages) | [@chroma-core/all](https://www.npmjs.com/package/@chroma-core/all)     
| Cloudflare Workers AI       | [@chroma-core/cloudflare-worker-ai](https://www.npmjs.com/package/@chroma-core/cloudflare-worker-ai)     
| Cohere                      | [@chroma-core/cohere](https://www.npmjs.com/package/@chroma-core/cohere) 
| Google Gemini               | [@chroma-core/google-gemini](https://www.npmjs.com/package/@chroma-core/google-gemini)     
| Hugging Face Server         | [@chroma-core/huggingface-server](https://www.npmjs.com/package/@chroma-core/huggingface-server)     
| Jina                        | [@chroma-core/jina](https://www.npmjs.com/package/@chroma-core/jina)     
| Mistral                     | [@chroma-core/mistral](https://www.npmjs.com/package/@chroma-core/mistral)     
| Morph                       | [@chroma-core/morph](https://www.npmjs.com/package/@chroma-core/morph)     
| Ollama                      | [@chroma-core/ollama](https://www.npmjs.com/package/@chroma-core/ollama)     
| OpenAI                      | [@chroma-core/openai](https://www.npmjs.com/package/@chroma-core/openai)     
| Qwen (via Chroma Cloud)     | [@chroma-core/chroma-cloud-qwen](https://www.npmjs.com/package/@chroma-core/chroma-cloud-qwen)
| Together AI                 | [@chroma-core/together-ai](https://www.npmjs.com/package/@chroma-core/together-ai)     
| Voyage AI                   | [@chroma-core/voyageai](https://www.npmjs.com/package/@chroma-core/voyageai)     

We welcome pull requests to add new Embedding Functions to the community.

---

## Default: all-MiniLM-L6-v2

Chroma's default embedding function uses the [Sentence Transformers](https://www.sbert.net/) `all-MiniLM-L6-v2` model to create embeddings. This embedding model can create sentence and document embeddings that can be used for a wide variety of tasks. This embedding function runs locally on your machine, and may require you download the model files (this will happen automatically).

If you don't specify an embedding function when creating a collection, Chroma will set it to be the `DefaultEmbeddingFunction`:

{% Tabs %}

{% Tab label="python" %}

```python
collection = client.create_collection(name="my_collection")
```

{% /Tab %}

{% Tab label="typescript" %}
Install the `@chroma-core/default-embed` package:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="npm" %}

```terminal
npm install @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="pnpm" %}

```terminal
pnpm add @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="yarn" %}

```terminal
yarn add @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="bun" %}

```terminal
bun add @chroma-core/default-embed
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Create a collection without providing an embedding function. It will automatically be set with the `DefaultEmbeddingFunction`:

```typescript
const collection = await client.createCollection({ name: "my-collection" });
```

{% /Tab %}

{% /Tabs %}

## Using Embedding Functions

Embedding functions can be linked to a collection and used whenever you call `add`, `update`, `upsert` or `query`.

{% Tabs %}

{% Tab label="python" %}

```python
# Set your OPENAI_API_KEY environment variable
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

collection = client.create_collection(
    name="my_collection",
    embedding_function=OpenAIEmbeddingFunction(
        model_name="text-embedding-3-small"
    )
)

# Chroma will use OpenAIEmbeddingFunction to embed your documents
collection.add(
    ids=["id1", "id2"],
    documents=["doc1", "doc2"]
)
```

{% /Tab %}

{% Tab label="typescript" %}
Install the `@chroma-core/openai` package:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="npm" %}

```terminal
npm install @chroma-core/openai
```

{% /Tab %}

{% Tab label="pnpm" %}

```terminal
pnpm add @chroma-core/openai
```

{% /Tab %}

{% Tab label="yarn" %}

```terminal
yarn add @chroma-core/openai
```

{% /Tab %}

{% Tab label="bun" %}

```terminal
bun add @chroma-core/openai
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Create a collection with the `OpenAIEmbeddingFunction`:

```typescript
// Set your OPENAI_API_KEY environment variable
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";

collection = await client.createCollection({
  name: "my_collection",
  embedding_function: new OpenAIEmbeddingFunction({
    modelName: "text-embedding-3-small",
  }),
});

// Chroma will use OpenAIEmbeddingFunction to embed your documents
await collection.add({
  ids: ["id1", "id2"],
  documents: ["doc1", "doc2"],
});
```

{% /Tab %}

{% /Tabs %}

You can also use embedding functions directly which can be handy for debugging.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

default_ef = DefaultEmbeddingFunction()
embeddings = default_ef(["foo"])
print(embeddings) # [[0.05035809800028801, 0.0626462921500206, -0.061827320605516434...]]

collection.query(query_embeddings=embeddings)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { DefaultEmbeddingFunction } from "@chroma-core/default-embed";

const defaultEF = new DefaultEmbeddingFunction();
const embeddings = await defaultEF.generate(["foo"]);
console.log(embeddings); // [[0.05035809800028801, 0.0626462921500206, -0.061827320605516434...]]

await collection.query({ queryEmbeddings: embeddings });
```

{% /Tab %}

{% /TabbedCodeBlock %}

## Custom Embedding Functions

You can create your own embedding function to use with Chroma; it just needs to implement `EmbeddingFunction`.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
from chromadb import Documents, EmbeddingFunction, Embeddings

class MyEmbeddingFunction(EmbeddingFunction):
    def __call__(self, input: Documents) -> Embeddings:
        # embed the documents somehow
        return embeddings
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { EmbeddingFunction } from "chromadb";

class MyEmbeddingFunction implements EmbeddingFunction {
  private api_key: string;

  constructor(api_key: string) {
    this.api_key = api_key;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    // do things to turn texts into embeddings with an api_key perhaps
    return embeddings;
  }
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

We welcome contributions! If you create an embedding function that you think would be useful to others, please consider [submitting a pull request](https://github.com/chroma-core/chroma).
