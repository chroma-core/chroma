---
id: chroma-cloud-qwen
name: Chroma Cloud Qwen
---

# Chroma Cloud Qwen

Chroma provides a convenient wrapper around Chroma Cloud's Qwen embedding API. This embedding function runs remotely on Chroma Cloud's servers, and requires a Chroma API key. You can get an API key by signing up for an account at [Chroma Cloud](https://www.trychroma.com/).

{% Tabs %}

{% Tab label="python" %}

This embedding function relies on the `httpx` python package, which you can install with `pip install httpx`.

```python
from chromadb.utils.embedding_functions import ChromaCloudQwenEmbeddingFunction, ChromaCloudQwenEmbeddingModel
import os

os.environ["CHROMA_API_KEY"] = "YOUR_API_KEY"
qwen_ef = ChromaCloudQwenEmbeddingFunction(
    model=ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
    task="nl_to_code"
)

texts = ["Hello, world!", "How are you?"]
embeddings = qwen_ef(texts)
```

You must pass in a `model` argument and `task` argument. The `task` parameter specifies the task for which embeddings are being generated. You can optionally provide custom `instructions` for both documents and queries.

{% /Tab %}

{% Tab label="typescript" %}

```typescript
// npm install @chroma-core/chroma-cloud-qwen

import { ChromaCloudQwenEmbeddingFunction, ChromaCloudQwenEmbeddingModel } from "@chroma-core/chroma-cloud-qwen";

const embedder = new ChromaCloudQwenEmbeddingFunction({
  apiKeyEnvVar: "CHROMA_API_KEY", // Or set CHROMA_API_KEY env var
  model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
  task: "nl_to_code",
});

// use directly
const embeddings = await embedder.generate(["document1", "document2"]);

// pass documents to query for .add and .query
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: embedder,
});
```

{% /Tab %}

{% /Tabs %}

{% Banner type="tip" %}
Visit Chroma Cloud [documentation](https://docs.trychroma.com/) for more information on available models and configuration.
{% /Banner %}
