---
id: morph
name: Morph
---

# Morph

Chroma provides a convenient wrapper around Morph's embedding API. This embedding function runs remotely on Morph's servers and requires an API key. You can get an API key by signing up for an account at [Morph](https://morphllm.com/?utm_source=docs.trychroma.com).

{% Tabs %}

{% Tab label="python" %}

This embedding function relies on the `openai` python package, which you can install with `pip install openai`.

```python
import chromadb.utils.embedding_functions as embedding_functions
morph_ef = embedding_functions.MorphEmbeddingFunction(
    api_key="YOUR_API_KEY",  # or set MORPH_API_KEY environment variable
    model_name="morph-embedding-v2"
)
morph_ef(input=["def calculate_sum(a, b):\n    return a + b", "class User:\n    def __init__(self, name):\n        self.name = name"])
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
// npm install @chroma-core/morph

import { MorphEmbeddingFunction } from "@chroma-core/morph";

const embedder = new MorphEmbeddingFunction({
  api_key: "apiKey", // or set MORPH_API_KEY environment variable
  model_name: "morph-embedding-v2",
});

// use directly
const embeddings = embedder.generate([
  "function calculate(a, b) { return a + b; }",
  "class User { constructor(name) { this.name = name; } }",
]);

// pass documents to the .add and .query methods
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: embedder,
});
const collectionGet = await client.getCollection({
  name: "name",
  embeddingFunction: embedder,
});
```

{% /Tab %}

{% /Tabs %}

For further details on Morph's models check the [documentation](https://docs.morphllm.com/api-reference/endpoint/embedding?utm_source=docs.trychroma.com).
