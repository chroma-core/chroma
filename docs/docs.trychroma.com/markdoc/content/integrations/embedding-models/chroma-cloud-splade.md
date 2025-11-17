---
id: chroma-cloud-splade
name: Chroma Cloud Splade
---

# Chroma Cloud Splade

Chroma provides a convenient wrapper around Chroma Cloud's Splade sparse embedding API. This embedding function runs remotely on Chroma Cloud's servers, and requires a Chroma API key. You can get an API key by signing up for an account at [Chroma Cloud](https://www.trychroma.com/).

Sparse embeddings are useful for retrieval tasks where you want to match on specific keywords or terms, rather than semantic similarity.

{% Tabs %}

{% Tab label="python" %}

This embedding function relies on the `httpx` python package, which you can install with `pip install httpx`.

```python
from chromadb.utils.embedding_functions import ChromaCloudSpladeEmbeddingFunction, ChromaCloudSpladeEmbeddingModel
import os

os.environ["CHROMA_API_KEY"] = "YOUR_API_KEY"
splade_ef = ChromaCloudSpladeEmbeddingFunction(
    model=ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1
)

texts = ["Hello, world!", "How are you?"]
sparse_embeddings = splade_ef(texts)
```

You can optionally pass in a `model` argument. By default, Chroma uses `prithivida/Splade_PP_en_v1`.

{% /Tab %}

{% Tab label="typescript" %}

```typescript
// npm install @chroma-core/chroma-cloud-splade

import { ChromaCloudSpladeEmbeddingFunction, ChromaCloudSpladeEmbeddingModel } from "@chroma-core/chroma-cloud-splade";

const embedder = new ChromaCloudSpladeEmbeddingFunction({
  apiKeyEnvVar: "CHROMA_API_KEY", // Or set CHROMA_API_KEY env var
  model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
});

// use directly
const sparseEmbeddings = await embedder.generate(["document1", "document2"]);

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
