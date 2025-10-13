---
id: mistral
name: Mistral
---

# Mistral

Chroma provides a convenient wrapper around Mistral's embedding API. This embedding function runs remotely on Mistral's servers, and requires an API key. You can get an API key by signing up for an account at [Mistral](https://mistral.ai/).

{% Tabs %}
{% Tab label="python" %}

This embedding function relies on the `mistralai` python package, which you can install with `pip install mistralai`.

```python
from chromadb.utils.embedding_functions import MistralEmbeddingFunction
import os

os.environ["MISTRAL_API_KEY"] = "************"
mistral_ef  = MistralEmbeddingFunction(model="mistral-embed")
mistral_ef(input=["document1","document2"])
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
// npm install @chroma-core/mistral

import { MistralEmbeddingFunction } from "@chroma-core/mistral";

const embedder = new MistralEmbeddingFunction({
  apiKey: "your-api-key", // Or set MISTRAL_API_KEY env var
  model: "mistral-embed",
});
```

{% /Tab %}
{% /Tabs %}

You must pass in a `model` argument, which selects the Mistral embedding model to use. You can see the supported embedding types and models in Mistral's docs [here](https://docs.mistral.ai/capabilities/embeddings/overview/)
