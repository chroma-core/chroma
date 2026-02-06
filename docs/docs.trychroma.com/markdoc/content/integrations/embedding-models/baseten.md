---
name: Baseten
id: baseten
---

# Baseten

Baseten is a model inference provider for dedicated deployments of any open-source, fine-tuned, or custom model, including embedding models. Baseten specializes in low-latency, high-throughput deployments using Baseten Embedding Inference (BEI), the fastest runtime on the market for embedding models.

Chroma provides a convenient integration with any OpenAI-compatible embedding model deployed on Baseten. Every embedding model deployed with BEI is compatible with the OpenAI SDK.

{% Banner type="tip" %}
Get started easily with an embedding model from Baseten's model library, like [Mixedbread Embed Large](https://www.baseten.co/library/mixedbread-embed-large-v1/).
{% /Banner %}

## Using Baseten models with Chroma

{% Tabs %}

{% Tab label="python" %}

This embedding function relies on the `openai` python package, which you can install with `pip install openai`.

You must set the `api_key` and `api_base`, replacing the `api_base` with the URL from the model deployed in your Baseten account.

```python
import os
import chromadb.utils.embedding_functions as embedding_functions

baseten_ef = embedding_functions.BasetenEmbeddingFunction(
                api_key=os.environ["BASETEN_API_KEY"],
                api_base="https://model-xxxxxxxx.api.baseten.co/environments/production/sync/v1",
            )

baseten_ef(input=["This is my first text to embed", "This is my second document"])
```

{% /Tab %}

{% Tab label="go" %}

The Go client supports Baseten BEI (Baseten Embeddings Inference) for deploying your own embedding models with GPU acceleration.

```go
import (
    "github.com/chroma-core/chroma/clients/go/pkg/embeddings/baseten"
)

// Create with API key directly
ef, err := baseten.NewBasetenEmbeddingFunction(
    baseten.WithAPIKey("YOUR_API_KEY"),
    baseten.WithBaseURL("https://model-xxxxxxxx.api.baseten.co/environments/production/sync"),
)

// Or use environment variable (BASETEN_API_KEY)
ef, err := baseten.NewBasetenEmbeddingFunction(
    baseten.WithEnvAPIKey(),
    baseten.WithBaseURL("https://model-xxxxxxxx.api.baseten.co/environments/production/sync"),
)
```

### Embedding Documents

```go
// Embed text documents
embeddings, err := ef.EmbedDocuments(ctx, []string{
    "This is my first text to embed",
    "This is my second document",
})

// Embed a single query
embedding, err := ef.EmbedQuery(ctx, "search query")
```

### Configuration Options

```go
ef, err := baseten.NewBasetenEmbeddingFunction(
    baseten.WithEnvAPIKey(),
    baseten.WithBaseURL("https://model-xxxxxxxx.api.baseten.co/environments/production/sync"),
    baseten.WithModelID("my-model"),         // Optional model identifier
    baseten.WithHTTPClient(customHTTPClient), // Optional custom HTTP client
    baseten.WithInsecure(),                  // Allow HTTP for local development
)
```

{% /Tab %}

{% /Tabs %}