---
id: cloudflare-workers-ai
name: Cloudflare Workers AI
---

# Cloudflare Workers AI

Chroma provides a wrapper around Cloudflare Workers AI embedding models. This embedding function runs remotely against the Cloudflare Workers AI servers, and will require an API key and a Cloudflare account. You can find more information in the [Cloudflare Workers AI Docs](https://developers.cloudflare.com/workers-ai/).

You can also optionally use the Cloudflare AI Gateway for a more customized solution by setting a `gateway_id` argument. See the [Cloudflare AI Gateway Docs](https://developers.cloudflare.com/ai-gateway/providers/workersai/) for more info.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
from chromadb.utils.embedding_functions import CloudflareWorkersAIEmbeddingFunction

os.environ["CHROMA_CLOUDFLARE_API_KEY"] = "<INSERT API KEY HERE>"

ef = CloudflareWorkersAIEmbeddingFunction(
                account_id="bd4502421ad9c8e8931d02a616e6845a",
                model_name="@cf/baai/bge-m3",
            )
ef(input=["This is my first text to embed", "This is my second document"])
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { JinaEmbeddingFunction } from 'chromadb';

process.env.CHROMA_CLOUDFLARE_API_KEY = "<INSERT API KEY HERE>"

const embedder = new CloudflareWorkersAIEmbeddingFunction({
    account_id="bd4502421ad9c8e8931d02a616e6845a",
    model_name="@cf/baai/bge-m3",
});

// use directly
embedder.generate(['This is my first text to embed', 'This is my second document']);
```

{% /Tab %}

{% /TabbedCodeBlock %}

You must pass in an `account_id` and `model_name` to the embedding function. It is recommended to set the `CHROMA_CLOUDFLARE_API_KEY` for the api key, but the embedding function also optionally takes in an `api_key` variable.
