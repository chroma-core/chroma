---
id: 'voyageai'
name: 'VoyageAI'
---

# VoyageAI

Chroma also provides a convenient wrapper around VoyageAI's embedding API. This embedding function runs remotely on VoyageAI’s servers, and requires an API key. You can get an API key by signing up for an account at [VoyageAI](https://dash.voyageai.com/).

{% Tabs %}
{% Tab label="python" %}

This embedding function relies on the `voyageai` python package, which you can install with `pip install voyageai`.

```python
import chromadb.utils.embedding_functions as embedding_functions
voyageai_ef  = embedding_functions.VoyageAIEmbeddingFunction(api_key="YOUR_API_KEY",  model_name="voyage-3-large")
voyageai_ef(input=["document1","document2"])
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { VoyageAIEmbeddingFunction } from 'chromadb';

const embedder = new VoyageAIEmbeddingFunction("apiKey", "model_name")

// use directly
const embeddings = embedder.generate(["document1","document2"])

// pass documents to query for .add and .query
const collection = await client.createCollection({name: "name", embeddingFunction: embedder})
const collectionGet = await client.getCollection({name: "name", embeddingFunction: embedder})
```

{% /Tab %}

{% /Tabs %}

### Multilingual model example

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
voyageai_ef  = embedding_functions.VoyageAIEmbeddingFunction(
        api_key="YOUR_API_KEY",
        model_name="voyage-3-large")

multilingual_texts  = [ 'Hello from VoyageAI!', 'مرحباً من VoyageAI!!',
        'Hallo von VoyageAI!', 'Bonjour de VoyageAI!',
        '¡Hola desde VoyageAI!', 'Olá do VoyageAI!',
        'Ciao da VoyageAI!', '您好，来自 VoyageAI！',
        'कोहिअर से VoyageAI!'  ]

voyageai_ef(input=multilingual_texts)

```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { VoyageAIEmbeddingFunction } from 'chromadb';

const embedder = new VoyageAIEmbeddingFunction("apiKey", "voyage-3-large")

multilingual_texts  = [ 'Hello from VoyageAI!', 'مرحباً من VoyageAI!!',
        'Hallo von VoyageAI!', 'Bonjour de VoyageAI!',
        '¡Hola desde VoyageAI!', 'Olá do VoyageAI!',
        'Ciao da VoyageAI!', '您好，来自 VoyageAI！',
        'कोहिअर से VoyageAI!'  ]

const embeddings = embedder.generate(multilingual_texts)

```

{% /Tab %}

{% /TabbedCodeBlock %}

For further details on VoyageAI's models check the [documentation](https://docs.voyageai.com/docs/introduction) and the [blogs](https://blog.voyageai.com/).
