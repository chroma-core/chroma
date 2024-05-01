---
title: OpenAI
---

{% tabs group="code-lang" hideContent=true %}
{% tab label="Python" %}
{% /tab %}
{% tab label="Javascript" %}
{% /tab %}
{% /tabs %}

Chroma provides a convenient wrapper around OpenAI's embedding API. This embedding function runs remotely on OpenAI's servers, and requires an API key. You can get an API key by signing up for an account at [OpenAI](https://openai.com/api/).

The following OpenAI Embedding Models are supported:

- `text-embedding-ada-002`
- `text-embedding-3-small`
- `text-embedding-3-large`

{% note type="default" title="More Info" %}
Visit OpenAI Embeddings [documentation](https://platform.openai.com/docs/guides/embeddings) for more information.
{% /note %}

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

This embedding function relies on the `openai` python package, which you can install with `pip install openai`.

You can pass in an optional `model_name` argument, which lets you choose which OpenAI embeddings model to use. By default, Chroma uses `text-embedding-ada-002`.

```python
import chromadb.utils.embedding_functions as embedding_functions
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
                api_key="YOUR_API_KEY",
                model_name="text-embedding-3-small"
            )
```

To use the OpenAI embedding models on other platforms such as Azure, you can use the `api_base` and `api_type` parameters:
```python
import chromadb.utils.embedding_functions as embedding_functions
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
                api_key="YOUR_API_KEY",
                api_base="YOUR_API_BASE_PATH",
                api_type="azure",
                api_version="YOUR_API_VERSION",
                model_name="text-embedding-3-small"
            )
```

{% /tab %}
{% tab label="Javascript" %}

You can pass in an optional `model` argument, which lets you choose which OpenAI embeddings model to use. By default, Chroma uses `text-embedding-ada-002`.

```javascript
const {OpenAIEmbeddingFunction} = require('chromadb');
const embeddingFunction = new OpenAIEmbeddingFunction({
    openai_api_key: "apiKey",
    model: "text-embedding-3-small"
})

// use directly
const embeddings = embeddingFunction.generate(["document1","document2"])

// pass documents to query for .add and .query
const collection = await client.createCollection({
    name: "name",
    embeddingFunction: embeddingFunction
})
const collection = await client.getCollection({
    name: "name",
    embeddingFunction: embeddingFunction
})
```

{% /tab %}
{% /tabs %}
