---
title: Cloudflare Workers AI
---

Chroma provides a convenient wrapper around Cloudflare Workers AI REST API. This embedding function runs remotely on a Cloudflare Workers AI. It requires an API key and an account Id or gateway endpoint. You can get an API key by signing up for an account at [Cloudflare Workers AI](https://cloudflare.com/).

Visit the [Cloudflare Workers AI documentation](https://developers.cloudflare.com/workers-ai/) for more information on getting started.

:::note
Currently cloudflare embeddings endpoints allow batches of maximum 100 documents in a single request. The EF has a hard limit of 100 documents per request, and will raise an error if you try to pass more than 100 documents.
:::

{% tabs group="code-lang"  %}
{% tab label="Python" %}

```python
import chromadb.utils.embedding_functions as embedding_functions
cf_ef = embedding_functions.CloudflareWorkersAIEmbeddingFunction(
                api_key = "YOUR_API_KEY",
                account_id = "YOUR_ACCOUNT_ID", # or gateway_endpoint
                model_name = "@cf/baai/bge-base-en-v1.5",
            )
cf_ef(input=["This is my first text to embed", "This is my second document"])
```

You can pass in an optional `model_name` argument, which lets you choose which Cloudflare Workers AI [model](https://developers.cloudflare.com/workers-ai/models/#text-embeddings) to use. By default, Chroma uses `@cf/baai/bge-base-en-v1.5`.

{% /tab %}
{% tab label="Javascript" %}

{% codetabs customHeader="js" %}
{% codetab label="ESM" %}

```js {% codetab=true %}
import {CloudflareWorkersAIEmbeddingFunction}  from "chromadb";
const embedder = new CloudflareWorkersAIEmbeddingFunction({
    apiToken: 'YOUR_API_KEY',
    accountId: "YOUR_ACCOUNT_ID", // or gatewayEndpoint
    model: '@cf/baai/bge-base-en-v1.5',
});

// use directly
const embeddings = embedder.generate(['document1', 'document2']);

// pass documents to query for .add and .query
const collection = await client.createCollection({name: "name", embeddingFunction: embedder})
const collectionGet = await client.getCollection({name:"name", embeddingFunction: embedder})
```
{% /codetab %}
{% codetab label="CJS" %}

```js {% codetab=true %}
const {CloudflareWorkersAIEmbeddingFunction} = require('chromadb');
const embedder = new CloudflareWorkersAIEmbeddingFunction({
    apiToken: 'YOUR_API_KEY',
    accountId: "YOUR_ACCOUNT_ID", // or gatewayEndpoint
    model: '@cf/baai/bge-base-en-v1.5',
});

// use directly
const embeddings = embedder.generate(['document1', 'document2']);

// pass documents to query for .add and .query
const collection = await client.createCollection({name: "name", embeddingFunction: embedder})
const collectionGet = await client.getCollection({name:"name", embeddingFunction: embedder})
```

{% /codetab %}
{% /codetabs %}

{% /tab %}

{% /tabs %}
