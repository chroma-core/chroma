---
title: 'Cohere'
---

{% tabs group="code-lang" hideContent=true %}
{% tab label="Python" %}
{% /tab %}
{% tab label="Javascript" %}
{% /tab %}
{% /tabs %}

Chroma also provides a convenient wrapper around Cohere's embedding API. This embedding function runs remotely on Cohere’s servers, and requires an API key. You can get an API key by signing up for an account at [Cohere](https://dashboard.cohere.ai/welcome/register).

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

This embedding function relies on the `cohere` python package, which you can install with `pip install cohere`.

```python
import chromadb.utils.embedding_functions as embedding_functions
cohere_ef  = embedding_functions.CohereEmbeddingFunction(api_key="YOUR_API_KEY",  model_name="large")
cohere_ef(texts=["document1","document2"])
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
const {CohereEmbeddingFunction} = require('chromadb');
const embedder = new CohereEmbeddingFunction("apiKey")

// use directly
const embeddings = embedder.generate(["document1","document2"])

// pass documents to query for .add and .query
const collection = await client.createCollection({name: "name", embeddingFunction: embedder})
const collectionGet = await client.getCollection({name:"name", embeddingFunction: embedder})
```

{% /tab %}
{% /tabs %}



You can pass in an optional `model_name` argument, which lets you choose which Cohere embeddings model to use. By default, Chroma uses `large` model. You can see the available models under `Get embeddings` section [here](https://docs.cohere.ai/reference/embed).


### Multilingual model example

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
cohere_ef  = embedding_functions.CohereEmbeddingFunction(
        api_key="YOUR_API_KEY",
        model_name="multilingual-22-12")

multilingual_texts  = [ 'Hello from Cohere!', 'مرحبًا من كوهير!',
        'Hallo von Cohere!', 'Bonjour de Cohere!',
        '¡Hola desde Cohere!', 'Olá do Cohere!',
        'Ciao da Cohere!', '您好，来自 Cohere！',
        'कोहेरे से नमस्ते!'  ]

cohere_ef(texts=multilingual_texts)

```

{% /tab %}
{% tab label="Javascript" %}

```javascript
const {CohereEmbeddingFunction} = require('chromadb');
const embedder = new CohereEmbeddingFunction("apiKey")

multilingual_texts  = [ 'Hello from Cohere!', 'مرحبًا من كوهير!',
        'Hallo von Cohere!', 'Bonjour de Cohere!',
        '¡Hola desde Cohere!', 'Olá do Cohere!',
        'Ciao da Cohere!', '您好，来自 Cohere！',
        'कोहेरे से नमस्ते!'  ]

const embeddings = embedder.generate(multilingual_texts)

```


{% /tab %}
{% /tabs %}

For more information on multilingual model you can read [here](https://docs.cohere.ai/docs/multilingual-language-models).
