---
title: Llama CPP Embeddings
---

Chroma provides a convenient wrapper around [Llama-CPP-Python](https://github.com/abetlen/llama-cpp-python)'
s [embeddings API](https://llama-cpp-python.readthedocs.io/en/latest/#embeddings). You can use
the `LlamaCppEmbeddingFunction` embedding function to generate embeddings for your documents with
a [model](https://github.com/abetlen/llama-cpp-python?tab=readme-ov-file#pulling-models-from-hugging-face-hub) of your choice.

## Using a local model

With the use of llama-cpp-python, you can use a local model to generate embeddings. You can pass the path to the model on creation of the `LlamaCppEmbeddingFunction`:

{% tabs group="code-lang"  %}
{% tab label="Python" %}

```python
import chromadb.utils.embedding_functions as embedding_functions

llamacpp_embbedder = embedding_functions.LlamaCppEmbeddingFunction(model_path="path/to/model")

embeddings = llamacpp_embbedder(["Embedded sentence number one",
                                  "Embedded sentence number two"])
```

{% /tab %}
{% tab label="Javascript" %}

{% codetabs customHeader="js" %}
{% codetab label="ESM" %}

```js {% codetab=true %}
import { LlamaCppEmbeddingFunction } from "chromadb";
const llamacpp_embbedder = new LlamaCppEmbeddingFunction({
  model_path: "path/to/model",
});

// use directly
const embeddings = llamacpp_embbedder.generate(["document1", "document2"]);

// pass documents to query for .add and .query
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: llamacpp_embbedder,
});
const collection = await client.getCollection({
  name: "name",
  embeddingFunction: llamacpp_embbedder,
});
```

{% /codetab %}
{% codetab label="CJS" %}

```js {% codetab=true %}
const { LlamaCppEmbeddingFunction } = require("chromadb");
const llamacpp_embbedder = new LlamaCppEmbeddingFunction({
  model_path: "path/to/model",
});

// use directly
const embeddings = llamacpp_embbedder.generate(["document1", "document2"]);

// pass documents to query for .add and .query
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: llamacpp_embbedder,
});
const collection = await client.getCollection({
  name: "name",
  embeddingFunction: llamacpp_embbedder,
});
```

{% /codetab %}
{% /codetabs %}

{% /tab %}

{% /tabs %}

## Using a model from Hugging Face

You can also use a model from the Hugging Face model hub. You can pass the repository name and file name on creation of the `LlamaCppEmbeddingFunction`:

{% tabs group="code-lang"  %}
{% tab label="Python" %}

```python
import chromadb.utils.embedding_functions as embedding_functions

llamacpp_embbedder = embedding_functions.LlamaCppEmbeddingFunction(
    repository_name="ChristianAzinn/gte-large-gguf", # e.g. "username/repo"
    file_name="*Q5_K_M.gguf" # file name, * is a wildcard for looking up the file
)

embeddings = llamacpp_embbedder(["Embedded sentence number one",
                                  "Embedded sentence number two"])
```

{% /tab %}
{% tab label="Javascript" %}

{% codetabs customHeader="js" %}
{% codetab label="ESM" %}

```js {% codetab=true %}
import { LlamaCppEmbeddingFunction } from "chromadb";

const llamacpp_embbedder = new LlamaCppEmbeddingFunction({
  repository_name: "ChristianAzinn/gte-large-gguf", // e.g. "username/repo"
  file_name: "*Q5_K_M.gguf", // file name, * is a wildcard for looking up the file
});

// use directly
const embeddings = llamacpp_embbedder.generate(["document1", "document2"]);

// pass documents to query for .add and .query
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: llamacpp_embbedder,
});

const collection = await client.getCollection({
  name: "name",
  embeddingFunction: llamacpp_embbedder,
});
```

{% /codetab %}
{% codetab label="CJS" %}

```js {% codetab=true %}
const { LlamaCppEmbeddingFunction } = require("chromadb");

const llamacpp_embbedder = new LlamaCppEmbeddingFunction({
  repository_name: "ChristianAzinn/gte-large-gguf", // e.g. "username/repo"
  file_name: "*Q5_K_M.gguf", // file name, * is a wildcard for looking up the file
});

// use directly
const embeddings = llamacpp_embbedder.generate(["document1", "document2"]);

// pass documents to query for .add and .query
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: llamacpp_embbedder,
});

const collection = await client.getCollection({
  name: "name",
  embeddingFunction: llamacpp_embbedder,
});
```

{% /codetab %}
{% /codetabs %}

{% /tab %}

{% /tabs %}
