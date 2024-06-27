---
title: FastEmbed
---

# FastEmbed

[FastEmbed](https://qdrant.github.io/fastembed/) is a lightweight, CPU-first Python library built for embedding generation.

This embedding function requires the `fastembed` package. To install it, run

```pip install fastembed```.

You can find a list of all the supported models [here](https://qdrant.github.io/fastembed/examples/Supported_Models/).

## Example usage

Using the default BAAI/bge-small-en-v1.5 model.

```python
from chromadb.utils.embedding_functions.fastembed_embedding_function import FastEmbedEmbeddingFunction
ef = FastEmbedEmbeddingFunction()
```

Additionally, you can also configure the cache directory, number of threads and other FastEmbed options.

```python
from chromadb.utils.embedding_functions import FastEmbedEmbeddingFunction
ef = FastEmbedEmbeddingFunction(model_name="nomic-ai/nomic-embed-text-v1.5", cache_dir="models_cache", threads=5)
```
