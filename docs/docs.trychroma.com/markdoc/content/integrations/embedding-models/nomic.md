---
id: nomic
name: Nomic
---

# Nomic

Chroma provides a convenient wrapper around Nomic's embedding API. This embedding function runs remotely on Nomic's servers, and requires an API key. You can get an API key by signing up for an account at [Nomic](https://atlas.nomic.ai/).

{% Tabs %}

{% Tab label="python" %}

This embedding function relies on the `nomic` python package, which you can install with `pip install nomic`.

```python
from chromadb.utils.embedding_functions import NomicEmbeddingFunction
import os

os.environ["NOMIC_API_KEY"] = "YOUR_API_KEY"
nomic_ef = NomicEmbeddingFunction(
    model="nomic-embed-text-v1",
    task_type="search_document",
    query_config={"task_type": "search_query"}
)

texts = ["Hello, world!", "How are you?"]
embeddings = nomic_ef(texts)
```

You must pass in a `model` argument and `task_type` argument. The `task_type` can be one of:
- `search_document`: Used to encode large documents in retrieval tasks at indexing time
- `search_query`: Used to encode user queries or questions in retrieval tasks
- `classification`: Used to encode text for text classification tasks
- `clustering`: Used for clustering or reranking tasks

The `query_config` parameter allows you to specify a different task type for queries, which is useful when you want to use `search_document` for documents and `search_query` for queries.

{% /Tab %}

{% /Tabs %}

{% Banner type="tip" %}
Visit Nomic [documentation](https://docs.nomic.ai/platform/embeddings-and-retrieval/text-embedding) for more information on available models and task types.
{% /Banner %}
