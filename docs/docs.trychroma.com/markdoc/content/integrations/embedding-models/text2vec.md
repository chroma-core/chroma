---
id: text2vec
name: Text2Vec
---

# Text2Vec

Chroma provides a convenient wrapper around the Text2Vec library. This embedding function runs locally and is particularly useful for Chinese text embeddings.

{% Tabs %}

{% Tab label="python" %}

This embedding function relies on the `text2vec` python package, which you can install with `pip install text2vec`.

```python
from chromadb.utils.embedding_functions import Text2VecEmbeddingFunction

text2vec_ef = Text2VecEmbeddingFunction(
    model_name="shibing624/text2vec-base-chinese"
)

texts = ["你好，世界！", "你好吗？"]
embeddings = text2vec_ef(texts)
```

You can pass in an optional `model_name` argument. By default, Chroma uses `shibing624/text2vec-base-chinese`.

{% /Tab %}

{% /Tabs %}

{% Banner type="tip" %}
Text2Vec is optimized for Chinese text embeddings. For English text, consider using Sentence Transformer or other embedding functions.
{% /Banner %}
