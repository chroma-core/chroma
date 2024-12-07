---
id: hugging-face
name: Hugging Face
---

# Hugging Face

Chroma also provides a convenient wrapper around HuggingFace's embedding API. This embedding function runs remotely on HuggingFace's servers, and requires an API key. You can get an API key by signing up for an account at [HuggingFace](https://huggingface.co/).

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
import chromadb.utils.embedding_functions as embedding_functions
huggingface_ef = embedding_functions.HuggingFaceEmbeddingFunction(
    api_key="YOUR_API_KEY",
    model_name="sentence-transformers/all-MiniLM-L6-v2"
)
```

You can pass in an optional `model_name` argument, which lets you choose which HuggingFace model to use. By default, Chroma uses `sentence-transformers/all-MiniLM-L6-v2`. You can see a list of all available models [here](https://huggingface.co/models).

