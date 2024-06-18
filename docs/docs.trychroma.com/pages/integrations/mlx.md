---
title: MLX Embeddings
---
Chroma provides a convenient wrapper around the [MLX](https://github.com/mlfoundations/openllama) framework to run embedding models using the BERT architecture. The code is available in the [MLX examples repo](https://github.com/ml-explore/mlx-examples/).

To use the MLXEmbeddingFunction, you need to provide the model folder path and model weights path.

A BERT model from hf needs to be converted to mlx format for it to usable. To convert the model please vist this [repo](https://github.com/ml-explore/mlx-examples/tree/main/bert).


{% tabs group="code-lang" %}
{% tab label="Python" %}

```python
import chromadb.utils.embedding_functions as embedding_functions

mlx_ef = embedding_functions.MLXEmbeddingFunction(bert_model="bert-base-uncased", weights_path="bert-base-uncased/model.npz")
texts = ["Hello, world!", "How are you?"]

embeddings = mlx_ef(texts)
```

{% /tab %}
{% /tabs %}
