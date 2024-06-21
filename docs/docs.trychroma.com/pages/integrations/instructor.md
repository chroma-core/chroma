---
title: Instructor
---

The [instructor-embeddings](https://github.com/HKUNLP/instructor-embedding) library is another option, especially when running on a machine with a cuda-capable GPU. They are a good local alternative to OpenAI (see the [Massive Text Embedding Benchmark](https://huggingface.co/blog/mteb) rankings).  The embedding function requires the InstructorEmbedding package. To install it, run ```pip install InstructorEmbedding```.

There are three models available. The default is `hkunlp/instructor-base`, and for better performance you can use `hkunlp/instructor-large` or `hkunlp/instructor-xl`. You can also specify whether to use `cpu` (default) or `cuda`. For example:

```python
#uses base model and cpu
import chromadb.utils.embedding_functions as embedding_functions
ef = embedding_functions.InstructorEmbeddingFunction()
```
or
```python
import chromadb.utils.embedding_functions as embedding_functions
ef = embedding_functions.InstructorEmbeddingFunction(
model_name="hkunlp/instructor-xl", device="cuda")
```
Keep in mind that the large and xl models are 1.5GB and 5GB respectively, and are best suited to running on a GPU.
