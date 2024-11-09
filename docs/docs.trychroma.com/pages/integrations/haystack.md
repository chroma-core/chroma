---
title: 💙 Haystack
---

[Haystack](https://github.com/deepset-ai/haystack) is an open-source LLM framework in Python. It provides [embedders](https://docs.haystack.deepset.ai/v2.0/docs/embedders), [generators](https://docs.haystack.deepset.ai/v2.0/docs/generators) and [rankers](https://docs.haystack.deepset.ai/v2.0/docs/rankers) via a number of LLM providers, tooling for [preprocessing](https://docs.haystack.deepset.ai/v2.0/docs/preprocessors) and data preparation, connectors to a number of vector databases including Chroma and more. Haystack allows you to build custom LLM applications using both components readily available in Haystack and [custom components](https://docs.haystack.deepset.ai/v2.0/docs/custom-components). Some of the most common applications you can build with Haystack are retrieval-augmented generation pipelines (RAG), question-answering and semantic search.

![](https://img.shields.io/github/stars/deepset-ai/haystack.svg?style=social&label=Star&maxAge=2400)

|[Docs](https://docs.haystack.deepset.ai/v2.0/docs) | [Github](https://github.com/deepset-ai/haystack) | [Haystack Integrations](https://haystack.deepset.ai/integrations) | [Tutorials](https://haystack.deepset.ai/tutorials) |

You can use Chroma together with Haystack by installing the integration and using the `ChromaDocumentStore`

### Installation

```bash
pip install chroma-haystack
```

### Usage

- The [Chroma Integration page](https://haystack.deepset.ai/integrations/chroma-documentstore)
- [Chroma + Haystack Example](https://colab.research.google.com/drive/1YpDetI8BRbObPDEVdfqUcwhEX9UUXP-m?usp=sharing)

#### Write documents into a ChromaDocumentStore

```python
import os
from pathlib import Path

from haystack import Pipeline
from haystack.components.converters import TextFileToDocument
from haystack.components.writers import DocumentWriter
from haystack_integrations.document_stores.chroma import ChromaDocumentStore

file_paths = ["data" / Path(name) for name in os.listdir("data")]

document_store = ChromaDocumentStore()

indexing = Pipeline()
indexing.add_component("converter", TextFileToDocument())
indexing.add_component("writer", DocumentWriter(document_store))

indexing.connect("converter", "writer")
indexing.run({"converter": {"sources": file_paths}})
```

#### Build RAG on top of Chroma

```python
from haystack_integrations.components.retrievers.chroma import ChromaQueryTextRetriever
from haystack.components.generators import HuggingFaceAPIGenerator
from haystack.components.builders import PromptBuilder
from haystack.utils import Secret

prompt = """
Answer the query based on the provided context.
If the context does not contain the answer, say 'Answer not found'.
Context:
{% for doc in documents %}
  {{ doc.content }}
{% endfor %}
query: {{query}}
Answer:
"""
prompt_builder = PromptBuilder(template=prompt)

llm = HuggingFaceAPIGenerator(api_type="serverless_inference_api", 
                              api_params={"model": "mistralai/Mixtral-8x7B-Instruct-v0.1"},
                              token=Secret.from_token("YOUR_HF_TOKEN"))
retriever = ChromaQueryTextRetriever(document_store)

querying = Pipeline()
querying.add_component("retriever", retriever)
querying.add_component("prompt_builder", prompt_builder)
querying.add_component("llm", llm)

querying.connect("retriever.documents", "prompt_builder.documents")
querying.connect("prompt_builder", "llm")

query = "What is the capital of France?" # query to search for in the documents

results = querying.run({"retriever": {"query": query, "top_k": 3},
                        "prompt_builder": {"query": query}})

print(results)
```
