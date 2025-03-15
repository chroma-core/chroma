---
id: arize-phoenix
name: Arize Phoenix
---

<center>
    <p style="text-align:center">
        <img alt="phoenix logo" src="https://storage.googleapis.com/arize-phoenix-assets/assets/phoenix-logo-light.svg" width="200"/>
        <br>
        <a href="https://docs.arize.com/phoenix/">Docs</a>
        |
        <a href="https://github.com/Arize-ai/phoenix">GitHub</a>
        |
        <a href="https://join.slack.com/t/arize-ai/shared_invite/zt-1px8dcmlf-fmThhDFD_V_48oU7ALan4Q">Community</a>
    </p>
</center>

<p align="right">
  <a href="https://github.com/Arize-ai/phoenix">
    <img src="https://img.shields.io/github/stars/Arize-ai/phoenix?style=social" alt="GitHub stars">
  </a>
</p>

[Arize Phoenix](https://github.com/Arize-ai/phoenix/) is an open-source observability and evaluation tool for AI agents, chatbots, and RAG applications. Phoenix allows you to trace calls made to your Chroma DB instances, view retrieved documents, and score document relevancy.

![Phoenix Chroma Integration Example](https://storage.googleapis.com/arize-phoenix-assets/assets/images/arize-phoenix-chroma-example-image.png)


## Tutorials
- [Trace and Evaluate an Agentic RAG app using Chroma](https://github.com/Arize-ai/phoenix/blob/main/tutorials/tracing/agentic_rag_tracing.ipynb) - This tutorial shows how you can trace and evaluate an Agentic RAG app that uses Chroma as its VectorDB.

## Getting Started
### Install and Launch Phoenix Locally
The following code will launch a local version of Phoenix. If you prefer, you can access a cloud instance instead through [Phoenix Cloud](https://app.phoenix.arize.com).

```bash
pip install arize-phoenix
phoenix serve
```

### Automatic Tracing:

Phoenix is built to automatically trace calls made to instrumentation libraries like Langchain and LlamaIndex. If you're using Chroma through one of those libraries, we recommend using one of Phoenix's auto-instrumentors instead of Manually Tracing.

* [Langchain](https://docs.arize.com/phoenix/tracing/integrations-tracing/langchain) Auto Instrumentation
* [LlamaIndex](https://docs.arize.com/phoenix/tracing/integrations-tracing/llamaindex) Auto Instrumentation
* [Haystack](https://docs.arize.com/phoenix/tracing/integrations-tracing/haystack) Auto Instrumentation

### Manual Tracing:

If you're not using one of the libraries above, you can manually instrument your app instead.

```python
import os
import chromadb
import chromadb.utils.embedding_functions as embedding_functions
from phoenix.otel import register

# configure the Phoenix tracer
tracer_provider = register(
    project_name="chroma-db-demo",
)

tracer = tracer_provider.get_tracer(__name__)

# set up a Chroma collection
chroma_client = chromadb.Client()
collection = chroma_client.create_collection(name="my_collection")

collection.add(
    documents=[
        "This is a document about pineapple",
        "This is a document about oranges"
    ],
    ids=["id1", "id2"]
)

# trace a retrieval call
with tracer.start_as_current_span(
    "chromadb-lookup",
    openinference_span_kind="retriever",
) as span:
  query = "This is a query document about pineapple"
  span.set_input(query)

  results = collection.query(
      query_texts=[query], # Chroma will embed this for you
      n_results=2 # how many results to return
  )

  documents = []
  for doc in results['documents'][0]:
    document = {'document.content': doc}
    documents.append(document)

  for i, document in enumerate(documents):
      for key, value in document.items():
          span.set_attribute(f"retrieval.documents.{i}.{key}", value)
```

## Want to Learn More?

* Arize's [Guide to LLM Evaluation](https://arize.com/llm-evaluation)
* Arize's [Guide to Agent Evaluation](https://arize.com/ai-agents/)

## Links & Resources

* Website: [Arize Phoenix](https://phoenix.arize.com/)
* Github: [Arize-ai/phoenix](https://github.com/Arize-ai/phoenix/)
* Slack: [Join Arize Community](https://join.slack.com/t/arize-ai/shared_invite/zt-1px8dcmlf-fmThhDFD~zBCQoUdRjuBjg)
* Twitter: [@ArizePhoenix](https://twitter.com/ArizePhoenix)
* Youtube: [@ArizeAI](https://www.youtube.com/@arizeai)

Arize Phoenix is licensed under the terms of the Elastic License 2.0 (ELv2). See [LICENSE](https://github.com/Arize-ai/phoenix/blob/main/LICENSE)