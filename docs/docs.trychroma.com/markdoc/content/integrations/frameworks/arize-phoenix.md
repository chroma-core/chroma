---
id: arize-phoenix
name: Arize Phoenix
---

# Arize Phoenix

[Arize Phoenix](https://github.com/Arize-ai/phoenix/) is an open-source observability and evaluation tool. It can be accessed as an online tool, or self-hosted.

- [Tutorial: Trace and Evaluate an Agentic RAG app using Chroma](https://github.com/Arize-ai/phoenix/blob/main/tutorials/tracing/agentic_rag_tracing.ipynb)

### Automatic Tracing:

Automatically trace calls to Chroma through LlamaIndex or Langchain:

```python
from openinference.instrumentation.llama_index import LlamaIndexInstrumentor
# from openinference.instrumentation.langchain import LangChainInstrumentor
from phoenix.otel import register
import os

os.environ["PHOENIX_CLIENT_HEADERS"] = f"api_key={phoenix_api_key}"

# configure the Phoenix tracer
tracer_provider = register(
    project_name="agentic-rag-demo",
    endpoint="https://app.phoenix.arize.com/v1/traces",  # change this endpoint if you're running Phoenix locally
    auto_instrument=True
)

```

### Manual Tracing:

```python
import os
import chromadb
import chromadb.utils.embedding_functions as embedding_functions
from phoenix.otel import register

os.environ["PHOENIX_CLIENT_HEADERS"] = f"api_key={phoenix_api_key}"

# configure the Phoenix tracer
tracer_provider = register(
    project_name="agentic-rag-demo-2",
    endpoint="https://app.phoenix.arize.com/v1/traces",  # change this endpoint if you're running Phoenix locally
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

For more information see the [Phoenix Website](https://phoenix.arize.com).
