---
id: ollama
name: Ollama
---

# Ollama

Chroma provides a convenient wrapper around [Ollama](https://github.com/ollama/ollama)'
s [embeddings API](https://github.com/ollama/ollama/blob/main/docs/api.md#generate-embeddings). You can use
the `OllamaEmbeddingFunction` embedding function to generate embeddings for your documents with
a [model](https://github.com/ollama/ollama?tab=readme-ov-file#model-library) of your choice.

{% TabbedCodeBlock  %}

{% Tab label="python" %}

```python
import chromadb.utils.embedding_functions as embedding_functions

ollama_ef = embedding_functions.OllamaEmbeddingFunction(
    url="http://localhost:11434/api/embeddings",
    model_name="llama2",
)

embeddings = ollama_ef(["This is my first text to embed",
                        "This is my second document"])
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { OllamaEmbeddingFunction } from "chromadb";
const embedder = new OllamaEmbeddingFunction({
    url: "http://127.0.0.1:11434/api/embeddings",
    model: "llama2"
})

// use directly
const embeddings = embedder.generate(["document1", "document2"])

// pass documents to query for .add and .query
let collection = await client.createCollection({
    name: "name",
    embeddingFunction: embedder
})
collection = await client.getCollection({
    name: "name",
    embeddingFunction: embedder
})
```

{% /Tab %}

{% /TabbedCodeBlock %}
