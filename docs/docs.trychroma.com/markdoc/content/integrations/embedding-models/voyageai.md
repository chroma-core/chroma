---
id: voyageai
name: VoyageAI
---

# VoyageAI

Chroma provides a convenient wrapper around VoyageAI's embedding API. This embedding function runs remotely on VoyageAI's servers, and requires an API key. You can get an API key by signing up for an account at [VoyageAI](https://dash.voyageai.com/).

VoyageAI offers various embedding models including:
- **General-purpose models** (e.g., `voyage-3.5`, `voyage-3.5-lite`, `voyage-3-large`, `voyage-3`, `voyage-2`)
- **Contextual embedding models** (e.g., `voyage-context-3`)
- **Multimodal models** (e.g., `voyage-multimodal-3.5`, `voyage-multimodal-3`)
- **Domain-specific models** (e.g., `voyage-code-3`, `voyage-finance-2`, `voyage-law-2`)

## Basic Usage

{% Tabs %}
{% Tab label="python" %}

This embedding function relies on the `voyageai` python package, which you can install with `pip install voyageai`.

```python
import chromadb.utils.embedding_functions as embedding_functions

# Basic usage with text embeddings
voyageai_ef = embedding_functions.VoyageAIEmbeddingFunction(
    api_key="YOUR_API_KEY",              # Or use api_key_env_var (default: "CHROMA_VOYAGE_API_KEY")
    model_name="voyage-3.5",             # Required: model to use
    input_type=None,                     # Optional: input type for the model
    truncation=True,                     # Whether to truncate inputs (default: True)
    dimensions=None,                     # Optional: output dimension for embeddings (e.g., 2048)
    embedding_type=None,                 # Optional: embedding type
    batch_size=None                      # Optional: max batch size for batching (e.g., 10)
)

# Generate embeddings (supports multilingual text)
embeddings = voyageai_ef(input=["document1", "document2"])
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
// npm install @chroma-core/voyageai

import { VoyageAIEmbeddingFunction } from "@chroma-core/voyageai";

const embedder = new VoyageAIEmbeddingFunction({
  apiKey: "apiKey",
  modelName: "voyage-3.5",
});

// use directly
const embeddings = embedder.generate(["document1", "document2"]);

// pass documents to query for .add and .query
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: embedder,
});
const collectionGet = await client.getCollection({
  name: "name",
  embeddingFunction: embedder,
});
```

{% /Tab %}

{% /Tabs %}

## Multimodal Embeddings

VoyageAI's multimodal models (e.g., `voyage-multimodal-3.5`, `voyage-multimodal-3`) can embed both text and images into the same vector space. The `voyage-multimodal-3.5` model additionally supports video inputs and offers flexible output dimensions (256, 512, 1024, 2048).

{% TabbedCodeBlock %}

{% Tab label="python" %}

For multimodal embeddings, you'll need to install Pillow: `pip install pillow`

```python
import chromadb
import chromadb.utils.embedding_functions as embedding_functions
import numpy as np
from PIL import Image

# Create multimodal embedding function
voyageai_ef = embedding_functions.VoyageAIEmbeddingFunction(
    api_key="YOUR_API_KEY",
    model_name="voyage-multimodal-3"
)

# Embed text documents
text_embeddings = voyageai_ef(["A photo of a cat", "A photo of a dog"])

# Embed images (as numpy arrays)
image = np.array(Image.open("path/to/image.jpg"))
image_embeddings = voyageai_ef([image])

# You can query images with text or vice versa
client = chromadb.Client()
collection = client.create_collection(
    name="multimodal_collection",
    embedding_function=voyageai_ef
)

# Add text documents
collection.add(
    ids=["doc1", "doc2"],
    documents=["A photo of a cat", "A photo of a dog"]
)

# Add images
collection.add(
    ids=["img1"],
    images=[image]
)

# Query with text to find similar images or documents
results = collection.query(
    query_texts=["feline animal"],
    n_results=2
)
```

{% /Tab %}

{% /TabbedCodeBlock %}

## Contextual Embeddings

VoyageAI's contextual models (e.g., `voyage-context-3`) generate embeddings that take into account the context of the entire batch. This is particularly useful when embedding related documents where understanding the relationships between them improves semantic search quality.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
import chromadb.utils.embedding_functions as embedding_functions
import chromadb

# Create contextual embedding function
voyageai_ef = embedding_functions.VoyageAIEmbeddingFunction(
    api_key="YOUR_API_KEY",
    model_name="voyage-context-3",
    dimensions=2048  # voyage-context-3 supports custom dimensions
)

# Example: Using contextual embeddings with a collection
client = chromadb.Client()
collection = client.create_collection(
    name="contextual_docs",
    embedding_function=voyageai_ef
)

# Add related documents - they will be embedded with contextual awareness
documents = [
    "Python is a high-level programming language.",
    "Python is also a type of snake found in tropical regions.",
    "Java is an island in Indonesia.",
    "Java is a popular object-oriented programming language.",
    "The Great Barrier Reef is located off the coast of Australia."
]

collection.add(
    ids=[f"doc{i}" for i in range(len(documents))],
    documents=documents
)

# Query for programming-related content
results = collection.query(
    query_texts=["programming languages"],
    n_results=3
)

# The contextual embeddings help distinguish between different meanings
# of "Python" and "Java" based on surrounding context
```

**Use Cases for Contextual Embeddings:**
- Embedding chapters from the same book
- Processing related articles or research papers
- Handling documents with ambiguous terms that need context
- Creating embeddings for conversations or threaded discussions

{% /Tab %}

{% /TabbedCodeBlock %}

## Token Counting

The VoyageAI embedding function provides a method to count tokens in your texts:

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
import chromadb.utils.embedding_functions as embedding_functions

voyageai_ef = embedding_functions.VoyageAIEmbeddingFunction(
    api_key="YOUR_API_KEY",
    model_name="voyage-3.5"
)

texts = ["Short text", "This is a much longer text with more tokens"]
token_counts = voyageai_ef.count_tokens(texts)
# Returns: [2, 9] (example counts)

# Get the token limit for the current model
token_limit = voyageai_ef.get_token_limit()
# Returns: 320000 for voyage-3.5
```

{% /Tab %}

{% /TabbedCodeBlock %}

For further details on VoyageAI's models check the [documentation](https://docs.voyageai.com/docs/introduction) and the [blogs](https://blog.voyageai.com/).
