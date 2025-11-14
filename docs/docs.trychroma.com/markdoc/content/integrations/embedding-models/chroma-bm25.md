---
id: chroma-bm25
name: Chroma BM25
---

# Chroma BM25

Chroma provides a built-in BM25 sparse embedding function. BM25 (Best Matching 25) is a ranking function used to estimate the relevance of documents to a given search query. This embedding function runs locally and does not require any external API keys.

Sparse embeddings are useful for retrieval tasks where you want to match on specific keywords or terms, rather than semantic similarity.

{% Tabs %}

{% Tab label="python" %}

```python
from chromadb.utils.embedding_functions import ChromaBm25EmbeddingFunction

bm25_ef = ChromaBm25EmbeddingFunction(
    k=1.2,
    b=0.75,
    avg_doc_length=256.0,
    token_max_length=40
)

texts = ["Hello, world!", "How are you?"]
sparse_embeddings = bm25_ef(texts)
```

You can customize the BM25 parameters:
- `k`: Controls term frequency saturation (default: 1.2)
- `b`: Controls document length normalization (default: 0.75)
- `avg_doc_length`: Average document length in tokens (default: 256.0)
- `token_max_length`: Maximum token length (default: 40)
- `stopwords`: Optional list of stopwords to exclude

{% /Tab %}

{% Tab label="typescript" %}

```typescript
// npm install @chroma-core/chroma-bm25

import { ChromaBm25EmbeddingFunction } from "@chroma-core/chroma-bm25";

const embedder = new ChromaBm25EmbeddingFunction({
  k: 1.2,
  b: 0.75,
  avgDocLength: 256.0,
  tokenMaxLength: 40,
});

// use directly
const sparseEmbeddings = await embedder.generate(["document1", "document2"]);

// pass documents to query for .add and .query
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: embedder,
});
```

{% /Tab %}

{% /Tabs %}

{% Banner type="tip" %}
BM25 is a classic information retrieval algorithm that works well for keyword-based search. For semantic search, consider using dense embedding functions instead.
{% /Banner %}
