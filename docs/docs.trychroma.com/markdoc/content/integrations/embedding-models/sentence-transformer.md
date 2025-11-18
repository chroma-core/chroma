---
id: sentence-transformer
name: Sentence Transformer
---

# Sentence Transformer

Chroma provides a convenient wrapper around the Sentence Transformers library. This embedding function runs locally and uses pre-trained models from Hugging Face.

{% TabbedCodeBlock %}

{% Tab label="python" %}

This embedding function relies on the `sentence_transformers` python package, which you can install with `pip install sentence_transformers`.

```python
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

sentence_transformer_ef = SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2",
    device="cpu",
    normalize_embeddings=False
)

texts = ["Hello, world!", "How are you?"]
embeddings = sentence_transformer_ef(texts)
```

You can pass in optional arguments:
- `model_name`: The name of the Sentence Transformer model to use (default: "all-MiniLM-L6-v2")
- `device`: Device used for computation, "cpu" or "cuda" (default: "cpu")
- `normalize_embeddings`: Whether to normalize returned vectors (default: False)

For a full list of available models, visit [Hugging Face Sentence Transformers](https://huggingface.co/sentence-transformers) or [SBERT documentation](https://www.sbert.net/docs/pretrained_models.html).

{% /Tab %}

{% Tab label="typescript" %}

```typescript
// npm install @chroma-core/sentence-transformer

import { SentenceTransformersEmbeddingFunction } from "@chroma-core/sentence-transformer";

const sentenceTransformerEF = new SentenceTransformersEmbeddingFunction({
  modelName: "all-MiniLM-L6-v2",
  device: "cpu",
  normalizeEmbeddings: false,
});

const texts = ["Hello, world!", "How are you?"];
const embeddings = await sentenceTransformerEF.generate(texts);
```

{% /Tab %}

{% /TabbedCodeBlock %}

{% Banner type="tip" %}
Sentence Transformers are great for semantic search tasks. Popular models include `all-MiniLM-L6-v2` (fast and efficient) and `all-mpnet-base-v2` (higher quality). Visit [SBERT documentation](https://www.sbert.net/docs/pretrained_models.html) for more model recommendations.
{% /Banner %}
