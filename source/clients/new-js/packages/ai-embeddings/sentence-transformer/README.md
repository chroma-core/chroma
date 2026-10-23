# Sentence Transformers Embedding Function for Chroma

This package provides a Sentence Transformers embedding provider for Chroma using transformers.js (`@huggingface/transformers`). It allows you to run Sentence Transformer models directly in Node.js without requiring a separate server.

## Installation

```bash
npm install @chroma-core/sentence-transformer
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { SentenceTransformersEmbeddingFunction } from '@chroma-core/sentence-transformer';

// Initialize the embedder with the default model (all-MiniLM-L6-v2)
const embedder = new SentenceTransformersEmbeddingFunction();

// Or initialize with a custom model
const customEmbedder = new SentenceTransformersEmbeddingFunction({
  modelName: 'Xenova/all-mpnet-base-v2', // Higher quality model
  device: 'cpu', // 'cpu' or 'gpu' (default: 'cpu')
  normalizeEmbeddings: false, // Whether to normalize embeddings (default: false)
  kwargs: { quantized: true }, // Optional: additional arguments like quantized
});

// Create a new ChromaClient
const client = new ChromaClient({
  path: 'http://localhost:8000',
});

// Create a collection with the embedder
const collection = await client.createCollection({
  name: 'my-collection',
  embeddingFunction: embedder,
});

// Add documents
await collection.add({
  ids: ["1", "2", "3"],
  documents: ["Document 1", "Document 2", "Document 3"],
});

// Query documents
const results = await collection.query({
  queryTexts: ["Sample query"],
  nResults: 2,
});
```

## Configuration Options

- **modelName**: The Sentence Transformer model to use (default: `"all-MiniLM-L6-v2"`)
  - **Short names** (recommended): Use short names like `"all-MiniLM-L6-v2"` for cross-client compatibility with Python. These are automatically resolved to `Xenova/all-MiniLM-L6-v2` for transformers.js.
  - **Full names**: You can also use full model identifiers like `Xenova/all-MiniLM-L6-v2` or `sentence-transformers/all-MiniLM-L6-v2` if you need to specify a particular variant.
  - Popular models: `all-MiniLM-L6-v2` (default), `all-mpnet-base-v2`, `bge-small-en-v1.5`
- **device**: Device to run the model on - `'cpu'` or `'gpu'` (default: `'cpu'`)
- **normalizeEmbeddings**: Whether to normalize returned vectors (default: `false`)
- **kwargs**: Additional arguments to pass to the model (e.g., `{ quantized: true }`)

## Supported Models

You can use any Sentence Transformer model that is compatible with transformers.js. Popular models include:

- `Xenova/all-MiniLM-L6-v2` - Fast, lightweight model (384 dimensions)
- `Xenova/all-mpnet-base-v2` - Higher quality model (768 dimensions)
- `sentence-transformers/all-MiniLM-L6-v2` - Alternative model identifier
- `sentence-transformers/all-mpnet-base-v2` - Alternative model identifier

Check the [transformers.js documentation](https://huggingface.co/docs/transformers.js) for more available models.

## Features

- **Local Execution**: Run models directly in Node.js without external API calls
- **Multiple Models**: Support for various Sentence Transformer models
- **GPU Support**: Optional GPU acceleration when available
- **No API Keys**: No external API keys required
- **Configurable Normalization**: Control whether embeddings are normalized

## Notes

- Models are downloaded and cached on first use
- You can pass `quantized: true` in `kwargs` for faster loading and reduced memory usage
- GPU support requires appropriate hardware and drivers
- Model loading may take some time on first use
