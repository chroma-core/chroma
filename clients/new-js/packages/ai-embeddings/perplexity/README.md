# Perplexity Embedding Function for Chroma

This package provides a Perplexity AI embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/perplexity
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { PerplexityEmbeddingFunction } from '@chroma-core/perplexity';

// Initialize the embedder
const embedder = new PerplexityEmbeddingFunction({
  apiKey: 'your-api-key', // Or set PERPLEXITY_API_KEY env var
  modelName: 'pplx-embed-v1-4b',
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

## Configuration

Set your Perplexity API key as an environment variable:

```bash
export PERPLEXITY_API_KEY=your-api-key
```

Get your API key from [Perplexity AI](https://www.perplexity.ai/).

## Configuration Options

- **apiKey**: Your Perplexity API key (or set via environment variable)
- **apiKeyEnvVar**: Environment variable name for API key (default: `PERPLEXITY_API_KEY`)
- **modelName**: Model to use for embeddings (default: `pplx-embed-v1-0.6b`)
- **dimensions**: Optional dimension reduction using Matryoshka representation learning

## Supported Models

Perplexity offers high-quality embedding models:

- `pplx-embed-v1-0.6b` - Lightweight model (default)
- `pplx-embed-v1-4b` - Larger model for maximum performance

Check the [Perplexity documentation](https://docs.perplexity.ai/) for the complete list of available models.

## Features

- **State-of-the-Art Quality**: High-performance embedding models
- **Matryoshka Embeddings**: Support for dimension reduction while maintaining quality
- **Base64 Decoding**: Automatic decoding of base64-encoded int8 embeddings