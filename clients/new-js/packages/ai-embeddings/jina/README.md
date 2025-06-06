# Jina Embedding Function for Chroma

This package provides a Jina AI embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/jina
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { JinaEmbeddingFunction } from '@chroma-core/jina';

// Initialize the embedder
const embedder = new JinaEmbeddingFunction({
  apiKey: 'your-api-key', // Or set JINA_API_KEY env var
  modelName: 'jina-embeddings-v2-base-en',
  // Optional configuration
  task: 'retrieval.passage',
  dimensions: 768,
  lateChunking: false,
  truncate: true,
  normalized: true,
  embeddingType: 'float'
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

Set your Jina API key as an environment variable:

```bash
export JINA_API_KEY=your-api-key
```

Get your API key from [Jina AI](https://jina.ai/).

## Configuration Options

- **apiKey**: Your Jina API key (or set via environment variable)
- **apiKeyEnvVar**: Environment variable name for API key (default: `JINA_API_KEY`)
- **modelName**: Model to use for embeddings
- **task**: Task type (e.g., `retrieval.passage`, `retrieval.query`)
- **dimensions**: Output embedding dimensions
- **lateChunking**: Enable late chunking for better long document handling
- **truncate**: Whether to truncate input text that exceeds model limits
- **normalized**: Whether to return normalized embeddings
- **embeddingType**: Output type (`float`, `base64`, `binary`)

## Supported Models

- `jina-embeddings-v2-base-en`
- `jina-embeddings-v2-small-en`
- `jina-clip-v1`

Check the [Jina AI documentation](https://docs.jina.ai/) for the complete list of available models and their capabilities.