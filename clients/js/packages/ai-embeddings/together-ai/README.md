# Together AI Embedding Function for Chroma

This package provides a Together AI embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/together-ai
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { TogetherAIEmbeddingFunction } from '@chroma-core/together-ai';

// Initialize the embedder
const embedder = new TogetherAIEmbeddingFunction({
  apiKey: 'your-api-key', // Or set TOGETHER_API_KEY env var
  modelName: 'togethercomputer/m2-bert-80M-8k-retrieval',
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

Set your Together AI API key as an environment variable:

```bash
export TOGETHER_API_KEY=your-api-key
```

Get your API key from [Together AI](https://together.ai/).

## Configuration Options

- **apiKey**: Your Together AI API key (or set via environment variable)
- **apiKeyEnvVar**: Environment variable name for API key (default: `TOGETHER_API_KEY`)
- **modelName**: Model to use for embeddings (required)

## Supported Models

Popular embedding models available through Together AI:

- `togethercomputer/m2-bert-80M-8k-retrieval`
- `togethercomputer/m2-bert-80M-32k-retrieval`
- `WhereIsAI/UAE-Large-V1`
- `BAAI/bge-large-en-v1.5`
- `BAAI/bge-base-en-v1.5`

Check the [Together AI documentation](https://docs.together.ai/docs/embedding-models) for the complete list of available embedding models and their specifications.

## Features

- **High Performance**: Optimized inference infrastructure for fast embeddings
- **Multiple Models**: Access to various state-of-the-art embedding models
- **Scalable**: Built for production workloads with reliable uptime