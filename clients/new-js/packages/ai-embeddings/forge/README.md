# Forge Embedding Function for Chroma

This package provides a Forge embedding provider for Chroma. Forge exposes an
OpenAI-compatible embeddings endpoint, so this provider uses the `openai` client
configured with the Forge base URL.

## Installation

```bash
npm install @chroma-core/forge
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { ForgeEmbeddingFunction } from '@chroma-core/forge';

// Initialize the embedder
const embedder = new ForgeEmbeddingFunction({
  apiKey: 'your-api-key', // Or set FORGE_API_KEY env var
  modelName: 'forge-pro',
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

Set your Forge API key as an environment variable:

```bash
export FORGE_API_KEY=your-api-key
```

Get your API key from [Voxell](https://voxell.ai).

## Configuration Options

- **apiKey**: Your Forge API key (or set via environment variable)
- **apiKeyEnvVar**: Environment variable name for API key (default: `FORGE_API_KEY`)
- **modelName**: Model to use for embeddings (default: `forge-pro`)
- **apiBase**: Base URL for the Forge API (default: `https://api.voxell.ai/v1`)
- **dimensions**: Optional dimension reduction using Matryoshka representation learning

## Supported Models

Forge offers the following embedding models:

- `forge-turbo` - 1024 dimensions
- `forge-pro` - 2560 dimensions (default)
- `forge-ultra-4k` - 4096 dimensions

## Features

- **OpenAI-Compatible API**: Uses the standard `openai` client with the Forge base URL.
- **Matryoshka Embeddings**: Support for dimension reduction while maintaining quality.
