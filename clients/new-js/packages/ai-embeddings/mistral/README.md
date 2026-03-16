# Mistral Embedding Function for Chroma

This package provides a Mistral embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/mistral
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { MistralEmbeddingFunction } from '@chroma-core/mistral';

// Initialize the embedder
const embedder = new MistralEmbeddingFunction({
  apiKey: 'your-api-key', // Or set MISTRAL_API_KEY env var
  model: 'mistral-embed',
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

Set your Mistral API key as an environment variable:

```bash
export MISTRAL_API_KEY=your-api-key
```

Alternatively, pass it directly to the constructor.

## Supported Models

- `mistral-embed` (default)

## Options

- `apiKey`: Your Mistral API key (optional if environment variable is set)
- `apiKeyEnvVar`: Environment variable name for API key (default: "MISTRAL_API_KEY")
- `model`: Model name to use (default: "mistral-embed")