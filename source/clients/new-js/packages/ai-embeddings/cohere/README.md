# Cohere Embedding Function for Chroma

This package provides a Cohere embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/cohere
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { CohereEmbeddingFunction } from '@chroma-core/cohere';

// Initialize the embedder
const embedder = new CohereEmbeddingFunction({
  apiKey: 'your-api-key', // Or set COHERE_API_KEY env var
  modelName: 'embed-english-v3.0', // Optional, defaults to 'embed-english-v3.0'
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