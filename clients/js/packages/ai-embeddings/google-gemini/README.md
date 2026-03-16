# Google Gemini Embedding Function for Chroma

This package provides a Google Gemini embedding provider for Chroma using the Google Generative AI SDK.

## Installation

```bash
npm install @chroma-core/google-gemini
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { GoogleGeminiEmbeddingFunction } from '@chroma-core/google-gemini';

// Initialize the embedder
const embedder = new GoogleGeminiEmbeddingFunction({
  apiKey: 'your-api-key', // Or set GEMINI_API_KEY env var
  modelName: 'text-embedding-004', // Optional, defaults to latest model
  taskType: 'RETRIEVAL_DOCUMENT', // Optional
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

Set your Google AI API key as an environment variable:

```bash
export GEMINI_API_KEY=your-api-key
```

Get your API key from the [Google AI Studio](https://aistudio.google.com/app/apikey).

## Configuration Options

- **apiKey**: Your Google AI API key (or set via environment variable)
- **apiKeyEnvVar**: Environment variable name for API key (default: `GEMINI_API_KEY`)
- **modelName**: Model to use for embeddings
- **taskType**: Task type for the embedding request (e.g., `RETRIEVAL_DOCUMENT`, `RETRIEVAL_QUERY`)

## Supported Models

- `text-embedding-004` (latest)
- `embedding-001`

Check the [Google AI documentation](https://ai.google.dev/models/gemini) for the most up-to-date list of available embedding models.