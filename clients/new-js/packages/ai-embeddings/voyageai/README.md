# Voyage AI Embedding Function for Chroma

This package provides a Voyage AI embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/voyageai
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { VoyageAIEmbeddingFunction } from '@chroma-core/voyageai';

// Initialize the embedder
const embedder = new VoyageAIEmbeddingFunction({
  apiKey: 'your-api-key', // Or set VOYAGE_API_KEY env var
  modelName: 'voyage-2',
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

Set your Voyage AI API key as an environment variable:

```bash
export VOYAGE_API_KEY=your-api-key
```

Get your API key from [Voyage AI](https://www.voyageai.com/).

## Configuration Options

- **apiKey**: Your Voyage AI API key (or set via environment variable)
- **apiKeyEnvVar**: Environment variable name for API key (default: `VOYAGE_API_KEY`)
- **modelName**: Model to use for embeddings (required)

## Supported Models

Voyage AI offers high-quality embedding models:

- `voyage-2` - Latest and most capable model
- `voyage-large-2` - Larger model for maximum performance
- `voyage-code-2` - Optimized for code and technical content
- `voyage-lite-02-instruct` - Lightweight instruction-following model

Check the [Voyage AI documentation](https://docs.voyageai.com/embeddings/) for the complete list of available models and their specifications.

## Features

- **State-of-the-Art Quality**: High-performance embedding models optimized for retrieval
- **Domain-Specific Models**: Specialized models for code, legal, and other domains
- **Efficient**: Competitive pricing and fast inference times