# OpenAI Embedding Function for Chroma

This package provides an OpenAI embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/openai
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { OpenAIEmbeddingFunction } from '@chroma-core/openai';

// Initialize the embedder
const embedder = new OpenAIEmbeddingFunction({
  apiKey: 'your-api-key', // Or set OPENAI_API_KEY env var
  modelName: 'text-embedding-3-small',
  // Optional: specify dimensions for supported models
  dimensions: 512,
  // Optional: specify organization ID
  organizationId: 'your-org-id',
  // Optional: specify API base (e.g. for Azure OpenAI)
  apiBase: 'your-api-base'
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

Set your OpenAI API key as an environment variable:

```bash
export OPENAI_API_KEY=your-api-key
```

Alternatively, pass it directly to the constructor.



## OpenAI-compatible gateways (DaoXE)

[DaoXE](https://daoxe.com) is a multi-model multi-protocol API gateway. For embedding models exposed via its OpenAI-compatible API, set `apiBase` to `https://daoxe.com/v1` and use an exact model ID available to your DaoXE account. Not available in mainland China.

```typescript
import { OpenAIEmbeddingFunction } from '@chroma-core/openai';

const embedder = new OpenAIEmbeddingFunction({
  apiKey: process.env.DAOXE_API_KEY,
  apiBase: 'https://daoxe.com/v1',
  // Replace with an exact embedding model ID from your DaoXE account catalog
  modelName: 'YOUR_DAOXE_EMBEDDING_MODEL_ID',
});
```

## Supported Models

- `text-embedding-3-small` (default)
- `text-embedding-3-large`
- `text-embedding-ada-002`

For models that support it (like `text-embedding-3-small` and `text-embedding-3-large`), you can specify custom dimensions to reduce the embedding size.
