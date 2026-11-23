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
  modelName: 'embed-v5.0-pro', // Optional, defaults to 'embed-v5.0-pro'
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

Queries passed to `collection.query({ queryTexts })` are embedded with Cohere's `search_query` input type, and documents with `search_document`.

## Options

| Option | Description |
| --- | --- |
| `modelName` | Model for documents (and queries, unless `queryModelName` is set). Defaults to `embed-v5.0-pro`. |
| `queryModelName` | Model for queries. `embed-v5.0-pro` and `embed-v5.0-fast` share one embedding space, so you can index with Pro and query with Fast. |
| `outputDimension` | Embedding size for models with Matryoshka output (`embed-v4.0` and newer), e.g. `1024`. Uses the model default when not set. |
| `inputType` | Input type for documents. Defaults to `search_document`. |
| `truncate` | `NONE`, `START` or `END`. |
| `embeddingType` | Embedding type to return. Defaults to `float`. |
| `image` | Set to `true` when the inputs are image data URIs. |

```typescript
// Index with Embed 5 Pro, query with Embed 5 Fast
const embedder = new CohereEmbeddingFunction({
  modelName: 'embed-v5.0-pro',
  queryModelName: 'embed-v5.0-fast',
});
```
