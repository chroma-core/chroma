# Chroma Embeddings

This package provides an embedding function for Chroma's hosted embedding service.

## Installation

```bash
npm install @chroma-core/chroma-embed
```

## Usage

```typescript
import { ChromaClient } from "chromadb";
import { ChromaEmbeddingFunction } from "@chroma-core/chroma-embed";

// Initialize the embedder
const embedder = new ChromaEmbeddingFunction({
  apiKey: "your-api-key", // Or set CHROMA_API_KEY env var
  modelId: "Qwen/Qwen3-Embedding-0.6B",
  task: "code",
});

// Create a new ChromaClient
const client = new ChromaClient({
  path: "http://localhost:8000",
});

// Create a collection with the embedder
const collection = await client.createCollection({
  name: "my-collection",
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

Set your Chroma API key as an environment variable:

```bash
export CHROMA_API_KEY=your-api-key
```

Get your API key on [Chroma's dashboard](https://trychroma.com/).

## Configuration Options

- **apiKey**: Your Jina API key (or set via environment variable)
- **apiKeyEnvVar**: Environment variable name for API key (default: `CHROMA_API_KEY`)
- **modelId**: Model to use for embeddings
- **task**: The task for which embeddings are being generated (e.g., `code`)

## Supported Models

- `Qwen/Qwen3-Embedding-0.6B`
- `BAAI/bge-m3`
