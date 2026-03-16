# Chroma Embeddings

This package provides an embedding function for the Qwen model family hosted on Chroma's cloud embedding service.

## Installation

```bash
npm install @chroma-core/chroma-cloud-qwen
```

## Usage

```typescript
import { ChromaClient } from "chromadb";
import {
  ChromaCloudQwenEmbeddingFunction,
  ChromaCloudQwenEmbeddingModel,
  ChromaCloudQwenEmbeddingTask,
} from "@chroma-core/chroma-cloud-qwen";

// Initialize the embedder
const embedder = new ChromaCloudQwenEmbeddingFunction({
  model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
  task: ChromaCloudQwenEmbeddingTask.CODE_TO_CODE,
  apiKeyEnvVar: "CHROMA_API_KEY",
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

Get your API key from [Chroma's dashboard](https://trychroma.com/).

## Configuration Options

- **model**: Model to use for embeddings
- **task**: The task for which embeddings are being generated
- **instruction_dict**: A dictionary mapping tasks and targets (documents/queries) to custom instructions for the specified Qwen model
- **apiKeyEnvVar**: Environment variable name for API key (default: `CHROMA_API_KEY`)

## Supported Models

- `Qwen/Qwen3-Embedding-0.6B`
