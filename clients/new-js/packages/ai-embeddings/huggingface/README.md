# HuggingFace Embedding Function for Chroma

This package provides a [HuggingFace Inference API](https://huggingface.co/docs/api-inference/index)
embedding provider for Chroma. It calls the hosted feature-extraction pipeline, bringing the JS
client to parity with the Python `HuggingFaceEmbeddingFunction`.

For a self-hosted [text-embeddings-inference](https://github.com/huggingface/text-embeddings-inference)
server, use [`@chroma-core/huggingface-server`](../huggingface-server) instead.

## Installation

```bash
npm install @chroma-core/huggingface
```

## Usage

```typescript
import { ChromaClient } from "chromadb";
import { HuggingfaceEmbeddingFunction } from "@chroma-core/huggingface";

// Initialize the embedder
const embedder = new HuggingfaceEmbeddingFunction({
  apiKey: "your-api-key", // Or set CHROMA_HUGGINGFACE_API_KEY env var
  modelName: "sentence-transformers/all-MiniLM-L6-v2",
});

const client = new ChromaClient({ path: "http://localhost:8000" });

const collection = await client.createCollection({
  name: "my-collection",
  embeddingFunction: embedder,
});

await collection.add({
  ids: ["1", "2", "3"],
  documents: ["Document 1", "Document 2", "Document 3"],
});

const results = await collection.query({
  queryTexts: ["Sample query"],
  nResults: 2,
});
```

## Configuration

Set your HuggingFace API key as an environment variable:

```bash
export CHROMA_HUGGINGFACE_API_KEY=your-api-key
```

Get your API key from [HuggingFace](https://huggingface.co/settings/tokens).

## Configuration Options

- **apiKey**: Your HuggingFace API key (or set via environment variable)
- **apiKeyEnvVar**: Environment variable name for the API key (default: `CHROMA_HUGGINGFACE_API_KEY`)
- **modelName**: Model to use for embeddings (default: `sentence-transformers/all-MiniLM-L6-v2`)

See the list of available models on [HuggingFace](https://huggingface.co/models?pipeline_tag=feature-extraction).
