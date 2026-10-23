# Default Embedding Function for Chroma

This package provides a default embedding function for Chroma using Hugging Face Transformers.js. It runs entirely in-browser or Node.js without requiring external API calls.

## Installation

```bash
npm install @chroma-core/default-embed
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { DefaultEmbeddingFunction } from '@chroma-core/default-embed';

// Initialize with default settings
const embedder = new DefaultEmbeddingFunction();

// Or customize the configuration
const customEmbedder = new DefaultEmbeddingFunction({
  modelName: 'Xenova/all-MiniLM-L6-v2', // Default model
  revision: 'main',
  dtype: 'fp32', // or 'uint8' for quantization
  wasm: false, // Set to true to use WASM backend
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

## Configuration Options

- **modelName**: Hugging Face model name (default: `Xenova/all-MiniLM-L6-v2`)
- **revision**: Model revision (default: `main`)
- **dtype**: Data type for quantization (`fp32`, `fp16`, `q8`, `uint8`, etc.)
- **quantized**: Deprecated, use `dtype` instead
- **wasm**: Use WASM backend for ONNX Runtime

## Features

- **No API Key Required**: Runs locally without external dependencies
- **Browser Compatible**: Works in both Node.js and browser environments
- **Quantization Support**: Reduce model size with various quantization options
- **WASM Backend**: Optional WASM support for better browser performance

The default model (`Xenova/all-MiniLM-L6-v2`) produces 384-dimensional embeddings and is suitable for most general-purpose semantic search tasks.