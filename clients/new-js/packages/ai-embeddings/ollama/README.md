# Ollama Embedding Function for Chroma

This package provides an Ollama embedding provider for Chroma, allowing you to use locally hosted Ollama models.

## Installation

```bash
npm install @chroma-core/ollama
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { OllamaEmbeddingFunction } from '@chroma-core/ollama';

// Initialize the embedder
const embedder = new OllamaEmbeddingFunction({
  url: 'http://localhost:11434', // Default Ollama server URL
  model: 'chroma/all-minilm-l6-v2-f32', // Default model
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

## Prerequisites

1. Install Ollama from [ollama.ai](https://ollama.ai/)
2. Start the Ollama server:
   ```bash
   ollama serve
   ```
3. Pull an embedding model:
   ```bash
   ollama pull chroma/all-minilm-l6-v2-f32
   ```

## Configuration Options

- **url**: Ollama server URL (default: `http://localhost:11434`)
- **model**: Model name to use for embeddings (default: `chroma/all-minilm-l6-v2-f32`)

## Supported Models

Popular embedding models available through Ollama:

- `chroma/all-minilm-l6-v2-f32` (default, 384 dimensions)
- `nomic-embed-text` (768 dimensions)
- `mxbai-embed-large` (1024 dimensions)
- `snowflake-arctic-embed`

Pull models using:
```bash
ollama pull <model-name>
```

## Browser Support

This package works in both Node.js and browser environments, automatically detecting the runtime and using the appropriate Ollama client.