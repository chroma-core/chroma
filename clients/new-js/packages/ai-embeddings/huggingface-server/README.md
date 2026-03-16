# Hugging Face Server Embedding Function for Chroma

This package provides a Hugging Face Inference Server embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/huggingface-server
```

## Usage

```typescript
import { ChromaClient } from 'chromadb';
import { HuggingfaceServerEmbeddingFunction } from '@chroma-core/huggingface-server';

// Initialize the embedder
const embedder = new HuggingfaceServerEmbeddingFunction({
  url: 'https://your-inference-server.com/embed', // Your inference server endpoint
  apiKey: 'your-api-key', // Optional, for authenticated servers
  // Or use environment variable
  apiKeyEnvVar: 'HF_API_KEY',
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

For authenticated servers, set your API key as an environment variable:

```bash
export HF_API_KEY=your-api-key
```

## Configuration Options

- **url**: URL of your Hugging Face inference server endpoint (required)
- **apiKey**: API key for authenticated servers (optional)
- **apiKeyEnvVar**: Environment variable name for API key (default: `HF_API_KEY`)

## Use Cases

This embedding function is ideal for:

- **Self-hosted Models**: Connect to your own Hugging Face Inference Server
- **Custom Endpoints**: Use specialized embedding models deployed on your infrastructure
- **Enterprise Deployments**: Maintain data privacy with on-premises inference servers
- **Hugging Face Inference Endpoints**: Connect to paid Hugging Face Inference Endpoints

## Server Requirements

Your Hugging Face inference server should:
1. Accept POST requests with JSON payload containing text inputs
2. Return embeddings as arrays of numbers
3. Follow the standard Hugging Face Inference API format

For more information on setting up a Hugging Face Inference Server, see the [Hugging Face documentation](https://huggingface.co/docs/transformers/main_classes/pipelines#transformers.FeatureExtractionPipeline).