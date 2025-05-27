# Cloudflare Workers AI Embedding Provider for Chroma

This package provides integration between [Cloudflare Workers AI](https://developers.cloudflare.com/workers-ai/) and Chroma, allowing you to use Cloudflare's embedding models with Chroma.

## Installation

```bash
npm install @chroma-core/cloudflare-worker-ai
```

## Usage

```javascript
import { ChromaClient } from 'chromadb';
import { CloudflareWorkerAIEmbeddingFunction } from '@chroma-core/cloudflare-worker-ai';

// Initialize the embedding function
const embedder = new CloudflareWorkerAIEmbeddingFunction({
  // Optional: Provide API key directly (recommended to use environment variables instead)
  // apiKey: 'your-cloudflare-api-token',
  
  // Optional: Provide Account ID directly (recommended to use environment variables instead)
  // accountId: 'your-cloudflare-account-id',
  
  // Optional: Specify environment variable names (defaults shown)
  apiKeyEnvVar: 'CLOUDFLARE_API_TOKEN',
  accountIdEnvVar: 'CLOUDFLARE_ACCOUNT_ID',
  
  // Optional: Specify model (default shown)
  model: '@cf/baai/bge-large-en-v1.5',
  
  // Optional: Specify dimensions for the embeddings
  dimensions: 1024
});

// Initialize Chroma client
const client = new ChromaClient();

// Create or get a collection with the embedding function
const collection = await client.getOrCreateCollection({
  name: 'my-collection',
  embeddingFunction: embedder
});

// Add documents
const ids = ['id1', 'id2'];
const documents = ['First document', 'Second document'];
await collection.add({
  ids,
  documents
});

// Query for similar documents
const results = await collection.query({
  queryTexts: ['Sample query text'],
  nResults: 2
});

console.log(results);
```

## Configuration

You'll need to set up the following environment variables:

- `CLOUDFLARE_API_TOKEN`: Your Cloudflare API token with AI access
- `CLOUDFLARE_ACCOUNT_ID`: Your Cloudflare account ID

Alternatively, you can provide these values directly to the constructor.

## Available Models

Cloudflare Workers AI supports various embedding models. Some common ones include:

- `@cf/baai/bge-large-en-v1.5` (default)
- `@cf/baai/bge-base-en-v1.5`
- `@cf/baai/bge-small-en-v1.5`

For a complete list of available models, check the [Cloudflare Workers AI documentation](https://developers.cloudflare.com/workers-ai/models/#embedding-models).
