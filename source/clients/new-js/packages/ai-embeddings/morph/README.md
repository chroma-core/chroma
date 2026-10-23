# @chroma-core/morph

Chroma integration for Morph embedding models.

## Installation

```bash
npm install @chroma-core/morph
```

## Usage

```typescript
import { MorphEmbeddingFunction } from '@chroma-core/morph';

// Initialize the embedding function
const morphEmbedding = new MorphEmbeddingFunction({
  api_key: 'your-morph-api-key', // or set MORPH_API_KEY env var
  model_name: 'morph-embedding-v2', // default
  api_base: 'https://api.morphllm.com/v1', // default
  encoding_format: 'float' // default
});

// Generate embeddings for code snippets
const codeSnippets = [
  'function calculateSum(a, b) { return a + b; }',
  'class User { constructor(name) { this.name = name; } }'
];

const embeddings = await morphEmbedding.generate(codeSnippets);
console.log(embeddings);
```

## Configuration

The `MorphEmbeddingFunction` constructor accepts the following options:

- `api_key` (optional): Your Morph API key. If not provided, it will read from the environment variable specified by `api_key_env_var`.
- `model_name` (optional): The Morph model to use. Defaults to `'morph-embedding-v2'`.
- `api_base` (optional): The base URL for the Morph API. Defaults to `'https://api.morphllm.com/v1'`.
- `encoding_format` (optional): The format for embeddings ('float' or 'base64'). Defaults to `'float'`.
- `api_key_env_var` (optional): The environment variable name for the API key. Defaults to `'MORPH_API_KEY'`.

## Environment Variables

Set your Morph API key as an environment variable:

```bash
export MORPH_API_KEY="your-morph-api-key"
```

## Features

- **Code-Optimized**: Morph embeddings are specifically designed for code and functional units
- **OpenAI-Compatible**: Uses the standard OpenAI SDK with Morph's API endpoint
- **High Performance**: State-of-the-art embeddings for code similarity and search
- **Batch Processing**: Supports multiple inputs in a single API call

## API Reference

For more information about Morph's embedding models and API, visit:
- [Morph Embedding API Documentation](https://docs.morphllm.com/api-reference/endpoint/embedding)
- [Morph Website](https://morphllm.com/)