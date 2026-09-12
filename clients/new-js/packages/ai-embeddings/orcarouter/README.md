# @chroma-core/orcarouter

Chroma integration for [OrcaRouter](https://www.orcarouter.ai) embedding models.

OrcaRouter is an OpenAI-compatible gateway that routes to 150+ frontier and
open-weight models through one endpoint, and runs gateway-level, zero-trust
security for AI agents on the same endpoint — screening every prompt/response
and governing every tool call on a default-deny basis, with no application code
changes.

## Installation

```bash
npm install @chroma-core/orcarouter
```

## Usage

```typescript
import { OrcaRouterEmbeddingFunction } from '@chroma-core/orcarouter';

// Initialize the embedding function
const orcarouterEmbedding = new OrcaRouterEmbeddingFunction({
  api_key: 'your-orcarouter-api-key', // or set ORCAROUTER_API_KEY env var
  model_name: 'openai/text-embedding-3-small', // default
  api_base: 'https://api.orcarouter.ai/v1', // default
  encoding_format: 'float' // default
});

// Generate embeddings
const texts = [
  'OrcaRouter routes to 150+ models through a single OpenAI-compatible endpoint.',
  'Zero-trust security for AI agents on the same endpoint.'
];

const embeddings = await orcarouterEmbedding.generate(texts);
console.log(embeddings);
```

## Configuration

The `OrcaRouterEmbeddingFunction` constructor accepts the following options:

- `api_key` (optional): Your OrcaRouter API key. If not provided, it will read from the environment variable specified by `api_key_env_var`.
- `model_name` (optional): The OrcaRouter embedding model to use. Defaults to `'openai/text-embedding-3-small'`.
- `api_base` (optional): The base URL for the OrcaRouter API. Defaults to `'https://api.orcarouter.ai/v1'`.
- `encoding_format` (optional): The format for embeddings ('float' or 'base64'). Defaults to `'float'`.
- `api_key_env_var` (optional): The environment variable name for the API key. Defaults to `'ORCAROUTER_API_KEY'`.

## Environment Variables

Set your OrcaRouter API key as an environment variable:

```bash
export ORCAROUTER_API_KEY="your-orcarouter-api-key"
```

## Features

- **Multi-Model Access**: One endpoint for 150+ frontier and open-weight models
- **OpenAI-Compatible**: Uses the standard OpenAI SDK with OrcaRouter's API endpoint
- **Zero-Trust Security**: Gateway-level screening of every prompt/response on a default-deny basis
- **Batch Processing**: Supports multiple inputs in a single API call

## API Reference

For more information about OrcaRouter's embedding models and API, visit:
- [OrcaRouter Embedding API Documentation](https://api.orcarouter.ai/v1)
- [OrcaRouter Website](https://www.orcarouter.ai)
