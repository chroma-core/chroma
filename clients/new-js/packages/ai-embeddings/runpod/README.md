# @chroma-core/runpod

RunPod embedding provider for Chroma.

## Installation

```bash
npm install @chroma-core/runpod
```

## Usage

```typescript
import { RunPodEmbeddingFunction } from "@chroma-core/runpod";

// Set your RUNPOD_API_KEY environment variable
const runpodEF = new RunPodEmbeddingFunction({
  endpointId: "your-endpoint-id",
  modelName: "your-model-name"
});

const embeddings = await runpodEF.generate(["Hello", "World"]);
```

## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `endpointId` | string | - | **Required**. RunPod endpoint ID for your embedding model |
| `modelName` | string | - | **Required**. Name of the model to use for embeddings |
| `apiKeyEnvVar` | string | `"RUNPOD_API_KEY"` | Environment variable containing RunPod API key |
| `apiKey` | string | - | RunPod API key (not recommended, use environment variable) |
| `timeout` | number | `300` | Request timeout in seconds |

## Environment Variables

- `RUNPOD_API_KEY`: Your RunPod API key

## Requirements

- Node.js >= 20
- RunPod SDK (`runpod-sdk`)

## About RunPod

RunPod provides serverless AI/ML endpoints with pay-per-second billing. This package allows you to use RunPod-hosted embedding models with Chroma.

For more information about RunPod, visit [runpod.io](https://runpod.io).
