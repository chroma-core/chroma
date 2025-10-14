# Chroma Cloud Splade Embeddings

This package provides a sparse embedding function for the Splade model family hosted on Chroma's cloud embedding service. Splade (Sparse Lexical and Expansion) embeddings are particularly effective for information retrieval tasks, combining the benefits of sparse representations with learned relevance.

## Installation

```bash
npm install @chroma-core/chroma-cloud-splade
```

## Usage

```typescript
import { ChromaClient } from "chromadb";
import {
  ChromaCloudSpladeEmbeddingFunction,
  ChromaCloudSpladeEmbeddingModel,
} from "@chroma-core/chroma-cloud-splade";

// Initialize the embedder
const embedder = new ChromaCloudSpladeEmbeddingFunction({
  model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
  apiKeyEnvVar: "CHROMA_API_KEY",
});

## Configuration

Set your Chroma API key as an environment variable:

```bash
export CHROMA_API_KEY=your-api-key
```

Get your API key from [Chroma's dashboard](https://trychroma.com/).

## Configuration Options

- **model**: Model to use for sparse embeddings (default: `SPLADE_PP_EN_V1`)
- **apiKeyEnvVar**: Environment variable name for API key (default: `CHROMA_API_KEY`)

## Supported Models

- `prithivida/Splade_PP_en_v1` - Splade++ English v1 model optimized for information retrieval
