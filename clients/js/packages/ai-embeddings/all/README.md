# @chroma-core/all

All AI embedding providers for Chroma in one package.

## Installation

```bash
npm install @chroma-core/all
```

## Usage

This package re-exports all available embedding functions:

```typescript
import {
  OpenAIEmbeddingFunction,
  CohereEmbeddingFunction,
  JinaEmbeddingFunction,
  GoogleGeminiEmbeddingFunction,
  // ... and all other providers
} from '@chroma-core/all';

// Use any embedding function
const openAIEF = new OpenAIEmbeddingFunction({
  apiKey: 'your-api-key',
  modelName: 'text-embedding-3-small'
});
```

## Included Providers

- OpenAI
- Cohere  
- Jina
- Google Gemini
- Hugging Face Server
- Ollama
- Together AI
- Voyage AI
- Cloudflare Worker AI
- Default Embedding

For specific provider documentation, see the individual package READMEs.