# @chroma-core/ai-embeddings-common

Common utilities and shared functionality for ChromaDB AI embedding packages.

## Installation

```bash
npm install @chroma-core/ai-embeddings-common
```

## Usage

This package provides shared utilities used by all Chroma embedding function packages:

```typescript
import { validateConfigSchema, snakeCase, isBrowser } from '@chroma-core/ai-embeddings-common';

// Convert camelCase to snake_case for API compatibility
const snakeCaseConfig = snakeCase({ modelName: 'text-embedding-3-small' });
// Result: { model_name: 'text-embedding-3-small' }

// Check if running in browser environment
if (isBrowser()) {
  // Browser-specific logic
}

// Validate embedding function configuration
validateConfigSchema(config, 'openai');
```

## Features

- **Schema Validation**: Validates embedding function configurations using JSON schemas
- **Case Conversion**: Converts camelCase JavaScript objects to snake_case for API compatibility
- **Environment Detection**: Utilities to detect browser vs Node.js environments
- **Type Safety**: Provides TypeScript types and interfaces for embedding function development

This package is primarily intended for internal use by other `@chroma-core` embedding packages.