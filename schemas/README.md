# Shared Schemas

This directory contains shared schemas that are used by multiple parts of the Chroma codebase. The purpose of having shared schemas is to ensure consistency across different implementations and languages.

## Directory Structure

- `embedding_functions/`: JSON schemas for embedding functions

## Usage in Python

```python
from chromadb.utils.embedding_functions.schemas import validate_config

# Validate a configuration
config = {
    "api_key_env_var": "CHROMA_OPENAI_API_KEY",
    "model_name": "text-embedding-ada-002"
}
validate_config(config, "openai")
```

## Usage in JavaScript

```typescript
import { validateConfig } from '@chromadb/core';

// Validate a configuration
const config = {
    api_key_env_var: "CHROMA_OPENAI_API_KEY",
    model_name: "text-embedding-ada-002"
};
validateConfig(config, "openai");
```

## Adding New Schemas

To add a new schema:

1. Create a new JSON file in the appropriate subdirectory (e.g., `embedding_functions/new_function.json`)
2. Define the schema following the JSON Schema Draft-07 specification
3. Update the relevant code in both Python and JavaScript to use the schema for validation

## Schema Versioning

Each schema includes a version number to support future changes. When making changes to a schema, increment the version number to ensure backward compatibility.
