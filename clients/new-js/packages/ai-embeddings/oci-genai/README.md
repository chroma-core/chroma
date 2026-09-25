# OCI Generative AI Embedding Function for Chroma

This package provides an [Oracle Cloud Infrastructure (OCI) Generative AI](https://docs.oracle.com/en-us/iaas/Content/generative-ai/home.htm) embedding provider for Chroma, using the official OCI TypeScript SDK (`oci-common` and `oci-generativeaiinference`).

## Installation

```bash
npm install @chroma-core/oci-genai
```

## Usage

```typescript
import { ChromaClient } from "chromadb";
import { OCIGenAIEmbeddingFunction } from "@chroma-core/oci-genai";

// Initialize the embedder. Credentials are read from ~/.oci/config; only the
// profile/config-file location is stored in the collection configuration.
const embedder = new OCIGenAIEmbeddingFunction({
  modelName: "cohere.embed-v4.0", // Optional, this is the default
  compartmentId: "ocid1.compartment.oc1..<your-compartment>",
  serviceEndpoint:
    "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com", // Optional
  authType: "API_KEY", // Optional: API_KEY | SECURITY_TOKEN | INSTANCE_PRINCIPAL | RESOURCE_PRINCIPAL
  authProfile: "DEFAULT", // Optional
});

// Create a new ChromaClient
const client = new ChromaClient({
  path: "http://localhost:8000",
});

// Create a collection with the embedder
const collection = await client.createCollection({
  name: "my-collection",
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

## Authentication

The embedder never stores secrets. It authenticates with one of:

- **`API_KEY`** (default): the `[profile]` in your OCI config file (`~/.oci/config`) with an API signing key. See [SDK and CLI configuration](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm).
- **`SECURITY_TOKEN`**: a session-token profile created with `oci session authenticate`.
- **`INSTANCE_PRINCIPAL`**: when running on an OCI compute instance.
- **`RESOURCE_PRINCIPAL`**: when running on OCI Functions, Data Science, etc.

The IAM policy of the tenancy must allow the principal to `use generative-ai-family` in the given compartment.

## Configuration Options

- **modelName**: OCI Generative AI embedding model (default: `cohere.embed-v4.0`)
- **compartmentId**: OCID of the compartment used to call the service (required)
- **serviceEndpoint**: inference endpoint; derived from the profile region when omitted
- **authType**: `API_KEY` (default), `SECURITY_TOKEN`, `INSTANCE_PRINCIPAL` or `RESOURCE_PRINCIPAL`
- **authProfile**: profile in the OCI config file (default: `DEFAULT`)
- **authFileLocation**: OCI config file path (default: `~/.oci/config`)
- **truncate**: `NONE`, `START` or `END` (default) for inputs longer than the model context
- **inputType**: pin `SEARCH_DOCUMENT`, `SEARCH_QUERY`, `CLASSIFICATION`, `CLUSTERING` or `IMAGE` for every request. By default documents use `SEARCH_DOCUMENT` and queries use `SEARCH_QUERY`.
- **outputDimensions**: requested embedding size for models that support it (e.g. 256, 512, 1024 or 1536 for `cohere.embed-v4.0`)

Requests are automatically split into batches of 96 inputs, the maximum accepted by the service.

## Supported Models

- `cohere.embed-v4.0` (1536 dimensions by default, supports `outputDimensions`)
- `cohere.embed-multilingual-v3.0` (1024 dimensions)
- `cohere.embed-english-v3.0` (1024 dimensions)
- `cohere.embed-multilingual-light-v3.0` / `cohere.embed-english-light-v3.0` (384 dimensions)

Check the [OCI Generative AI documentation](https://docs.oracle.com/en-us/iaas/Content/generative-ai/embed-models.htm) for the models available in your region.
