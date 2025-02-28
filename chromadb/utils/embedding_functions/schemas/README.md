# Embedding Function Schemas

This directory contains JSON schemas for all embedding functions in Chroma. These schemas are used to validate the configuration of embedding functions and ensure that they are properly configured.

## Schema Structure

Each schema follows the JSON Schema Draft-07 specification and includes:

- `version`: The version of the schema
- `title`: The title of the schema
- `description`: A description of the schema
- `properties`: The properties that can be configured for the embedding function
- `required`: The properties that are required for the embedding function
- `additionalProperties`: Whether additional properties are allowed (always set to `false` to ensure strict validation)

## Available Schemas

The following schemas are available:

- `amazon_bedrock.json`: Schema for the Amazon Bedrock embedding function
- `chroma_langchain.json`: Schema for the Chroma Langchain embedding function
- `cohere.json`: Schema for the Cohere embedding function
- `default.json`: Schema for the default embedding function
- `google_generative_ai.json`: Schema for the Google Generative AI embedding function
- `google_palm.json`: Schema for the Google PaLM embedding function
- `google_vertex.json`: Schema for the Google Vertex embedding function
- `huggingface.json`: Schema for the HuggingFace embedding function
- `huggingface_server.json`: Schema for the HuggingFace embedding server
- `instructor.json`: Schema for the Instructor embedding function
- `jina.json`: Schema for the Jina embedding function
- `ollama.json`: Schema for the Ollama embedding function
- `onnx_mini_lm_l6_v2.json`: Schema for the ONNX MiniLM L6 V2 embedding function
- `open_clip.json`: Schema for the OpenCLIP embedding function
- `openai.json`: Schema for the OpenAI embedding function
- `roboflow.json`: Schema for the Roboflow embedding function
- `sentence_transformer.json`: Schema for the SentenceTransformer embedding function
- `text2vec.json`: Schema for the Text2Vec embedding function
- `voyageai.json`: Schema for the VoyageAI embedding function

## Usage

The schemas can be used to validate the configuration of embedding functions using the `validate_config` function:

```python
from chromadb.utils.embedding_functions.schemas import validate_config

# Validate a configuration
config = {
    "api_key_env_var": "CHROMA_OPENAI_API_KEY",
    "model_name": "text-embedding-ada-002"
}
validate_config(config, "openai")
```

## Cross-Language Support

These schemas are designed to be used across different client libraries. They can be loaded and used by any language that supports JSON Schema validation.

## Adding New Schemas

To add a new schema:

1. Create a new JSON file in this directory with the name of the embedding function (e.g., `new_function.json`)
2. Define the schema following the JSON Schema Draft-07 specification
3. Update the embedding function to use the schema for validation

## Schema Versioning

Each schema includes a version number to support future changes to embedding function configurations. When making changes to a schema, increment the version number to ensure backward compatibility.
