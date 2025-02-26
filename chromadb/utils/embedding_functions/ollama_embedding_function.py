# This file is maintained for backward compatibility
# It imports and re-exports the OllamaEmbeddingFunction from the new location

from chromadb.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["OllamaEmbeddingFunction"]
