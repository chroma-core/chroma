# This file is maintained for backward compatibility
# It imports and re-exports the CohereEmbeddingFunction from the new location

from chromadb.embedding_functions.cohere_embedding_function import (
    CohereEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["CohereEmbeddingFunction"]
