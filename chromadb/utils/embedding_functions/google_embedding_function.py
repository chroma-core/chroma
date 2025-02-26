# This file is maintained for backward compatibility
# It imports and re-exports the Google embedding functions from the new location

from chromadb.embedding_functions.google_embedding_function import (
    GooglePalmEmbeddingFunction,
    GoogleGenerativeAiEmbeddingFunction,
    GoogleVertexEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = [
    "GooglePalmEmbeddingFunction",
    "GoogleGenerativeAiEmbeddingFunction",
    "GoogleVertexEmbeddingFunction",
]
