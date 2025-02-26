# This file is maintained for backward compatibility
# It imports and re-exports the VoyageAIEmbeddingFunction from the new location

from chromadb.embedding_functions.voyageai_embedding_function import (
    VoyageAIEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["VoyageAIEmbeddingFunction"]
