# This file is maintained for backward compatibility
# It imports and re-exports the Text2VecEmbeddingFunction from the new location

from chromadb.embedding_functions.text2vec_embedding_function import (
    Text2VecEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["Text2VecEmbeddingFunction"]
