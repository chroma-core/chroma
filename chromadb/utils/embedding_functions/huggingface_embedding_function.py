# This file is maintained for backward compatibility
# It imports and re-exports the HuggingFace embedding functions from the new location

from chromadb.embedding_functions.huggingface_embedding_function import (
    HuggingFaceEmbeddingFunction,
    HuggingFaceEmbeddingServer,
)

# Re-export everything for backward compatibility
__all__ = ["HuggingFaceEmbeddingFunction", "HuggingFaceEmbeddingServer"]
