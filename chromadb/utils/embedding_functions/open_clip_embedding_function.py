# This file is maintained for backward compatibility
# It imports and re-exports the OpenCLIPEmbeddingFunction from the new location

from chromadb.embedding_functions.open_clip_embedding_function import (
    OpenCLIPEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["OpenCLIPEmbeddingFunction"]
