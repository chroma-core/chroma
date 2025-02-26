# This file is maintained for backward compatibility
# It imports and re-exports the RoboflowEmbeddingFunction from the new location

from chromadb.embedding_functions.roboflow_embedding_function import (
    RoboflowEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["RoboflowEmbeddingFunction"]
