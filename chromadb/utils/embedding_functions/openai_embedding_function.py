# This file is maintained for backward compatibility
# It imports and re-exports the OpenAIEmbeddingFunction from the new location

from chromadb.embedding_functions.openai_embedding_function import (
    OpenAIEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["OpenAIEmbeddingFunction"]
