# This file is maintained for backward compatibility
# It imports and re-exports the SentenceTransformerEmbeddingFunction from the new location

from chromadb.embedding_functions.sentence_transformer_embedding_function import (
    SentenceTransformerEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["SentenceTransformerEmbeddingFunction"]
