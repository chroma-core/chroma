# This file is maintained for backward compatibility
# It imports and re-exports the ChromaLangchainEmbeddingFunction and create_langchain_embedding from the new location

from chromadb.embedding_functions.chroma_langchain_embedding_function import (
    ChromaLangchainEmbeddingFunction,
    create_langchain_embedding,
)

# Re-export everything for backward compatibility
__all__ = ["ChromaLangchainEmbeddingFunction", "create_langchain_embedding"]
