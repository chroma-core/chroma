# This file is maintained for backward compatibility
# It imports and re-exports the AmazonBedrockEmbeddingFunction from the new location

from chromadb.embedding_functions.amazon_bedrock_embedding_function import (
    AmazonBedrockEmbeddingFunction,
)

# Re-export everything for backward compatibility
__all__ = ["AmazonBedrockEmbeddingFunction"]
