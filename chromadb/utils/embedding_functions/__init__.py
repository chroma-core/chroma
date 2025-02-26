from typing import Optional, Set, cast

from chromadb.api.types import Documents

# Import all embedding functions from the new location
from chromadb.embedding_functions import (
    EmbeddingFunction,
    Space,
    CohereEmbeddingFunction,
    OpenAIEmbeddingFunction,
    HuggingFaceEmbeddingFunction,
    HuggingFaceEmbeddingServer,
    SentenceTransformerEmbeddingFunction,
    GooglePalmEmbeddingFunction,
    GoogleGenerativeAiEmbeddingFunction,
    GoogleVertexEmbeddingFunction,
    OllamaEmbeddingFunction,
    InstructorEmbeddingFunction,
    JinaEmbeddingFunction,
    VoyageAIEmbeddingFunction,
    ONNXMiniLM_L6_V2,
    OpenCLIPEmbeddingFunction,
    RoboflowEmbeddingFunction,
    Text2VecEmbeddingFunction,
    AmazonBedrockEmbeddingFunction,
    ChromaLangchainEmbeddingFunction,
    register_embedding_function,
    config_to_embedding_function,
    known_embedding_functions,
)

# Re-export the create_langchain_embedding function for backward compatibility
from chromadb.embedding_functions.chroma_langchain_embedding_function import (  # noqa: F401
    create_langchain_embedding,
)

try:
    from chromadb.is_thin_client import is_thin_client
except ImportError:
    is_thin_client = False


# Define and export the default embedding function for backward compatibility
def DefaultEmbeddingFunction() -> Optional[EmbeddingFunction[Documents]]:
    if is_thin_client:
        return None
    else:
        return cast(
            EmbeddingFunction[Documents],
            ONNXMiniLM_L6_V2(),
        )


# Get all the class names for backward compatibility
_all_classes: Set[str] = {
    "CohereEmbeddingFunction",
    "OpenAIEmbeddingFunction",
    "HuggingFaceEmbeddingFunction",
    "HuggingFaceEmbeddingServer",
    "SentenceTransformerEmbeddingFunction",
    "GooglePalmEmbeddingFunction",
    "GoogleGenerativeAiEmbeddingFunction",
    "GoogleVertexEmbeddingFunction",
    "OllamaEmbeddingFunction",
    "InstructorEmbeddingFunction",
    "JinaEmbeddingFunction",
    "VoyageAIEmbeddingFunction",
    "ONNXMiniLM_L6_V2",
    "OpenCLIPEmbeddingFunction",
    "RoboflowEmbeddingFunction",
    "Text2VecEmbeddingFunction",
    "AmazonBedrockEmbeddingFunction",
    "ChromaLangchainEmbeddingFunction",
}


def get_builtins() -> Set[str]:
    return _all_classes


# Re-export everything for backward compatibility
__all__ = [
    "EmbeddingFunction",
    "Space",
    "CohereEmbeddingFunction",
    "OpenAIEmbeddingFunction",
    "HuggingFaceEmbeddingFunction",
    "HuggingFaceEmbeddingServer",
    "SentenceTransformerEmbeddingFunction",
    "GooglePalmEmbeddingFunction",
    "GoogleGenerativeAiEmbeddingFunction",
    "GoogleVertexEmbeddingFunction",
    "OllamaEmbeddingFunction",
    "InstructorEmbeddingFunction",
    "JinaEmbeddingFunction",
    "VoyageAIEmbeddingFunction",
    "ONNXMiniLM_L6_V2",
    "OpenCLIPEmbeddingFunction",
    "RoboflowEmbeddingFunction",
    "Text2VecEmbeddingFunction",
    "AmazonBedrockEmbeddingFunction",
    "ChromaLangchainEmbeddingFunction",
    "DefaultEmbeddingFunction",
    "create_langchain_embedding",
    "register_embedding_function",
    "config_to_embedding_function",
    "known_embedding_functions",
]
