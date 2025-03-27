from typing import Dict, Any, Type, Set
from chromadb.api.types import (
    EmbeddingFunction,
    Embeddings,
    Documents,
)

# Import all embedding functions
from chromadb.utils.embedding_functions.cohere_embedding_function import (
    CohereEmbeddingFunction,
)
from chromadb.utils.embedding_functions.openai_embedding_function import (
    OpenAIEmbeddingFunction,
)
from chromadb.utils.embedding_functions.huggingface_embedding_function import (
    HuggingFaceEmbeddingFunction,
    HuggingFaceEmbeddingServer,
)
from chromadb.utils.embedding_functions.sentence_transformer_embedding_function import (
    SentenceTransformerEmbeddingFunction,
)
from chromadb.utils.embedding_functions.google_embedding_function import (
    GooglePalmEmbeddingFunction,
    GoogleGenerativeAiEmbeddingFunction,
    GoogleVertexEmbeddingFunction,
)
from chromadb.utils.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)
from chromadb.utils.embedding_functions.instructor_embedding_function import (
    InstructorEmbeddingFunction,
)
from chromadb.utils.embedding_functions.jina_embedding_function import (
    JinaEmbeddingFunction,
)
from chromadb.utils.embedding_functions.voyageai_embedding_function import (
    VoyageAIEmbeddingFunction,
)
from chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 import ONNXMiniLM_L6_V2
from chromadb.utils.embedding_functions.open_clip_embedding_function import (
    OpenCLIPEmbeddingFunction,
)
from chromadb.utils.embedding_functions.roboflow_embedding_function import (
    RoboflowEmbeddingFunction,
)
from chromadb.utils.embedding_functions.text2vec_embedding_function import (
    Text2VecEmbeddingFunction,
)
from chromadb.utils.embedding_functions.amazon_bedrock_embedding_function import (
    AmazonBedrockEmbeddingFunction,
)
from chromadb.utils.embedding_functions.chroma_langchain_embedding_function import (
    ChromaLangchainEmbeddingFunction,
)

try:
    from chromadb.is_thin_client import is_thin_client
except ImportError:
    is_thin_client = False

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
    "DefaultEmbeddingFunction",
}


def get_builtins() -> Set[str]:
    return _all_classes


class DefaultEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(self) -> None:
        if is_thin_client:
            return

    def __call__(self, input: Documents) -> Embeddings:
        # Delegate to ONNXMiniLM_L6_V2
        return ONNXMiniLM_L6_V2()(input)

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "DefaultEmbeddingFunction":
        DefaultEmbeddingFunction.validate_config(config)
        return DefaultEmbeddingFunction()

    @staticmethod
    def name() -> str:
        return "default"

    def get_config(self) -> Dict[str, Any]:
        return {}

    def max_tokens(self) -> int:
        return 256

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        return


# Dictionary of supported embedding functions
known_embedding_functions: Dict[str, Type[EmbeddingFunction]] = {  # type: ignore
    "cohere": CohereEmbeddingFunction,
    "openai": OpenAIEmbeddingFunction,
    "huggingface": HuggingFaceEmbeddingFunction,
    "huggingface_server": HuggingFaceEmbeddingServer,
    "sentence_transformer": SentenceTransformerEmbeddingFunction,
    "google_palm": GooglePalmEmbeddingFunction,
    "google_generative_ai": GoogleGenerativeAiEmbeddingFunction,
    "google_vertex": GoogleVertexEmbeddingFunction,
    "ollama": OllamaEmbeddingFunction,
    "instructor": InstructorEmbeddingFunction,
    "jina": JinaEmbeddingFunction,
    "voyageai": VoyageAIEmbeddingFunction,
    "onnx_mini_lm_l6_v2": ONNXMiniLM_L6_V2,
    "open_clip": OpenCLIPEmbeddingFunction,
    "roboflow": RoboflowEmbeddingFunction,
    "text2vec": Text2VecEmbeddingFunction,
    "amazon_bedrock": AmazonBedrockEmbeddingFunction,
    "chroma_langchain": ChromaLangchainEmbeddingFunction,
    "default": DefaultEmbeddingFunction,
}


# Function to register custom embedding functions
def register_embedding_function(ef_class: Type[EmbeddingFunction]) -> None:  # type: ignore
    """Register a custom embedding function.

    Args:
        ef_class: The embedding function class to register.
    """
    name = ef_class.name()
    known_embedding_functions[name] = ef_class


# Function to convert config to embedding function
def config_to_embedding_function(config: Dict[str, Any]) -> EmbeddingFunction:  # type: ignore
    """Convert a config dictionary to an embedding function.

    Args:
        config: The config dictionary.

    Returns:
        The embedding function.
    """
    if "name" not in config:
        raise ValueError("Config must contain a 'name' field.")

    name = config["name"]
    if name not in known_embedding_functions:
        raise ValueError(f"Unsupported embedding function: {name}")

    ef_config = config.get("config", {})

    if known_embedding_functions[name] is None:
        raise ValueError(f"Unsupported embedding function: {name}")

    return known_embedding_functions[name].build_from_config(ef_config)


__all__ = [
    "EmbeddingFunction",
    "DefaultEmbeddingFunction",
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
    "register_embedding_function",
    "config_to_embedding_function",
    "known_embedding_functions",
]
