from typing import Dict, Any, Type
from chromadb.embedding_functions.embedding_function import EmbeddingFunction, Space
from chromadb.api.types import Embeddable

# Import all embedding functions
from chromadb.embedding_functions.cohere_embedding_function import (
    CohereEmbeddingFunction,
)
from chromadb.embedding_functions.openai_embedding_function import (
    OpenAIEmbeddingFunction,
)
from chromadb.embedding_functions.huggingface_embedding_function import (
    HuggingFaceEmbeddingFunction,
    HuggingFaceEmbeddingServer,
)
from chromadb.embedding_functions.sentence_transformer_embedding_function import (
    SentenceTransformerEmbeddingFunction,
)
from chromadb.embedding_functions.google_embedding_function import (
    GooglePalmEmbeddingFunction,
    GoogleGenerativeAiEmbeddingFunction,
    GoogleVertexEmbeddingFunction,
)
from chromadb.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)
from chromadb.embedding_functions.instructor_embedding_function import (
    InstructorEmbeddingFunction,
)
from chromadb.embedding_functions.jina_embedding_function import JinaEmbeddingFunction
from chromadb.embedding_functions.voyageai_embedding_function import (
    VoyageAIEmbeddingFunction,
)
from chromadb.embedding_functions.onnx_mini_lm_l6_v2 import ONNXMiniLM_L6_V2
from chromadb.embedding_functions.open_clip_embedding_function import (
    OpenCLIPEmbeddingFunction,
)
from chromadb.embedding_functions.roboflow_embedding_function import (
    RoboflowEmbeddingFunction,
)
from chromadb.embedding_functions.text2vec_embedding_function import (
    Text2VecEmbeddingFunction,
)
from chromadb.embedding_functions.amazon_bedrock_embedding_function import (
    AmazonBedrockEmbeddingFunction,
)
from chromadb.embedding_functions.chroma_langchain_embedding_function import (
    ChromaLangchainEmbeddingFunction,
)

# Dictionary of supported embedding functions
supported_embedding_functions: Dict[str, Type[EmbeddingFunction[Embeddable]]] = {
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
}


# Function to register custom embedding functions
def register_embedding_function(ef_class: Type[EmbeddingFunction[Embeddable]]) -> None:
    """Register a custom embedding function.

    Args:
        ef_class: The embedding function class to register.
    """
    # Create an instance to get the name
    ef_instance = ef_class()
    name = ef_instance.name()
    supported_embedding_functions[name] = ef_class


# Function to convert config to embedding function
def config_to_embedding_function(
    config: Dict[str, Any]
) -> EmbeddingFunction[Embeddable]:
    """Convert a config dictionary to an embedding function.

    Args:
        config: The config dictionary.

    Returns:
        The embedding function.
    """
    if "name" not in config:
        raise ValueError("Config must contain a 'name' field.")

    name = config["name"]
    if name not in supported_embedding_functions:
        raise ValueError(f"Unsupported embedding function: {name}")

    ef_config = config.get("config", {})

    # Handle each embedding function type specifically
    if name == "cohere":
        return CohereEmbeddingFunction.build_from_config(ef_config)
    elif name == "openai":
        return OpenAIEmbeddingFunction.build_from_config(ef_config)
    elif name == "huggingface":
        return HuggingFaceEmbeddingFunction.build_from_config(ef_config)
    elif name == "huggingface_server":
        return HuggingFaceEmbeddingServer.build_from_config(ef_config)
    elif name == "sentence_transformer":
        return SentenceTransformerEmbeddingFunction.build_from_config(ef_config)
    elif name == "google_palm":
        return GooglePalmEmbeddingFunction.build_from_config(ef_config)
    elif name == "google_generative_ai":
        return GoogleGenerativeAiEmbeddingFunction.build_from_config(ef_config)
    elif name == "google_vertex":
        return GoogleVertexEmbeddingFunction.build_from_config(ef_config)
    elif name == "ollama":
        return OllamaEmbeddingFunction.build_from_config(ef_config)
    elif name == "instructor":
        return InstructorEmbeddingFunction.build_from_config(ef_config)
    elif name == "jina":
        return JinaEmbeddingFunction.build_from_config(ef_config)
    elif name == "voyageai":
        return VoyageAIEmbeddingFunction.build_from_config(ef_config)
    elif name == "onnx_mini_lm_l6_v2":
        return ONNXMiniLM_L6_V2.build_from_config(ef_config)
    elif name == "open_clip":
        return OpenCLIPEmbeddingFunction.build_from_config(ef_config)
    elif name == "roboflow":
        return RoboflowEmbeddingFunction.build_from_config(ef_config)
    elif name == "text2vec":
        return Text2VecEmbeddingFunction.build_from_config(ef_config)
    elif name == "amazon_bedrock":
        return AmazonBedrockEmbeddingFunction.build_from_config(ef_config)
    elif name == "chroma_langchain":
        return ChromaLangchainEmbeddingFunction.build_from_config(ef_config)
    else:
        # This should never happen due to the check above
        raise ValueError(f"Unsupported embedding function: {name}")


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
    "register_embedding_function",
    "config_to_embedding_function",
    "supported_embedding_functions",
]
