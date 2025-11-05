from typing import Dict, Any, Type, Set
from chromadb.api.types import (
    EmbeddingFunction,
    DefaultEmbeddingFunction,
    SparseEmbeddingFunction,
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
    JinaQueryConfig,
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
from chromadb.utils.embedding_functions.baseten_embedding_function import (
    BasetenEmbeddingFunction,
)
from chromadb.utils.embedding_functions.cloudflare_workers_ai_embedding_function import (
    CloudflareWorkersAIEmbeddingFunction,
)
from chromadb.utils.embedding_functions.together_ai_embedding_function import (
    TogetherAIEmbeddingFunction,
)
from chromadb.utils.embedding_functions.mistral_embedding_function import (
    MistralEmbeddingFunction,
)
from chromadb.utils.embedding_functions.morph_embedding_function import (
    MorphEmbeddingFunction,
)
from chromadb.utils.embedding_functions.huggingface_sparse_embedding_function import (
    HuggingFaceSparseEmbeddingFunction,
)
from chromadb.utils.embedding_functions.fastembed_sparse_embedding_function import (
    FastembedSparseEmbeddingFunction,
)
from chromadb.utils.embedding_functions.bm25_embedding_function import (
    Bm25EmbeddingFunction,
)
from chromadb.utils.embedding_functions.chroma_cloud_qwen_embedding_function import (
    ChromaCloudQwenEmbeddingFunction,
)
from chromadb.utils.embedding_functions.chroma_cloud_splade_embedding_function import (
    ChromaCloudSpladeEmbeddingFunction,
)
from chromadb.utils.embedding_functions.chroma_bm25_embedding_function import (
    ChromaBm25EmbeddingFunction,
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
    "MistralEmbeddingFunction",
    "MorphEmbeddingFunction",
    "VoyageAIEmbeddingFunction",
    "ONNXMiniLM_L6_V2",
    "OpenCLIPEmbeddingFunction",
    "RoboflowEmbeddingFunction",
    "Text2VecEmbeddingFunction",
    "AmazonBedrockEmbeddingFunction",
    "ChromaLangchainEmbeddingFunction",
    "BasetenEmbeddingFunction",
    "CloudflareWorkersAIEmbeddingFunction",
    "TogetherAIEmbeddingFunction",
    "DefaultEmbeddingFunction",
    "HuggingFaceSparseEmbeddingFunction",
    "FastembedSparseEmbeddingFunction",
    "Bm25EmbeddingFunction",
    "ChromaCloudQwenEmbeddingFunction",
    "ChromaCloudSpladeEmbeddingFunction",
    "ChromaBm25EmbeddingFunction",
}


def get_builtins() -> Set[str]:
    return _all_classes


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
    "mistral": MistralEmbeddingFunction,
    "morph": MorphEmbeddingFunction,
    "voyageai": VoyageAIEmbeddingFunction,
    "onnx_mini_lm_l6_v2": ONNXMiniLM_L6_V2,
    "open_clip": OpenCLIPEmbeddingFunction,
    "roboflow": RoboflowEmbeddingFunction,
    "text2vec": Text2VecEmbeddingFunction,
    "amazon_bedrock": AmazonBedrockEmbeddingFunction,
    "chroma_langchain": ChromaLangchainEmbeddingFunction,
    "baseten": BasetenEmbeddingFunction,
    "default": DefaultEmbeddingFunction,
    "cloudflare_workers_ai": CloudflareWorkersAIEmbeddingFunction,
    "together_ai": TogetherAIEmbeddingFunction,
    "chroma-cloud-qwen": ChromaCloudQwenEmbeddingFunction,
}

sparse_known_embedding_functions: Dict[str, Type[SparseEmbeddingFunction]] = {  # type: ignore
    "huggingface_sparse": HuggingFaceSparseEmbeddingFunction,
    "fastembed_sparse": FastembedSparseEmbeddingFunction,
    "bm25": Bm25EmbeddingFunction,
    "chroma-cloud-splade": ChromaCloudSpladeEmbeddingFunction,
    "chroma_bm25": ChromaBm25EmbeddingFunction,
}


def register_embedding_function(ef_class=None):  # type: ignore
    """Register a custom embedding function.

    Can be used as a decorator:
        @register_embedding_function
        class MyEmbedding(EmbeddingFunction):
            @classmethod
            def name(cls): return "my_embedding"

    Or directly:
        register_embedding_function(MyEmbedding)

    Args:
        ef_class: The embedding function class to register.
    """

    def _register(cls):  # type: ignore
        try:
            name = cls.name()
            known_embedding_functions[name] = cls
        except Exception as e:
            raise ValueError(f"Failed to register embedding function: {e}")
        return cls  # Return the class unchanged

    # If called with a class, register it immediately
    if ef_class is not None:
        return _register(ef_class)  # type: ignore

    # If called without arguments, return a decorator
    return _register


def register_sparse_embedding_function(ef_class=None):  # type: ignore
    """Register a custom sparse embedding function.

    Can be used as a decorator:
        @register_sparse_embedding_function
        class MySparseEmbeddingFunction(SparseEmbeddingFunction):
            @classmethod
            def name(cls): return "my_sparse_embedding"
    """

    def _register(cls):  # type: ignore
        try:
            name = cls.name()
            sparse_known_embedding_functions[name] = cls
        except Exception as e:
            raise ValueError(f"Failed to register sparse embedding function: {e}")
        return cls  # Return the class unchanged

    if ef_class is not None:
        return _register(ef_class)  # type: ignore

    return _register


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
    "BasetenEmbeddingFunction",
    "CloudflareWorkersAIEmbeddingFunction",
    "HuggingFaceEmbeddingFunction",
    "HuggingFaceEmbeddingServer",
    "SentenceTransformerEmbeddingFunction",
    "GooglePalmEmbeddingFunction",
    "GoogleGenerativeAiEmbeddingFunction",
    "GoogleVertexEmbeddingFunction",
    "OllamaEmbeddingFunction",
    "InstructorEmbeddingFunction",
    "JinaEmbeddingFunction",
    "JinaQueryConfig",
    "MistralEmbeddingFunction",
    "MorphEmbeddingFunction",
    "VoyageAIEmbeddingFunction",
    "ONNXMiniLM_L6_V2",
    "OpenCLIPEmbeddingFunction",
    "RoboflowEmbeddingFunction",
    "Text2VecEmbeddingFunction",
    "AmazonBedrockEmbeddingFunction",
    "ChromaLangchainEmbeddingFunction",
    "TogetherAIEmbeddingFunction",
    "HuggingFaceSparseEmbeddingFunction",
    "FastembedSparseEmbeddingFunction",
    "Bm25EmbeddingFunction",
    "ChromaCloudQwenEmbeddingFunction",
    "ChromaCloudSpladeEmbeddingFunction",
    "ChromaBm25EmbeddingFunction",
    "register_embedding_function",
    "config_to_embedding_function",
    "known_embedding_functions",
]
