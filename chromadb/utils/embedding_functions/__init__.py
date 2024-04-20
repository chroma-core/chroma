import inspect
import sys
from typing import List, Optional

from chromadb.api.types import Documents, EmbeddingFunction
from chromadb.utils.embedding_functions.amazon_bedrock_embedding_function import (
    AmazonBedrockEmbeddingFunction,
)
from chromadb.utils.embedding_functions.chroma_langchain_embedding_function import (
    create_langchain_embedding,
)
from chromadb.utils.embedding_functions.cohere_embedding_function import (
    CohereEmbeddingFunction,
)
from chromadb.utils.embedding_functions.google_embedding_function import (
    GoogleGenerativeAiEmbeddingFunction,
    GooglePalmEmbeddingFunction,
    GoogleVertexEmbeddingFunction,
)
from chromadb.utils.embedding_functions.huggingface_embedding_function import (
    HuggingFaceEmbeddingFunction,
    HuggingFaceEmbeddingServer,
)
from chromadb.utils.embedding_functions.instructor_embedding_function import (
    InstructorEmbeddingFunction,
)
from chromadb.utils.embedding_functions.jina_embedding_function import (
    JinaEmbeddingFunction,
)
from chromadb.utils.embedding_functions.ollama_embedding_function import (
    OllamaEmbeddingFunction,
)
from chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 import (
    ONNXMiniLM_L6_V2,
    _verify_sha256,
)
from chromadb.utils.embedding_functions.open_clip_embedding_function import (
    OpenCLIPEmbeddingFunction,
)
from chromadb.utils.embedding_functions.openai_embedding_function import (
    OpenAIEmbeddingFunction,
)
from chromadb.utils.embedding_functions.roboflow_embedding_function import (
    RoboflowEmbeddingFunction,
)
from chromadb.utils.embedding_functions.sentence_transformer_embedding_function import (
    SentenceTransformerEmbeddingFunction,
)
from chromadb.utils.embedding_functions.text2vec_embedding_function import (
    Text2VecEmbeddingFunction,
)

__all__ = [
    "AmazonBedrockEmbeddingFunction",
    "create_langchain_embedding",
    "CohereEmbeddingFunction",
    "GoogleGenerativeAiEmbeddingFunction",
    "GooglePalmEmbeddingFunction",
    "GoogleVertexEmbeddingFunction",
    "HuggingFaceEmbeddingFunction",
    "HuggingFaceEmbeddingServer",
    "InstructorEmbeddingFunction",
    "JinaEmbeddingFunction",
    "OllamaEmbeddingFunction",
    "OpenCLIPEmbeddingFunction",
    "ONNXMiniLM_L6_V2",
    "OpenAIEmbeddingFunction",
    "RoboflowEmbeddingFunction",
    "SentenceTransformerEmbeddingFunction",
    "Text2VecEmbeddingFunction",
    "_verify_sha256",
]

try:
    from chromadb.is_thin_client import is_thin_client
except ImportError:
    is_thin_client = False


def DefaultEmbeddingFunction() -> Optional[EmbeddingFunction[Documents]]:
    if is_thin_client:
        return None
    else:
        return ONNXMiniLM_L6_V2()


_classes = [
    name
    for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
    if obj.__module__ == __name__
]


def get_builtins() -> List[str]:
    return _classes
