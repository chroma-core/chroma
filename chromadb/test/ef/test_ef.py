from unittest.mock import MagicMock

from chromadb.utils import embedding_functions
from chromadb.utils.embedding_functions import (
    EmbeddingFunction,
    register_embedding_function,
)
from typing import Dict, Any
import numpy as np
import pytest
from chromadb.api.types import (
    Embeddings,
    Space,
    Embeddable,
    SparseEmbeddingFunction,
)
from chromadb.api.models.CollectionCommon import validation_context


def test_get_builtins_holds() -> None:
    """
    Ensure that `get_builtins` is consistent after the ef migration.

    This test is intended to be temporary until the ef migration is complete as
    these expected builtins are likely to grow as long as users add new
    embedding functions.

    REMOVE ME ON THE NEXT EF ADDITION
    """
    expected_builtins = {
        "AmazonBedrockEmbeddingFunction",
        "BasetenEmbeddingFunction",
        "CloudflareWorkersAIEmbeddingFunction",
        "CohereEmbeddingFunction",
        "VoyageAIEmbeddingFunction",
        "GoogleGenerativeAiEmbeddingFunction",
        "GooglePalmEmbeddingFunction",
        "GoogleVertexEmbeddingFunction",
        "GoogleGeminiEmbeddingFunction",
        "GoogleGenaiEmbeddingFunction",  # Backward compatibility alias
        "HuggingFaceEmbeddingFunction",
        "HuggingFaceEmbeddingServer",
        "InstructorEmbeddingFunction",
        "JinaEmbeddingFunction",
        "MistralEmbeddingFunction",
        "MorphEmbeddingFunction",
        "NomicEmbeddingFunction",
        "ONNXMiniLM_L6_V2",
        "OllamaEmbeddingFunction",
        "OpenAIEmbeddingFunction",
        "OpenCLIPEmbeddingFunction",
        "RoboflowEmbeddingFunction",
        "SentenceTransformerEmbeddingFunction",
        "Text2VecEmbeddingFunction",
        "ChromaLangchainEmbeddingFunction",
        "TogetherAIEmbeddingFunction",
        "DefaultEmbeddingFunction",
        "HuggingFaceSparseEmbeddingFunction",
        "FastembedSparseEmbeddingFunction",
        "Bm25EmbeddingFunction",
        "ChromaCloudQwenEmbeddingFunction",
        "ChromaCloudSpladeEmbeddingFunction",
        "ChromaBm25EmbeddingFunction",
        "PerplexityEmbeddingFunction",
    }

    assert expected_builtins == embedding_functions.get_builtins()


def test_default_ef_exists() -> None:
    assert hasattr(embedding_functions, "DefaultEmbeddingFunction")
    default_ef = embedding_functions.DefaultEmbeddingFunction()

    assert default_ef is not None
    assert isinstance(default_ef, EmbeddingFunction) or isinstance(
        default_ef, SparseEmbeddingFunction
    )


def test_ef_imports() -> None:
    for ef in embedding_functions.get_builtins():
        # Langchain embedding function is a special snowflake
        if ef == "ChromaLangchainEmbeddingFunction":
            continue
        assert hasattr(embedding_functions, ef)
        assert isinstance(getattr(embedding_functions, ef), type)
        assert issubclass(
            getattr(embedding_functions, ef), EmbeddingFunction
        ) or issubclass(getattr(embedding_functions, ef), SparseEmbeddingFunction)


@register_embedding_function
class CustomEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __init__(self, dim: int = 3):
        self._dim = dim

    @validation_context("custom_ef_call")
    def __call__(self, input: Embeddable) -> Embeddings:
        raise Exception("This is a test exception")

    @staticmethod
    def name() -> str:
        return "custom_ef"

    def get_config(self) -> Dict[str, Any]:
        return {"dim": self._dim}

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "CustomEmbeddingFunction":
        return CustomEmbeddingFunction(dim=config["dim"])

    def default_space(self) -> Space:
        return "cosine"


def test_langchain_embed_query_delegates_to_langchain() -> None:
    """Verify that embed_query calls langchain's embed_query (not embed_documents)
    so that asymmetric retrieval models produce query-specific representations."""
    from chromadb.utils.embedding_functions.chroma_langchain_embedding_function import (
        ChromaLangchainEmbeddingFunction,
    )

    mock_lc = MagicMock()
    mock_lc.embed_query.side_effect = lambda text: [float(ord(c)) for c in text[:3]]
    mock_lc.embed_documents.return_value = [[0.0]]

    ef = ChromaLangchainEmbeddingFunction.__new__(ChromaLangchainEmbeddingFunction)
    ef.embedding_function = mock_lc

    results = ef.embed_query(input=["hi", "bye"])

    assert mock_lc.embed_query.call_count == 2
    mock_lc.embed_documents.assert_not_called()
    assert len(results) == 2
    assert all(isinstance(r, np.ndarray) and r.dtype == np.float32 for r in results)


def test_validation_context_with_custom_ef() -> None:
    custom_ef = CustomEmbeddingFunction()

    with pytest.raises(Exception) as excinfo:
        custom_ef(["test data"])

    original_msg = "This is a test exception"
    expected_msg = f"{original_msg} in custom_ef_call."
    assert str(excinfo.value) == expected_msg
    assert excinfo.value.args == (expected_msg,)
