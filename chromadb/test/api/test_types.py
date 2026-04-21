import pytest
from typing import List, cast, Dict, Any
from chromadb.api.types import Documents, Image, Document, Embeddings
from chromadb.utils.embedding_functions import (
    EmbeddingFunction,
    register_embedding_function,
)
import numpy as np


def random_embeddings() -> Embeddings:
    return cast(
        Embeddings, [embedding for embedding in np.random.random(size=(10, 10))]
    )


def random_image() -> Image:
    return np.random.randint(0, 255, size=(10, 10, 3), dtype=np.int64)


def random_documents() -> List[Document]:
    return [str(random_image()) for _ in range(10)]


def test_embedding_function_results_format_when_response_is_valid() -> None:
    valid_embeddings = random_embeddings()

    @register_embedding_function
    class TestEmbeddingFunction(EmbeddingFunction[Documents]):
        def __init__(self) -> None:
            pass

        @staticmethod
        def name() -> str:
            return "test"

        @staticmethod
        def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
            return TestEmbeddingFunction()

        def get_config(self) -> Dict[str, Any]:
            return {}

        def __call__(self, input: Documents) -> Embeddings:
            return valid_embeddings

        @staticmethod
        def validate_config(config: Dict[str, Any]) -> None:
            pass

        def validate_config_update(
            self, old_config: Dict[str, Any], new_config: Dict[str, Any]
        ) -> None:
            pass

    ef = TestEmbeddingFunction()

    embeddings = ef(random_documents())
    for i, e in enumerate(embeddings):
        assert np.array_equal(e, valid_embeddings[i])


def test_embedding_function_results_format_when_response_is_invalid() -> None:
    invalid_embedding = {"error": "test"}

    @register_embedding_function
    class TestEmbeddingFunction(EmbeddingFunction[Documents]):
        def __init__(self) -> None:
            pass

        @staticmethod
        def name() -> str:
            return "test"

        @staticmethod
        def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
            return TestEmbeddingFunction()

        def get_config(self) -> Dict[str, Any]:
            return {}

        @staticmethod
        def validate_config(config: Dict[str, Any]) -> None:
            pass

        def validate_config_update(
            self, old_config: Dict[str, Any], new_config: Dict[str, Any]


def test_default_embedding_function_caches_onnx_instance() -> None:
    """DefaultEmbeddingFunction should cache ONNXMiniLM_L6_V2 instance across calls (#6941).

    Before the fix, __call__ constructed a fresh ONNXMiniLM_L6_V2() on every
    invocation, triggering cold lazy-init of the tokenizer (~5ms) and ONNX
    session. After the fix, the instance is created once and reused.
    """
    from chromadb.api.types import DefaultEmbeddingFunction
    from unittest.mock import patch, MagicMock

    ef = DefaultEmbeddingFunction()
    # _ef should start as None
    assert ef._ef is None

    # Create a mock ONNX instance
    mock_onnx = MagicMock()
    mock_onnx.return_value = [[0.1] * 384]

    # Simulate that __call__ has already created and cached the instance
    ef._ef = mock_onnx

    # Now calling __call__ should reuse the cached instance
    with patch(
        "chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2.ONNXMiniLM_L6_V2"
    ) as MockONNX:
        result = ef(["test"])
        # ONNXMiniLM_L6_V2 should NOT be instantiated again
        MockONNX.assert_not_called()
        # The cached mock should have been called
        mock_onnx.assert_called_once_with(["test"])
