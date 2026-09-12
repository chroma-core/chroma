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
        ) -> None:
            pass

        def __call__(self, input: Documents) -> Embeddings:
            # Return something that's not a valid Embeddings type
            return cast(Embeddings, invalid_embedding)

    ef = TestEmbeddingFunction()

    # The EmbeddingFunction protocol should validate the return value
    # but we need to bypass the protocol's __call__ wrapper for this test
    with pytest.raises(ValueError):
        # This should raise a ValueError during normalization/validation
        result = ef.__call__(random_documents())
        # The normalize_embeddings function will raise a ValueError when given an invalid embedding
        from chromadb.api.types import normalize_embeddings

        normalize_embeddings(result)


def test_embedding_function_input_validation_and_normalization() -> None:
    @register_embedding_function
    class MockIdentityEF(EmbeddingFunction[Documents]):
        def __init__(self) -> None:
            pass

        @staticmethod
        def name() -> str:
            return "mock_identity"

        @staticmethod
        def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
            return MockIdentityEF()

        def get_config(self) -> Dict[str, Any]:
            return {}

        @staticmethod
        def validate_config(config: Dict[str, Any]) -> None:
            pass

        def validate_config_update(
            self, old_config: Dict[str, Any], new_config: Dict[str, Any]
        ) -> None:
            pass

        def __call__(self, input: Documents) -> Embeddings:
            # Under the wrapper, input must always arrive as a List[Document]
            assert isinstance(input, list)
            return [np.ones(10, dtype=np.float32) for _ in input]

    ef = MockIdentityEF()

    res_str = ef("test sentence")  # type: ignore[arg-type]
    assert len(res_str) == 1
    assert res_str[0].shape == (10,)

    res_list = ef(["doc1", "doc2", "doc3"])
    assert len(res_list) == 3

    assert ef([]) == []

    with pytest.raises(ValueError, match="Expected input to be a non-empty string"):
        ef(None)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Expected input to be a list or str"):
        ef(42)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Expected document to be a str"):
        ef(["valid", 123])  # type: ignore[list-item]
