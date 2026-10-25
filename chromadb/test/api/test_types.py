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


def test_normalize_embeddings_empty_list() -> None:
    from chromadb.api.types import normalize_embeddings

    # Empty list should raise ValueError
    with pytest.raises(ValueError, match="Expected Embeddings to be non-empty list"):
        normalize_embeddings([])

    # List containing empty embedding should raise descriptive ValueError, not IndexError
    with pytest.raises(
        ValueError,
        match="Expected each embedding in the embeddings to be a non-empty list, got an empty list at pos 0",
    ):
        normalize_embeddings([[]])

    with pytest.raises(
        ValueError,
        match="Expected each embedding in the embeddings to be a non-empty list, got an empty list at pos 0",
    ):
        normalize_embeddings([[], [1.0, 2.0]])

    with pytest.raises(
        ValueError,
        match="Expected each embedding in the embeddings to be a non-empty list, got an empty list at pos 1",
    ):
        normalize_embeddings([[1.0, 2.0], []])


def test_normalize_insert_record_set_empty_embeddings() -> None:
    from chromadb.api.types import normalize_insert_record_set

    with pytest.raises(
        ValueError,
        match="Expected each embedding in the embeddings to be a non-empty list, got an empty list at pos 0",
    ):
        normalize_insert_record_set(ids=["1"], embeddings=[[]])
