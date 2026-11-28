import pytest
from typing import List, cast, Dict, Any
from chromadb import errors
from chromadb.api.types import (
    Documents,
    Image,
    Document,
    Embeddings,
    validate_ids,
)
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


def duplicate_ids_message(n_unique_duplicates: int) -> str:
    """Return the message validate_ids raises for n uniquely-duplicated IDs."""
    # Zero padded so that lexicographic order matches creation order.
    ids = [f"id{i:02d}" for i in range(n_unique_duplicates)] * 2
    with pytest.raises(errors.DuplicateIDError) as excinfo:
        validate_ids(ids)
    return str(excinfo.value)


def test_validate_ids_sorts_duplicates_below_report_limit() -> None:
    # Set iteration order depends on the hash seed, so joining the set directly
    # made this message unstable between processes.
    assert duplicate_ids_message(4) == (
        "Expected IDs to be unique, found duplicates of: id00, id01, id02, id03"
    )


def test_validate_ids_at_report_limit_lists_all_without_eliding() -> None:
    message = duplicate_ids_message(10)
    assert ", ..., " not in message
    assert message == (
        "Expected IDs to be unique, found 10 duplicated IDs: "
        + ", ".join(f"id{i:02d}" for i in range(10))
    )


def test_validate_ids_above_report_limit_elides_the_middle() -> None:
    assert duplicate_ids_message(12) == (
        "Expected IDs to be unique, found 12 duplicated IDs: "
        "id00, id01, id02, id03, id04, ..., id07, id08, id09, id10, id11"
    )
