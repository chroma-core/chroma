import pytest
from typing import List, cast, Dict, Any
from chromadb.api.types import Documents, Image, Document, Embeddings
from chromadb.errors import DuplicateIDError
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

def _collection_for_validation_tests():
    """Build a CollectionCommon without a live client, for validation-only tests."""
    from uuid import uuid4

    from chromadb.api.models.CollectionCommon import CollectionCommon
    from chromadb.types import Collection as CollectionModel

    collection_model = CollectionModel(
        id=uuid4(),
        name="validation_test_collection",
        configuration_json={},
        serialized_schema=None,
        metadata=None,
        dimension=4,
        tenant="tenant",
        database="database",
        version=0,
        log_position=0,
    )
    return CollectionCommon(
        client=cast(Any, object()),
        model=collection_model,
        embedding_function=None,
    )


def _query_with_ids(collection, ids):
    return collection._validate_and_prepare_query_request(
        query_embeddings=[np.array([1.0, 2.0, 3.0, 4.0])],
        query_texts=None,
        query_images=None,
        query_uris=None,
        ids=ids,
        n_results=1,
        where=None,
        where_document=None,
        include=["documents"],
    )


def test_query_ids_are_validated() -> None:
    """query() must validate its ids filter the same way get() and delete() do."""
    collection = _collection_for_validation_tests()

    # Valid ids pass through untouched
    request = _query_with_ids(collection, ids=["a", "b"])
    assert request["ids"] == ["a", "b"]

    # Non-string ids must be rejected client-side, not forwarded to the server
    with pytest.raises(ValueError, match="Expected ID to be a str"):
        _query_with_ids(collection, ids=[123])

    # Duplicate ids must be rejected, like get() and delete() do
    with pytest.raises(DuplicateIDError, match="Expected IDs to be unique"):
        _query_with_ids(collection, ids=["a", "a"])
