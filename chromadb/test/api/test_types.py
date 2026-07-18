import pytest
from types import SimpleNamespace
from typing import List, cast, Dict, Any
from chromadb.api.types import Documents, Image, Document, Embeddings
from chromadb.api.models.CollectionCommon import CollectionCommon
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


def test_get_request_does_not_mutate_caller_include_list() -> None:
    """Regression test for a bug where _validate_and_prepare_get_request
    mutated the caller's `include` list in place.

    When "data" is requested, Chroma needs to also fetch "uris" internally
    so the collection's data loader can resolve the images/documents. The
    old code did `request_include = include` (an alias, not a copy) and
    then appended "uris" onto it, which silently mutated whatever list
    object the caller passed in. A caller reusing a shared/constant include
    list across multiple get()/query() calls would see it grow a "uris"
    entry after the first call, which is surprising and can leak URIs into
    results the caller never asked for on subsequent calls.
    """
    fake_self = SimpleNamespace(_data_loader=lambda uris: None)
    caller_include = ["documents", "data"]

    request = CollectionCommon._validate_and_prepare_get_request(
        fake_self,
        ids=None,
        where=None,
        where_document=None,
        include=caller_include,
    )

    # The caller's original list must be left untouched.
    assert caller_include == ["documents", "data"]
    # The internal request still needs "uris" so the data loader can run.
    assert request["include"] == ["documents", "data", "uris"]

    # Calling it again with the same list object should behave identically,
    # not accumulate state from the previous call.
    request_again = CollectionCommon._validate_and_prepare_get_request(
        fake_self,
        ids=None,
        where=None,
        where_document=None,
        include=caller_include,
    )
    assert caller_include == ["documents", "data"]
    assert request_again["include"] == ["documents", "data", "uris"]


def test_query_request_does_not_mutate_caller_include_list() -> None:
    """Same regression as above, for _validate_and_prepare_query_request."""
    fake_self = SimpleNamespace(_data_loader=lambda uris: None)
    caller_include = ["documents", "data"]

    request = CollectionCommon._validate_and_prepare_query_request(
        fake_self,
        query_embeddings=[[0.1, 0.2, 0.3]],
        query_texts=None,
        query_images=None,
        query_uris=None,
        ids=None,
        n_results=5,
        where=None,
        where_document=None,
        include=caller_include,
    )

    assert caller_include == ["documents", "data"]
    assert request["include"] == ["documents", "data", "uris"]
