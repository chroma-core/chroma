import pytest
import chromadb
from typing import List, cast, Dict, Any
from chromadb.api.types import Documents, Image, Document, Embeddings, validate_where
from chromadb.utils.embedding_functions import (
    EmbeddingFunction,
    register_embedding_function,
)
import numpy as np


@pytest.mark.parametrize("operator", ["$gt", "$gte", "$lt", "$lte"])
@pytest.mark.parametrize("operand", [True, False])
def test_validate_where_rejects_boolean_ordering_operand(
    operator: str, operand: bool
) -> None:
    with pytest.raises(ValueError, match="Expected operand value to be an int or a float"):
        validate_where({"value": {operator: operand}})


@pytest.mark.parametrize("populated", [False, True])
def test_collection_rejects_boolean_ordering_operand(populated: bool) -> None:
    client = chromadb.EphemeralClient()
    collection = client.create_collection(
        "boolean-ordering-operand", embedding_function=None
    )
    try:
        if populated:
            collection.add(ids=["one"], embeddings=[[1.0, 2.0]], metadatas=[{"value": 2}])
        with pytest.raises(
            ValueError, match="Expected operand value to be an int or a float"
        ):
            collection.get(where={"value": {"$gt": True}})
    finally:
        client.delete_collection(collection.name)


@pytest.mark.parametrize("operator", ["$gt", "$gte", "$lt", "$lte"])
@pytest.mark.parametrize("operand", [1, 1.5])
def test_validate_where_accepts_numeric_ordering_operand(
    operator: str, operand: float
) -> None:
    validate_where({"value": {operator: operand}})
    validate_where({"value": {"$eq": True}})
    validate_where({"value": {"$ne": False}})


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
