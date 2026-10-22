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


@pytest.mark.parametrize(
    "operator",
    [
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$ne",
        "$eq",
        "$in",
        "$nin",
        "$contains",
        "$not_contains",
    ],
)
def test_validate_where_rejects_top_level_operator_key(operator: str) -> None:
    # A query operator used where a metadata field name is expected (i.e. the
    # user forgot the field name, e.g. {"$in": [1, 2]} instead of
    # {"field": {"$in": [1, 2]}}) must be rejected, not silently accepted.
    from chromadb.api.types import validate_where

    operand: Any = [1, 2] if operator in ("$in", "$nin") else 1
    with pytest.raises(ValueError):
        validate_where({operator: operand})


def test_validate_where_accepts_operator_in_field_expression() -> None:
    # The same operators remain valid inside a field expression.
    from chromadb.api.types import validate_where

    validate_where({"age": {"$gt": 5}})
    validate_where({"tag": {"$in": ["a", "b"]}})
    validate_where({"$and": [{"age": {"$gt": 5}}, {"age": {"$lt": 10}}]})
