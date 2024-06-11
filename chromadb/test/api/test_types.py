import pytest
from typing import List, cast
from chromadb.api.types import EmbeddingFunction, Documents, Image, Document, Embeddings
import numpy as np


def random_embeddings() -> Embeddings:
    return cast(Embeddings, np.random.random(size=(10, 10)).tolist())


def random_image() -> Image:
    return np.random.randint(0, 255, size=(10, 10, 3), dtype=np.int32)


def random_documents() -> List[Document]:
    return [str(random_image()) for _ in range(10)]


def test_embedding_function_results_format_when_response_is_valid() -> None:
    valid_embeddings = random_embeddings()

    class TestValidEmbeddingFunction(EmbeddingFunction[Documents]):
        def __call__(self, input: Documents) -> Embeddings:
            return valid_embeddings

    ef = TestValidEmbeddingFunction()
    assert valid_embeddings == ef(random_documents())


def test_embedding_function_results_format_when_response_is_invalid() -> None:
    invalid_embedding = {"error": "test"}

    class TestInvalidEmbeddingFunction(EmbeddingFunction[Documents]):
        def __call__(self, input: Documents) -> Embeddings:
            return cast(Embeddings, invalid_embedding)

    ef = TestInvalidEmbeddingFunction()
    with pytest.raises(ValueError) as e:
        ef(random_documents())
    assert e.type is ValueError
