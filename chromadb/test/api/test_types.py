import pytest
from typing import List, cast
from chromadb.api.types import EmbeddingFunction, Documents, Image, Document, Embeddings
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

    class TestEmbeddingFunction(EmbeddingFunction[Documents]):
        def __call__(self, input: Documents) -> Embeddings:
            return valid_embeddings

    ef = TestEmbeddingFunction()

    embeddings = ef(random_documents())
    for i, e in enumerate(embeddings):
        assert e.tolist() == valid_embeddings[i].tolist()


def test_embedding_function_results_format_when_response_is_invalid() -> None:
    invalid_embedding = {"error": "test"}

    class TestEmbeddingFunction(EmbeddingFunction[Documents]):
        def __call__(self, input: Documents) -> Embeddings:
            return cast(Embeddings, invalid_embedding)

    ef = TestEmbeddingFunction()
    with pytest.raises(ValueError) as e:
        ef(random_documents())
    assert e.type is ValueError
