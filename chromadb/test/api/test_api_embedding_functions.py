from typing import cast
from chromadb.utils.embedding_functions import (
    DefaultEmbeddingFunction,
)
from chromadb.api.types import EmbeddingFunction, Documents


# test default embedding function
def test_default_embedding() -> None:
    embedding_function = cast(EmbeddingFunction[Documents], DefaultEmbeddingFunction())
    docs = ["this is a test" for _ in range(64)]
    embeddings = embedding_function(docs)
    assert len(embeddings) == 64
