import sys
import types
from typing import List

import numpy as np
import pytest

from chromadb.api import ClientAPI
from chromadb.utils.embedding_functions.chroma_langchain_embedding_function import (
    create_langchain_embedding,
)


class _LangchainEmbeddings:
    """Stand-in for langchain_core.embeddings.Embeddings."""


class _FakeLangchainEmbeddings(_LangchainEmbeddings):
    """Embeds documents and queries differently, like many LangChain models."""

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return [[float(len(text)), 0.0, 1.0] for text in texts]

    def embed_query(self, text: str) -> List[float]:
        return [float(len(text)), 1.0, 0.0]


@pytest.fixture
def fake_langchain_core(monkeypatch: pytest.MonkeyPatch) -> None:
    langchain_core = types.ModuleType("langchain_core")
    embeddings = types.ModuleType("langchain_core.embeddings")
    embeddings.Embeddings = _LangchainEmbeddings  # type: ignore[attr-defined]
    langchain_core.embeddings = embeddings  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "langchain_core", langchain_core)
    monkeypatch.setitem(sys.modules, "langchain_core.embeddings", embeddings)


def test_langchain_ef_embed_query_accepts_chroma_input(
    fake_langchain_core: None,
) -> None:
    ef = create_langchain_embedding(_FakeLangchainEmbeddings())

    # Chroma calls embed_query(input=[...]) for query texts.
    embeddings = ef.embed_query(input=["hello", "hi"])

    assert len(embeddings) == 2
    assert all(isinstance(e, np.ndarray) for e in embeddings)
    assert np.array_equal(embeddings[0], np.array([5.0, 1.0, 0.0], dtype=np.float32))
    assert np.array_equal(embeddings[1], np.array([2.0, 1.0, 0.0], dtype=np.float32))


def test_langchain_ef_embed_query_single_string(fake_langchain_core: None) -> None:
    ef = create_langchain_embedding(_FakeLangchainEmbeddings())

    # A single string still returns one LangChain-style vector.
    assert ef.embed_query("hello") == [5.0, 1.0, 0.0]


def test_langchain_ef_collection_query_texts(
    fake_langchain_core: None, client: ClientAPI
) -> None:
    client.reset()
    ef = create_langchain_embedding(_FakeLangchainEmbeddings())
    collection = client.create_collection("langchain_ef_query", embedding_function=ef)
    collection.add(ids=["a", "b"], documents=["hello", "hi"])

    result = collection.query(query_texts=["hello"], n_results=1, include=["distances"])

    assert result["ids"] == [["a"]]
