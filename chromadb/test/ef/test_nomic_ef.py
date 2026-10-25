import sys
import types
from typing import Any, Dict, List

import numpy as np
import pytest

from chromadb.utils.embedding_functions.nomic_embedding_function import (
    NomicEmbeddingFunction,
)


class _FakeNomicEmbed:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def text(self, texts: List[str], model: str, task_type: str) -> Dict[str, Any]:
        self.calls.append({"texts": texts, "model": model, "task_type": task_type})
        # nomic.embed.text returns a dict, see nomic/embed.py (_text_atlas).
        return {
            "embeddings": [[float(len(text)), 0.5] for text in texts],
            "usage": {"prompt_tokens": 1, "total_tokens": 1},
            "model": model,
            "inference_mode": "remote",
        }


@pytest.fixture
def fake_embed(monkeypatch: pytest.MonkeyPatch) -> _FakeNomicEmbed:
    embed = _FakeNomicEmbed()
    nomic = types.ModuleType("nomic")
    nomic.embed = embed  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nomic", nomic)
    monkeypatch.setenv("NOMIC_API_KEY", "test-key")
    return embed


def test_nomic_ef_embeds_documents(fake_embed: _FakeNomicEmbed) -> None:
    ef = NomicEmbeddingFunction(
        model="nomic-embed-text-v1.5",
        task_type="search_document",
        query_config=None,
    )

    embeddings = ef(["hello", "hi"])

    assert len(embeddings) == 2
    assert np.allclose(embeddings[0], [5.0, 0.5])
    assert np.allclose(embeddings[1], [2.0, 0.5])
    assert fake_embed.calls[0]["task_type"] == "search_document"


def test_nomic_ef_embeds_queries(fake_embed: _FakeNomicEmbed) -> None:
    ef = NomicEmbeddingFunction(
        model="nomic-embed-text-v1.5",
        task_type="search_document",
        query_config={"task_type": "search_query"},
    )

    embeddings = ef.embed_query(["hello"])

    assert len(embeddings) == 1
    assert np.allclose(embeddings[0], [5.0, 0.5])
    assert fake_embed.calls[0]["task_type"] == "search_query"
