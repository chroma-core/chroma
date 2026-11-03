import sys
import types
from typing import Any, Dict, List, Optional, Tuple, cast

import numpy as np
import pytest

from chromadb.utils.embedding_functions.huggingface_embedding_function import (
    HuggingFaceEmbeddingFunction,
    HuggingFaceEmbeddingServer,
)
from chromadb.utils.embedding_functions.sentence_transformer_embedding_function import (
    SentenceTransformerEmbeddingFunction,
)

BAD_INPUTS = [123, None, np.zeros((2, 2, 3), dtype=np.uint8)]


class _MockResponse:
    def __init__(self, data: Any) -> None:
        self._data = data

    def json(self) -> Any:
        return self._data


class _MockSession:
    """Stand in for the httpx.Client, recording every request it receives."""

    def __init__(self, dim: int = 3) -> None:
        self.requests: List[Tuple[Optional[str], Optional[Dict[str, Any]]]] = []
        self._dim = dim

    def post(self, url: str, json: Optional[Dict[str, Any]] = None) -> _MockResponse:
        self.requests.append((url, json))
        inputs = json["inputs"] if json is not None else []
        return _MockResponse([[1.0] * self._dim for _ in inputs])


def _hf_ef(kind: str, monkeypatch: pytest.MonkeyPatch) -> Any:
    if kind == "api":
        monkeypatch.setenv("CHROMA_HUGGINGFACE_API_KEY", "test-api-key")
        ef: Any = HuggingFaceEmbeddingFunction()
    else:
        ef = HuggingFaceEmbeddingServer(url="http://localhost:9999/embed")
    ef._session = _MockSession()
    return ef


@pytest.mark.parametrize("kind", ["api", "server"])
@pytest.mark.parametrize("bad_input", BAD_INPUTS)
def test_hf_rejects_unsupported_items(
    kind: str, bad_input: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unsupported items must be rejected before any request leaves the client."""
    ef = _hf_ef(kind, monkeypatch)
    session = cast(_MockSession, ef._session)

    with pytest.raises(ValueError, match="only supports text documents"):
        ef(["hello world", bad_input])

    assert session.requests == []


def test_hf_valid_documents_still_embed(monkeypatch: pytest.MonkeyPatch) -> None:
    for kind in ["api", "server"]:
        ef = _hf_ef(kind, monkeypatch)
        assert len(ef(["hello world"])) == 1
        assert len(cast(_MockSession, ef._session).requests) == 1


class _FakeSentenceTransformer:
    inputs: List[Any] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def encode(self, sentences: Any, **kwargs: Any) -> Any:
        _FakeSentenceTransformer.inputs = list(sentences)
        return np.zeros((len(_FakeSentenceTransformer.inputs), 2), dtype=np.float32)


def _st_ef(monkeypatch: pytest.MonkeyPatch) -> Any:
    fake_module = types.ModuleType("sentence_transformers")
    setattr(fake_module, "SentenceTransformer", _FakeSentenceTransformer)
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)
    _FakeSentenceTransformer.inputs = []
    return SentenceTransformerEmbeddingFunction(model_name="fake-model")


@pytest.mark.parametrize("bad_input", BAD_INPUTS)
def test_st_rejects_unsupported_items(
    bad_input: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    ef = _st_ef(monkeypatch)

    with pytest.raises(ValueError, match="only supports text documents"):
        ef(["hello world", bad_input])

    # The model never sees the unsupported input
    assert _FakeSentenceTransformer.inputs == []


def test_st_valid_documents_still_embed(monkeypatch: pytest.MonkeyPatch) -> None:
    ef = _st_ef(monkeypatch)

    assert len(ef(["hello world", "goodbye"])) == 2
    assert _FakeSentenceTransformer.inputs == ["hello world", "goodbye"]
