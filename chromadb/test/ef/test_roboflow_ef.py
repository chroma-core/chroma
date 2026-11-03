import pytest

from chromadb.utils.embedding_functions.roboflow_embedding_function import (
    RoboflowEmbeddingFunction,
)

PIL = pytest.importorskip("PIL", reason="PIL not installed")


class _MockResponse:
    def __init__(self) -> None:
        self._data = {"embeddings": [[0.1, 0.2, 0.3]]}

    def json(self) -> dict:
        return self._data


def test_roboflow_ef_rejects_unsupported_input_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsupported input items must raise instead of being silently skipped."""
    monkeypatch.setattr("httpx.post", lambda *args, **kwargs: _MockResponse())

    ef = RoboflowEmbeddingFunction(api_key="test-api-key")

    # A valid document still produces one embedding
    assert len(ef(["hello world"])) == 1

    # A mixed input list must not silently drop the unsupported item
    with pytest.raises(ValueError, match="str"):
        ef(["hello world", 123])

    with pytest.raises(ValueError, match="str"):
        ef(["hello world", None])
