from typing import Any
from unittest.mock import MagicMock

import pytest

from chromadb.utils.embedding_functions.cloudflare_workers_ai_embedding_function import (
    CloudflareWorkersAIEmbeddingFunction,
)
from chromadb.utils.embedding_functions.huggingface_embedding_function import (
    HuggingFaceEmbeddingFunction,
    HuggingFaceEmbeddingServer,
)


def _mock_session(payload: Any) -> MagicMock:
    session = MagicMock()
    session.post.return_value.json.return_value = payload
    return session


def test_cloudflare_error_response_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHROMA_CLOUDFLARE_API_KEY", "bad-key")
    ef = CloudflareWorkersAIEmbeddingFunction(
        model_name="@cf/baai/bge-base-en-v1.5", account_id="acc"
    )
    ef._session = _mock_session(
        {
            "success": False,
            "errors": [{"code": 10000, "message": "Authentication error"}],
            "result": None,
        }
    )
    with pytest.raises(RuntimeError, match="Authentication error"):
        ef(["hello"])


def test_huggingface_error_response_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHROMA_HUGGINGFACE_API_KEY", "bad-key")
    ef = HuggingFaceEmbeddingFunction(model_name="some/model")
    ef._session = _mock_session({"error": "Authorization header is invalid"})
    with pytest.raises(RuntimeError, match="Authorization header is invalid"):
        ef(["hello"])


def test_huggingface_server_error_response_raises_runtime_error() -> None:
    ef = HuggingFaceEmbeddingServer(url="http://localhost:8080/embed")
    ef._session = _mock_session({"error": "Model is overloaded"})
    with pytest.raises(RuntimeError, match="Model is overloaded"):
        ef(["hello"])
