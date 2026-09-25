from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from chromadb.utils.embedding_functions.cloudflare_workers_ai_embedding_function import (
    CloudflareWorkersAIEmbeddingFunction,
    CloudflareWorkersAIError,
)


def _make_ef(response_payload: Any) -> CloudflareWorkersAIEmbeddingFunction:
    """Build the EF with a stubbed httpx client returning the given JSON payload."""
    with patch.object(httpx, "Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = response_payload
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client
        return CloudflareWorkersAIEmbeddingFunction(
            model_name="@cf/baai/bge-base-en-v1.5",
            account_id="test-account",
            api_key="test-api-key",
        )


def test_success_returns_embeddings() -> None:
    ef = _make_ef({"result": {"data": [[0.1, 0.2], [0.3, 0.4]]}})
    embeddings = ef(["hello", "world"])
    assert len(embeddings) == 2
    assert list(embeddings[0]) == [0.1, 0.2]
    assert list(embeddings[1]) == [0.3, 0.4]


def test_missing_result_raises_typed_error_not_keyerror() -> None:
    # Regression test for chroma-core/chroma#7284: an error payload without a
    # "result" key must not leak an unhandled KeyError.
    ef = _make_ef({"detail": "Unauthorized"})
    with pytest.raises(CloudflareWorkersAIError) as exc_info:
        ef(["hello"])
    assert "Unauthorized" in str(exc_info.value)
    # Backward compatibility: still catchable as RuntimeError.
    assert isinstance(exc_info.value, RuntimeError)


def test_missing_data_in_result_raises_typed_error() -> None:
    ef = _make_ef({"result": {}})
    with pytest.raises(CloudflareWorkersAIError):
        ef(["hello"])


def test_non_dict_result_raises_typed_error() -> None:
    # A drifted payload like {"result": None} must not leak a TypeError.
    ef = _make_ef({"result": None, "detail": "bad gateway"})
    with pytest.raises(CloudflareWorkersAIError) as exc_info:
        ef(["hello"])
    assert "bad gateway" in str(exc_info.value)


def test_error_without_detail_uses_unknown_error() -> None:
    ef = _make_ef({"unexpected": "shape"})
    with pytest.raises(CloudflareWorkersAIError) as exc_info:
        ef(["hello"])
    assert "Unknown error" in str(exc_info.value)
