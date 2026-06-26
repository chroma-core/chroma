import os
import sys
import types
from unittest.mock import Mock

import pytest

from chromadb.utils.embedding_functions.openai_embedding_function import (
    OpenAIEmbeddingFunction,
)


def test_with_embedding_dimensions() -> None:
    if os.environ.get("OPENAI_API_KEY") is None:
        pytest.skip("OPENAI_API_KEY not set")
    ef = OpenAIEmbeddingFunction(
        api_key=os.environ["OPENAI_API_KEY"],
        model_name="text-embedding-3-small",
        dimensions=64,
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 64


def test_with_embedding_dimensions_not_working_with_old_model() -> None:
    if os.environ.get("OPENAI_API_KEY") is None:
        pytest.skip("OPENAI_API_KEY not set")
    ef = OpenAIEmbeddingFunction(api_key=os.environ["OPENAI_API_KEY"], dimensions=64)
    with pytest.raises(
        Exception, match="This model does not support specifying dimensions"
    ):
        ef(["hello world"])


def test_with_incorrect_api_key() -> None:
    pytest.importorskip("openai", reason="openai not installed")
    ef = OpenAIEmbeddingFunction(api_key="incorrect_api_key", dimensions=64)
    with pytest.raises(Exception, match="Incorrect API key provided"):
        ef(["hello world"])


def test_azure_openai_requires_deployment_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    openai_stub = types.SimpleNamespace(OpenAI=Mock(), AzureOpenAI=Mock())
    monkeypatch.setitem(sys.modules, "openai", openai_stub)

    with pytest.raises(
        ValueError, match="deployment_id must be specified for Azure OpenAI"
    ):
        OpenAIEmbeddingFunction(
            api_key="test-api-key",
            api_type="azure",
            api_base="https://example.openai.azure.com",
            api_version="2024-02-01",
        )


def test_azure_openai_passes_deployment_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    default_client = object()
    azure_client = object()

    openai_stub = types.SimpleNamespace(
        OpenAI=Mock(return_value=default_client),
        AzureOpenAI=Mock(return_value=azure_client),
    )
    monkeypatch.setitem(sys.modules, "openai", openai_stub)

    ef = OpenAIEmbeddingFunction(
        api_key="test-api-key",
        api_type="azure",
        api_base="https://example.openai.azure.com",
        api_version="2024-02-01",
        deployment_id="text-embedding-3-small",
        default_headers={"x-test-header": "value"},
    )

    assert ef.client is azure_client
    openai_stub.AzureOpenAI.assert_called_once_with(
        api_key="test-api-key",
        api_version="2024-02-01",
        azure_endpoint="https://example.openai.azure.com",
        azure_deployment="text-embedding-3-small",
        default_headers={"x-test-header": "value"},
    )
