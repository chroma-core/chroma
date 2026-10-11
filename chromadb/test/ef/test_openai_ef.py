import os
from unittest.mock import MagicMock, patch

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


def test_azure_openai_accepts_azure_deployment_alias() -> None:
    """Matches Azure OpenAI SDK naming; see chroma-core/chroma#1770."""
    pytest.importorskip("openai", reason="openai not installed")
    with patch("openai.OpenAI"), patch("openai.AzureOpenAI") as mock_azure_cls:
        mock_azure_cls.return_value = MagicMock()
        ef = OpenAIEmbeddingFunction(
            api_key="fake-key",
            model_name="text-embedding-ada-002",
            api_type="azure",
            api_version="2023-05-15",
            api_base="https://example.openai.azure.com",
            azure_deployment="my-deployment",
        )
    mock_azure_cls.assert_called_once()
    _, kwargs = mock_azure_cls.call_args
    assert kwargs["azure_deployment"] == "my-deployment"
    assert ef.get_config()["deployment_id"] == "my-deployment"
    assert "azure_deployment" not in ef.get_config()


def test_azure_deployment_and_deployment_id_must_agree() -> None:
    pytest.importorskip("openai", reason="openai not installed")
    with pytest.raises(ValueError, match="cannot both be set"):
        OpenAIEmbeddingFunction(
            api_key="fake-key",
            model_name="text-embedding-ada-002",
            api_type="azure",
            api_version="2023-05-15",
            api_base="https://example.openai.azure.com",
            deployment_id="dep-a",
            azure_deployment="dep-b",
        )


def test_azure_openai_accepts_matching_deployment_names() -> None:
    pytest.importorskip("openai", reason="openai not installed")
    with patch("openai.OpenAI"), patch("openai.AzureOpenAI") as mock_azure_cls:
        mock_azure_cls.return_value = MagicMock()
        OpenAIEmbeddingFunction(
            api_key="fake-key",
            model_name="text-embedding-ada-002",
            api_type="azure",
            api_version="2023-05-15",
            api_base="https://example.openai.azure.com",
            deployment_id="my-deployment",
            azure_deployment="my-deployment",
        )
    mock_azure_cls.assert_called_once()
    _, kwargs = mock_azure_cls.call_args
    assert kwargs["azure_deployment"] == "my-deployment"
