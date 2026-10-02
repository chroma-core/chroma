import os
from unittest.mock import patch, MagicMock

import pytest

from chromadb.utils.embedding_functions.openai_embedding_function import (
    OpenAIEmbeddingFunction,
)


class TestOpenAIEmbeddingFunctionAzure:
    """Unit tests for Azure OpenAI path in OpenAIEmbeddingFunction."""

    def test_azure_requires_api_version(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ValueError when api_type is azure but api_version is missing."""
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        with pytest.raises(ValueError, match="api_version must be specified"):
            OpenAIEmbeddingFunction(
                api_type="azure",
                api_base="https://test.openai.azure.com",
                deployment_id="test-deployment",
            )

    def test_azure_requires_deployment_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ValueError when api_type is azure but deployment_id is missing."""
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        with pytest.raises(ValueError, match="deployment_id must be specified"):
            OpenAIEmbeddingFunction(
                api_type="azure",
                api_base="https://test.openai.azure.com",
                api_version="2023-05-15",
            )

    def test_azure_requires_api_base(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ValueError when api_type is azure but api_base is missing."""
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        with pytest.raises(ValueError, match="api_base must be specified"):
            OpenAIEmbeddingFunction(
                api_type="azure",
                api_version="2023-05-15",
                deployment_id="test-deployment",
            )

    def test_azure_uses_deployment_id_as_model(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Azure path uses deployment_id as the model parameter in API calls."""
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")

        mock_embeddings = MagicMock()
        mock_embeddings.create.return_value = MagicMock(data=[])

        mock_azure_openai = MagicMock()
        mock_azure_openai.return_value.embeddings = mock_embeddings

        mock_openai = MagicMock()
        mock_openai.OpenAI = MagicMock()
        mock_openai.AzureOpenAI = mock_azure_openai

        with patch.dict("sys.modules", {"openai": mock_openai}):
            ef = OpenAIEmbeddingFunction(
                api_type="azure",
                api_base="https://test.openai.azure.com",
                api_version="2023-05-15",
                deployment_id="my-azure-deployment",
                model_name="text-embedding-ada-002",
            )
            ef(["hello world"])

        mock_embeddings.create.assert_called_once()
        call_kwargs = mock_embeddings.create.call_args.kwargs
        assert call_kwargs["model"] == "my-azure-deployment"

    def test_standard_openai_uses_model_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Standard OpenAI path uses model_name as the model parameter."""
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")

        mock_embeddings = MagicMock()
        mock_embeddings.create.return_value = MagicMock(data=[])

        mock_openai_instance = MagicMock()
        mock_openai_instance.embeddings = mock_embeddings

        mock_openai = MagicMock()
        mock_openai.OpenAI.return_value = mock_openai_instance

        with patch.dict("sys.modules", {"openai": mock_openai}):
            ef = OpenAIEmbeddingFunction(
                model_name="text-embedding-3-small",
            )
            ef(["hello world"])

        mock_embeddings.create.assert_called_once()
        call_kwargs = mock_embeddings.create.call_args.kwargs
        assert call_kwargs["model"] == "text-embedding-3-small"

    def test_azure_dimensions_uses_deployment_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dimensions check works with deployment_id for Azure path."""
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")

        mock_embeddings = MagicMock()
        mock_embeddings.create.return_value = MagicMock(data=[])

        mock_azure_openai = MagicMock()
        mock_azure_openai.return_value.embeddings = mock_embeddings

        mock_openai = MagicMock()
        mock_openai.OpenAI = MagicMock()
        mock_openai.AzureOpenAI = mock_azure_openai

        with patch.dict("sys.modules", {"openai": mock_openai}):
            ef = OpenAIEmbeddingFunction(
                api_type="azure",
                api_base="https://test.openai.azure.com",
                api_version="2023-05-15",
                deployment_id="text-embedding-3-large-deployment",
                model_name="text-embedding-ada-002",
                dimensions=1024,
            )
            ef(["hello world"])

        call_kwargs = mock_embeddings.create.call_args.kwargs
        assert call_kwargs["model"] == "text-embedding-3-large-deployment"
        assert call_kwargs["dimensions"] == 1024

    def test_azure_client_receives_correct_args(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """AzureOpenAI client receives correct azure_deployment and other params."""
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")

        mock_embeddings = MagicMock()
        mock_embeddings.create.return_value = MagicMock(data=[])

        mock_azure_openai = MagicMock()
        mock_azure_openai.return_value.embeddings = mock_embeddings

        mock_openai = MagicMock()
        mock_openai.OpenAI = MagicMock()
        mock_openai.AzureOpenAI = mock_azure_openai

        with patch.dict("sys.modules", {"openai": mock_openai}):
            OpenAIEmbeddingFunction(
                api_type="azure",
                api_base="https://test.openai.azure.com",
                api_version="2023-05-15",
                deployment_id="my-deployment",
                default_headers={"X-Custom": "value"},
            )

        mock_azure_openai.assert_called_once()
        call_kwargs = mock_azure_openai.call_args.kwargs
        assert call_kwargs["azure_deployment"] == "my-deployment"
        assert call_kwargs["azure_endpoint"] == "https://test.openai.azure.com"
        assert call_kwargs["api_version"] == "2023-05-15"
        assert call_kwargs["api_key"] == "test-key"
        assert call_kwargs["default_headers"] == {"X-Custom": "value"}
