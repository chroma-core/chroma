"""
Unit tests for the Azure OpenAI path in OpenAIEmbeddingFunction.

The openai package is imported lazily inside __init__, so we patch at the
openai package level. Tests run without a real API key.

Covers the fix for: https://github.com/chroma-core/chroma/issues/1770
"""
import os
from unittest.mock import MagicMock, patch, call
import pytest

pytest.importorskip("openai", reason="openai not installed")

from chromadb.utils.embedding_functions.openai_embedding_function import (  # noqa: E402
    OpenAIEmbeddingFunction,
)

AZURE_PARAMS = dict(
    api_type="azure",
    api_base="https://my-resource.openai.azure.com",
    api_version="2023-05-15",
    deployment_id="my-deployment",
    model_name="text-embedding-ada-002",
)
_ENV = {"OPENAI_API_KEY": "fake-key"}


def _make_ef(**overrides):
    """Build an OpenAIEmbeddingFunction with Azure params, patching the clients."""
    kwargs = {**AZURE_PARAMS, **overrides}
    with patch.dict(os.environ, _ENV), \
         patch("openai.OpenAI", return_value=MagicMock()) as mock_std, \
         patch("openai.AzureOpenAI", return_value=MagicMock()) as mock_azure:
        ef = OpenAIEmbeddingFunction(**kwargs)
        return ef, mock_azure


class TestAzureOpenAIValidation:
    def test_missing_api_version_raises(self):
        with patch.dict(os.environ, _ENV), \
             patch("openai.OpenAI", return_value=MagicMock()):
            with pytest.raises(ValueError, match="api_version must be specified"):
                OpenAIEmbeddingFunction(**{**AZURE_PARAMS, "api_version": None})

    def test_missing_deployment_id_raises(self):
        with patch.dict(os.environ, _ENV), \
             patch("openai.OpenAI", return_value=MagicMock()):
            with pytest.raises(ValueError, match="deployment_id must be specified"):
                OpenAIEmbeddingFunction(**{**AZURE_PARAMS, "deployment_id": None})

    def test_missing_api_base_raises(self):
        with patch.dict(os.environ, _ENV), \
             patch("openai.OpenAI", return_value=MagicMock()):
            with pytest.raises(ValueError, match="api_base must be specified"):
                OpenAIEmbeddingFunction(**{**AZURE_PARAMS, "api_base": None})


class TestAzureOpenAIClientConstruction:
    def test_azure_client_receives_azure_deployment(self):
        """AzureOpenAI must be constructed with azure_deployment=deployment_id."""
        _, mock_azure = _make_ef()
        _, kwargs = mock_azure.call_args
        assert kwargs["azure_deployment"] == "my-deployment"

    def test_azure_client_receives_azure_endpoint(self):
        _, mock_azure = _make_ef()
        _, kwargs = mock_azure.call_args
        assert kwargs["azure_endpoint"] == "https://my-resource.openai.azure.com"

    def test_azure_client_receives_api_version(self):
        _, mock_azure = _make_ef()
        _, kwargs = mock_azure.call_args
        assert kwargs["api_version"] == "2023-05-15"

    def test_azure_client_receives_api_key(self):
        _, mock_azure = _make_ef()
        _, kwargs = mock_azure.call_args
        assert kwargs["api_key"] == "fake-key"

    def test_azure_client_receives_default_headers(self):
        headers = {"X-Custom": "value"}
        _, mock_azure = _make_ef(default_headers=headers)
        _, kwargs = mock_azure.call_args
        assert kwargs["default_headers"] == headers


class TestAzureOpenAIConfig:
    def test_get_config_includes_deployment_id(self):
        ef, _ = _make_ef()
        assert ef.get_config()["deployment_id"] == "my-deployment"

    def test_get_config_round_trips_via_build_from_config(self):
        ef, _ = _make_ef()
        config = ef.get_config()
        with patch.dict(os.environ, _ENV), \
             patch("openai.OpenAI", return_value=MagicMock()), \
             patch("openai.AzureOpenAI", return_value=MagicMock()):
            ef2 = OpenAIEmbeddingFunction.build_from_config(config)
        assert ef2.deployment_id == ef.deployment_id
        assert ef2.api_type == ef.api_type
        assert ef2.api_version == ef.api_version
        assert ef2.api_base == ef.api_base
