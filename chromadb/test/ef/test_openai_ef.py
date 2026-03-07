import os

import pytest

from chromadb.utils.embedding_functions.openai_embedding_function import (
    OpenAIEmbeddingFunction,
)
from chromadb.errors import InvalidArgumentError


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


def test_azure_requires_deployment_id() -> None:
    """Azure OpenAI should require deployment_id parameter."""
    pytest.importorskip("openai", reason="openai not installed")
    with pytest.raises(InvalidArgumentError, match="deployment_id must be specified"):
        OpenAIEmbeddingFunction(
            api_key="test_key",
            api_type="azure",
            api_base="https://example.openai.azure.com",
            api_version="2023-05-15",
            # Missing deployment_id should raise
        )


def test_azure_requires_api_version() -> None:
    """Azure OpenAI should require api_version parameter."""
    pytest.importorskip("openai", reason="openai not installed")
    with pytest.raises(InvalidArgumentError, match="api_version must be specified"):
        OpenAIEmbeddingFunction(
            api_key="test_key",
            api_type="azure",
            api_base="https://example.openai.azure.com",
            deployment_id="my-deployment",
            # Missing api_version should raise
        )


def test_azure_requires_api_base() -> None:
    """Azure OpenAI should require api_base parameter."""
    pytest.importorskip("openai", reason="openai not installed")
    with pytest.raises(InvalidArgumentError, match="api_base must be specified"):
        OpenAIEmbeddingFunction(
            api_key="test_key",
            api_type="azure",
            api_version="2023-05-15",
            deployment_id="my-deployment",
            # Missing api_base should raise
        )
