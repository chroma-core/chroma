import os
import pytest
import numpy as np
from chromadb.utils.embedding_functions.orcarouter_embedding_function import (
    OrcaRouterEmbeddingFunction,
)


def test_orcarouter_embedding_function_with_api_key() -> None:
    """Test OrcaRouter embedding function when API key is available."""
    if os.environ.get("ORCAROUTER_API_KEY") is None:
        pytest.skip("ORCAROUTER_API_KEY not set")

    ef = OrcaRouterEmbeddingFunction(
        model_name="openai/text-embedding-3-small"
    )

    # Test with plain text snippets
    text_snippets = [
        "OrcaRouter routes to 150+ models through one OpenAI-compatible endpoint.",
        "Zero-trust security for AI agents on the same endpoint.",
    ]

    embeddings = ef(text_snippets)
    assert embeddings is not None
    assert len(embeddings) == 2
    assert all(isinstance(emb, np.ndarray) for emb in embeddings)
    assert all(len(emb) > 0 for emb in embeddings)


def test_orcarouter_embedding_function_with_custom_parameters() -> None:
    """Test OrcaRouter embedding function with custom parameters."""
    if os.environ.get("ORCAROUTER_API_KEY") is None:
        pytest.skip("ORCAROUTER_API_KEY not set")

    ef = OrcaRouterEmbeddingFunction(
        model_name="openai/text-embedding-3-small",
        api_base="https://api.orcarouter.ai/v1",
        encoding_format="float",
        api_key_env_var="ORCAROUTER_API_KEY"
    )

    # Test with a simple snippet
    text_snippet = ["OrcaRouter zero-trust gateway"]

    embeddings = ef(text_snippet)
    assert embeddings is not None
    assert len(embeddings) == 1
    assert isinstance(embeddings[0], np.ndarray)
    assert len(embeddings[0]) > 0


def test_orcarouter_embedding_function_config_roundtrip() -> None:
    """Test that OrcaRouter embedding function configuration can be saved and restored."""
    try:
        import openai
    except ImportError:
        pytest.skip("openai package not installed")
    if os.environ.get("ORCAROUTER_API_KEY") is None:
        pytest.skip("ORCAROUTER_API_KEY not set")

    ef = OrcaRouterEmbeddingFunction(
        model_name="openai/text-embedding-3-small",
        api_base="https://api.orcarouter.ai/v1",
        encoding_format="float",
        api_key_env_var="ORCAROUTER_API_KEY"
    )

    # Get configuration
    config = ef.get_config()

    # Verify configuration contains expected keys
    assert "model_name" in config
    assert "api_base" in config
    assert "encoding_format" in config
    assert "api_key_env_var" in config

    # Verify values
    assert config["model_name"] == "openai/text-embedding-3-small"
    assert config["api_base"] == "https://api.orcarouter.ai/v1"
    assert config["encoding_format"] == "float"
    assert config["api_key_env_var"] == "ORCAROUTER_API_KEY"

    # Test building from config
    new_ef = OrcaRouterEmbeddingFunction.build_from_config(config)
    new_config = new_ef.get_config()

    # Configurations should match
    assert config == new_config


def test_orcarouter_embedding_function_name() -> None:
    """Test that OrcaRouter embedding function returns correct name."""
    assert OrcaRouterEmbeddingFunction.name() == "orcarouter"


def test_orcarouter_embedding_function_spaces() -> None:
    """Test that OrcaRouter embedding function supports expected spaces."""
    try:
        import openai
    except ImportError:
        pytest.skip("openai package not installed")
    if os.environ.get("ORCAROUTER_API_KEY") is None:
        pytest.skip("ORCAROUTER_API_KEY not set")

    ef = OrcaRouterEmbeddingFunction(
        model_name="openai/text-embedding-3-small",
        api_key_env_var="ORCAROUTER_API_KEY"
    )

    # Test default space
    assert ef.default_space() == "cosine"

    # Test supported spaces
    supported_spaces = ef.supported_spaces()
    assert "cosine" in supported_spaces
    assert "l2" in supported_spaces
    assert "ip" in supported_spaces


def test_orcarouter_embedding_function_validate_config() -> None:
    """Test that OrcaRouter embedding function validates configuration correctly."""
    # Valid configuration
    valid_config = {
        "model_name": "openai/text-embedding-3-small",
        "api_key_env_var": "ORCAROUTER_API_KEY"
    }

    # This should not raise an exception
    OrcaRouterEmbeddingFunction.validate_config(valid_config)

    # Invalid configuration (missing required fields)
    invalid_config = {
        "model_name": "openai/text-embedding-3-small"
        # Missing api_key_env_var
    }

    with pytest.raises(Exception):
        OrcaRouterEmbeddingFunction.validate_config(invalid_config)
