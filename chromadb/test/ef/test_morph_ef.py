import os
import pytest
import numpy as np
from chromadb.utils.embedding_functions.morph_embedding_function import (
    MorphEmbeddingFunction,
)


def test_morph_embedding_function_with_api_key() -> None:
    """Test Morph embedding function when API key is available."""
    if os.environ.get("MORPH_API_KEY") is None:
        pytest.skip("MORPH_API_KEY not set")

    ef = MorphEmbeddingFunction(
        model_name="morph-embedding-v2"
    )

    # Test with code snippets (Morph's specialty)
    code_snippets = [
        "def hello_world():\n    print('Hello, World!')",
        "class Calculator:\n    def add(self, a, b):\n        return a + b"
    ]

    embeddings = ef(code_snippets)
    assert embeddings is not None
    assert len(embeddings) == 2
    assert all(isinstance(emb, np.ndarray) for emb in embeddings)
    assert all(len(emb) > 0 for emb in embeddings)


def test_morph_embedding_function_with_custom_parameters() -> None:
    """Test Morph embedding function with custom parameters."""
    if os.environ.get("MORPH_API_KEY") is None:
        pytest.skip("MORPH_API_KEY not set")

    ef = MorphEmbeddingFunction(
        model_name="morph-embedding-v2",
        api_base="https://api.morphllm.com/v1",
        encoding_format="float",
        api_key_env_var="MORPH_API_KEY"
    )

    # Test with a simple function
    code_snippet = ["function add(a, b) { return a + b; }"]

    embeddings = ef(code_snippet)
    assert embeddings is not None
    assert len(embeddings) == 1
    assert isinstance(embeddings[0], np.ndarray)
    assert len(embeddings[0]) > 0


def test_morph_embedding_function_config_roundtrip() -> None:
    """Test that Morph embedding function configuration can be saved and restored."""
    try:
        import openai
    except ImportError:
        pytest.skip("openai package not installed")

    ef = MorphEmbeddingFunction(
        model_name="morph-embedding-v2",
        api_base="https://api.morphllm.com/v1",
        encoding_format="float",
        api_key_env_var="MORPH_API_KEY"
    )

    # Get configuration
    config = ef.get_config()

    # Verify configuration contains expected keys
    assert "model_name" in config
    assert "api_base" in config
    assert "encoding_format" in config
    assert "api_key_env_var" in config

    # Verify values
    assert config["model_name"] == "morph-embedding-v2"
    assert config["api_base"] == "https://api.morphllm.com/v1"
    assert config["encoding_format"] == "float"
    assert config["api_key_env_var"] == "MORPH_API_KEY"

    # Test building from config
    new_ef = MorphEmbeddingFunction.build_from_config(config)
    new_config = new_ef.get_config()

    # Configurations should match
    assert config == new_config


def test_morph_embedding_function_name() -> None:
    """Test that Morph embedding function returns correct name."""
    assert MorphEmbeddingFunction.name() == "morph"


def test_morph_embedding_function_spaces() -> None:
    """Test that Morph embedding function supports expected spaces."""
    try:
        import openai
    except ImportError:
        pytest.skip("openai package not installed")

    ef = MorphEmbeddingFunction(
        model_name="morph-embedding-v2",
        api_key_env_var="MORPH_API_KEY"
    )

    # Test default space
    assert ef.default_space() == "cosine"

    # Test supported spaces
    supported_spaces = ef.supported_spaces()
    assert "cosine" in supported_spaces
    assert "l2" in supported_spaces
    assert "ip" in supported_spaces


def test_morph_embedding_function_validate_config() -> None:
    """Test that Morph embedding function validates configuration correctly."""
    # Valid configuration
    valid_config = {
        "model_name": "morph-embedding-v2",
        "api_key_env_var": "MORPH_API_KEY"
    }

    # This should not raise an exception
    MorphEmbeddingFunction.validate_config(valid_config)

    # Invalid configuration (missing required fields)
    invalid_config = {
        "model_name": "morph-embedding-v2"
        # Missing api_key_env_var
    }

    with pytest.raises(Exception):
        MorphEmbeddingFunction.validate_config(invalid_config)