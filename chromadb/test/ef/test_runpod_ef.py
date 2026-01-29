import os
from typing import Any, Dict
import pytest  # type: ignore
from chromadb.utils.embedding_functions.runpod_embedding_function import (
    RunPodEmbeddingFunction,
)

MODEL_NAME = "insert-model-name-here"
ENDPOINT_ID ="insert-endpoint-id-here"

def test_runpod_embedding_function_with_api_key() -> None:
    """Test RunPod embedding function when API key is available."""
    if os.environ.get("RUNPOD_API_KEY") is None:
        pytest.skip("RUNPOD_API_KEY not set")

    endpoint_id = os.environ.get("RUNPOD_ENDPOINT_ID", ENDPOINT_ID)
    ef = RunPodEmbeddingFunction(
        endpoint_id=endpoint_id,
        model_name=MODEL_NAME
    )
    embeddings = ef(["This is a test document"])
    assert len(embeddings) == 1
    assert len(embeddings[0]) > 0  # Should have some embedding dimension


def test_runpod_embedding_function_without_api_key() -> None:
    """Test RunPod embedding function without API key raises error."""
    # Temporarily remove API key if it exists
    original_key = os.environ.get("RUNPOD_API_KEY")
    if original_key:
        del os.environ["RUNPOD_API_KEY"]
    
    try:
        with pytest.raises(ValueError, match="environment variable is not set"):
            RunPodEmbeddingFunction(
                endpoint_id="test_endpoint",
                model_name="test-model"
            )
    finally:
        # Restore original key if it existed
        if original_key:
            os.environ["RUNPOD_API_KEY"] = original_key


def test_runpod_embedding_function_config() -> None:
    """Test RunPod embedding function configuration."""
    ef = RunPodEmbeddingFunction(
        api_key="test_key",
        endpoint_id="test_endpoint",
        model_name="test-model",
        timeout=120
    )
    
    config = ef.get_config()
    assert config["endpoint_id"] == "test_endpoint"
    assert config["model_name"] == "test-model"
    assert config["timeout"] == 120
    assert config["api_key_env_var"] == "RUNPOD_API_KEY"


def test_runpod_embedding_function_build_from_config() -> None:
    """Test creating RunPod embedding function from config."""
    endpoint_id = os.environ.get("RUNPOD_ENDPOINT_ID", ENDPOINT_ID)
    config: Dict[str, Any] = {
        "api_key_env_var": "RUNPOD_API_KEY",
        "endpoint_id": endpoint_id,
        "model_name": MODEL_NAME,
        "timeout": 240
    }
    
    ef = RunPodEmbeddingFunction.build_from_config(config)
    assert isinstance(ef, RunPodEmbeddingFunction)
    assert ef.endpoint_id == endpoint_id
    assert ef.model_name == MODEL_NAME
    assert ef.timeout == 240


def test_runpod_embedding_function_validate_config() -> None:
    """Test RunPod embedding function config validation."""
    endpoint_id = os.environ.get("RUNPOD_ENDPOINT_ID", ENDPOINT_ID)
    valid_config: Dict[str, Any] = {
        "api_key_env_var": "RUNPOD_API_KEY",
        "endpoint_id": endpoint_id,
        "model_name": MODEL_NAME
    }
    
    # Should not raise any exception
    RunPodEmbeddingFunction.validate_config(valid_config)
    
    # Test invalid config
    invalid_config: Dict[str, Any] = {
        "endpoint_id": endpoint_id
        # Missing required model_name
    }
    
    with pytest.raises(Exception):  # ValidationError from schema validation
        RunPodEmbeddingFunction.validate_config(invalid_config)


def test_runpod_embedding_function_update_config() -> None:
    """Test RunPod embedding function config update validation."""
    ef = RunPodEmbeddingFunction(
        api_key="test_key",
        endpoint_id="test_endpoint",
        model_name="test-model"
    )
    
    old_config = ef.get_config()
    
    # Should allow updating timeout
    new_config: Dict[str, Any] = {"timeout": 180}
    ef.validate_config_update(old_config, new_config)
    
    # Should not allow changing model_name
    new_config_invalid: Dict[str, Any] = {"model_name": "new-model"}
    with pytest.raises(ValueError, match="model name cannot be changed"):
        ef.validate_config_update(old_config, new_config_invalid)


def test_runpod_embedding_function_name() -> None:
    """Test RunPod embedding function name."""
    ef = RunPodEmbeddingFunction(
        api_key="test_key",
        endpoint_id="test_endpoint",
        model_name="test-model"
    )
    
    assert ef.name() == "runpod" 