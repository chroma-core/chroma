import os
import pytest
import numpy as np
from unittest.mock import patch, MagicMock
from chromadb.utils.embedding_functions.chroma_cloud_embedding_function import (
    ChromaCloudEmbeddingFunction,
    ChromaCloudEmbeddingModel,
)

TEST_API_KEY = "test-chroma-api-key"
TEST_TENANT_UUID = "a1b2c3d4-e5f6-7890-1234-56789abcdef0"
API_KEY_ENV_VAR = "CHROMA_API_KEY"


@pytest.fixture(autouse=True)
def set_env_var():
    """Fixture to set and unset the environment variable for tests."""
    original_value = os.environ.get(API_KEY_ENV_VAR)
    os.environ[API_KEY_ENV_VAR] = TEST_API_KEY
    yield
    if original_value is None:
        del os.environ[API_KEY_ENV_VAR]
    else:
        os.environ[API_KEY_ENV_VAR] = original_value


def test_chroma_cloud_embedding_function_init() -> None:
    """Test the initialization of the ChromaCloudEmbeddingFunction."""
    # Test initialization with api_key parameter (should raise a warning)
    with pytest.warns(DeprecationWarning):
        ef = ChromaCloudEmbeddingFunction(
            model="baai/bge-m3",
            tenant_uuid=TEST_TENANT_UUID,
            api_key=TEST_API_KEY,
        )
    assert ef.api_key == TEST_API_KEY

    # Test initialization with environment variable
    ef = ChromaCloudEmbeddingFunction(
        model="baai/bge-m3",
        tenant_uuid=TEST_TENANT_UUID,
        api_key_env_var=API_KEY_ENV_VAR,
    )
    assert ef.api_key == TEST_API_KEY

    # Test initialization failure when no API key is found
    os.environ.pop(API_KEY_ENV_VAR, None)
    with pytest.raises(ValueError, match=f"The {API_KEY_ENV_VAR} environment variable is not set."):
        ChromaCloudEmbeddingFunction(
            model="baai/bge-m3",
            tenant_uuid=TEST_TENANT_UUID,
            api_key_env_var=API_KEY_ENV_VAR,
        )


@patch("httpx.Client")
def test_chroma_cloud_embedding_function_call(mock_httpx_client: MagicMock) -> None:
    """Test the __call__ method of the ChromaCloudEmbeddingFunction."""
    # Mock the response from the Chroma API
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": [
            {"embedding": [0.1, 0.2, 0.3]},
            {"embedding": [0.4, 0.5, 0.6]},
        ]
    }
    mock_httpx_client.return_value.post.return_value = mock_response

    ef = ChromaCloudEmbeddingFunction(
        model="baai/bge-m3",
        tenant_uuid=TEST_TENANT_UUID,
        api_key_env_var=API_KEY_ENV_VAR,
    )

    documents = ["This is a test document.", "This is another test document."]
    embeddings = ef(documents)

    assert embeddings is not None
    assert len(embeddings) == 2
    assert all(isinstance(emb, np.ndarray) for emb in embeddings)
    assert embeddings[0].shape == (3,)
    np.testing.assert_array_equal(embeddings[0], np.array([0.1, 0.2, 0.3], dtype=np.float32))

    # Test with empty input
    assert ef([]) == []


def test_chroma_cloud_embedding_function_config_roundtrip() -> None:
    """Test that the configuration can be saved and restored."""
    ef = ChromaCloudEmbeddingFunction(
        model="baai/bge-m3",
        tenant_uuid=TEST_TENANT_UUID,
        api_key_env_var=API_KEY_ENV_VAR,
    )

    # Get the config (only api_key_env_var should be returned)
    config = ef.get_config()
    assert config == {"api_key_env_var": API_KEY_ENV_VAR}

    # The build_from_config method needs more than get_config provides,
    # as model and tenant_uuid are required for instantiation.
    full_config = {
        "model": "baai/bge-m3",
        "tenant_uuid": TEST_TENANT_UUID,
        "api_key_env_var": API_KEY_ENV_VAR,
    }

    # Test building from a full config
    new_ef = ChromaCloudEmbeddingFunction.build_from_config(full_config)
    assert isinstance(new_ef, ChromaCloudEmbeddingFunction)
    assert new_ef.model == "baai/bge-m3"
    assert new_ef.tenant_uuid == TEST_TENANT_UUID
    assert new_ef.api_key_env_var == API_KEY_ENV_VAR


def test_chroma_cloud_embedding_function_name_and_spaces() -> None:
    """Test the name, default_space, and supported_spaces methods."""
    ef = ChromaCloudEmbeddingFunction(
        model="baai/bge-m3",
        tenant_uuid=TEST_TENANT_UUID,
        api_key_env_var=API_KEY_ENV_VAR,
    )

    assert ef.name() == "chroma_hosted"
    assert ef.default_space() == "cosine"
    supported_spaces = ef.supported_spaces()
    assert "cosine" in supported_spaces
    assert "l2" in supported_spaces
    assert "ip" in supported_spaces


def test_chroma_cloud_embedding_function_validate_update() -> None:
    """Test that updating the model is not allowed."""
    ef = ChromaCloudEmbeddingFunction(
        model="baai/bge-m3",
        tenant_uuid=TEST_TENANT_UUID,
        api_key_env_var=API_KEY_ENV_VAR,
    )
    with pytest.raises(ValueError, match="The model name cannot be changed"):
        ef.validate_config_update(
            old_config={}, new_config={"model": "new/model"}
        )


@patch("chromadb.utils.embedding_functions.chroma_cloud_embedding_function.validate_config_schema")
def test_chroma_cloud_embedding_function_validate_config(mock_validate: MagicMock) -> None:
    """Test that the static validate_config method calls the schema validator."""
    valid_config = {
        "model": "baai/bge-m3",
        "tenant_uuid": TEST_TENANT_UUID,
        "api_key_env_var": API_KEY_ENV_VAR,
    }
    ChromaCloudEmbeddingFunction.validate_config(valid_config)
    mock_validate.assert_called_once_with(valid_config, "chroma_hosted")
