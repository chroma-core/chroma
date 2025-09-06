import os
import pytest
import httpx
from unittest.mock import patch, Mock

from chromadb.utils.embedding_functions.jina_embedding_function import (
    JinaEmbeddingFunction,
)


def test_jina_ef_timeout_init():
    """Test that the httpx.Client is initialized with the correct timeout value."""
    with patch("httpx.Client") as mock_client:
        mock_session = Mock()
        mock_client.return_value = mock_session

        # Test with timeout
        ef_with_timeout = JinaEmbeddingFunction(api_key="test_key", timeout=10.0)
        mock_client.assert_called_once_with(timeout=10.0)
        assert ef_with_timeout.timeout == 10.0

        # Reset mock
        mock_client.reset_mock()

        # Test without timeout (default behavior)
        ef_no_timeout = JinaEmbeddingFunction(api_key="test_key")
        mock_client.assert_called_once_with()
        assert ef_no_timeout.timeout is None


def test_jina_ef_timeout_config_serialization():
    """Test that timeout is properly serialized in config methods."""
    # Test with timeout
    ef = JinaEmbeddingFunction(api_key="test_key", timeout=30.0)
    config = ef.get_config()
    
    assert "timeout" in config
    assert config["timeout"] == 30.0
    
    # Test without timeout
    ef_no_timeout = JinaEmbeddingFunction(api_key="test_key")
    config_no_timeout = ef_no_timeout.get_config()
    
    assert "timeout" in config_no_timeout
    assert config_no_timeout["timeout"] is None


def test_jina_ef_build_from_config_with_timeout():
    """Test that build_from_config properly handles timeout parameter."""
    config = {
        "api_key_env_var": "TEST_JINA_API_KEY",
        "model_name": "test-model",
        "timeout": 45.0,
    }
    
    # Set fake environment variable
    os.environ["TEST_JINA_API_KEY"] = "fake_key"
    
    try:
        ef = JinaEmbeddingFunction.build_from_config(config)
        assert ef.timeout == 45.0
    finally:
        # Clean up
        if "TEST_JINA_API_KEY" in os.environ:
            del os.environ["TEST_JINA_API_KEY"]


def test_jina_ef_build_from_config_without_timeout():
    """Test that build_from_config works when timeout is not specified."""
    config = {
        "api_key_env_var": "TEST_JINA_API_KEY",
        "model_name": "test-model",
    }
    
    # Set fake environment variable
    os.environ["TEST_JINA_API_KEY"] = "fake_key"
    
    try:
        ef = JinaEmbeddingFunction.build_from_config(config)
        assert ef.timeout is None
    finally:
        # Clean up
        if "TEST_JINA_API_KEY" in os.environ:
            del os.environ["TEST_JINA_API_KEY"]


@patch("httpx.Client")
def test_jina_ef_timeout_request_propagation(mock_client):
    """Test that timeout exceptions are properly propagated."""
    # Create a mock session that raises a timeout exception
    mock_session = Mock()
    mock_post = Mock()
    mock_post.side_effect = httpx.ReadTimeout("Request timeout")
    mock_session.post = mock_post
    mock_session.headers = Mock()
    mock_session.headers.update = Mock()
    
    mock_client.return_value = mock_session
    
    # Create embedding function with short timeout
    ef = JinaEmbeddingFunction(api_key="test_key", timeout=0.1)
    
    # Test that timeout exception is propagated
    with pytest.raises(httpx.ReadTimeout, match="Request timeout"):
        ef(["some text"])
    
    # Verify the post method was called
    mock_post.assert_called_once()


@patch("httpx.Client")
def test_jina_ef_successful_request_mock(mock_client):
    """Test successful request handling with mocked response."""
    # Mock successful response
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {"index": 0, "embedding": [0.1, 0.2, 0.3]},
            {"index": 1, "embedding": [0.4, 0.5, 0.6]},
        ]
    }
    
    mock_session = Mock()
    mock_session.post.return_value = mock_response
    mock_session.headers = Mock()
    mock_session.headers.update = Mock()
    
    mock_client.return_value = mock_session
    
    # Create embedding function
    ef = JinaEmbeddingFunction(api_key="test_key", timeout=30.0)
    
    # Test successful call
    result = ef(["hello", "world"])
    
    assert len(result) == 2
    assert len(result[0]) == 3
    assert len(result[1]) == 3
    
    # Verify the request was made with correct parameters
    mock_session.post.assert_called_once()
    call_args = mock_session.post.call_args
    
    # Check that the API URL and JSON payload are correct
    assert call_args[0][0] == "https://api.jina.ai/v1/embeddings"
    json_payload = call_args[1]["json"]
    assert json_payload["input"] == ["hello", "world"]
    assert json_payload["model"] == "jina-embeddings-v2-base-en"


def test_jina_ef_validates_text_input():
    """Test that non-text inputs are rejected."""
    ef = JinaEmbeddingFunction(api_key="test_key")
    
    # Test with non-string input
    with pytest.raises(ValueError, match="Jina AI only supports text documents"):
        ef([123, 456])  # Non-string input should fail


def test_jina_ef_default_timeout_behavior():
    """Test that default behavior (no timeout) works correctly."""
    with patch("httpx.Client") as mock_client:
        mock_session = Mock()
        mock_client.return_value = mock_session
        
        # Create without timeout
        ef = JinaEmbeddingFunction(api_key="test_key")
        
        # Verify client was created without timeout parameter
        mock_client.assert_called_once_with()
        
        # Verify timeout attribute is None
        assert ef.timeout is None