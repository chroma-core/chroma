import pytest
from unittest.mock import MagicMock
from chromadb.api.shared_system_client import SharedSystemClient
from chromadb.api.base_http_client import BaseHTTPClient
from chromadb.config import System
from typing import Optional, Dict, Generator


@pytest.fixture(autouse=True)
def clear_cache() -> Generator[None, None, None]:
    """Automatically clear the system cache before and after each test."""
    SharedSystemClient.clear_system_cache()
    yield
    SharedSystemClient.clear_system_cache()


def create_mock_http_client(
    api_url: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> MagicMock:
    """Create a mock BaseHTTPClient instance with the specified configuration."""
    mock_server_api = MagicMock(spec=BaseHTTPClient)

    mock_server_api.get_api_url.return_value = api_url or ""
    mock_server_api.get_request_headers.return_value = headers or {}

    return mock_server_api


def register_mock_system(system_id: str, mock_server_api: MagicMock) -> MagicMock:
    """Register a mock system with the given ID and server API."""
    mock_system = MagicMock(spec=System)
    mock_system.instance.return_value = mock_server_api
    SharedSystemClient._identifier_to_system[system_id] = mock_system
    return mock_system


def test_extracts_api_key_from_chroma_cloud_client() -> None:
    mock_server_api = create_mock_http_client(
        api_url="https://api.trychroma.com/api/v2",
        headers={"X-Chroma-Token": "test-api-key-123"},
    )
    register_mock_system("test-id", mock_server_api)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key == "test-api-key-123"


def test_extracts_api_key_with_lowercase_header() -> None:
    mock_server_api = create_mock_http_client(
        api_url="https://api.trychroma.com/api/v2",
        headers={"x-chroma-token": "test-api-key-456"},
    )
    register_mock_system("test-id", mock_server_api)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key == "test-api-key-456"


def test_extracts_api_key_from_gcp_chroma_cloud_client() -> None:
    mock_server_api = create_mock_http_client(
        api_url="https://dummy.gcp.trychroma.com/api/v2",
        headers={"X-Chroma-Token": "gcp-test-api-key"},
    )
    register_mock_system("test-id", mock_server_api)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key == "gcp-test-api-key"


def test_skips_non_chroma_cloud_clients() -> None:
    mock_server_api = create_mock_http_client(
        api_url="https://localhost:8000/api/v2",
        headers={"X-Chroma-Token": "local-api-key"},
    )
    register_mock_system("test-id", mock_server_api)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key is None


def test_skips_clients_without_api_url() -> None:
    mock_server_api = create_mock_http_client(
        api_url=None,
        headers={"X-Chroma-Token": "test-api-key"},
    )
    register_mock_system("test-id", mock_server_api)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key is None


def test_returns_none_when_no_api_key_in_headers() -> None:
    mock_server_api = create_mock_http_client(
        api_url="https://api.trychroma.com/api/v2",
        headers={},
    )
    register_mock_system("test-id", mock_server_api)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key is None


def test_returns_first_api_key_found_from_multiple_clients() -> None:
    mock_server_api_1 = create_mock_http_client(
        api_url="https://api.trychroma.com/api/v2",
        headers={"X-Chroma-Token": "first-key"},
    )
    mock_server_api_2 = create_mock_http_client(
        api_url="https://api.trychroma.com/api/v2",
        headers={"X-Chroma-Token": "second-key"},
    )
    register_mock_system("test-id-1", mock_server_api_1)
    register_mock_system("test-id-2", mock_server_api_2)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key == "first-key"


def test_handles_exception_gracefully() -> None:
    mock_system = MagicMock(spec=System)
    mock_system.instance.side_effect = Exception("Test exception")
    SharedSystemClient._identifier_to_system["test-id"] = mock_system

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key is None


def test_returns_none_when_no_clients_exist() -> None:
    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key is None


def test_skips_non_http_clients() -> None:
    """Test that non-BaseHTTPClient instances are skipped."""
    mock_server_api = MagicMock()  # Not a BaseHTTPClient
    register_mock_system("test-id", mock_server_api)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key is None


def test_extracts_api_key_with_mixed_case_header() -> None:
    mock_server_api = create_mock_http_client(
        api_url="https://api.trychroma.com/api/v2",
        headers={"X-CHROMA-TOKEN": "mixed-case-key"},
    )
    register_mock_system("test-id", mock_server_api)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key == "mixed-case-key"


def test_multiple_clients_returns_one_key() -> None:
    """Test that multiple clients return one of the available keys."""
    mock_api_1 = create_mock_http_client(
        api_url="https://api.trychroma.com/api/v2",
        headers={"X-Chroma-Token": "key-1"},
    )
    mock_api_2 = create_mock_http_client(
        api_url="https://api.trychroma.com/api/v2",
        headers={"X-Chroma-Token": "key-2"},
    )
    register_mock_system("id-1", mock_api_1)
    register_mock_system("id-2", mock_api_2)

    api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()

    assert api_key in ["key-1", "key-2"]
