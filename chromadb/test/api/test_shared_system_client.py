import pytest
import threading
from unittest.mock import MagicMock
from chromadb.api.shared_system_client import SharedSystemClient
from chromadb.api.base_http_client import BaseHTTPClient
from chromadb.config import Settings, System
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


def test_retain_system_reuses_existing_identifier() -> None:
    system = MagicMock(spec=System)
    system.settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=8000,
    )
    SharedSystemClient._identifier_to_system["existing"] = system

    identifier = SharedSystemClient._retain_system(system)

    assert identifier == "existing"
    assert SharedSystemClient._identifier_to_system == {"existing": system}
    assert SharedSystemClient._identifier_to_refcount == {"existing": 1}

    SharedSystemClient._release_system(identifier)

    system.stop.assert_called_once_with()
    assert SharedSystemClient._identifier_to_system == {}
    assert SharedSystemClient._identifier_to_refcount == {}


def test_retain_system_registers_untracked_system_once() -> None:
    system = MagicMock(spec=System)
    system.settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=8000,
    )

    identifier = SharedSystemClient._retain_system(system)
    retained_identifier = SharedSystemClient._retain_system(system)

    assert retained_identifier == identifier
    assert SharedSystemClient._identifier_to_system == {identifier: system}
    assert SharedSystemClient._identifier_to_refcount == {identifier: 2}

    SharedSystemClient._release_system(identifier)
    system.stop.assert_not_called()
    SharedSystemClient._release_system(retained_identifier)

    system.stop.assert_called_once_with()
    assert SharedSystemClient._identifier_to_system == {}
    assert SharedSystemClient._identifier_to_refcount == {}


def test_retain_system_registers_local_collision_separately() -> None:
    settings = Settings(is_persistent=False)
    existing_system = MagicMock(spec=System)
    existing_system.settings = settings
    retained_system = MagicMock(spec=System)
    retained_system.settings = settings
    SharedSystemClient._identifier_to_system["ephemeral"] = existing_system
    SharedSystemClient._identifier_to_refcount["ephemeral"] = 1

    identifier = SharedSystemClient._retain_system(retained_system)

    assert identifier != "ephemeral"
    assert SharedSystemClient._identifier_to_system["ephemeral"] is existing_system
    assert SharedSystemClient._identifier_to_system[identifier] is retained_system
    assert SharedSystemClient._identifier_to_refcount == {
        "ephemeral": 1,
        identifier: 1,
    }

    settings_identifier = SharedSystemClient._create_and_retain_system(settings)

    assert settings_identifier == identifier
    assert SharedSystemClient._identifier_to_refcount == {
        "ephemeral": 1,
        identifier: 2,
    }

    SharedSystemClient._release_system(settings_identifier)
    SharedSystemClient._release_system(identifier)

    retained_system.stop.assert_called_once_with()
    existing_system.stop.assert_not_called()
    assert SharedSystemClient._identifier_to_system == {"ephemeral": existing_system}
    assert SharedSystemClient._identifier_to_refcount == {"ephemeral": 1}


def test_retain_system_during_final_release_cannot_revive_system() -> None:
    system = MagicMock(spec=System)
    system.settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=8000,
    )
    SharedSystemClient._identifier_to_system["existing"] = system
    SharedSystemClient._identifier_to_refcount["existing"] = 1
    stop_started = threading.Event()
    allow_stop = threading.Event()
    retain_errors: list[Exception] = []

    def blocking_stop() -> None:
        stop_started.set()
        assert allow_stop.wait(timeout=5)

    def retain_system() -> None:
        try:
            SharedSystemClient._retain_system(system)
        except Exception as error:
            retain_errors.append(error)

    system.stop.side_effect = blocking_stop
    release_thread = threading.Thread(
        target=SharedSystemClient._release_system,
        args=("existing",),
    )
    retain_thread = threading.Thread(target=retain_system)

    release_thread.start()
    assert stop_started.wait(timeout=5)
    try:
        retain_thread.start()
        retain_thread.join(timeout=5)

        assert not retain_thread.is_alive()
        assert len(retain_errors) == 1
        assert isinstance(retain_errors[0], ValueError)
        assert "final reference" in str(retain_errors[0])
    finally:
        allow_stop.set()
        release_thread.join(timeout=5)
        retain_thread.join(timeout=5)

    assert not release_thread.is_alive()
    assert SharedSystemClient._identifier_to_system == {}
    assert SharedSystemClient._identifier_to_refcount == {}
