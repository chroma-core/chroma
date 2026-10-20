import os
import re
import tempfile

import pytest
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


# ---------------------------------------------------------------------------
# Persistent system identifier tests
#
# A persistent local system is identified by the filesystem identity of its
# persist directory (st_dev/st_ino) rather than by the path string, so that
# every spelling of the same directory shares one system instance.
# ---------------------------------------------------------------------------


def persistent_settings(persist_directory: str) -> Settings:
    return Settings(
        chroma_api_impl="chromadb.api.segment.SegmentAPI",
        is_persistent=True,
        persist_directory=persist_directory,
        anonymized_telemetry=True,
    )


def test_same_directory_spellings_share_identifier() -> None:
    """Absolute, trailing-separator, and dot-segment spellings of the same
    directory must resolve to a single system identifier."""
    with tempfile.TemporaryDirectory() as tmpdir:
        spellings = [
            tmpdir,
            tmpdir + os.sep,
            os.path.join(tmpdir, os.pardir, os.path.basename(tmpdir)),
            tmpdir + os.sep + os.curdir,
        ]
        identifiers = [
            SharedSystemClient._get_identifier_from_settings(persistent_settings(s))
            for s in spellings
        ]
        assert len(set(identifiers)) == 1
        # The identifier is the filesystem identity of the directory.
        expected = os.stat(tmpdir)
        assert identifiers[0] == f"dev:{expected.st_dev}:ino:{expected.st_ino}"


def test_nonexistent_directory_identifier_is_deterministic() -> None:
    """A path that does not exist yet must not crash, and after the directory
    is created the identifier must stay stable so the system is not
    duplicated on a later lookup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        missing = os.path.join(tmpdir, "not-created-yet")
        assert not os.path.exists(missing)

        first = SharedSystemClient._get_identifier_from_settings(
            persistent_settings(missing)
        )
        second = SharedSystemClient._get_identifier_from_settings(
            persistent_settings(missing + os.sep)
        )
        assert first == second
        assert re.fullmatch(r"dev:\d+:ino:\d+", first)


def test_uncreatable_directory_falls_back_to_normalized_path() -> None:
    """When the directory identity cannot be determined, the client falls
    back to a deterministic, normalized absolute path without crashing."""
    if os.name == "nt":
        pytest.skip("/dev/null parent semantics differ on Windows")
    missing = os.path.join(os.devnull, "not-a-directory")
    settings_1 = persistent_settings(missing)
    settings_2 = persistent_settings(missing + os.sep)

    first = SharedSystemClient._get_identifier_from_settings(settings_1)
    second = SharedSystemClient._get_identifier_from_settings(settings_2)

    assert first == second
    assert first.startswith("path:")
    assert first == f"path:{os.path.abspath(missing)}"


def test_symlinked_directory_dedupes() -> None:
    """A symlink pointing at the persist directory must be treated as the
    same system as the real path."""
    if os.name == "nt":
        pytest.skip("creating symlinks is restricted on Windows")
    with tempfile.TemporaryDirectory() as tmpdir:
        real = os.path.join(tmpdir, "real")
        os.makedirs(real)
        link = os.path.join(tmpdir, "link")
        os.symlink(real, link)
        # The two spellings are genuinely different strings; only the
        # filesystem identity of the directory makes them compare equal.
        assert os.path.abspath(real) != os.path.abspath(link)

        real_identifier = SharedSystemClient._get_identifier_from_settings(
            persistent_settings(real)
        )
        link_identifier = SharedSystemClient._get_identifier_from_settings(
            persistent_settings(link)
        )
        assert real_identifier == link_identifier


def test_different_directories_get_different_identifiers() -> None:
    """Distinct directories must not collapse onto one system."""
    with tempfile.TemporaryDirectory() as tmpdir:
        a = os.path.join(tmpdir, "a")
        b = os.path.join(tmpdir, "b")
        os.makedirs(a)
        os.makedirs(b)
        identifier_a = SharedSystemClient._get_identifier_from_settings(
            persistent_settings(a)
        )
        identifier_b = SharedSystemClient._get_identifier_from_settings(
            persistent_settings(b)
        )
        assert identifier_a != identifier_b


def test_second_client_with_same_directory_shares_system() -> None:
    """Two clients created for the same directory share one System."""
    with tempfile.TemporaryDirectory() as tmpdir:
        client_1 = SharedSystemClient(settings=persistent_settings(tmpdir))
        try:
            client_2 = SharedSystemClient(settings=persistent_settings(tmpdir))
            try:
                assert len(SharedSystemClient._identifier_to_system) == 1
                assert client_1._system is client_2._system
            finally:
                SharedSystemClient._release_system(client_2._identifier)
        finally:
            SharedSystemClient._release_system(client_1._identifier)


def test_second_client_with_equivalent_path_spelling_shares_system() -> None:
    """Two clients whose settings spell the same directory differently share
    one System instead of failing with a settings mismatch."""
    with tempfile.TemporaryDirectory() as tmpdir:
        spellings = [
            tmpdir,
            tmpdir + os.sep,
            os.path.join(tmpdir, os.pardir, os.path.basename(tmpdir)),
        ]
        clients = []
        try:
            for spelling in spellings:
                clients.append(
                    SharedSystemClient(settings=persistent_settings(spelling))
                )
            assert len(SharedSystemClient._identifier_to_system) == 1
            assert all(
                client._system is clients[0]._system for client in clients[1:]
            )
        finally:
            for client in clients:
                SharedSystemClient._release_system(client._identifier)


def test_different_settings_are_still_rejected() -> None:
    """The settings guard must keep rejecting genuinely different settings,
    even when the persist directory is the same."""
    with tempfile.TemporaryDirectory() as tmpdir:
        client_1 = SharedSystemClient(settings=persistent_settings(tmpdir))
        try:
            other_settings = persistent_settings(tmpdir)
            other_settings.allow_reset = not other_settings.allow_reset
            with pytest.raises(ValueError):
                SharedSystemClient(settings=other_settings)
        finally:
            SharedSystemClient._release_system(client_1._identifier)
