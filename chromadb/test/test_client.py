from typing import Generator
from unittest.mock import patch
import chromadb
from chromadb.config import Settings
from chromadb.api import ClientAPI
import chromadb.server.fastapi
import pytest
import tempfile


@pytest.fixture
def ephemeral_api() -> Generator[ClientAPI, None, None]:
    client = chromadb.EphemeralClient()
    yield client
    client.clear_system_cache()


@pytest.fixture
def persistent_api() -> Generator[ClientAPI, None, None]:
    client = chromadb.PersistentClient(
        path=tempfile.gettempdir() + "/test_server",
    )
    yield client
    client.clear_system_cache()


@pytest.fixture
def http_api() -> Generator[ClientAPI, None, None]:
    with patch("chromadb.api.client.Client._validate_tenant_database"):
        client = chromadb.HttpClient()
        yield client
        client.clear_system_cache()


def test_ephemeral_client(ephemeral_api: ClientAPI) -> None:
    settings = ephemeral_api.get_settings()
    assert settings.is_persistent is False


def test_persistent_client(persistent_api: ClientAPI) -> None:
    settings = persistent_api.get_settings()
    assert settings.is_persistent is True


def test_http_client(http_api: ClientAPI) -> None:
    settings = http_api.get_settings()
    assert settings.chroma_api_impl == "chromadb.api.fastapi.FastAPI"


def test_http_client_with_inconsistent_host_settings() -> None:
    try:
        chromadb.HttpClient(settings=Settings(chroma_server_host="127.0.0.1"))
    except ValueError as e:
        assert (
            str(e)
            == "Chroma server host provided in settings[127.0.0.1] is different to the one provided in HttpClient: [localhost]"
        )


def test_http_client_with_inconsistent_port_settings() -> None:
    try:
        chromadb.HttpClient(
            port="8002",
            settings=Settings(
                chroma_server_http_port="8001",
            ),
        )
    except ValueError as e:
        assert (
            str(e)
            == "Chroma server http port provided in settings[8001] is different to the one provided in HttpClient: [8002]"
        )
