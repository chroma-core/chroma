import chromadb
from chromadb.api import API
import chromadb.server.fastapi
import pytest
import tempfile


@pytest.fixture
def ephemeral_api() -> API:
    return chromadb.EphemeralClient()


@pytest.fixture
def persistent_api() -> API:
    return chromadb.PersistentClient(
        path=tempfile.gettempdir() + "/test_server",
    )


@pytest.fixture
def http_api() -> API:
    return chromadb.HttpClient()


def test_ephemeral_client(ephemeral_api: API) -> None:
    settings = ephemeral_api.get_settings()
    assert settings.is_persistent is False


def test_persistent_client(persistent_api: API) -> None:
    settings = persistent_api.get_settings()
    assert settings.is_persistent is True


def test_http_client(http_api: API) -> None:
    settings = http_api.get_settings()
    assert settings.chroma_api_impl == "chromadb.api.fastapi.FastAPI"
