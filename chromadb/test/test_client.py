import chromadb
from chromadb.api import API
import chromadb.server.fastapi
import pytest
import tempfile
import gc


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

def test_multiple_persistent_client() -> None:
    with pytest.raises(RuntimeError):
        a = chromadb.PersistentClient(
            path=tempfile.gettempdir() + "/test_server",
        )
        b = chromadb.PersistentClient(
            path=tempfile.gettempdir() + "/test_server",
        )

def test_multiple_ephemeral_client() -> None:
    with pytest.raises(RuntimeError):
        a = chromadb.EphemeralClient()
        b = chromadb.EphemeralClient()

def test_gc_ephemeral_client() -> None:
    # need to manually gargabe collect otherwise tests fail from above instantiations.
    gc.collect()
    a = chromadb.EphemeralClient()
    del a
    gc.collect()
    b = chromadb.EphemeralClient()

def test_gc_persistent_client() -> None:
    # need to manually gargabe collect otherwise tests fail from above instantiations.
    gc.collect()
    a = chromadb.PersistentClient(path=tempfile.gettempdir() + "/test_server",)
    del a
    gc.collect()
    b = chromadb.PersistentClient(path=tempfile.gettempdir() + "/test_server",)
