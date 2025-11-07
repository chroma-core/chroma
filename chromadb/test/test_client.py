import asyncio
from typing import Any, Callable, Generator, cast, Dict, Tuple
from unittest.mock import MagicMock, patch
import chromadb
from chromadb.config import Settings, System
from chromadb.api import ClientAPI
import chromadb.server.fastapi
from chromadb.api.fastapi import FastAPI
import pytest
import tempfile
import os


@pytest.fixture
def ephemeral_api() -> Generator[ClientAPI, None, None]:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")
    client = chromadb.EphemeralClient()
    yield client
    client.clear_system_cache()


@pytest.fixture
def persistent_api() -> Generator[ClientAPI, None, None]:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")
    client = chromadb.PersistentClient(
        path=tempfile.gettempdir() + "/test_server",
    )
    yield client
    client.clear_system_cache()


HttpAPIFactory = Callable[..., ClientAPI]


@pytest.fixture(params=["sync_client", "async_client"])
def http_api_factory(
    request: pytest.FixtureRequest,
) -> Generator[HttpAPIFactory, None, None]:
    if request.param == "sync_client":
        with patch("chromadb.api.client.Client._validate_tenant_database"):
            with patch("chromadb.api.client.Client.get_user_identity"):
                yield chromadb.HttpClient
    else:
        with patch("chromadb.api.async_client.AsyncClient._validate_tenant_database"):
            with patch("chromadb.api.async_client.AsyncClient.get_user_identity"):

                def factory(*args: Any, **kwargs: Any) -> Any:
                    cls = asyncio.get_event_loop().run_until_complete(
                        chromadb.AsyncHttpClient(*args, **kwargs)
                    )
                    return cls

                yield cast(HttpAPIFactory, factory)


@pytest.fixture()
def http_api(http_api_factory: HttpAPIFactory) -> Generator[ClientAPI, None, None]:
    if os.environ.get("CHROMA_SERVER_HTTP_PORT") is not None:
        port = int(os.environ.get("CHROMA_SERVER_HTTP_PORT"))  # type: ignore
        client = http_api_factory(port=port)
    else:
        client = http_api_factory()
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
    assert (
        settings.chroma_api_impl == "chromadb.api.fastapi.FastAPI"
        or settings.chroma_api_impl == "chromadb.api.async_fastapi.AsyncFastAPI"
    )


def test_http_client_with_inconsistent_host_settings(
    http_api_factory: HttpAPIFactory,
) -> None:
    try:
        http_api_factory(settings=Settings(chroma_server_host="127.0.0.1"))
    except ValueError as e:
        assert (
            str(e)
            == "Chroma server host provided in settings[127.0.0.1] is different to the one provided in HttpClient: [localhost]"
        )


def test_http_client_with_inconsistent_port_settings(
    http_api_factory: HttpAPIFactory,
) -> None:
    try:
        http_api_factory(
            port=8002,
            settings=Settings(
                chroma_server_http_port=8001,
            ),
        )
    except ValueError as e:
        assert (
            str(e)
            == "Chroma server http port provided in settings[8001] is different to the one provided in HttpClient: [8002]"
        )


def make_sync_client_factory() -> Tuple[Callable[..., Any], Dict[str, Any]]:
    captured: Dict[str, Any] = {}

    # takes any positional args to match httpx.Client
    def factory(*_: Any, **kwargs: Any) -> Any:
        captured.update(kwargs)
        session = MagicMock()
        session.headers = {}
        return session

    return factory, captured


def test_fastapi_uses_http_limits_from_settings() -> None:
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=9000,
        chroma_server_ssl_verify=True,
        chroma_http_keepalive_secs=12.5,
        chroma_http_max_connections=64,
        chroma_http_max_keepalive_connections=16,
    )
    system = System(settings)

    factory, captured = make_sync_client_factory()

    with patch.object(FastAPI, "require", side_effect=[MagicMock(), MagicMock()]):
        with patch("chromadb.api.fastapi.httpx.Client", side_effect=factory):
            api = FastAPI(system)

    api.stop()
    limits = captured["limits"]
    assert limits.keepalive_expiry == 12.5
    assert limits.max_connections == 64
    assert limits.max_keepalive_connections == 16
    assert captured["timeout"] is None
    assert captured["verify"] is True
