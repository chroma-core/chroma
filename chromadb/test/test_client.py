import asyncio
import threading
from typing import Any, Awaitable, Callable, Generator, cast, Dict, Tuple
from unittest.mock import MagicMock, patch
import chromadb
from chromadb.config import Settings, System
from chromadb.api import ClientAPI
import chromadb.server.fastapi
from chromadb.api.async_fastapi import AsyncFastAPI
from chromadb.api.fastapi import FastAPI
import httpx
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


def _run_async(coro: Awaitable[Any]) -> Any:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(coro)


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
                    cls = _run_async(chromadb.AsyncHttpClient(*args, **kwargs))
                    return cls

                yield cast(HttpAPIFactory, factory)


@pytest.fixture()
def no_current_event_loop() -> Generator[None, None, None]:
    """Exercise async-client construction when no event loop is installed.

    These tests are parameterized over sync and async HTTP clients. The async
    factory uses _run_async(), which intentionally creates and installs an
    event loop when asyncio.get_event_loop() raises RuntimeError. Apply this
    state consistently to both inconsistent-settings tests so they exercise the
    same setup and do not depend on whichever event-loop state a previous test
    happened to leave behind.
    """
    try:
        previous_loop = asyncio.get_event_loop()
    except RuntimeError:
        previous_loop = None

    asyncio.set_event_loop(None)
    try:
        yield
    finally:
        # _run_async() may have installed a new loop; close it before restoring
        # the loop that was present before the fixture ran.
        try:
            current_loop = asyncio.get_event_loop()
        except RuntimeError:
            current_loop = None

        if current_loop is not None and current_loop is not previous_loop:
            current_loop.close()
        asyncio.set_event_loop(previous_loop)


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


@pytest.mark.usefixtures("no_current_event_loop")
def test_http_client_with_inconsistent_host_settings(
    http_api_factory: HttpAPIFactory,
) -> None:
    with pytest.raises(ValueError) as e:
        http_api_factory(settings=Settings(chroma_server_host="127.0.0.1"))

    assert (
        str(e.value)
        == "Chroma server host provided in settings[127.0.0.1] is different to the one provided in HttpClient: [localhost]"
    )


@pytest.mark.usefixtures("no_current_event_loop")
def test_http_client_with_inconsistent_port_settings(
    http_api_factory: HttpAPIFactory,
) -> None:
    with pytest.raises(ValueError) as e:
        http_api_factory(
            port=8002,
            settings=Settings(
                chroma_server_http_port=8001,
            ),
        )

    assert (
        str(e.value)
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


def make_async_fastapi(host: str, port: int, token: str = "") -> AsyncFastAPI:
    # No server is needed: building the client and asking for its httpx client
    # does not open a connection.
    headers = {"Authorization": f"Bearer {token}"} if token else None
    settings = Settings(
        chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
        chroma_server_host=host,
        chroma_server_http_port=port,
        chroma_server_headers=headers,
        anonymized_telemetry=False,
    )
    return AsyncFastAPI(System(settings))


async def test_async_fastapi_instances_do_not_share_client_or_credentials() -> None:
    api_a = make_async_fastapi("host-a", 8000, "TOKEN-A")
    api_b = make_async_fastapi("host-b", 8001, "TOKEN-B")

    client_a = api_a._get_client()
    client_b = api_b._get_client()

    assert client_a is not client_b
    assert client_a.headers["Authorization"] == "Bearer TOKEN-A"
    assert client_b.headers["Authorization"] == "Bearer TOKEN-B"
    # httpx lowercases header names when they are converted to a dict
    assert api_a.get_request_headers()["authorization"] == "Bearer TOKEN-A"
    assert api_b.get_request_headers()["authorization"] == "Bearer TOKEN-B"

    await api_a._cleanup()
    await api_b._cleanup()


async def test_async_fastapi_instance_without_credentials_does_not_inherit_them() -> (
    None
):
    api_a = make_async_fastapi("host-a", 8000, "TOKEN-A")
    api_b = make_async_fastapi("host-b", 8001)

    api_a._get_client()
    client_b = api_b._get_client()

    assert "Authorization" not in client_b.headers

    await api_a._cleanup()
    await api_b._cleanup()


async def test_async_fastapi_reuses_client_within_instance_and_event_loop() -> None:
    api = make_async_fastapi("host-a", 8000, "TOKEN-A")

    assert api._get_client() is api._get_client()
    assert len(api._clients) == 1

    await api._cleanup()


def test_async_fastapi_client_is_not_shared_across_event_loops() -> None:
    # The cache is still keyed by event loop: a client built on one loop must
    # not be handed out on another. Both loops are kept alive at once, because
    # the key is hash(loop) and a freed loop's hash can be reused by a new one.
    api = make_async_fastapi("host-a", 8000, "TOKEN-A")
    barrier = threading.Barrier(2)
    clients: list[httpx.AsyncClient] = []

    async def grab() -> None:
        clients.append(api._get_client())
        barrier.wait(timeout=10)  # hold this loop open until the other has a client

    threads = [threading.Thread(target=lambda: asyncio.run(grab())) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(clients) == 2
    assert clients[0] is not clients[1]
    assert len(api._clients) == 2


async def test_async_fastapi_cleanup_only_closes_its_own_clients() -> None:
    api_a = make_async_fastapi("host-a", 8000, "TOKEN-A")
    api_b = make_async_fastapi("host-b", 8001, "TOKEN-B")
    client_a = api_a._get_client()
    client_b = api_b._get_client()

    await api_a._cleanup()

    assert client_a.is_closed
    assert not client_b.is_closed
    assert api_a._clients == {}
    assert api_b._get_client() is client_b

    await api_b._cleanup()
    assert client_b.is_closed


def test_persistent_client_close() -> None:
    """Test that close() properly releases resources in PersistentClient."""
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a client, add some data, and close it
        client = chromadb.PersistentClient(path=tmpdir)
        collection = client.create_collection("test_collection")
        collection.add(
            ids=["id1", "id2"],
            documents=["doc1", "doc2"],
            metadatas=[{"key": "value1"}, {"key": "value2"}],
        )

        # Save a reference to the system before close() removes it from the cache
        system = client._system

        # Close the client
        client.close()

        # Verify the system is stopped
        assert system._running is False

        # Create a new client with the same path to verify data was persisted
        client2 = chromadb.PersistentClient(path=tmpdir)
        collection2 = client2.get_collection("test_collection")
        results = collection2.get()
        assert len(results["ids"]) == 2
        assert "id1" in results["ids"]
        assert "id2" in results["ids"]

        client2.close()
        client.clear_system_cache()
        client2.clear_system_cache()


def test_persistent_client_context_manager() -> None:
    """Test that PersistentClient works as a context manager."""
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Use client as context manager
        with chromadb.PersistentClient(path=tmpdir) as client:
            # Save a reference to the system before close() removes it from the cache
            system = client._system
            collection = client.create_collection("test_collection")
            collection.add(
                ids=["id1", "id2"],
                documents=["doc1", "doc2"],
                metadatas=[{"key": "value1"}, {"key": "value2"}],
            )

        # Verify the system is stopped after context exit
        assert system._running is False

        # Verify data was persisted
        with chromadb.PersistentClient(path=tmpdir) as client2:
            collection2 = client2.get_collection("test_collection")
            results = collection2.get()
            assert len(results["ids"]) == 2

        client.clear_system_cache()
        client2.clear_system_cache()


def test_ephemeral_client_close() -> None:
    """Test that close() works with EphemeralClient."""
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")

    client = chromadb.EphemeralClient()
    # Save a reference to the system before close() removes it from the cache
    system = client._system
    collection = client.create_collection("test_collection")
    collection.add(ids=["id1"], documents=["doc1"])

    # Close the client
    client.close()

    # Verify the system is stopped
    assert system._running is False

    client.clear_system_cache()


def test_ephemeral_client_context_manager() -> None:
    """Test that EphemeralClient works as a context manager."""
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")

    with chromadb.EphemeralClient() as client:
        # Save a reference to the system before close() removes it from the cache
        system = client._system
        collection = client.create_collection("test_collection")
        collection.add(ids=["id1"], documents=["doc1"])
        assert system._running is True

    # Verify the system is stopped after context exit
    assert system._running is False

    client.clear_system_cache()


def test_client_close_idempotent() -> None:
    """Test that calling close() multiple times is a safe no-op."""
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")

    with tempfile.TemporaryDirectory() as tmpdir:
        client = chromadb.PersistentClient(path=tmpdir)
        collection = client.create_collection("test_collection")
        collection.add(ids=["id1"], documents=["doc1"])

        # First close should work normally
        client.close()

        # Second close should be a no-op, not raise KeyError
        client.close()

        # Third close should also be safe
        client.close()

        client.clear_system_cache()


def test_rust_bindings_api_stop_closes_bindings() -> None:
    """Test RustBindingsAPI.stop() closes the underlying Rust bindings."""
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Integration test only")

    from chromadb.api.rust import RustBindingsAPI

    api = RustBindingsAPI.__new__(RustBindingsAPI)
    bindings = MagicMock()
    api.bindings = bindings
    api._running = True

    api.stop()

    bindings.close.assert_called_once_with()
    assert hasattr(api, "bindings") is False
    assert api._running is False
