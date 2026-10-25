import asyncio
from typing import Any, Awaitable, Callable, Generator, cast, Dict, Tuple
from unittest.mock import AsyncMock, MagicMock, patch
import chromadb
import httpx
from chromadb.config import Settings, System
from chromadb.api import AsyncServerAPI, ClientAPI, ServerAPI
from chromadb.api.async_client import AsyncAdminClient, AsyncClient
from chromadb.api.async_fastapi import AsyncFastAPI
from chromadb.api.client import AdminClient, Client
from chromadb.api.shared_system_client import SharedSystemClient
from chromadb.auth import UserIdentity
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
        captured["session"] = session
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
    captured["session"].close.assert_called_once_with()
    assert api._running is False


def test_fastapi_stop_marks_component_stopped_when_close_fails() -> None:
    api = FastAPI.__new__(FastAPI)
    api._session = MagicMock()
    api._session.close.side_effect = RuntimeError("close failed")
    api._running = True

    with pytest.raises(RuntimeError, match="close failed"):
        api.stop()

    assert api._running is False


def test_fastapi_closes_session_when_initialization_fails() -> None:
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=9000,
        chroma_client_auth_provider=(
            "chromadb.auth.basic_authn.BasicAuthClientProvider"
        ),
    )
    system = System(settings)
    session = MagicMock()
    session.headers = {}

    with (
        patch.object(
            FastAPI,
            "require",
            side_effect=[MagicMock(), MagicMock(), RuntimeError("auth setup failed")],
        ),
        patch("chromadb.api.fastapi.httpx.Client", return_value=session),
        pytest.raises(RuntimeError, match="auth setup failed"),
    ):
        FastAPI(system)

    session.close.assert_called_once_with()


def test_client_component_failure_removes_unretained_system() -> None:
    SharedSystemClient.clear_system_cache()
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=9000,
        chroma_client_auth_provider=(
            "chromadb.auth.basic_authn.BasicAuthClientProvider"
        ),
        anonymized_telemetry=False,
    )
    session = MagicMock()
    session.headers = {}
    initialization_error = RuntimeError("auth setup failed")

    try:
        with (
            patch.object(
                FastAPI,
                "require",
                side_effect=[MagicMock(), MagicMock(), initialization_error],
            ),
            patch("chromadb.api.fastapi.httpx.Client", return_value=session),
            pytest.raises(RuntimeError, match="auth setup failed") as exc_info,
        ):
            Client(settings=settings)

        assert exc_info.value is initialization_error
        session.close.assert_called_once_with()
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        SharedSystemClient.clear_system_cache()


def test_client_rollback_preserves_error_when_transport_close_fails() -> None:
    SharedSystemClient.clear_system_cache()
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=9000,
        anonymized_telemetry=False,
    )
    identity = UserIdentity(
        user_id="test",
        tenant="tenant",
        databases=["database"],
    )
    initialization_error = ValueError("validation failed")
    session = MagicMock()
    session.headers = {}
    session.close.side_effect = RuntimeError("cleanup failed")

    try:
        with (
            patch.object(FastAPI, "require", side_effect=[MagicMock(), MagicMock()]),
            patch("chromadb.api.fastapi.httpx.Client", return_value=session),
            patch.object(Client, "get_user_identity", return_value=identity),
            patch.object(
                Client,
                "_validate_tenant_database",
                side_effect=initialization_error,
            ),
            pytest.raises(ValueError, match="validation failed") as exc_info,
        ):
            Client(
                tenant="tenant",
                database="database",
                settings=settings,
            )

        assert exc_info.value is initialization_error
        session.close.assert_called_once_with()
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        SharedSystemClient.clear_system_cache()


@pytest.mark.parametrize("admin_cls", [AdminClient, AsyncAdminClient])
def test_retained_admin_failure_releases_system(admin_cls: Any) -> None:
    SharedSystemClient.clear_system_cache()
    system = MagicMock(spec=System)
    system.settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=9000,
    )
    initialization_error = RuntimeError("server setup failed")
    system.instance.side_effect = initialization_error

    try:
        with pytest.raises(RuntimeError, match="server setup failed") as exc_info:
            admin_cls(settings=system.settings, _system=system)

        assert exc_info.value is initialization_error
        system.stop.assert_called_once_with()
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        SharedSystemClient.clear_system_cache()


def create_no_network_cloud_client() -> Client:
    identity = UserIdentity(
        user_id="test",
        tenant="tenant",
        databases=["database"],
    )
    with (
        patch.object(Client, "get_user_identity", return_value=identity),
        patch.object(Client, "_validate_tenant_database", return_value=None),
    ):
        return cast(
            Client,
            chromadb.CloudClient(
                api_key="not-a-real-key",
                tenant="tenant",
                database="database",
                settings=Settings(anonymized_telemetry=False),
                cloud_host="127.0.0.1",
                cloud_port=9,
                enable_ssl=False,
            ),
        )


def test_http_client_close_releases_transport_and_system() -> None:
    SharedSystemClient.clear_system_cache()
    client = None
    sessions = []

    try:
        client = create_no_network_cloud_client()

        systems = dict(SharedSystemClient._identifier_to_system)
        unique_systems = list(
            {id(system): system for system in systems.values()}.values()
        )
        for system in unique_systems:
            session = getattr(system.instance(ServerAPI), "_session", None)
            if session is not None:
                sessions.append(session)

        assert len(unique_systems) == 1
        assert len(sessions) == 1
        assert set(systems) == set(SharedSystemClient._identifier_to_refcount)
        admin_client = cast(AdminClient, client._admin_client)
        assert admin_client._system is client._system
        assert admin_client._server is client._server

        client.close()

        assert sessions[0].is_closed
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        if client is not None:
            client.close()
        for session in sessions:
            if not session.is_closed:
                session.close()
        SharedSystemClient.clear_system_cache()


def test_repeated_http_client_close_does_not_grow_cache() -> None:
    SharedSystemClient.clear_system_cache()
    sessions = []

    try:
        for _ in range(25):
            client = create_no_network_cloud_client()
            session = cast(FastAPI, client._server)._session
            sessions.append(session)

            client.close()

            assert session.is_closed
            assert SharedSystemClient._identifier_to_system == {}
            assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        for session in sessions:
            if not session.is_closed:
                session.close()
        SharedSystemClient.clear_system_cache()


def test_async_from_system_reuses_supplied_system() -> None:
    SharedSystemClient.clear_system_cache()
    identity = UserIdentity(
        user_id="test",
        tenant="tenant",
        databases=["database"],
    )
    system = System(
        Settings(
            chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
            chroma_server_host="localhost",
            chroma_server_http_port=8000,
            anonymized_telemetry=False,
        )
    )
    system.start()

    try:
        with (
            patch.object(
                AsyncClient,
                "get_user_identity",
                new=AsyncMock(return_value=identity),
            ),
            patch.object(
                AsyncClient,
                "_validate_tenant_database",
                new=AsyncMock(return_value=None),
            ),
        ):
            client = cast(
                AsyncClient,
                _run_async(
                    AsyncClient.from_system_async(
                        system,
                        tenant="tenant",
                        database="database",
                    )
                ),
            )

        assert client._system is system
        admin_client = cast(AsyncAdminClient, client._admin_client)
        assert admin_client._system is system
        assert admin_client._server is client._server
        assert len(SharedSystemClient._identifier_to_system) == 1
        assert set(SharedSystemClient._identifier_to_system) == set(
            SharedSystemClient._identifier_to_refcount
        )
        assert SharedSystemClient._identifier_to_refcount[client._identifier] == 2

        SharedSystemClient._release_system(cast(Any, client._admin_client)._identifier)
        SharedSystemClient._release_system(client._identifier)

        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
        assert system._running is False
    finally:
        if system._running:
            system.stop()
        SharedSystemClient.clear_system_cache()


def test_async_initialization_failure_releases_system() -> None:
    SharedSystemClient.clear_system_cache()
    system = MagicMock(spec=System)
    system.settings = Settings(
        chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=8000,
        anonymized_telemetry=False,
    )
    system.instance.return_value = MagicMock()
    initialization_error = RuntimeError("identity failed")

    try:
        with (
            patch.object(
                AsyncClient,
                "get_user_identity",
                new=AsyncMock(side_effect=initialization_error),
            ),
            pytest.raises(RuntimeError, match="identity failed") as exc_info,
        ):
            _run_async(AsyncClient.from_system_async(system))

        assert exc_info.value is initialization_error
        system.stop.assert_called_once_with()
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        SharedSystemClient.clear_system_cache()


@pytest.mark.parametrize("cancel_during_validation", [False, True])
def test_async_cancellation_releases_system(cancel_during_validation: bool) -> None:
    SharedSystemClient.clear_system_cache()
    system = MagicMock(spec=System)
    system.settings = Settings(
        chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=8000,
        anonymized_telemetry=False,
    )
    system.instance.return_value = MagicMock()
    identity = UserIdentity(
        user_id="test",
        tenant="tenant",
        databases=["database"],
    )
    cancellation = asyncio.CancelledError()

    try:
        with (
            patch.object(
                AsyncClient,
                "get_user_identity",
                new=AsyncMock(
                    return_value=identity,
                    side_effect=cancellation if not cancel_during_validation else None,
                ),
            ),
            patch.object(
                AsyncClient,
                "_validate_tenant_database",
                new=AsyncMock(
                    side_effect=cancellation if cancel_during_validation else None
                ),
            ),
            pytest.raises(asyncio.CancelledError) as exc_info,
        ):
            _run_async(
                AsyncClient.from_system_async(
                    system,
                    tenant="tenant",
                    database="database",
                )
            )

        assert exc_info.value is cancellation
        system.stop.assert_called_once_with()
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        SharedSystemClient.clear_system_cache()


@pytest.mark.parametrize(
    "initialization_error",
    [RuntimeError("identity failed"), asyncio.CancelledError()],
)
async def test_async_rollback_awaits_transport_cleanup(
    initialization_error: BaseException,
) -> None:
    SharedSystemClient.clear_system_cache()
    AsyncFastAPI._clients = {}
    system = System(
        Settings(
            chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
            chroma_server_host="localhost",
            chroma_server_http_port=8000,
            anonymized_telemetry=False,
        )
    )
    api = system.instance(AsyncServerAPI)
    system.start()
    session = MagicMock()
    session.aclose = AsyncMock()

    try:
        with patch(
            "chromadb.api.async_fastapi.httpx.AsyncClient", return_value=session
        ):
            cast(AsyncFastAPI, api)._get_client()

        with (
            patch.object(
                AsyncClient,
                "get_user_identity",
                new=AsyncMock(side_effect=initialization_error),
            ),
            pytest.raises(type(initialization_error)) as exc_info,
        ):
            await AsyncClient.from_system_async(system)

        assert exc_info.value is initialization_error
        session.aclose.assert_awaited_once_with()
        assert cast(AsyncFastAPI, api)._clients == {}
        assert system._running is False
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        if cast(AsyncFastAPI, api)._clients:
            await cast(AsyncFastAPI, api)._cleanup()
        AsyncFastAPI._clients = {}
        SharedSystemClient.clear_system_cache()


async def test_async_rollback_cleanup_survives_cancellation() -> None:
    SharedSystemClient.clear_system_cache()
    AsyncFastAPI._clients = {}
    system = System(
        Settings(
            chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
            chroma_server_host="localhost",
            chroma_server_http_port=8000,
            anonymized_telemetry=False,
        )
    )
    api = system.instance(AsyncServerAPI)
    system.start()
    initialization_error = RuntimeError("identity failed")
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def blocking_aclose() -> None:
        cleanup_started.set()
        await allow_cleanup.wait()
        cleanup_finished.set()

    session = MagicMock()
    session.aclose = AsyncMock(side_effect=blocking_aclose)

    try:
        with patch(
            "chromadb.api.async_fastapi.httpx.AsyncClient", return_value=session
        ):
            cast(AsyncFastAPI, api)._get_client()

        with patch.object(
            AsyncClient,
            "get_user_identity",
            new=AsyncMock(side_effect=initialization_error),
        ):
            rollback_task = asyncio.create_task(AsyncClient.from_system_async(system))
            await cleanup_started.wait()
            rollback_task.cancel()
            await asyncio.sleep(0)
            allow_cleanup.set()

            with pytest.raises(RuntimeError) as exc_info:
                await rollback_task

        assert exc_info.value is initialization_error
        session.aclose.assert_awaited_once_with()
        assert cleanup_finished.is_set()
        assert cast(AsyncFastAPI, api)._clients == {}
        assert system._running is False
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        allow_cleanup.set()
        if cast(AsyncFastAPI, api)._clients:
            await cast(AsyncFastAPI, api)._cleanup()
        AsyncFastAPI._clients = {}
        SharedSystemClient.clear_system_cache()


def test_http_client_initialization_rollback_closes_resources() -> None:
    SharedSystemClient.clear_system_cache()
    identity = UserIdentity(
        user_id="test",
        tenant="tenant",
        databases=["database"],
    )
    initialization_error = RuntimeError("validation failed")
    created_sessions = []
    httpx_client = httpx.Client

    def create_session(*args: Any, **kwargs: Any) -> httpx.Client:
        session = httpx_client(*args, **kwargs)
        created_sessions.append(session)
        return session

    try:
        with (
            patch.object(Client, "get_user_identity", return_value=identity),
            patch.object(
                Client,
                "_validate_tenant_database",
                side_effect=initialization_error,
            ),
            patch("chromadb.api.fastapi.httpx.Client", side_effect=create_session),
            pytest.raises(RuntimeError, match="validation failed") as exc_info,
        ):
            chromadb.CloudClient(
                api_key="not-a-real-key",
                tenant="tenant",
                database="database",
                settings=Settings(anonymized_telemetry=False),
                cloud_host="127.0.0.1",
                cloud_port=9,
                enable_ssl=False,
            )

        assert exc_info.value is initialization_error
        assert created_sessions
        assert all(session.is_closed for session in created_sessions)
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        for session in created_sessions:
            if not session.is_closed:
                session.close()
        SharedSystemClient.clear_system_cache()


def test_http_client_context_manager_closes_resources() -> None:
    SharedSystemClient.clear_system_cache()
    session = None

    try:
        with create_no_network_cloud_client() as client:
            session = cast(FastAPI, client._server)._session
            assert session.is_closed is False

        assert session.is_closed
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        if session is not None and not session.is_closed:
            session.close()
        SharedSystemClient.clear_system_cache()


def test_http_client_close_is_idempotent() -> None:
    SharedSystemClient.clear_system_cache()
    client = create_no_network_cloud_client()
    session = cast(FastAPI, client._server)._session

    try:
        client.close()
        client.close()
        client.close()

        assert session.is_closed
        assert SharedSystemClient._identifier_to_system == {}
        assert SharedSystemClient._identifier_to_refcount == {}
    finally:
        client.close()
        if not session.is_closed:
            session.close()
        SharedSystemClient.clear_system_cache()


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
