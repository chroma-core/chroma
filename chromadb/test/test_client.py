import asyncio
import os
import re
import shutil
import uuid
from typing import Any, Callable, Generator, cast
from unittest.mock import patch

import psutil

import chromadb
from chromadb import AsyncClientAPI
from chromadb.config import Settings
from chromadb.api import ClientAPI
import chromadb.server.fastapi
import pytest
import tempfile

from testcontainers.chroma import ChromaContainer


@pytest.fixture
def ephemeral_api() -> Generator[ClientAPI, None, None]:
    client = chromadb.EphemeralClient()
    yield client
    client.clear_system_cache()


@pytest.fixture
def persistent_api() -> Generator[ClientAPI, None, None]:
    client = chromadb.PersistentClient(
        path=tempfile.gettempdir() + "/test_server",
        settings=Settings(
            allow_reset=True,
        ),
    )
    yield client
    client.clear_system_cache()
    shutil.rmtree(tempfile.gettempdir() + "/test_server", ignore_errors=True)


HttpAPIFactory = Callable[..., ClientAPI]


@pytest.fixture(params=["sync_client", "async_client"])
def http_api_factory(
    request: pytest.FixtureRequest,
) -> Generator[HttpAPIFactory, None, None]:
    if request.param == "sync_client":
        with patch("chromadb.api.client.Client._validate_tenant_database"):
            yield chromadb.HttpClient
    else:
        with patch("chromadb.api.async_client.AsyncClient._validate_tenant_database"):

            def factory(*args: Any, **kwargs: Any) -> Any:
                cls = asyncio.get_event_loop().run_until_complete(
                    chromadb.AsyncHttpClient(*args, **kwargs)
                )
                return cls

            yield cast(HttpAPIFactory, factory)


@pytest.fixture()
def http_api(http_api_factory: HttpAPIFactory) -> Generator[ClientAPI, None, None]:
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


def test_persistent_client_close() -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )
    persistent_api = chromadb.PersistentClient(
        path=os.path.join(tempfile.gettempdir(), "test_server-" + uuid.uuid4().hex),
        settings=Settings(),
    )
    current_process = psutil.Process()
    col = persistent_api.create_collection("test")
    temp_persist_dir = persistent_api.get_settings().persist_directory
    col1 = persistent_api.create_collection("test1" + uuid.uuid4().hex)
    col.add(ids=["1"], documents=["test"])
    col1.add(ids=["1"], documents=["test1"])
    open_files = current_process.open_files()
    filtered_open_files = [
        file for file in open_files if re.search(re.escape(temp_persist_dir), file.path)
    ]
    assert len(filtered_open_files) > 0
    persistent_api.close()
    open_files = current_process.open_files()
    post_filtered_open_files = [
        file
        for file in open_files
        if re.search(re.escape(temp_persist_dir) + ".*chroma.sqlite3", file.path)
        or re.search(re.escape(temp_persist_dir) + ".*data_level0.bin", file.path)
    ]
    assert len(post_filtered_open_files) == 0


def test_persistent_client_use_after_close() -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )
    persistent_api = chromadb.PersistentClient(
        path=os.path.join(tempfile.gettempdir(), "test_server-" + uuid.uuid4().hex),
        settings=Settings(),
    )
    current_process = psutil.Process()
    col = persistent_api.create_collection("test" + uuid.uuid4().hex)
    temp_persist_dir = persistent_api.get_settings().persist_directory
    col.add(ids=["1"], documents=["test"])
    open_files = current_process.open_files()
    filtered_open_files = [
        file
        for file in open_files
        if re.search(re.escape(temp_persist_dir) + ".*chroma.sqlite3", file.path)
        or re.search(re.escape(temp_persist_dir) + ".*data_level0.bin", file.path)
    ]
    assert len(filtered_open_files) > 0
    persistent_api.close()
    open_files = current_process.open_files()
    post_filtered_open_files = [
        file
        for file in open_files
        if re.search(re.escape(temp_persist_dir) + ".*chroma.sqlite3", file.path)
        or re.search(re.escape(temp_persist_dir) + ".*data_level0.bin", file.path)
    ]
    assert len(post_filtered_open_files) == 0
    with pytest.raises(RuntimeError, match="Component not running"):
        col.add(ids=["1"], documents=["test"])
    with pytest.raises(RuntimeError, match="Component not running"):
        col.delete(ids=["1"])
    with pytest.raises(RuntimeError, match="Component not running"):
        col.update(ids=["1"], documents=["test1231"])
    with pytest.raises(RuntimeError, match="Component not running"):
        col.upsert(ids=["1"], documents=["test1231"])
    with pytest.raises(RuntimeError, match="Component not running"):
        col.count()
    with pytest.raises(RuntimeError, match="Component not running"):
        persistent_api.create_collection("test1")
    with pytest.raises(RuntimeError, match="Component not running"):
        persistent_api.get_collection("test")
    with pytest.raises(RuntimeError, match="Component not running"):
        persistent_api.get_or_create_collection("test")
    with pytest.raises(RuntimeError, match="Component not running"):
        persistent_api.list_collections()
    with pytest.raises(RuntimeError, match="Component not running"):
        persistent_api.delete_collection("test")
    with pytest.raises(RuntimeError, match="Component not running"):
        persistent_api.count_collections()
    with pytest.raises(RuntimeError, match="Component not running"):
        persistent_api.heartbeat()


@pytest.fixture(params=["sync_client", "async_client"])
def http_client_with_tc(
    request: pytest.FixtureRequest,
) -> Generator[ClientAPI, None, None]:
    with ChromaContainer() as chroma:
        config = chroma.get_config()
        if request.param == "sync_client":
            http_api = chromadb.HttpClient(host=config["host"], port=config["port"])
            yield http_api
            http_api.clear_system_cache()
        else:

            async def init_client() -> AsyncClientAPI:
                http_api = await chromadb.AsyncHttpClient(
                    host=config["host"], port=config["port"]
                )
                return http_api

            yield asyncio.get_event_loop().run_until_complete(init_client())


def get_connection_count(api_client: ClientAPI) -> int:
    if isinstance(api_client, AsyncClientAPI):
        connections = 0
        for k, client in api_client._server._clients.items():
            _pool = client._transport._pool  # type: ignore
            connections += len(_pool._connections)
        return connections
    else:
        _pool = api_client._server._session._transport._pool  # type: ignore
        return len(_pool._connections)


def test_http_client_close(http_client_with_tc: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )

    async def run_in_async(c: AsyncClientAPI):
        col = await c.create_collection("test" + uuid.uuid4().hex)
        await col.add(ids=["1"], documents=["test"])
        assert get_connection_count(http_client_with_tc) > 0
        await c.close()
        assert get_connection_count(http_client_with_tc) == 0

    if isinstance(http_client_with_tc, AsyncClientAPI):
        asyncio.get_event_loop().run_until_complete(
            run_in_async(cast(AsyncClientAPI, http_client_with_tc))
        )
    else:
        col = http_client_with_tc.create_collection("test" + uuid.uuid4().hex)
        col.add(ids=["1"], documents=["test"])
        assert get_connection_count(http_client_with_tc) > 0
        http_client_with_tc.close()
        assert get_connection_count(http_client_with_tc) == 0


def test_http_client_use_after_close(http_client_with_tc: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )

    async def run_in_async(c: AsyncClientAPI):
        col = await c.create_collection("test" + uuid.uuid4().hex)
        await col.add(ids=["1"], documents=["test"])
        assert get_connection_count(http_client_with_tc) > 0
        await c.close()
        assert get_connection_count(http_client_with_tc) == 0
        with pytest.raises(RuntimeError, match="Component not running"):
            await c.heartbeat()
        with pytest.raises(RuntimeError, match="Component not running"):
            await col.add(ids=["1"], documents=["test"])
        with pytest.raises(RuntimeError, match="Component not running"):
            await col.delete(ids=["1"])
        with pytest.raises(RuntimeError, match="Component not running"):
            await col.update(ids=["1"], documents=["test1231"])
        with pytest.raises(RuntimeError, match="Component not running"):
            await col.upsert(ids=["1"], documents=["test1231"])
        with pytest.raises(RuntimeError, match="Component not running"):
            await col.count()
        with pytest.raises(RuntimeError, match="Component not running"):
            await c.create_collection("test1")
        with pytest.raises(RuntimeError, match="Component not running"):
            await c.get_collection("test")
        with pytest.raises(RuntimeError, match="Component not running"):
            await c.get_or_create_collection("test")
        with pytest.raises(RuntimeError, match="Component not running"):
            await c.list_collections()

    if isinstance(http_client_with_tc, AsyncClientAPI):
        asyncio.get_event_loop().run_until_complete(
            run_in_async(cast(AsyncClientAPI, http_client_with_tc))
        )
    else:
        col = http_client_with_tc.create_collection("test" + uuid.uuid4().hex)
        col.add(ids=["1"], documents=["test"])
        assert get_connection_count(http_client_with_tc) > 0
        http_client_with_tc.close()
        assert get_connection_count(http_client_with_tc) == 0
        with pytest.raises(RuntimeError, match="Component not running"):
            http_client_with_tc.heartbeat()
        with pytest.raises(RuntimeError, match="Component not running"):
            col.add(ids=["1"], documents=["test"])
        with pytest.raises(RuntimeError, match="Component not running"):
            col.delete(ids=["1"])
        with pytest.raises(RuntimeError, match="Component not running"):
            col.update(ids=["1"], documents=["test1231"])
        with pytest.raises(RuntimeError, match="Component not running"):
            col.upsert(ids=["1"], documents=["test1231"])
        with pytest.raises(RuntimeError, match="Component not running"):
            col.count()
        with pytest.raises(RuntimeError, match="Component not running"):
            http_client_with_tc.create_collection("test1")
        with pytest.raises(RuntimeError, match="Component not running"):
            http_client_with_tc.get_collection("test")
        with pytest.raises(RuntimeError, match="Component not running"):
            http_client_with_tc.get_or_create_collection("test")
        with pytest.raises(RuntimeError, match="Component not running"):
            http_client_with_tc.list_collections()
        with pytest.raises(RuntimeError, match="Component not running"):
            http_client_with_tc.delete_collection("test")
        with pytest.raises(RuntimeError, match="Component not running"):
            http_client_with_tc.count_collections()
        with pytest.raises(RuntimeError, match="Component not running"):
            http_client_with_tc.heartbeat()
