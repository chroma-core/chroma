import json
import os
import re
import shutil
import time
from typing import Generator
from unittest.mock import patch
from pytest_httpserver import HTTPServer
import psutil

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
        settings=Settings(
            allow_reset=True,
        ),
    )
    yield client
    client.clear_system_cache()
    shutil.rmtree(tempfile.gettempdir() + "/test_server", ignore_errors=True)


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


def test_persistent_client_close(persistent_api: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )
    persistent_api.reset()
    current_process = psutil.Process()
    col = persistent_api.create_collection("test")
    temp_persist_dir = persistent_api.get_settings().persist_directory.replace(
        "\\", "\\\\"
    )
    col1 = persistent_api.create_collection("test1")
    col.add(ids=["1"], documents=["test"])
    col1.add(ids=["1"], documents=["test1"])
    open_files = current_process.open_files()
    filtered_open_files = [
        file
        for file in open_files
        if re.search(rf"{temp_persist_dir}.*chroma.sqlite3", file.path)
        or re.search(rf"{temp_persist_dir}.*data_level0.bin", file.path)
    ]
    assert len(filtered_open_files) > 0
    persistent_api.close()
    open_files = current_process.open_files()
    post_filtered_open_files = [
        file
        for file in open_files
        if re.search(rf"{temp_persist_dir}.*chroma.sqlite3", file.path)
        or re.search(rf"{temp_persist_dir}.*data_level0.bin", file.path)
    ]
    assert len(post_filtered_open_files) == 0


def test_persistent_client_double_close(persistent_api: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )
    persistent_api.reset()
    current_process = psutil.Process()
    col = persistent_api.create_collection("test")
    temp_persist_dir = persistent_api.get_settings().persist_directory.replace(
        "\\", "\\\\"
    )
    col.add(ids=["1"], documents=["test"])
    open_files = current_process.open_files()
    filtered_open_files = [
        file
        for file in open_files
        if re.search(rf"{temp_persist_dir}.*chroma.sqlite3", file.path)
        or re.search(rf"{temp_persist_dir}.*data_level0.bin", file.path)
    ]
    assert len(filtered_open_files) > 0
    persistent_api.close()
    open_files = current_process.open_files()
    post_filtered_open_files = [
        file
        for file in open_files
        if re.search(rf"{temp_persist_dir}.*chroma.sqlite3", file.path)
        or re.search(rf"{temp_persist_dir}.*data_level0.bin", file.path)
    ]
    assert len(post_filtered_open_files) == 0
    with pytest.raises(RuntimeError, match="Component not running or already closed"):
        persistent_api.close()


def test_persistent_client_use_after_close(persistent_api: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )
    persistent_api.reset()
    current_process = psutil.Process()
    col = persistent_api.create_collection("test")
    temp_persist_dir = persistent_api.get_settings().persist_directory.replace(
        "\\", "\\\\"
    )
    col.add(ids=["1"], documents=["test"])
    open_files = current_process.open_files()
    assert any(
        [
            re.search(rf"{temp_persist_dir}.*chroma.sqlite3", file.path) is not None
            for file in open_files
        ]
    )
    assert any(
        [
            re.search(rf"{temp_persist_dir}.*data_level0.bin", file.path) is not None
            for file in open_files
        ]
    )
    persistent_api.close()
    open_files = current_process.open_files()
    assert all(
        [
            re.search(rf"{temp_persist_dir}.*chroma.sqlite3", file.path) is None
            for file in open_files
        ]
    )
    assert all(
        [
            re.search(rf"{temp_persist_dir}.*data_level0.bin", file.path) is None
            for file in open_files
        ]
    )
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


def _instrument_http_server(httpserver: HTTPServer) -> None:
    httpserver.expect_request("/api/v1/tenants/default_tenant").respond_with_data(
        "default_tenant"
    )
    httpserver.expect_request(
        "/api/v1/databases/default_database?tenant=default_tenant"
    ).respond_with_data(json.dumps({"version": "0.0.1"}))
    httpserver.expect_request("/api/v1/collections").respond_with_data(
        json.dumps(
            {
                "name": "x",
                "id": "4ca8f010-b535-4778-9262-c6f3812e17b6",
                "metadata": None,
                "tenant": "default_tenant",
                "database": "default_database",
            }
        )
    )
    httpserver.expect_request("/api/v1/pre-flight-checks").respond_with_data(
        json.dumps(
            {
                "max_batch_size": 10000,
            }
        )
    )
    httpserver.expect_request(
        "/api/v1/collections/4ca8f010-b535-4778-9262-c6f3812e17b6/add"
    ).respond_with_data(json.dumps({}))
    httpserver.expect_request("/api/v1").respond_with_data(
        json.dumps({"nanosecond heartbeat": time.time_ns()})
    )


def test_http_client_close(http_api: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )
    with HTTPServer(port=8000) as httpserver:
        _instrument_http_server(httpserver)
        col = http_api.create_collection("test")
        col.add(ids=["1"], documents=["test"])
        _pool_manager = http_api._server._session.get_adapter("http://").poolmanager  # type: ignore
        assert len(_pool_manager.pools._container) > 0
        http_api.close()
        assert len(_pool_manager.pools._container) == 0


def test_http_client_double_close(http_api: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )
    with HTTPServer(port=8000) as httpserver:
        _instrument_http_server(httpserver)
        http_api.heartbeat()
        _pool_manager = http_api._server._session.get_adapter("http://").poolmanager  # type: ignore
        assert len(_pool_manager.pools._container) > 0
        http_api.close()
        assert len(_pool_manager.pools._container) == 0
        with pytest.raises(
            RuntimeError, match="Component not running or already closed"
        ):
            http_api.close()


def test_http_client_use_after_close(http_api: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY") == "1":
        pytest.skip(
            "Skipping test that closes the persistent client in integration test"
        )
    with HTTPServer(port=8000) as httpserver:
        _instrument_http_server(httpserver)
        http_api.heartbeat()
        col = http_api.create_collection("test")
        col.add(ids=["1"], documents=["test"])
        _pool_manager = http_api._server._session.get_adapter("http://").poolmanager  # type: ignore
        assert len(_pool_manager.pools._container) > 0
        http_api.close()
        assert len(_pool_manager.pools._container) == 0
        with pytest.raises(RuntimeError, match="Component not running"):
            http_api.heartbeat()
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
            http_api.create_collection("test1")
        with pytest.raises(RuntimeError, match="Component not running"):
            http_api.get_collection("test")
        with pytest.raises(RuntimeError, match="Component not running"):
            http_api.get_or_create_collection("test")
        with pytest.raises(RuntimeError, match="Component not running"):
            http_api.list_collections()
        with pytest.raises(RuntimeError, match="Component not running"):
            http_api.delete_collection("test")
        with pytest.raises(RuntimeError, match="Component not running"):
            http_api.count_collections()
        with pytest.raises(RuntimeError, match="Component not running"):
            http_api.heartbeat()
