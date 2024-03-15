import json
import shutil
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
    current_process = psutil.Process()
    col = persistent_api.create_collection("test")
    col.add(ids=["1"], documents=["test"])
    open_files = current_process.open_files()
    assert any(["test_server/chroma.sqlite3" in file.path for file in open_files])
    assert any(["data_level0.bin" in file.path for file in open_files])
    persistent_api.close()
    open_files = current_process.open_files()
    assert all(["test_server/chroma.sqlite3" not in file.path for file in open_files])
    assert all(["data_level0.bin" not in file.path for file in open_files])


def test_http_client_close(http_api: ClientAPI) -> None:
    with HTTPServer(port=8000) as httpserver:
        # Define the response
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
        col = http_api.create_collection("test")
        col.add(ids=["1"], documents=["test"])
        _pool_manager = http_api._server._session.get_adapter("http://").poolmanager  # type: ignore
        assert len(_pool_manager.pools._container) > 0
        http_api.close()
        assert len(_pool_manager.pools._container) == 0
