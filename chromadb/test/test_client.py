import json
import socket
import time
import uuid
from typing import Generator, Any, Optional
from unittest.mock import patch

from pytest_httpserver import HTTPServer

import chromadb
from chromadb.config import Settings
from chromadb.api import ClientAPI
import chromadb.server.fastapi
import pytest
import tempfile
from chromadb.utils.net import RetryStrategy


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


@pytest.fixture
def retry_session(httpserver: HTTPServer) -> chromadb.api.ClientAPI:
    httpserver.expect_request("/api/v1/tenants/default_tenant").respond_with_data(
        json.dumps({"name": "default_tenant"})
    )
    httpserver.expect_request(
        "/api/v1/databases/default_database",
        query_string="tenant=default_tenant",
    ).respond_with_data(
        json.dumps(
            {
                "id": f"{uuid.uuid4()}",
                "name": "default_database",
                "tenant": "default_tenant",
            }
        )
    )
    return chromadb.HttpClient(host=httpserver.host, port=httpserver.port)


@pytest.fixture
def retry_session_with_custom_retry(httpserver: HTTPServer) -> chromadb.api.ClientAPI:
    httpserver.expect_request("/api/v1/tenants/default_tenant").respond_with_data(
        json.dumps({"name": "default_tenant"})
    )
    httpserver.expect_request(
        "/api/v1/databases/default_database",
        query_string="tenant=default_tenant",
    ).respond_with_data(
        json.dumps(
            {
                "id": f"{uuid.uuid4()}",
                "name": "default_database",
                "tenant": "default_tenant",
            }
        )
    )
    return chromadb.HttpClient(
        host=httpserver.host,
        port=httpserver.port,
        retry=RetryStrategy(total=2, status_codes=(504,)),
    )


def test_retry_on_429(
    httpserver: HTTPServer, retry_session: chromadb.api.ClientAPI
) -> None:
    retry_after_header = {"Retry-After": "2"}  # wait for 2 sec before trying again

    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        "Too Many Requests", status=429, headers=retry_after_header
    )
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        "Too Many Requests", status=429, headers=retry_after_header
    )
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        json.dumps({"nanosecond heartbeat": 1715365335819568533}), status=200
    )
    start_time = time.time()
    retry_session.heartbeat()
    assert (
        time.time() - start_time > 2 * 2
    )  # ensure that we respect the Retry-After header


def test_retry_on_504(
    httpserver: HTTPServer, retry_session: chromadb.api.ClientAPI
) -> None:
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        "Gateway Timeout", status=504
    )

    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        "Gateway Timeout", status=504
    )
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        json.dumps({"nanosecond heartbeat": 1715365335819568533}), status=200
    )
    retry_session.heartbeat()


def test_retry_on_503(
    httpserver: HTTPServer, retry_session: chromadb.api.ClientAPI
) -> None:
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        "Service Unavailable", status=503
    )

    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        "Service Unavailable", status=503
    )
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        json.dumps({"nanosecond heartbeat": 1715365335819568533}), status=200
    )
    retry_session.heartbeat()


def test_retry_on_503_exceeding_max(
    httpserver: HTTPServer, retry_session: chromadb.api.ClientAPI
) -> None:
    httpserver.expect_request("/api/v1").respond_with_data(
        "Service Unavailable", status=503
    )

    with pytest.raises(Exception, match="Max retries exceeded with url"):
        retry_session.heartbeat()


def test_no_retry_on_400(
    httpserver: HTTPServer, retry_session: chromadb.api.ClientAPI
) -> None:
    httpserver.expect_request("/api/v1").respond_with_data("Bad Request", status=400)

    with pytest.raises(Exception, match="Bad Request"):
        retry_session.heartbeat()


def test_no_retry_on_400_with_custom_retry(
    httpserver: HTTPServer, retry_session_with_custom_retry: chromadb.api.ClientAPI
) -> None:
    httpserver.expect_request("/api/v1").respond_with_data("Bad Request", status=400)

    with pytest.raises(Exception, match="Bad Request"):
        retry_session_with_custom_retry.heartbeat()


def test_no_retry_on_429_with_custom_retry(
    httpserver: HTTPServer, retry_session_with_custom_retry: chromadb.api.ClientAPI
) -> None:
    httpserver.expect_request("/api/v1").respond_with_data(
        "Too Many Requests", status=429
    )

    with pytest.raises(Exception, match="Too Many Requests"):
        retry_session_with_custom_retry.heartbeat()


def test_no_retry_on_504_with_custom_retry(
    httpserver: HTTPServer, retry_session_with_custom_retry: chromadb.api.ClientAPI
) -> None:
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        "Gateway Timeout", status=504
    )
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        "Gateway Timeout", status=504
    )
    httpserver.expect_oneshot_request("/api/v1").respond_with_data(
        json.dumps({"nanosecond heartbeat": 1715365335819568533}), status=200
    )
    retry_session_with_custom_retry.heartbeat()


def server(max_retries: int = 1) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("localhost", 9999))
    s.listen(1)
    retries = 0
    while retries < max_retries:
        conn, _ = s.accept()
        conn.close()  # Close the connection immediately
        retries += 1


@pytest.fixture
def connect_retries() -> int:
    return 3


@pytest.fixture
def local_tcp_server(connect_retries: int) -> Generator[None, None, None]:
    import threading

    t = threading.Thread(target=server, args=(connect_retries,))
    t.start()
    yield
    t.join()


def test_with_connection_error(
    local_tcp_server: Optional[Any], connect_retries: int
) -> None:
    start_time = time.time()
    with pytest.raises(Exception):
        chromadb.HttpClient(
            host="localhost",
            port=9999,
            retry=RetryStrategy(connect=connect_retries - 1, backoff_factor=1),
        )
        assert (
            time.time() - start_time > 4
        )  # ensure that we have attempted to connect twice
