import os
import pytest
import chromadb
import traceback
import tempfile
import httpx
import shutil
from datetime import datetime, timedelta
from chromadb.api.fastapi import FastAPI
from chromadb.api import ClientAPI
from typing import Generator
from chromadb.config import Settings

persist_dir = tempfile.mkdtemp()


def test_ssl_self_signed(client_ssl: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping test for integration test")
    client_ssl.heartbeat()


def test_ssl_self_signed_without_ssl_verify(client_ssl: ClientAPI) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping test for integration test")
    client_ssl.heartbeat()
    _port = client_ssl._server._settings.chroma_server_http_port  # type: ignore[attr-defined]
    with pytest.raises(ValueError) as e:
        chromadb.HttpClient(ssl=True, port=_port)
    stack_trace = traceback.format_exception(
        type(e.value), e.value, e.value.__traceback__
    )
    client_ssl.clear_system_cache()
    assert "CERTIFICATE_VERIFY_FAILED" in "".join(stack_trace)


# test get_version
def test_get_version(client: ClientAPI) -> None:
    client.reset()
    version = client.get_version()

    # assert version matches the pattern x.y.z
    import re

    assert re.match(r"\d+\.\d+\.\d+", version)


def test_reset(client: ClientAPI) -> None:
    client.reset()
    client.create_collection("testspace")
    client.create_collection("testspace2")

    # get collection does not throw an error
    collections = client.list_collections()
    assert len(collections) == 2

    client.reset()
    collections = client.list_collections()
    assert len(collections) == 0


def test_heartbeat(client: ClientAPI) -> None:
    heartbeat_ns = client.heartbeat()
    assert isinstance(heartbeat_ns, int)

    heartbeat_s = heartbeat_ns // 10**9
    heartbeat = datetime.fromtimestamp(heartbeat_s)
    assert heartbeat > datetime.now() - timedelta(seconds=10)


def test_pre_flight_checks(client: ClientAPI) -> None:
    if not isinstance(client, FastAPI):
        pytest.skip("Not a FastAPI instance")

    resp = httpx.get(f"{client._api_url}/pre-flight-checks")
    assert resp.status_code == 200
    assert resp.json() is not None
    assert "max_batch_size" in resp.json().keys()


def test_max_batch_size(client: ClientAPI) -> None:
    print(client)
    batch_size = client.get_max_batch_size()
    assert batch_size > 0
