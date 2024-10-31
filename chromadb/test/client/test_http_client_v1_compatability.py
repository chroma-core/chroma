import multiprocessing
from unittest.mock import patch

from multiprocessing.connection import Connection

from chromadb.config import System
from chromadb.test.conftest import _fastapi_fixture
from chromadb.api import ServerAPI
from chromadb.test.utils.cross_version import switch_to_version, install_version

VERSIONED_MODULES = ["pydantic", "numpy"]


def try_old_client(old_version: str, port: int, conn: Connection) -> None:
    try:
        old_module = switch_to_version(old_version, VERSIONED_MODULES)
        settings = old_module.Settings()
        settings.chroma_server_http_port = port
        with patch("chromadb.api.client.Client._validate_tenant_database"):
            api = old_module.HttpClient(settings=settings, port=port)

        # Try a few operations and ensure they work
        col = api.get_or_create_collection(name="test")
        col.add(
            ids=["1", "2", "3"],
            documents=["test document 1", "test document 2", "test document 3"],
        )
        col.get(ids=["1", "2", "3"])
    except Exception as e:
        conn.send(e)
        raise e


def test_http_client_bw_compatibility() -> None:
    # Start the v2 server
    api_fixture = _fastapi_fixture()
    sys: System = next(api_fixture)
    sys.reset_state()
    api = sys.instance(ServerAPI)
    api.heartbeat()
    port = sys.settings.chroma_server_http_port

    old_version = "0.5.11"  # Module with known v1 client
    install_version(old_version)

    ctx = multiprocessing.get_context("spawn")
    conn1, conn2 = multiprocessing.Pipe()
    p = ctx.Process(
        target=try_old_client,
        args=(old_version, port, conn2),
    )
    p.start()
    p.join()

    if conn1.poll():
        e = conn1.recv()
        raise e

    p.close()
