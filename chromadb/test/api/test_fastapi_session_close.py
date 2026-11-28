# Test that the sync FastAPI component closes its httpx session on stop.
#
# Regression test for the connection leak in #8012: the FastAPI component
# created an httpx.Client in __init__ but had no stop() override, so
# System.stop() never closed it and its pooled keep-alive connections
# accumulated until the open-file limit was hit.

import httpx
import pytest

from chromadb.api.fastapi import FastAPI
from chromadb.config import System, Settings


@pytest.fixture
def fastapi_system() -> System:
    system = System(
        Settings(
            chroma_api_impl="chromadb.api.fastapi.FastAPI",
            chroma_server_host="localhost",
            chroma_server_http_port=8000,
            anonymized_telemetry=False,
            allow_reset=True,
        )
    )
    system.start()
    yield system
    system.stop()


def test_fastapi_stop_closes_the_http_session(fastapi_system: System) -> None:
    fastapi = fastapi_system.instance(FastAPI)
    session = fastapi._session
    assert isinstance(session, httpx.Client)
    assert not session.is_closed

    fastapi.stop()

    assert session.is_closed


def test_system_stop_closes_the_http_session(fastapi_system: System) -> None:
    fastapi = fastapi_system.instance(FastAPI)
    session = fastapi._session
    assert not session.is_closed

    # System.stop() stops every component in reverse dependency order; the
    # session must be closed by the time it returns (issue #8012: it stayed
    # open, leaking one pooled connection set per client).
    system = fastapi_system
    system.stop()

    assert session.is_closed
