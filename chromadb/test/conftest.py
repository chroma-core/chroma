from chromadb.config import Settings
from chromadb import Client
from chromadb.api import API
import chromadb.server.fastapi

# from requests.exceptions import ConnectionError
import hypothesis
import tempfile
import os
import uvicorn
import time
from multiprocessing import Process
import pytest
from typing import Generator, List, Callable
import shutil
import logging
import sys
import random
import socket

logger = logging.getLogger(__name__)

hypothesis.settings.register_profile(
    "dev",
    deadline=30000,
    suppress_health_check=[
        hypothesis.HealthCheck.data_too_large,
        hypothesis.HealthCheck.large_base_example,
    ],
)
hypothesis.settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "dev"))


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]  # type: ignore


def _run_server(port: int) -> None:
    """Run a Chroma server locally"""
    sys.stdin = open(0)
    # sys.stdout = open(str(os.getpid()) + ".out", "a")
    # sys.stderr = open(str(os.getpid()) + "_error.out", "a")
    persist_directory = (
        tempfile.gettempdir() + "/test_server" + str(random.randint(0, 100000))
    )
    settings = Settings(
        chroma_api_impl="local",
        chroma_db_impl="duckdb",
        persist_directory=persist_directory,
    )
    server = chromadb.server.fastapi.FastAPI(settings)
    uvicorn.run(server.app(), host="0.0.0.0", port=port, log_level="info")


def _await_server(api: API, attempts: int = 0) -> None:
    try:
        api.heartbeat()
    except Exception as e:
        if attempts > 15:
            logger.error("Test server failed to start after 15 attempts")
            raise e
        else:
            logger.info("Waiting for server to start...")
            time.sleep(4)
            _await_server(api, attempts + 1)


def fastapi() -> Generator[API, None, None]:
    """Fixture generator that launches a server in a separate process, and yields a
    fastapi client connect to it"""
    port = find_free_port()
    print("STARTING A SERVER")
    logger.info(f"Running test FastAPI server on port {port}")
    proc = Process(target=_run_server, args=(port,), daemon=True)
    proc.start()
    api = chromadb.Client(
        Settings(
            chroma_api_impl="rest",
            chroma_server_host="localhost",
            chroma_server_http_port=str(port),
        )
    )
    _await_server(api)
    yield api
    proc.kill()


def duckdb() -> Generator[API, None, None]:
    """Fixture generator for duckdb"""
    client = Client(
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb",
            persist_directory=tempfile.gettempdir() + "/test_memory",
        )
    )
    yield client


def duckdb_parquet() -> Generator[API, None, None]:
    """Fixture generator for duckdb+parquet"""

    save_path = tempfile.gettempdir() + "/test_persist"
    client = Client(
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb+parquet",
            persist_directory=save_path,
        )
    )
    yield client
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def integration_api() -> Generator[API, None, None]:
    """Fixture generator for returning a client configured via environmenet
    variables, intended for externally configured integration tests
    """
    yield chromadb.Client()


def fixtures() -> List[Callable[[], Generator[API, None, None]]]:
    api_fixtures = [duckdb, duckdb_parquet, fastapi]
    if "CHROMA_INTEGRATION_TEST" in os.environ:
        api_fixtures.append(integration_api)
    if "CHROMA_INTEGRATION_TEST_ONLY" in os.environ:
        api_fixtures = [integration_api]
    return api_fixtures


@pytest.fixture(scope="module", params=fixtures())
def api(request: pytest.FixtureRequest) -> Generator[API, None, None]:
    yield next(request.param())
