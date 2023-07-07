from chromadb.config import Settings, System
from chromadb.api import API
import chromadb.server.fastapi
from requests.exceptions import ConnectionError
import hypothesis
import tempfile
import os
import uvicorn
import time
import pytest
from typing import Generator, List, Callable
import shutil
import logging
import socket
import multiprocessing

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # This will only run when testing


logger = logging.getLogger(__name__)

hypothesis.settings.register_profile(
    "dev",
    deadline=30000,
    suppress_health_check=[
        hypothesis.HealthCheck.data_too_large,
        hypothesis.HealthCheck.large_base_example,
        hypothesis.HealthCheck.function_scoped_fixture,
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
    settings = Settings(
        chroma_api_impl="local",
        chroma_db_impl="duckdb",
        persist_directory=tempfile.gettempdir() + "/test_server",
    )
    server = chromadb.server.fastapi.FastAPI(settings)
    uvicorn.run(server.app(), host="0.0.0.0", port=port, log_level="error")


def _await_server(api: API, attempts: int = 0) -> None:
    try:
        api.heartbeat()
    except ConnectionError as e:
        if attempts > 15:
            logger.error("Test server failed to start after 15 attempts")
            raise e
        else:
            logger.info("Waiting for server to start...")
            time.sleep(4)
            _await_server(api, attempts + 1)


def fastapi() -> Generator[System, None, None]:
    """Fixture generator that launches a server in a separate process, and yields a
    fastapi client connect to it"""
    port = find_free_port()
    logger.info(f"Running test FastAPI server on port {port}")
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(target=_run_server, args=(port,), daemon=True)
    proc.start()
    settings = Settings(
        chroma_api_impl="rest",
        chroma_server_host="localhost",
        chroma_server_http_port=str(port),
        allow_reset=True,
    )
    system = System(settings)
    api = system.instance(API)
    system.start()
    _await_server(api)
    yield system
    system.stop()
    proc.kill()


def duckdb() -> Generator[System, None, None]:
    """Fixture generator for duckdb"""
    settings = Settings(
        chroma_api_impl="local",
        chroma_db_impl="duckdb",
        persist_directory=tempfile.gettempdir(),
        allow_reset=True,
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()


def duckdb_parquet() -> Generator[System, None, None]:
    """Fixture generator for duckdb+parquet"""

    save_path = tempfile.gettempdir() + "/tests"
    settings = Settings(
        chroma_api_impl="local",
        chroma_db_impl="duckdb+parquet",
        persist_directory=save_path,
        allow_reset=True,
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def integration() -> Generator[System, None, None]:
    """Fixture generator for returning a client configured via environmenet
    variables, intended for externally configured integration tests
    """
    settings = Settings(allow_reset=True)
    system = System(settings)
    system.start()
    yield system
    system.stop()


def sqlite() -> Generator[System, None, None]:
    """Fixture generator for segment-based API using in-memory Sqlite"""
    settings = Settings(
        chroma_api_impl="chromadb.api.segment.SegmentAPI",
        chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
        sqlite_database=":memory:",
        allow_reset=True,
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()


def system_fixtures() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [duckdb, duckdb_parquet, fastapi, sqlite]
    if "CHROMA_INTEGRATION_TEST" in os.environ:
        fixtures.append(integration)
    if "CHROMA_INTEGRATION_TEST_ONLY" in os.environ:
        fixtures = [integration]
    return fixtures


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: pytest.FixtureRequest) -> Generator[API, None, None]:
    yield next(request.param())


@pytest.fixture(scope="function")
def api(system: System) -> Generator[API, None, None]:
    system.reset_state()
    api = system.instance(API)
    yield api
