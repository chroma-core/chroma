from chromadb.config import Settings
from chromadb import Client
from chromadb.api import API
import chromadb.server.fastapi
from requests.exceptions import ConnectionError
import hypothesis
import tempfile
import os
import uvicorn
import time
from multiprocessing import Process
import pytest
from typing import Generator, List, Callable
import shutil

hypothesis.settings.register_profile(
    "dev",
    deadline=30000,
    suppress_health_check=[
        hypothesis.HealthCheck.data_too_large,
        hypothesis.HealthCheck.large_base_example,
    ],
)
hypothesis.settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "dev"))


def _run_server() -> None:
    """Run a Chroma server locally"""
    settings = Settings(
        chroma_api_impl="local",
        chroma_db_impl="duckdb",
        persist_directory=tempfile.gettempdir() + "/test_server",
    )
    server = chromadb.server.fastapi.FastAPI(settings)
    uvicorn.run(server.app(), host="0.0.0.0", port=6666, log_level="error")


def _await_server(api: API, attempts: int = 0) -> None:
    try:
        api.heartbeat()
    except ConnectionError as e:
        if attempts > 10:
            raise e
        else:
            time.sleep(2)
            _await_server(api, attempts + 1)


def fastapi() -> Generator[API, None, None]:
    """Fixture generator that launches a server in a separate process, and yields a
    fastapi client connect to it"""
    proc = Process(target=_run_server, args=(), daemon=True)
    proc.start()
    api = chromadb.Client(
        Settings(
            chroma_api_impl="rest",
            chroma_server_host="localhost",
            chroma_server_http_port="6666",
        )
    )
    _await_server(api)
    yield api
    proc.kill()


def duckdb() -> Generator[API, None, None]:
    """Fixture generator for duckdb"""
    yield Client(
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb",
            persist_directory=tempfile.gettempdir(),
        )
    )


def duckdb_parquet() -> Generator[API, None, None]:
    """Fixture generator for duckdb+parquet"""

    save_path = tempfile.gettempdir() + "/tests"
    yield Client(
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb+parquet",
            persist_directory=save_path,
        )
    )
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
