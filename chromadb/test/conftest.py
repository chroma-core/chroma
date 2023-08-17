from chromadb.config import Settings, System
from chromadb.api import API
from chromadb.ingest import Producer
import chromadb.server.fastapi
from requests.exceptions import ConnectionError
import hypothesis
import tempfile
import os
import uvicorn
import time
import pytest
from typing import (
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Callable,
    Union,
    Dict,
    Any,
)
from typing_extensions import Protocol
import shutil
import logging
import socket
import multiprocessing

from chromadb.types import SeqId, SubmitEmbeddingRecord

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # This will only run when testing

logger = logging.getLogger(__name__)

hypothesis.settings.register_profile(
    "dev",
    deadline=45000,
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


def _run_server(
    port: int,
    is_persistent: bool = False,
    persist_directory: Optional[str] = None,
    chroma_server_auth_provider: Optional[str] = None,
    chroma_server_auth_provider_config: Optional[Union[str, Dict[str, Any]]] = None,
) -> None:
    """Run a Chroma server locally"""
    if is_persistent and persist_directory:
        settings = Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            is_persistent=is_persistent,
            persist_directory=persist_directory,
            allow_reset=True,
            chroma_server_auth_provider=chroma_server_auth_provider,
            chroma_server_auth_provider_config=chroma_server_auth_provider_config,
        )
    else:
        settings = Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            is_persistent=False,
            allow_reset=True,
            chroma_server_auth_provider=chroma_server_auth_provider,
            chroma_server_auth_provider_config=chroma_server_auth_provider_config,
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


def _fastapi_fixture(
    is_persistent: bool = False,
    chroma_server_auth_provider: Optional[str] = None,
    chroma_server_auth_provider_config: Optional[Union[str, Dict[str, Any]]] = None,
    chroma_client_auth_provider: Optional[str] = None,
    chroma_client_auth_provider_config: Optional[Union[str, Dict[str, Any]]] = None,
) -> Generator[System, None, None]:
    """Fixture generator that launches a server in a separate process, and yields a
    fastapi client connect to it"""

    port = find_free_port()
    logger.info(f"Running test FastAPI server on port {port}")
    ctx = multiprocessing.get_context("spawn")
    args: Tuple[
        int, bool, Optional[str], Optional[str], Optional[Union[str, Dict[str, Any]]]
    ] = (
        port,
        False,
        None,
        chroma_server_auth_provider,
        chroma_server_auth_provider_config,
    )
    persist_directory = None
    if is_persistent:
        persist_directory = tempfile.mkdtemp()
        args = (
            port,
            is_persistent,
            persist_directory,
            chroma_server_auth_provider,
            chroma_server_auth_provider_config,
        )
    proc = ctx.Process(target=_run_server, args=args, daemon=True)
    proc.start()
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=str(port),
        allow_reset=True,
        chroma_client_auth_provider=chroma_client_auth_provider,
        chroma_client_auth_provider_config=chroma_client_auth_provider_config,
    )
    system = System(settings)
    api = system.instance(API)
    system.start()
    _await_server(api)
    yield system
    system.stop()
    proc.kill()
    if is_persistent and persist_directory is not None:
        if os.path.exists(persist_directory):
            shutil.rmtree(persist_directory)


def fastapi() -> Generator[System, None, None]:
    return _fastapi_fixture(is_persistent=False)


def fastapi_persistent() -> Generator[System, None, None]:
    return _fastapi_fixture(is_persistent=True)


def fastapi_server_auth() -> Generator[System, None, None]:
    return _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.config.BasicAuthServerProvider",
        chroma_server_auth_provider_config={"username": "admin", "password": "admin"},
        chroma_client_auth_provider="chromadb.config.BasicAuthClientProvider",
        chroma_client_auth_provider_config={"username": "admin", "password": "admin"},
    )


def fastapi_server_auth_param() -> Generator[System, None, None]:
    for item in _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.config.BasicAuthServerProvider",
        chroma_server_auth_provider_config={"username": "admin", "password": "admin"},
        chroma_client_auth_provider="chromadb.config.BasicAuthClientProvider",
        chroma_client_auth_provider_config={"username": "admin", "password": "admin"},
    ):
        yield item


def fastapi_server_auth_file() -> Generator[System, None, None]:
    server_auth_file = os.path.abspath(os.path.join(".", "server-auth"))
    client_auth_file = os.path.abspath(os.path.join(".", "client-auth"))
    with open(server_auth_file, "w") as f:
        f.write("admin:admin")
    with open(client_auth_file, "w") as f:
        f.write("admin:admin")
    for item in _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.config.BasicAuthServerProvider",
        chroma_server_auth_provider_config=server_auth_file,
        chroma_client_auth_provider="chromadb.config.BasicAuthClientProvider",
        chroma_client_auth_provider_config=client_auth_file,
    ):
        yield item
    os.remove(server_auth_file)
    os.remove(client_auth_file)


def fastapi_server_auth_env() -> Generator[System, None, None]:
    os.environ["CHROMA_SERVER_AUTH_BASIC_USERNAME"] = "admin"
    os.environ["CHROMA_SERVER_AUTH_BASIC_PASSWORD"] = "admin"
    os.environ["CHROMA_CLIENT_AUTH_BASIC_USERNAME"] = "admin"
    os.environ["CHROMA_CLIENT_AUTH_BASIC_PASSWORD"] = "admin"
    for item in _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.config.BasicAuthServerProvider",
        chroma_client_auth_provider="chromadb.config.BasicAuthClientProvider",
    ):
        yield item


@pytest.fixture(scope="function")
def fastapi_server_auth_invalid_cred() -> Generator[System, None, None]:
    return _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.config.BasicAuthServerProvider",
        chroma_server_auth_provider_config={"username": "admin", "password": "admin"},
        chroma_client_auth_provider="chromadb.config.BasicAuthClientProvider",
        chroma_client_auth_provider_config={"username": "admin", "password": "wrong"},
    )


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
        is_persistent=False,
        allow_reset=True,
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()


def sqlite_persistent() -> Generator[System, None, None]:
    """Fixture generator for segment-based API using persistent Sqlite"""
    save_path = tempfile.mkdtemp()
    settings = Settings(
        chroma_api_impl="chromadb.api.segment.SegmentAPI",
        chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
        allow_reset=True,
        is_persistent=True,
        persist_directory=save_path,
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def system_fixtures() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [
        fastapi,
        fastapi_persistent,
        sqlite,
        sqlite_persistent,
        fastapi_server_auth,
    ]
    if "CHROMA_INTEGRATION_TEST" in os.environ:
        fixtures.append(integration)
    if "CHROMA_INTEGRATION_TEST_ONLY" in os.environ:
        fixtures = [integration]
    return fixtures


def system_fixtures_auth() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [
        fastapi_server_auth_param,
        fastapi_server_auth_file,
        fastapi_server_auth_env,
    ]
    return fixtures


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: pytest.FixtureRequest) -> Generator[API, None, None]:
    yield next(request.param())


@pytest.fixture(scope="module", params=system_fixtures_auth())
def system_auth(request: pytest.FixtureRequest) -> Generator[API, None, None]:
    yield next(request.param())


@pytest.fixture(scope="function")
def api(system: System) -> Generator[API, None, None]:
    system.reset_state()
    api = system.instance(API)
    yield api


@pytest.fixture(scope="function")
def api_wrong_cred(
    fastapi_server_auth_invalid_cred: Generator[System, None, None]
) -> Generator[API, None, None]:
    api = next(fastapi_server_auth_invalid_cred).instance(API)
    yield api


@pytest.fixture(scope="function")
def api_with_server_auth(system_auth: System) -> Generator[API, None, None]:
    _sys = system_auth
    _sys.reset_state()
    api = _sys.instance(API)
    yield api


# Producer / Consumer fixtures #


class ProducerFn(Protocol):
    def __call__(
        self,
        producer: Producer,
        topic: str,
        embeddings: Iterator[SubmitEmbeddingRecord],
        n: int,
    ) -> Tuple[Sequence[SubmitEmbeddingRecord], Sequence[SeqId]]:
        ...


def produce_n_single(
    producer: Producer,
    topic: str,
    embeddings: Iterator[SubmitEmbeddingRecord],
    n: int,
) -> Tuple[Sequence[SubmitEmbeddingRecord], Sequence[SeqId]]:
    submitted_embeddings = []
    seq_ids = []
    for _ in range(n):
        e = next(embeddings)
        seq_id = producer.submit_embedding(topic, e)
        submitted_embeddings.append(e)
        seq_ids.append(seq_id)
    return submitted_embeddings, seq_ids


def produce_n_batch(
    producer: Producer,
    topic: str,
    embeddings: Iterator[SubmitEmbeddingRecord],
    n: int,
) -> Tuple[Sequence[SubmitEmbeddingRecord], Sequence[SeqId]]:
    submitted_embeddings = []
    seq_ids: Sequence[SeqId] = []
    for _ in range(n):
        e = next(embeddings)
        submitted_embeddings.append(e)
    seq_ids = producer.submit_embeddings(topic, submitted_embeddings)
    return submitted_embeddings, seq_ids


def produce_fn_fixtures() -> List[ProducerFn]:
    return [produce_n_single, produce_n_batch]


@pytest.fixture(scope="module", params=produce_fn_fixtures())
def produce_fns(
    request: pytest.FixtureRequest,
) -> Generator[ProducerFn, None, None]:
    yield request.param
