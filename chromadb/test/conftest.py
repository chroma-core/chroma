import logging
import multiprocessing
import os
import shutil
import socket
import tempfile
import time
from typing import (
    Any,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Callable,
)

import hypothesis
import pytest
import uvicorn
from requests.exceptions import ConnectionError
from typing_extensions import Protocol

import chromadb.server.fastapi
from chromadb.api import ClientAPI, ServerAPI
from chromadb.config import Settings, System
from chromadb.db.mixins import embeddings_queue
from chromadb.ingest import Producer
from chromadb.types import SeqId, SubmitEmbeddingRecord
from chromadb.api.client import Client as ClientCreator

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


NOT_CLUSTER_ONLY = os.getenv("CHROMA_CLUSTER_TEST_ONLY") != "1"


def skip_if_not_cluster() -> pytest.MarkDecorator:
    return pytest.mark.skipif(
        NOT_CLUSTER_ONLY,
        reason="Requires Kubernetes to be running with a valid config",
    )


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
    chroma_server_auth_credentials_provider: Optional[str] = None,
    chroma_server_auth_credentials_file: Optional[str] = None,
    chroma_server_auth_credentials: Optional[str] = None,
    chroma_server_auth_token_transport_header: Optional[str] = None,
    chroma_server_authz_provider: Optional[str] = None,
    chroma_server_authz_config_file: Optional[str] = None,
    chroma_server_authz_config: Optional[Dict[str, Any]] = None,
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
            chroma_server_auth_credentials_provider=chroma_server_auth_credentials_provider,
            chroma_server_auth_credentials_file=chroma_server_auth_credentials_file,
            chroma_server_auth_credentials=chroma_server_auth_credentials,
            chroma_server_auth_token_transport_header=chroma_server_auth_token_transport_header,
            chroma_server_authz_provider=chroma_server_authz_provider,
            chroma_server_authz_config_file=chroma_server_authz_config_file,
            chroma_server_authz_config=chroma_server_authz_config,
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
            chroma_server_auth_credentials_provider=chroma_server_auth_credentials_provider,
            chroma_server_auth_credentials_file=chroma_server_auth_credentials_file,
            chroma_server_auth_credentials=chroma_server_auth_credentials,
            chroma_server_auth_token_transport_header=chroma_server_auth_token_transport_header,
            chroma_server_authz_provider=chroma_server_authz_provider,
            chroma_server_authz_config_file=chroma_server_authz_config_file,
            chroma_server_authz_config=chroma_server_authz_config,
        )
    server = chromadb.server.fastapi.FastAPI(settings)
    uvicorn.run(
        server.app(),
        host="0.0.0.0",
        port=port,
        log_level="error",
        timeout_keep_alive=30,
    )


def _await_server(api: ServerAPI, attempts: int = 0) -> None:
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
    chroma_server_auth_credentials_provider: Optional[str] = None,
    chroma_client_auth_provider: Optional[str] = None,
    chroma_server_auth_credentials_file: Optional[str] = None,
    chroma_client_auth_credentials: Optional[str] = None,
    chroma_server_auth_credentials: Optional[str] = None,
    chroma_client_auth_token_transport_header: Optional[str] = None,
    chroma_server_auth_token_transport_header: Optional[str] = None,
    chroma_server_authz_provider: Optional[str] = None,
    chroma_server_authz_config_file: Optional[str] = None,
    chroma_server_authz_config: Optional[Dict[str, Any]] = None,
) -> Generator[System, None, None]:
    """Fixture generator that launches a server in a separate process, and yields a
    fastapi client connect to it"""

    port = find_free_port()
    logger.info(f"Running test FastAPI server on port {port}")
    ctx = multiprocessing.get_context("spawn")
    args: Tuple[
        int,
        bool,
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[Dict[str, Any]],
    ] = (
        port,
        False,
        None,
        chroma_server_auth_provider,
        chroma_server_auth_credentials_provider,
        chroma_server_auth_credentials_file,
        chroma_server_auth_credentials,
        chroma_server_auth_token_transport_header,
        chroma_server_authz_provider,
        chroma_server_authz_config_file,
        chroma_server_authz_config,
    )
    persist_directory = None
    if is_persistent:
        persist_directory = tempfile.mkdtemp()
        args = (
            port,
            is_persistent,
            persist_directory,
            chroma_server_auth_provider,
            chroma_server_auth_credentials_provider,
            chroma_server_auth_credentials_file,
            chroma_server_auth_credentials,
            chroma_server_auth_token_transport_header,
            chroma_server_authz_provider,
            chroma_server_authz_config_file,
            chroma_server_authz_config,
        )
    proc = ctx.Process(target=_run_server, args=args, daemon=True)
    proc.start()
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=str(port),
        allow_reset=True,
        chroma_client_auth_provider=chroma_client_auth_provider,
        chroma_client_auth_credentials=chroma_client_auth_credentials,
        chroma_client_auth_token_transport_header=chroma_client_auth_token_transport_header,
    )
    system = System(settings)
    api = system.instance(ServerAPI)
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


def basic_http_client() -> Generator[System, None, None]:
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_http_port="8000",
        allow_reset=True,
    )
    system = System(settings)
    api = system.instance(ServerAPI)
    _await_server(api)
    system.start()
    yield system
    system.stop()


def fastapi_server_basic_auth() -> Generator[System, None, None]:
    server_auth_file = os.path.abspath(os.path.join(".", "server.htpasswd"))
    with open(server_auth_file, "w") as f:
        f.write("admin:$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS\n")
    for item in _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.auth.basic.BasicAuthServerProvider",
        chroma_server_auth_credentials_provider="chromadb.auth.providers.HtpasswdFileServerAuthCredentialsProvider",
        chroma_server_auth_credentials_file="./server.htpasswd",
        chroma_client_auth_provider="chromadb.auth.basic.BasicAuthClientProvider",
        chroma_client_auth_credentials="admin:admin",
    ):
        yield item
    os.remove(server_auth_file)


def fastapi_server_basic_auth_param() -> Generator[System, None, None]:
    server_auth_file = os.path.abspath(os.path.join(".", "server.htpasswd"))
    with open(server_auth_file, "w") as f:
        f.write("admin:$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS\n")
    for item in _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.auth.basic.BasicAuthServerProvider",
        chroma_server_auth_credentials_provider="chromadb.auth.providers.HtpasswdFileServerAuthCredentialsProvider",
        chroma_server_auth_credentials_file="./server.htpasswd",
        chroma_client_auth_provider="chromadb.auth.basic.BasicAuthClientProvider",
        chroma_client_auth_credentials="admin:admin",
    ):
        yield item
    os.remove(server_auth_file)


# TODO we need a generator for auth providers
def fastapi_server_basic_auth_file() -> Generator[System, None, None]:
    server_auth_file = os.path.abspath(os.path.join(".", "server.htpasswd"))
    with open(server_auth_file, "w") as f:
        f.write("admin:$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS\n")
    for item in _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.auth.basic.BasicAuthServerProvider",
        chroma_server_auth_credentials_provider="chromadb.auth.providers.HtpasswdFileServerAuthCredentialsProvider",
        chroma_server_auth_credentials_file="./server.htpasswd",
        chroma_client_auth_provider="chromadb.auth.basic.BasicAuthClientProvider",
        chroma_client_auth_credentials="admin:admin",
    ):
        yield item
    os.remove(server_auth_file)


def fastapi_server_basic_auth_shorthand() -> Generator[System, None, None]:
    server_auth_file = os.path.abspath(os.path.join(".", "server.htpasswd"))
    with open(server_auth_file, "w") as f:
        f.write("admin:$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS\n")
    for item in _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="basic",
        chroma_server_auth_credentials_provider="htpasswd_file",
        chroma_server_auth_credentials_file="./server.htpasswd",
        chroma_client_auth_provider="basic",
        chroma_client_auth_credentials="admin:admin",
    ):
        yield item
    os.remove(server_auth_file)


def fastapi_server_basic_auth_invalid_cred() -> Generator[System, None, None]:
    server_auth_file = os.path.abspath(os.path.join(".", "server.htpasswd"))
    with open(server_auth_file, "w") as f:
        f.write("admin:$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS\n")
    for item in _fastapi_fixture(
        is_persistent=False,
        chroma_server_auth_provider="chromadb.auth.basic.BasicAuthServerProvider",
        chroma_server_auth_credentials_provider="chromadb.auth.providers.HtpasswdFileServerAuthCredentialsProvider",
        chroma_server_auth_credentials_file="./server.htpasswd",
        chroma_client_auth_provider="chromadb.auth.basic.BasicAuthClientProvider",
        chroma_client_auth_credentials="admin:admin1",
    ):
        yield item
    os.remove(server_auth_file)


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
    fixtures = [fastapi, fastapi_persistent, sqlite, sqlite_persistent]
    if "CHROMA_INTEGRATION_TEST" in os.environ:
        fixtures.append(integration)
    if "CHROMA_INTEGRATION_TEST_ONLY" in os.environ:
        fixtures = [integration]
    if "CHROMA_CLUSTER_TEST_ONLY" in os.environ:
        fixtures = [basic_http_client]
    return fixtures


def system_fixtures_auth() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [
        fastapi_server_basic_auth_param,
        fastapi_server_basic_auth_file,
        fastapi_server_basic_auth_shorthand,
    ]
    return fixtures


def system_fixtures_wrong_auth() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [fastapi_server_basic_auth_invalid_cred]
    return fixtures


@pytest.fixture(scope="module", params=system_fixtures_wrong_auth())
def system_wrong_auth(
    request: pytest.FixtureRequest,
) -> Generator[ServerAPI, None, None]:
    yield next(request.param())


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: pytest.FixtureRequest) -> Generator[ServerAPI, None, None]:
    yield next(request.param())


@pytest.fixture(scope="module", params=system_fixtures_auth())
def system_auth(request: pytest.FixtureRequest) -> Generator[ServerAPI, None, None]:
    yield next(request.param())


@pytest.fixture(scope="function")
def api(system: System) -> Generator[ServerAPI, None, None]:
    system.reset_state()
    api = system.instance(ServerAPI)
    yield api


@pytest.fixture(scope="function")
def client(system: System) -> Generator[ClientAPI, None, None]:
    system.reset_state()
    client = ClientCreator.from_system(system)
    yield client
    client.clear_system_cache()


@pytest.fixture(scope="function")
def api_wrong_cred(
    system_wrong_auth: System,
) -> Generator[ServerAPI, None, None]:
    system_wrong_auth.reset_state()
    api = system_wrong_auth.instance(ServerAPI)
    yield api


@pytest.fixture(scope="function")
def api_with_server_auth(system_auth: System) -> Generator[ServerAPI, None, None]:
    _sys = system_auth
    _sys.reset_state()
    api = _sys.instance(ServerAPI)
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


def pytest_configure(config):  # type: ignore
    embeddings_queue._called_from_test = True
