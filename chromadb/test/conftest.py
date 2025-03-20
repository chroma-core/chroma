import multiprocessing
import os
import socket
import subprocess
import tempfile
import time
from typing import (
    Any,
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Callable,
    cast,
)
from uuid import UUID
import yaml

import hypothesis
import pytest
from httpx import ConnectError
from typing_extensions import Protocol

from chromadb.api.async_fastapi import AsyncFastAPI
from chromadb.api.fastapi import FastAPI
from chromadb.api import ClientAPI, ServerAPI, BaseAPI
from chromadb.config import Settings, System
from chromadb.db.mixins import embeddings_queue
from chromadb.ingest import Producer
from chromadb.types import SeqId, OperationRecord
from chromadb.api.client import Client as ClientCreator, AdminClient
from chromadb.api.async_client import (
    AsyncAdminClient,
    AsyncClient as AsyncClientCreator,
)
from chromadb.utils.async_to_sync import async_class_to_sync
import logging
import sys
import numpy as np
from unittest.mock import MagicMock
from pytest import MonkeyPatch
from chromadb.api.types import Documents, Embeddings
import chromadb_rust_bindings

logger = logging.getLogger(__name__)

VALID_PRESETS = ["fast", "normal", "slow"]
CURRENT_PRESET = os.getenv("PROPERTY_TESTING_PRESET", "fast")

if CURRENT_PRESET not in VALID_PRESETS:
    raise ValueError(
        f"Invalid property testing preset: {CURRENT_PRESET}. Must be one of {VALID_PRESETS}."
    )

hypothesis.settings.register_profile(
    "base",
    deadline=45000,
    suppress_health_check=[
        hypothesis.HealthCheck.data_too_large,
        hypothesis.HealthCheck.large_base_example,
        hypothesis.HealthCheck.function_scoped_fixture,
    ],
)

hypothesis.settings.register_profile(
    "fast", hypothesis.settings.get_profile("base"), max_examples=50
)
# Hypothesis's default max_examples is 100
hypothesis.settings.register_profile(
    "normal", hypothesis.settings.get_profile("base"), max_examples=100
)
hypothesis.settings.register_profile(
    "slow",
    hypothesis.settings.get_profile("base"),
    max_examples=500,
    stateful_step_count=100,
)

hypothesis.settings.load_profile(CURRENT_PRESET)


def reset(api: BaseAPI) -> None:
    api.reset()


def override_hypothesis_profile(
    fast: Optional[hypothesis.settings] = None,
    normal: Optional[hypothesis.settings] = None,
    slow: Optional[hypothesis.settings] = None,
) -> Optional[hypothesis.settings]:
    """Override Hypothesis settings for specific profiles.

    For example, to override max_examples only when the current profile is 'fast':

    override_hypothesis_profile(
        fast=hypothesis.settings(max_examples=50),
    )

    Settings will be merged with the default/active profile.
    """

    allowable_override_keys = [
        "deadline",
        "max_examples",
        "stateful_step_count",
        "suppress_health_check",
    ]

    override_profiles = {
        "fast": fast,
        "normal": normal,
        "slow": slow,
    }

    overriding_profile = override_profiles.get(CURRENT_PRESET)

    if overriding_profile is not None:
        overridden_settings = {
            key: value
            for key, value in overriding_profile.__dict__.items()
            if key in allowable_override_keys
        }

        return hypothesis.settings(hypothesis.settings.default, **overridden_settings)

    return cast(hypothesis.settings, hypothesis.settings.default)


NOT_CLUSTER_ONLY = os.getenv("CHROMA_CLUSTER_TEST_ONLY") != "1"
COMPACTION_SLEEP = 120


def skip_if_not_cluster() -> pytest.MarkDecorator:
    return pytest.mark.skipif(
        NOT_CLUSTER_ONLY,
        reason="Requires Kubernetes to be running with a valid config",
    )


def generate_self_signed_certificate() -> None:
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "openssl.cnf"
    )
    print(f"Config path: {config_path}")  # Debug print to verify path
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:4096",
            "-keyout",
            "serverkey.pem",
            "-out",
            "servercert.pem",
            "-days",
            "365",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-config",
            config_path,
        ]
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
) -> None:
    """Run a Chroma server locally"""
    config = {
        "port": port,
        "allow_reset": True,
        "open_telemetry": {
            "service_name": "chroma",
            "endpoint": "http://otel-collector:4317",
        },
    }

    if is_persistent and persist_directory:
        config["persist_path"] = persist_directory

    tmp_config_dir = tempfile.mkdtemp()
    tmp_config_path = os.path.join(tmp_config_dir, "config.yaml")
    with open(tmp_config_path, "w") as f:
        f.write(yaml.dump(config))

    # TODO(@codetheweb): this should use a method from the Rust bindings to start the server and wait for it to be ready instead of going through the CLI. That would let us avoid the sleep below.
    chromadb_rust_bindings.cli(["chroma", "run", tmp_config_path])


def _await_server(api: ServerAPI, attempts: int = 0) -> None:
    try:
        api.heartbeat()
    except ConnectError as e:
        if attempts > 15:
            raise e
        else:
            time.sleep(4)
            _await_server(api, attempts + 1)


def http_server_fixture(
    is_persistent: bool = False,
    chroma_api_impl: str = "chromadb.api.fastapi.FastAPI",
    chroma_client_auth_provider: Optional[str] = None,
    chroma_client_auth_credentials: Optional[str] = None,
    chroma_auth_token_transport_header: Optional[str] = None,
    chroma_server_ssl_certfile: Optional[str] = None,
    chroma_overwrite_singleton_tenant_database_access_from_auth: Optional[bool] = False,
) -> Generator[System, None, None]:
    port = find_free_port()
    ctx = multiprocessing.get_context("spawn")
    args: Tuple[
        int,
        bool,
        Optional[str],
    ] = (
        port,
        False,
        None,
    )

    def run(args: Any) -> Generator[System, None, None]:
        proc = ctx.Process(target=_run_server, args=args, daemon=True)
        proc.start()
        settings = Settings(
            chroma_api_impl=chroma_api_impl,
            chroma_server_host="localhost",
            chroma_server_http_port=port,
            allow_reset=True,
            chroma_client_auth_provider=chroma_client_auth_provider,
            chroma_client_auth_credentials=chroma_client_auth_credentials,
            chroma_auth_token_transport_header=chroma_auth_token_transport_header,
            chroma_server_ssl_verify=chroma_server_ssl_certfile,
            chroma_server_ssl_enabled=True if chroma_server_ssl_certfile else False,
            chroma_overwrite_singleton_tenant_database_access_from_auth=chroma_overwrite_singleton_tenant_database_access_from_auth,
        )
        system = System(settings)
        api = system.instance(ServerAPI)
        system.start()
        _await_server(api if isinstance(api, FastAPI) else async_class_to_sync(api))
        yield system
        system.stop()
        proc.kill()
        proc.join()

    if is_persistent:
        persist_directory = tempfile.TemporaryDirectory()
        args = (
            port,
            is_persistent,
            persist_directory.name,
        )

        yield from run(args)

        try:
            persist_directory.cleanup()

        # (Older versions of Python throw NotADirectoryError sometimes instead of PermissionError)
        # (when we drop support for Python < 3.10, we should use ignore_cleanup_errors=True with the context manager instead)
        except (PermissionError, NotADirectoryError) as e:
            # todo: what's holding onto directory contents on Windows?
            if os.name == "nt":
                pass
            else:
                raise e

    else:
        yield from run(args)


@pytest.fixture
def http_server() -> Generator[System, None, None]:
    yield from http_server_fixture(is_persistent=False)


def start_http_server_and_get_client() -> Generator[System, None, None]:
    return http_server_fixture(is_persistent=False)


def start_persistent_http_server_and_get_client() -> Generator[System, None, None]:
    return http_server_fixture(is_persistent=True)


def start_http_server_and_get_async_client() -> Generator[System, None, None]:
    return http_server_fixture(
        is_persistent=False,
        chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
    )


def basic_http_client() -> Generator[System, None, None]:
    port = 8000
    host = "localhost"

    if os.getenv("CHROMA_SERVER_HOST"):
        host = os.getenv("CHROMA_SERVER_HOST", "").split(":")[0]
        port = int(os.getenv("CHROMA_SERVER_HOST", "").split(":")[1])

    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_http_port=port,
        chroma_server_host=host,
        allow_reset=True,
    )
    system = System(settings)
    api = system.instance(ServerAPI)
    _await_server(api)
    system.start()
    yield system
    system.stop()


def integration() -> Generator[System, None, None]:
    """Fixture generator for returning a client configured via environmenet
    variables, intended for externally configured integration tests
    """
    settings = Settings(allow_reset=True)
    system = System(settings)
    system.start()
    yield system
    system.stop()


def rust_ephemeral_fixture() -> Generator[System, None, None]:
    """Fixture generator for system using ephemeral Rust bindings"""
    settings = Settings(
        chroma_api_impl="chromadb.api.rust.RustBindingsAPI",
        chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
        is_persistent=False,
        allow_reset=True,
        persist_directory="",
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()


def rust_persistent_fixture() -> Generator[System, None, None]:
    """Fixture generator for system using Rust bindings"""
    save_path = tempfile.TemporaryDirectory()
    settings = Settings(
        chroma_api_impl="chromadb.api.rust.RustBindingsAPI",
        chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
        is_persistent=True,
        allow_reset=True,
        persist_directory=save_path.name,
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()


@pytest.fixture
def sqlite() -> Generator[System, None, None]:
    yield from rust_ephemeral_fixture()


@pytest.fixture
def sqlite_persistent() -> Generator[System, None, None]:
    yield from rust_persistent_fixture()


def system_fixtures() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [
        start_http_server_and_get_client,
        start_http_server_and_get_async_client,
        start_persistent_http_server_and_get_client,
    ]
    if "CHROMA_INTEGRATION_TEST" in os.environ:
        fixtures.append(integration)
    if "CHROMA_INTEGRATION_TEST_ONLY" in os.environ:
        fixtures = [integration]
    if "CHROMA_CLUSTER_TEST_ONLY" in os.environ:
        fixtures = [basic_http_client]
    if "CHROMA_RUST_BINDINGS_TEST_ONLY" in os.environ:
        fixtures = [rust_ephemeral_fixture, rust_persistent_fixture]
    return fixtures


def system_http_server_fixtures() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [
        fixture
        for fixture in system_fixtures()
        if fixture
        not in [
            rust_ephemeral_fixture,
            rust_persistent_fixture,
        ]
    ]
    return fixtures


@pytest.fixture(scope="module", params=system_http_server_fixtures())
def system_http_server(
    request: pytest.FixtureRequest,
) -> Generator[ServerAPI, None, None]:
    yield from request.param()


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: pytest.FixtureRequest) -> Generator[ServerAPI, None, None]:
    yield from request.param()


@async_class_to_sync
class AsyncClientCreatorSync(AsyncClientCreator):
    pass


@async_class_to_sync
class AsyncAdminClientSync(AsyncAdminClient):
    pass


@pytest.fixture(scope="function")
def api(system: System) -> Generator[ServerAPI, None, None]:
    system.reset_state()
    api = system.instance(ServerAPI)

    if isinstance(api, AsyncFastAPI):
        transformed = async_class_to_sync(api)
        yield transformed
    else:
        yield api


class ClientFactories:
    """This allows consuming tests to be parameterized by async/sync versions of the client and papers over the async implementation.
    If you don't need to manually construct clients, use the `client` fixture instead.
    """

    _system: System
    # Need to track created clients so we can call .clear_system_cache() during teardown
    _created_clients: List[ClientAPI] = []

    def __init__(self, system: System):
        self._system = system

    def create_client(self, *args: Any, **kwargs: Any) -> ClientCreator:
        if kwargs.get("settings") is None:
            kwargs["settings"] = self._system.settings

        if (
            self._system.settings.chroma_api_impl
            == "chromadb.api.async_fastapi.AsyncFastAPI"
        ):
            client = cast(ClientCreator, AsyncClientCreatorSync.create(*args, **kwargs))
            self._created_clients.append(client)
            return client

        client = ClientCreator(*args, **kwargs)
        self._created_clients.append(client)
        return client

    def create_client_from_system(self) -> ClientCreator:
        if (
            self._system.settings.chroma_api_impl
            == "chromadb.api.async_fastapi.AsyncFastAPI"
        ):
            client = cast(
                ClientCreator, AsyncClientCreatorSync.from_system_async(self._system)
            )
            self._created_clients.append(client)
            return client

        client = ClientCreator.from_system(self._system)
        self._created_clients.append(client)
        return client

    def create_admin_client(self, *args: Any, **kwargs: Any) -> AdminClient:
        if (
            self._system.settings.chroma_api_impl
            == "chromadb.api.async_fastapi.AsyncFastAPI"
        ):
            return cast(AdminClient, AsyncAdminClientSync(*args, **kwargs))

        return AdminClient(*args, **kwargs)

    def create_admin_client_from_system(self) -> AdminClient:
        if (
            self._system.settings.chroma_api_impl
            == "chromadb.api.async_fastapi.AsyncFastAPI"
        ):
            return cast(AdminClient, AsyncAdminClientSync.from_system(self._system))

        return AdminClient.from_system(self._system)


@pytest.fixture(scope="function")
def client_factories(system: System) -> Generator[ClientFactories, None, None]:
    system.reset_state()

    factories = ClientFactories(system)
    yield factories

    while len(factories._created_clients) > 0:
        client = factories._created_clients.pop()
        client.clear_system_cache()
        del client


@pytest.fixture(scope="function")
def client(system: System) -> Generator[ClientAPI, None, None]:
    system.reset_state()

    if system.settings.chroma_api_impl == "chromadb.api.async_fastapi.AsyncFastAPI":
        client = cast(Any, AsyncClientCreatorSync.from_system_async(system))
        yield client
        client.clear_system_cache()
    else:
        client = ClientCreator.from_system(system)
        yield client
        client.clear_system_cache()


@pytest.fixture(scope="function")
def http_client(system_http_server: System) -> Generator[ClientAPI, None, None]:
    system_http_server.reset_state()

    if (
        system_http_server.settings.chroma_api_impl
        == "chromadb.api.async_fastapi.AsyncFastAPI"
    ):
        client = cast(Any, AsyncClientCreatorSync.from_system_async(system_http_server))
        yield client
        client.clear_system_cache()
    else:
        client = ClientCreator.from_system(system_http_server)
        yield client
        client.clear_system_cache()


@pytest.fixture(scope="function")
def client_ssl(system_ssl: System) -> Generator[ClientAPI, None, None]:
    system_ssl.reset_state()
    client = ClientCreator.from_system(system_ssl)
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
def api_with_authn_rbac_authz(
    system_authn_rbac_authz: System,
) -> Generator[ServerAPI, None, None]:
    system_authn_rbac_authz.reset_state()
    api = system_authn_rbac_authz.instance(ServerAPI)
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
        collection_id: UUID,
        embeddings: Iterator[OperationRecord],
        n: int,
    ) -> Tuple[Sequence[OperationRecord], Sequence[SeqId]]:
        ...


def produce_n_single(
    producer: Producer,
    collection_id: UUID,
    embeddings: Iterator[OperationRecord],
    n: int,
) -> Tuple[Sequence[OperationRecord], Sequence[SeqId]]:
    submitted_embeddings = []
    seq_ids = []
    for _ in range(n):
        e = next(embeddings)
        seq_id = producer.submit_embedding(collection_id, e)
        submitted_embeddings.append(e)
        seq_ids.append(seq_id)
    return submitted_embeddings, seq_ids


def produce_n_batch(
    producer: Producer,
    collection_id: UUID,
    embeddings: Iterator[OperationRecord],
    n: int,
) -> Tuple[Sequence[OperationRecord], Sequence[SeqId]]:
    submitted_embeddings = []
    seq_ids: Sequence[SeqId] = []
    for _ in range(n):
        e = next(embeddings)
        submitted_embeddings.append(e)
    seq_ids = producer.submit_embeddings(collection_id, submitted_embeddings)
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


def is_client_in_process(client: ClientAPI) -> bool:
    """Returns True if the client is in-process (a SQLite client), False if it's out-of-process (a HTTP client)."""
    return client.get_settings().chroma_server_http_port is None


@pytest.fixture(autouse=True)
def log_tests(request: pytest.FixtureRequest) -> Generator[None, None, None]:
    """Automatically logs the start and end of each test."""
    test_name = request.node.name
    logger.debug(f"Starting test: {test_name}")

    # Yield control back to the test, allowing it to execute
    yield

    logger.debug(f"Finished test: {test_name}")


@pytest.fixture
def mock_embeddings() -> Callable[[Documents], Embeddings]:
    """Return mock embeddings for testing"""

    def _mock_embeddings(input: Documents) -> Embeddings:
        return [np.array([0.1, 0.2, 0.3], dtype=np.float32) for _ in input]

    return _mock_embeddings


@pytest.fixture
def mock_common_deps(monkeypatch: MonkeyPatch) -> MonkeyPatch:
    """Mock common dependencies"""
    # Create mock modules
    mock_modules = {
        "PIL": MagicMock(),
        "torch": MagicMock(),
        "openai": MagicMock(),
        "cohere": MagicMock(),
        "sentence_transformers": MagicMock(),
        "ollama": MagicMock(),
        "InstructorEmbedding": MagicMock(),
        "voyageai": MagicMock(),
        "text2vec": MagicMock(),
        "open_clip": MagicMock(),
        "boto3": MagicMock(),
    }

    # Mock all modules at once using monkeypatch.setitem
    monkeypatch.setattr(sys, "modules", dict(sys.modules, **mock_modules))

    # Mock submodules and attributes
    mock_attributes = {
        "PIL.Image": MagicMock(),
        "sentence_transformers.SentenceTransformer": MagicMock(),
        "ollama.Client": MagicMock(),
        "InstructorEmbedding.INSTRUCTOR": MagicMock(),
        "voyageai.Client": MagicMock(),
        "text2vec.SentenceModel": MagicMock(),
    }

    # Setup OpenCLIP mock with specific behavior
    mock_model = MagicMock()
    mock_model.encode_text.return_value = np.array([[0.1, 0.2, 0.3]])
    mock_model.encode_image.return_value = np.array([[0.1, 0.2, 0.3]])
    mock_modules["open_clip"].create_model_and_transforms.return_value = (
        mock_model,
        MagicMock(),
        mock_model,
    )

    # Mock all attributes
    for path, mock in mock_attributes.items():
        monkeypatch.setattr(path, mock, raising=False)

    return monkeypatch
