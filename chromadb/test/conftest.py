import multiprocessing
import os
import socket
import subprocess
import tempfile
import time
from typing import (
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Callable,
)
from uuid import UUID

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
from chromadb.types import SeqId, OperationRecord
from chromadb.api.client import Client as ClientCreator

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
    "slow", hypothesis.settings.get_profile("base"), max_examples=200
)

hypothesis.settings.load_profile(CURRENT_PRESET)


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

    return hypothesis.settings.default


NOT_CLUSTER_ONLY = os.getenv("CHROMA_CLUSTER_TEST_ONLY") != "1"
MEMBERLIST_SLEEP = 5
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
    chroma_server_authn_provider: Optional[str] = None,
    chroma_server_authn_credentials_file: Optional[str] = None,
    chroma_server_authn_credentials: Optional[str] = None,
    chroma_auth_token_transport_header: Optional[str] = None,
    chroma_server_authz_provider: Optional[str] = None,
    chroma_server_authz_config_file: Optional[str] = None,
    chroma_server_ssl_certfile: Optional[str] = None,
    chroma_server_ssl_keyfile: Optional[str] = None,
    chroma_overwrite_singleton_tenant_database_access_from_auth: Optional[bool] = False,
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
            chroma_server_authn_provider=chroma_server_authn_provider,
            chroma_server_authn_credentials_file=chroma_server_authn_credentials_file,
            chroma_server_authn_credentials=chroma_server_authn_credentials,
            chroma_auth_token_transport_header=chroma_auth_token_transport_header,
            chroma_server_authz_provider=chroma_server_authz_provider,
            chroma_server_authz_config_file=chroma_server_authz_config_file,
            chroma_overwrite_singleton_tenant_database_access_from_auth=chroma_overwrite_singleton_tenant_database_access_from_auth,
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
            chroma_server_authn_provider=chroma_server_authn_provider,
            chroma_server_authn_credentials_file=chroma_server_authn_credentials_file,
            chroma_server_authn_credentials=chroma_server_authn_credentials,
            chroma_auth_token_transport_header=chroma_auth_token_transport_header,
            chroma_server_authz_provider=chroma_server_authz_provider,
            chroma_server_authz_config_file=chroma_server_authz_config_file,
            chroma_overwrite_singleton_tenant_database_access_from_auth=chroma_overwrite_singleton_tenant_database_access_from_auth,
        )
    server = chromadb.server.fastapi.FastAPI(settings)
    uvicorn.run(
        server.app(),
        host="0.0.0.0",
        port=port,
        log_level="error",
        timeout_keep_alive=30,
        ssl_keyfile=chroma_server_ssl_keyfile,
        ssl_certfile=chroma_server_ssl_certfile,
    )


def _await_server(api: ServerAPI, attempts: int = 0) -> None:
    try:
        api.heartbeat()
    except ConnectionError as e:
        if attempts > 15:
            raise e
        else:
            time.sleep(4)
            _await_server(api, attempts + 1)


def _fastapi_fixture(
    is_persistent: bool = False,
    chroma_server_authn_provider: Optional[str] = None,
    chroma_client_auth_provider: Optional[str] = None,
    chroma_server_authn_credentials_file: Optional[str] = None,
    chroma_server_authn_credentials: Optional[str] = None,
    chroma_client_auth_credentials: Optional[str] = None,
    chroma_auth_token_transport_header: Optional[str] = None,
    chroma_server_authz_provider: Optional[str] = None,
    chroma_server_authz_config_file: Optional[str] = None,
    chroma_server_ssl_certfile: Optional[str] = None,
    chroma_server_ssl_keyfile: Optional[str] = None,
    chroma_overwrite_singleton_tenant_database_access_from_auth: Optional[bool] = False,
) -> Generator[System, None, None]:
    """Fixture generator that launches a server in a separate process, and yields a
    fastapi client connect to it"""

    port = find_free_port()
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
        Optional[str],
        Optional[bool],
    ] = (
        port,
        False,
        None,
        chroma_server_authn_provider,
        chroma_server_authn_credentials_file,
        chroma_server_authn_credentials,
        chroma_auth_token_transport_header,
        chroma_server_authz_provider,
        chroma_server_authz_config_file,
        chroma_server_ssl_certfile,
        chroma_server_ssl_keyfile,
        chroma_overwrite_singleton_tenant_database_access_from_auth,
    )

    def run(args):
        proc = ctx.Process(target=_run_server, args=args, daemon=True)
        proc.start()
        settings = Settings(
            chroma_api_impl="chromadb.api.fastapi.FastAPI",
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
        _await_server(api)
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
            chroma_server_authn_provider,
            chroma_server_authn_credentials_file,
            chroma_server_authn_credentials,
            chroma_auth_token_transport_header,
            chroma_server_authz_provider,
            chroma_server_authz_config_file,
            chroma_server_ssl_certfile,
            chroma_server_ssl_keyfile,
            chroma_overwrite_singleton_tenant_database_access_from_auth,
        )

        yield from run(args)

        try:
            persist_directory.cleanup()

        except PermissionError as e:
            # todo: what's holding onto directory contents on Windows?
            if os.name == "nt" and e.winerror:
                pass

            raise e

    else:
        yield from run(args)


def fastapi() -> Generator[System, None, None]:
    return _fastapi_fixture(is_persistent=False)


def fastapi_persistent() -> Generator[System, None, None]:
    return _fastapi_fixture(is_persistent=True)


def fastapi_ssl() -> Generator[System, None, None]:
    generate_self_signed_certificate()
    return _fastapi_fixture(
        is_persistent=False,
        chroma_server_ssl_certfile="./servercert.pem",
        chroma_server_ssl_keyfile="./serverkey.pem",
    )


def basic_http_client() -> Generator[System, None, None]:
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_http_port=8000,
        chroma_server_host="localhost",
        allow_reset=True,
    )
    system = System(settings)
    api = system.instance(ServerAPI)
    _await_server(api)
    system.start()
    yield system
    system.stop()


def fastapi_server_basic_auth_valid_cred_single_user() -> Generator[System, None, None]:
    # This (and similar usage below) should use the delete_on_close parameter
    # instead of delete=False, but it's only available in Python 3.12 and later.
    # We must explicitly close the file before spawning a subprocess to avoid
    # file locking issues on Windows.
    with tempfile.NamedTemporaryFile("w", suffix=".htpasswd", delete=False) as f:
        f.write("admin:$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS\n")
        f.close()

        for item in _fastapi_fixture(
            is_persistent=False,
            chroma_server_authn_provider="chromadb.auth.basic_authn.BasicAuthenticationServerProvider",
            chroma_server_authn_credentials_file=f.name,
            chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
            chroma_client_auth_credentials="admin:admin",
        ):
            yield item


def fastapi_server_basic_auth_valid_cred_multiple_users() -> (
    Generator[System, None, None]
):
    creds = {
        "user": "$2y$10$kY9hn.Wlfcj7n1Cnjmy1kuIhEFIVBsfbNWLQ5ahoKmdc2HLA4oP6i",
        "user2": "$2y$10$CymQ63tic/DRj8dD82915eoM4ke3d6RaNKU4dj4IVJlHyea0yeGDS",
        "admin": "$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS",
    }
    with tempfile.NamedTemporaryFile("w", suffix=".htpasswd", delete=False) as f:
        for user, cred in creds.items():
            f.write(f"{user}:{cred}\n")
        f.close()

        for item in _fastapi_fixture(
            is_persistent=False,
            chroma_server_authn_provider="chromadb.auth.basic_authn.BasicAuthenticationServerProvider",
            chroma_server_authn_credentials_file=f.name,
            chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
            chroma_client_auth_credentials="admin:admin",
        ):
            yield item


def fastapi_server_basic_auth_invalid_cred() -> Generator[System, None, None]:
    with tempfile.NamedTemporaryFile("w", suffix=".htpasswd", delete=False) as f:
        f.write("admin:$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS\n")
        f.close()

        for item in _fastapi_fixture(
            is_persistent=False,
            chroma_server_authn_provider="chromadb.auth.basic_authn.BasicAuthenticationServerProvider",
            chroma_server_authn_credentials_file=f.name,
            chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
            chroma_client_auth_credentials="admin:admin1",
        ):
            yield item


def fastapi_server_basic_authn_rbac_authz() -> Generator[System, None, None]:
    with tempfile.NamedTemporaryFile(
        "w", suffix=".htpasswd", delete=False
    ) as server_authn_file:
        server_authn_file.write(
            "admin:$2y$05$e5sRb6NCcSH3YfbIxe1AGu2h5K7OOd982OXKmd8WyQ3DRQ4MvpnZS\n"
        )
        server_authn_file.close()

        with tempfile.NamedTemporaryFile(
            "w", suffix=".authz", delete=False
        ) as server_authz_file:
            server_authz_file.write(
                """
roles_mapping:
    admin:
        actions:
            [
                "system:reset",
                "tenant:create_tenant",
                "tenant:get_tenant",
                "db:create_database",
                "db:get_database",
                "db:list_collections",
                "db:create_collection",
                "db:get_or_create_collection",
                "collection:get_collection",
                "collection:delete_collection",
                "collection:update_collection",
                "collection:add",
                "collection:delete",
                "collection:get",
                "collection:query",
                "collection:peek",
                "collection:update",
                "collection:upsert",
                "collection:count",
            ]
users:
- id: admin
  role: admin
    """
            )
            server_authz_file.close()

            for item in _fastapi_fixture(
                is_persistent=False,
                chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
                chroma_client_auth_credentials="admin:admin",
                chroma_server_authn_provider="chromadb.auth.basic_authn.BasicAuthenticationServerProvider",
                chroma_server_authn_credentials_file=server_authn_file.name,
                chroma_server_authz_provider="chromadb.auth.simple_rbac_authz.SimpleRBACAuthorizationProvider",
                chroma_server_authz_config_file=server_authz_file.name,
            ):
                yield item


def fastapi_fixture_admin_and_singleton_tenant_db_user() -> (
    Generator[System, None, None]
):
    with tempfile.NamedTemporaryFile("w", suffix=".authn", delete=False) as f:
        f.write(
            """
users:
  - id: admin
    tokens:
      - admin-token
  - id: singleton_user
    tenant: singleton_tenant
    databases:
      - singleton_database
    tokens:
      - singleton-token
"""
        )
        f.close()

        for item in _fastapi_fixture(
            is_persistent=False,
            chroma_overwrite_singleton_tenant_database_access_from_auth=True,
            chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
            chroma_client_auth_credentials="admin-token",
            chroma_server_authn_provider="chromadb.auth.token_authn.TokenAuthenticationServerProvider",
            chroma_server_authn_credentials_file=f.name,
        ):
            yield item


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
    with tempfile.TemporaryDirectory() as save_path:
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
        fastapi_server_basic_auth_valid_cred_single_user,
        fastapi_server_basic_auth_valid_cred_multiple_users,
    ]
    return fixtures


def system_fixtures_authn_rbac_authz() -> (
    List[Callable[[], Generator[System, None, None]]]
):
    fixtures = [fastapi_server_basic_authn_rbac_authz]
    return fixtures


def system_fixtures_root_and_singleton_tenant_db_user() -> (
    List[Callable[[], Generator[System, None, None]]]
):
    fixtures = [fastapi_fixture_admin_and_singleton_tenant_db_user]
    return fixtures


def system_fixtures_wrong_auth() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [fastapi_server_basic_auth_invalid_cred]
    return fixtures


def system_fixtures_ssl() -> List[Callable[[], Generator[System, None, None]]]:
    fixtures = [fastapi_ssl]
    return fixtures


@pytest.fixture(scope="module", params=system_fixtures_wrong_auth())
def system_wrong_auth(
    request: pytest.FixtureRequest,
) -> Generator[ServerAPI, None, None]:
    yield from request.param()


@pytest.fixture(scope="module", params=system_fixtures_authn_rbac_authz())
def system_authn_rbac_authz(
    request: pytest.FixtureRequest,
) -> Generator[ServerAPI, None, None]:
    yield from request.param()


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: pytest.FixtureRequest) -> Generator[ServerAPI, None, None]:
    yield from request.param()


@pytest.fixture(scope="module", params=system_fixtures_ssl())
def system_ssl(request: pytest.FixtureRequest) -> Generator[ServerAPI, None, None]:
    yield from request.param()


@pytest.fixture(scope="module", params=system_fixtures_auth())
def system_auth(request: pytest.FixtureRequest) -> Generator[ServerAPI, None, None]:
    yield from request.param()


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
