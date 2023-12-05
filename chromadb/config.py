import importlib
import inspect
import logging
import os
from abc import ABC
from graphlib import TopologicalSorter
from typing import Optional, List, Any, Dict, Set, Iterable
from typing import Type, TypeVar, cast

from overrides import EnforceOverrides
from overrides import override
from typing_extensions import Literal
import platform


in_pydantic_v2 = False
try:
    from pydantic import BaseSettings
except ImportError:
    in_pydantic_v2 = True
    from pydantic.v1 import BaseSettings
    from pydantic.v1 import validator

if not in_pydantic_v2:
    from pydantic import validator  # type: ignore # noqa

# The thin client will have a flag to control which implementations to use
is_thin_client = False
try:
    from chromadb.is_thin_client import is_thin_client  # type: ignore
except ImportError:
    is_thin_client = False

logger = logging.getLogger(__name__)

LEGACY_ERROR = """\033[91mYou are using a deprecated configuration of Chroma.

\033[94mIf you do not have data you wish to migrate, you only need to change how you construct
your Chroma client. Please see the "New Clients" section of https://docs.trychroma.com/migration.
________________________________________________________________________________________________

If you do have data you wish to migrate, we have a migration tool you can use in order to
migrate your data to the new Chroma architecture.
Please `pip install chroma-migrate` and run `chroma-migrate` to migrate your data and then
change how you construct your Chroma client.

See https://docs.trychroma.com/migration for more information or join our discord at https://discord.gg/8g5FESbj for help!\033[0m"""

_legacy_config_keys = {
    "chroma_db_impl",
}

_legacy_config_values = {
    "duckdb",
    "duckdb+parquet",
    "clickhouse",
    "local",
    "rest",
    "chromadb.db.duckdb.DuckDB",
    "chromadb.db.duckdb.PersistentDuckDB",
    "chromadb.db.clickhouse.Clickhouse",
    "chromadb.api.local.LocalAPI",
}

# TODO: Don't use concrete types here to avoid circular deps. Strings are fine for right here!
_abstract_type_keys: Dict[str, str] = {
    # NOTE: this is to support legacy api construction. Use ServerAPI instead
    "chromadb.api.API": "chroma_api_impl",
    "chromadb.api.ServerAPI": "chroma_api_impl",
    "chromadb.telemetry.product.ProductTelemetryClient": "chroma_product_telemetry_impl",
    "chromadb.ingest.Producer": "chroma_producer_impl",
    "chromadb.ingest.Consumer": "chroma_consumer_impl",
    "chromadb.ingest.CollectionAssignmentPolicy": "chroma_collection_assignment_policy_impl",  # noqa
    "chromadb.db.system.SysDB": "chroma_sysdb_impl",
    "chromadb.segment.SegmentManager": "chroma_segment_manager_impl",
    "chromadb.segment.distributed.SegmentDirectory": "chroma_segment_directory_impl",
    "chromadb.segment.distributed.MemberlistProvider": "chroma_memberlist_provider_impl",
}

DEFAULT_TENANT = "default_tenant"
DEFAULT_DATABASE = "default_database"


class Settings(BaseSettings):  # type: ignore
    environment: str = ""

    # Legacy config has to be kept around because pydantic will error
    # on nonexisting keys
    chroma_db_impl: Optional[str] = None
    # Can be "chromadb.api.segment.SegmentAPI" or "chromadb.api.fastapi.FastAPI"
    chroma_api_impl: str = "chromadb.api.segment.SegmentAPI"
    chroma_product_telemetry_impl: str = "chromadb.telemetry.product.posthog.Posthog"
    # Required for backwards compatibility
    chroma_telemetry_impl: str = chroma_product_telemetry_impl

    # New architecture components
    chroma_sysdb_impl: str = "chromadb.db.impl.sqlite.SqliteDB"
    chroma_producer_impl: str = "chromadb.db.impl.sqlite.SqliteDB"
    chroma_consumer_impl: str = "chromadb.db.impl.sqlite.SqliteDB"
    chroma_segment_manager_impl: str = (
        "chromadb.segment.impl.manager.local.LocalSegmentManager"
    )

    # Distributed architecture specific components
    chroma_segment_directory_impl: str = "chromadb.segment.impl.distributed.segment_directory.RendezvousHashSegmentDirectory"
    chroma_memberlist_provider_impl: str = "chromadb.segment.impl.distributed.segment_directory.CustomResourceMemberlistProvider"
    chroma_collection_assignment_policy_impl: str = (
        "chromadb.ingest.impl.simple_policy.SimpleAssignmentPolicy"
    )
    worker_memberlist_name: str = "worker-memberlist"
    chroma_coordinator_host = "localhost"

    tenant_id: str = "default"
    topic_namespace: str = "default"

    is_persistent: bool = False
    persist_directory: str = "./chroma"

    chroma_server_host: Optional[str] = None
    chroma_server_headers: Optional[Dict[str, str]] = None
    chroma_server_http_port: Optional[str] = None
    chroma_server_ssl_enabled: Optional[bool] = False
    chroma_server_api_default_path: Optional[str] = "/api/v1"
    chroma_server_grpc_port: Optional[str] = None
    # eg ["http://localhost:3000"]
    chroma_server_cors_allow_origins: List[str] = []

    @validator("chroma_server_nofile", pre=True, always=True, allow_reuse=True)
    def empty_str_to_none(cls, v: str) -> Optional[str]:
        if type(v) is str and v.strip() == "":
            return None
        return v

    chroma_server_nofile: Optional[int] = None

    pulsar_broker_url: Optional[str] = None
    pulsar_admin_port: Optional[str] = "8080"
    pulsar_broker_port: Optional[str] = "6650"

    chroma_server_auth_provider: Optional[str] = None

    @validator("chroma_server_auth_provider", pre=True, always=True, allow_reuse=True)
    def chroma_server_auth_provider_non_empty(
        cls: Type["Settings"], v: str
    ) -> Optional[str]:
        if v and not v.strip():
            raise ValueError(
                "chroma_server_auth_provider cannot be empty or just whitespace"
            )
        return v

    chroma_server_auth_configuration_provider: Optional[str] = None
    chroma_server_auth_configuration_file: Optional[str] = None
    chroma_server_auth_credentials_provider: Optional[str] = None
    chroma_server_auth_credentials_file: Optional[str] = None
    chroma_server_auth_credentials: Optional[str] = None

    @validator(
        "chroma_server_auth_credentials_file", pre=True, always=True, allow_reuse=True
    )
    def chroma_server_auth_credentials_file_non_empty_file_exists(
        cls: Type["Settings"], v: str
    ) -> Optional[str]:
        if v and not v.strip():
            raise ValueError(
                "chroma_server_auth_credentials_file cannot be empty or just whitespace"
            )
        if v and not os.path.isfile(os.path.join(v)):
            raise ValueError(
                f"chroma_server_auth_credentials_file [{v}] does not exist"
            )
        return v

    chroma_client_auth_provider: Optional[str] = None
    chroma_server_auth_ignore_paths: Dict[str, List[str]] = {
        "/api/v1": ["GET"],
        "/api/v1/heartbeat": ["GET"],
        "/api/v1/version": ["GET"],
    }

    chroma_client_auth_credentials_provider: Optional[
        str
    ] = "chromadb.auth.providers.ConfigurationClientAuthCredentialsProvider"
    chroma_client_auth_protocol_adapter: Optional[
        str
    ] = "chromadb.auth.providers.RequestsClientAuthProtocolAdapter"
    chroma_client_auth_credentials_file: Optional[str] = None
    chroma_client_auth_credentials: Optional[str] = None
    chroma_client_auth_token_transport_header: Optional[str] = None
    chroma_server_auth_token_transport_header: Optional[str] = None

    chroma_server_authz_provider: Optional[str] = None

    chroma_server_authz_ignore_paths: Dict[str, List[str]] = {
        "/api/v1": ["GET"],
        "/api/v1/heartbeat": ["GET"],
        "/api/v1/version": ["GET"],
    }
    chroma_server_authz_config_file: Optional[str] = None

    chroma_server_authz_config: Optional[Dict[str, Any]] = None

    @validator(
        "chroma_server_authz_config_file", pre=True, always=True, allow_reuse=True
    )
    def chroma_server_authz_config_file_non_empty_file_exists(
        cls: Type["Settings"], v: str
    ) -> Optional[str]:
        if v and not v.strip():
            raise ValueError(
                "chroma_server_authz_config_file cannot be empty or just whitespace"
            )
        if v and not os.path.isfile(os.path.join(v)):
            raise ValueError(f"chroma_server_authz_config_file [{v}] does not exist")
        return v

    chroma_server_authz_config_provider: Optional[
        str
    ] = "chromadb.auth.authz.LocalUserConfigAuthorizationConfigurationProvider"

    anonymized_telemetry: bool = True

    chroma_otel_collection_endpoint: Optional[str] = ""
    chroma_otel_service_name: Optional[str] = "chromadb"
    chroma_otel_collection_headers: Dict[str, str] = {}
    chroma_otel_granularity: Optional[str] = None

    allow_reset: bool = False

    migrations: Literal["none", "validate", "apply"] = "apply"
    # you cannot change the hash_algorithm after migrations have already been applied once
    # this is intended to be a first-time setup configuration
    migrations_hash_algorithm: Literal["md5", "sha256"] = "md5"

    def require(self, key: str) -> Any:
        """Return the value of a required config key, or raise an exception if it is not
        set"""
        val = self[key]
        if val is None:
            raise ValueError(f"Missing required config value '{key}'")
        return val

    def __getitem__(self, key: str) -> Any:
        val = getattr(self, key)
        # Error on legacy config values
        if isinstance(val, str) and val in _legacy_config_values:
            raise ValueError(LEGACY_ERROR)
        return val

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


T = TypeVar("T", bound="Component")


class Component(ABC, EnforceOverrides):
    _dependencies: Set["Component"]
    _system: "System"
    _running: bool

    def __init__(self, system: "System"):
        self._dependencies = set()
        self._system = system
        self._running = False

    def require(self, type: Type[T]) -> T:
        """Get a Component instance of the given type, and register as a dependency of
        that instance."""
        inst = self._system.instance(type)
        self._dependencies.add(inst)
        return inst

    def dependencies(self) -> Set["Component"]:
        """Return the full set of components this component depends on."""
        return self._dependencies

    def stop(self) -> None:
        """Idempotently stop this component's execution and free all associated
        resources."""
        logger.debug(f"Stopping component {self.__class__.__name__}")
        self._running = False

    def start(self) -> None:
        """Idempotently start this component's execution"""
        logger.debug(f"Starting component {self.__class__.__name__}")
        self._running = True

    def reset_state(self) -> None:
        """Reset this component's state to its initial blank state. Only intended to be
        called from tests."""
        logger.debug(f"Resetting component {self.__class__.__name__}")


class System(Component):
    settings: Settings
    _instances: Dict[Type[Component], Component]

    def __init__(self, settings: Settings):
        if is_thin_client:
            # The thin client is a system with only the API component
            if settings["chroma_api_impl"] != "chromadb.api.fastapi.FastAPI":
                raise RuntimeError(
                    "Chroma is running in http-only client mode, and can only be run with 'chromadb.api.fastapi.FastAPI' as the chroma_api_impl. \
            see https://docs.trychroma.com/usage-guide?lang=py#using-the-python-http-only-client for more information."
                )
        # Validate settings don't contain any legacy config values
        for key in _legacy_config_keys:
            if settings[key] is not None:
                raise ValueError(LEGACY_ERROR)

        # Apply the nofile limit if set
        if settings["chroma_server_nofile"] is not None:
            if platform.system() != "Windows":
                import resource

                curr_soft, curr_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                desired_soft = settings["chroma_server_nofile"]
                # Validate
                if desired_soft > curr_hard:
                    logging.warning(
                        f"chroma_server_nofile cannot be set to a value greater than the current hard limit of {curr_hard}. Keeping soft limit at {curr_soft}"
                    )
                # Apply
                elif desired_soft > curr_soft:
                    try:
                        resource.setrlimit(
                            resource.RLIMIT_NOFILE, (desired_soft, curr_hard)
                        )
                        logger.info(f"Set chroma_server_nofile to {desired_soft}")
                    except Exception as e:
                        logger.error(
                            f"Failed to set chroma_server_nofile to {desired_soft}: {e} nofile soft limit will remain at {curr_soft}"
                        )
                # Don't apply if reducing the limit
                elif desired_soft < curr_soft:
                    logger.warning(
                        f"chroma_server_nofile is set to {desired_soft}, but this is less than current soft limit of {curr_soft}. chroma_server_nofile will not be set."
                    )
            else:
                logger.warning(
                    "chroma_server_nofile is not supported on Windows. chroma_server_nofile will not be set."
                )

        self.settings = settings
        self._instances = {}
        super().__init__(self)

    def instance(self, type: Type[T]) -> T:
        """Return an instance of the component type specified. If the system is running,
        the component will be started as well."""

        if inspect.isabstract(type):
            type_fqn = get_fqn(type)
            if type_fqn not in _abstract_type_keys:
                raise ValueError(f"Cannot instantiate abstract type: {type}")
            key = _abstract_type_keys[type_fqn]
            fqn = self.settings.require(key)
            type = get_class(fqn, type)

        if type not in self._instances:
            impl = type(self)
            self._instances[type] = impl
            if self._running:
                impl.start()

        inst = self._instances[type]
        return cast(T, inst)

    def components(self) -> Iterable[Component]:
        """Return the full set of all components and their dependencies in dependency
        order."""
        sorter: TopologicalSorter[Component] = TopologicalSorter()
        for component in self._instances.values():
            sorter.add(component, *component.dependencies())

        return sorter.static_order()

    @override
    def start(self) -> None:
        super().start()
        for component in self.components():
            component.start()

    @override
    def stop(self) -> None:
        super().stop()
        for component in reversed(list(self.components())):
            component.stop()

    @override
    def reset_state(self) -> None:
        """Reset the state of this system and all constituents in reverse dependency order"""
        if not self.settings.allow_reset:
            raise ValueError(
                "Resetting is not allowed by this configuration (to enable it, set `allow_reset` to `True` in your Settings() or include `ALLOW_RESET=TRUE` in your environment variables)"
            )
        for component in reversed(list(self.components())):
            component.reset_state()


C = TypeVar("C")


def get_class(fqn: str, type: Type[C]) -> Type[C]:
    """Given a fully qualifed class name, import the module and return the class"""
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cast(Type[C], cls)


def get_fqn(cls: Type[object]) -> str:
    """Given a class, return its fully qualified name"""
    return f"{cls.__module__}.{cls.__name__}"
