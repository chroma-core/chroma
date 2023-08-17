import base64
import importlib
import inspect
import logging
import os
from abc import ABC, abstractmethod
from graphlib import TopologicalSorter
from typing import Optional, List, Any, Dict, Set, Iterable, Union
from typing import Type, TypeVar, cast

import requests
from overrides import override
from overrides import overrides, EnforceOverrides
from pydantic import BaseSettings, SecretStr
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.types import ASGIApp
from typing_extensions import Literal

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
    "chromadb.api.API": "chroma_api_impl",
    "chromadb.telemetry.Telemetry": "chroma_telemetry_impl",
    "chromadb.ingest.Producer": "chroma_producer_impl",
    "chromadb.ingest.Consumer": "chroma_consumer_impl",
    "chromadb.db.system.SysDB": "chroma_sysdb_impl",
    "chromadb.segment.SegmentManager": "chroma_segment_manager_impl",
}


class ClientAuthProvider(ABC, EnforceOverrides):
    def __init__(self, settings: "Settings") -> None:
        self._settings = settings

    @abstractmethod
    def authenticate(self, session: requests.Session) -> None:
        pass


class ServerAuthProvider(ABC, EnforceOverrides):
    def __init__(self, settings: "Settings") -> None:
        self._settings = settings

    @abstractmethod
    def authenticate(self, request: Request) -> Union[Response, None]:
        pass


class Settings(BaseSettings):  # type: ignore
    environment: str = ""

    # Legacy config has to be kept around because pydantic will error on nonexisting keys
    chroma_db_impl: Optional[str] = None

    chroma_api_impl: str = "chromadb.api.segment.SegmentAPI"  # Can be "chromadb.api.segment.SegmentAPI" or "chromadb.api.fastapi.FastAPI"
    chroma_telemetry_impl: str = "chromadb.telemetry.posthog.Posthog"

    # New architecture components
    chroma_sysdb_impl: str = "chromadb.db.impl.sqlite.SqliteDB"
    chroma_producer_impl: str = "chromadb.db.impl.sqlite.SqliteDB"
    chroma_consumer_impl: str = "chromadb.db.impl.sqlite.SqliteDB"
    chroma_segment_manager_impl: str = (
        "chromadb.segment.impl.manager.local.LocalSegmentManager"
    )

    tenant_id: str = "default"
    topic_namespace: str = "default"

    is_persistent: bool = False
    persist_directory: str = "./chroma"

    chroma_server_host: Optional[str] = None
    chroma_server_headers: Optional[Dict[str, str]] = None
    chroma_server_http_port: Optional[str] = None
    chroma_server_ssl_enabled: Optional[bool] = False
    chroma_server_grpc_port: Optional[str] = None
    chroma_server_cors_allow_origins: List[str] = []  # eg ["http://localhost:3000"]
    # eg ["chromadb.api.fastapi.middlewares.auth.AuthMiddleware"]
    chroma_server_middlewares: List[str] = []

    chroma_server_auth_provider: Optional[str] = None
    chroma_server_auth_provider_config: Optional[Union[str, Dict[str, Any]]] = None
    chroma_client_auth_provider: Optional[str] = None
    chroma_client_auth_provider_config: Optional[Union[str, Dict[str, Any]]] = None
    anonymized_telemetry: bool = True

    allow_reset: bool = False

    migrations: Literal["none", "validate", "apply"] = "apply"

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
    auth_provider: Optional["ClientAuthProvider"]
    _instances: Dict[Type[Component], Component]

    def __init__(self, settings: Settings):
        if is_thin_client:
            # The thin client is a system with only the API component
            if settings["chroma_api_impl"] != "chromadb.api.fastapi.FastAPI":
                raise RuntimeError(
                    "Chroma is running in http-only client mode, and can only be run with 'chromadb.api.fastapi.FastAPI' as the chroma_api_impl. \
            see https://docs.trychroma.com/usage-guide?lang=py#using-the-python-http-only-client for more information."
                )
        if (
            settings.chroma_client_auth_provider is not None
            and settings.chroma_client_auth_provider.strip() != ""
        ):
            logger.debug(
                f"Client Auth Provider: {settings.chroma_client_auth_provider}"
            )
            self.auth_provider = get_class(
                settings.chroma_client_auth_provider, ClientAuthProvider
            )(settings)
        else:
            self.auth_provider = None
        # Validate settings don't contain any legacy config values
        for key in _legacy_config_keys:
            if settings[key] is not None:
                raise ValueError(LEGACY_ERROR)

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


class BasicAuthClientProvider(ClientAuthProvider):
    _basic_auth_token: SecretStr

    def __init__(self, settings: "Settings") -> None:
        super().__init__(settings)
        self._settings = settings
        if os.environ.get("CHROMA_CLIENT_AUTH_BASIC_USERNAME") and os.environ.get(
            "CHROMA_CLIENT_AUTH_BASIC_PASSWORD"
        ):
            self._basic_auth_token = _create_token(
                os.environ.get("CHROMA_CLIENT_AUTH_BASIC_USERNAME", ""),
                os.environ.get("CHROMA_CLIENT_AUTH_BASIC_PASSWORD", ""),
            )
        elif isinstance(
            self._settings.chroma_client_auth_provider_config, str
        ) and os.path.exists(self._settings.chroma_client_auth_provider_config):
            with open(self._settings.chroma_client_auth_provider_config) as f:
                # read first line of file which should be user:password
                _auth_data = f.readline().strip().split(":")
                # validate auth data
                if len(_auth_data) != 2:
                    raise ValueError("Invalid auth data")
                self._basic_auth_token = _create_token(_auth_data[0], _auth_data[1])
        elif self._settings.chroma_client_auth_provider_config and isinstance(
            self._settings.chroma_client_auth_provider_config, dict
        ):
            self._basic_auth_token = _create_token(
                self._settings.chroma_client_auth_provider_config["username"],
                self._settings.chroma_client_auth_provider_config["password"],
            )
        else:
            raise ValueError("Basic auth credentials not found")

    @overrides
    def authenticate(self, session: requests.Session) -> None:
        session.headers.update(
            {"Authorization": f"Basic {self._basic_auth_token.get_secret_value()}"}
        )


class ChromaAuthMiddleware(BaseHTTPMiddleware):  # type: ignore
    def __init__(self, app: ASGIApp, settings: "Settings") -> None:
        super().__init__(app)
        self._settings = settings
        self._settings.require("chroma_server_auth_provider")
        if settings.chroma_server_auth_provider:
            _cls = get_class(settings.chroma_server_auth_provider, ServerAuthProvider)
            logger.debug(f"Server Auth Provider: {_cls}")
            self._auth_provider = _cls(settings)

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        response = self._auth_provider.authenticate(request)
        if response is not None:
            return response
        return await call_next(request)


def _create_token(username: str, password: str) -> SecretStr:
    return SecretStr(
        base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    )


class BasicAuthServerProvider(ServerAuthProvider):
    _basic_auth_token: SecretStr
    _ignore_auth_paths: List[str] = ["/api/v1", "/api/v1/heartbeat", "/api/v1/version"]

    def __init__(self, settings: "Settings") -> None:
        super().__init__(settings)
        self._settings = settings
        self._basic_auth_token = SecretStr("")
        if os.environ.get("CHROMA_SERVER_AUTH_BASIC_USERNAME") and os.environ.get(
            "CHROMA_SERVER_AUTH_BASIC_PASSWORD"
        ):
            self._basic_auth_token = _create_token(
                os.environ.get("CHROMA_SERVER_AUTH_BASIC_USERNAME", ""),
                os.environ.get("CHROMA_SERVER_AUTH_BASIC_PASSWORD", ""),
            )
            self._ignore_auth_paths = os.environ.get(
                "CHROMA_SERVER_AUTH_IGNORE_PATHS", ",".join(self._ignore_auth_paths)
            ).split(",")
        elif isinstance(
            self._settings.chroma_server_auth_provider_config, str
        ) and os.path.exists(self._settings.chroma_server_auth_provider_config):
            with open(self._settings.chroma_server_auth_provider_config) as f:
                # read first line of file which should be user:password
                _auth_data = f.readline().strip().split(":")
                # validate auth data
                if len(_auth_data) != 2:
                    raise ValueError("Invalid auth data")
                self._basic_auth_token = _create_token(_auth_data[0], _auth_data[1])
            self._ignore_auth_paths = os.environ.get(
                "CHROMA_SERVER_AUTH_IGNORE_PATHS", ",".join(self._ignore_auth_paths)
            ).split(",")
        elif self._settings.chroma_server_auth_provider_config and isinstance(
            self._settings.chroma_server_auth_provider_config, dict
        ):
            # encode the username and password base64
            self._basic_auth_token = _create_token(
                self._settings.chroma_server_auth_provider_config["username"],
                self._settings.chroma_server_auth_provider_config["password"],
            )
            if "ignore_auth_paths" in self._settings.chroma_server_auth_provider_config:
                self._ignore_auth_paths = (
                    self._settings.chroma_server_auth_provider_config[
                        "ignore_auth_paths"
                    ]
                )
        else:
            raise ValueError("Basic auth credentials not found")

    @overrides
    def authenticate(self, request: Request) -> Union[Response, None]:
        auth_header = request.headers.get("Authorization", "").split()
        # Check if the header exists and the token is correct
        if request.url.path in self._ignore_auth_paths:
            logger.debug(f"Skipping auth for path {request.url.path}")
            return None
        if (
            len(auth_header) != 2
            or auth_header[1] != self._basic_auth_token.get_secret_value()
        ):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        return None


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
