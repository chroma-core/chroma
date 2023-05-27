from pydantic import BaseSettings
from typing import Optional, List, Any, Dict, TypeVar, Set, cast, Iterable, Type
from typing_extensions import Literal
from abc import ABC, abstractmethod
import importlib
import logging
import chromadb.db
import chromadb.api
import chromadb.telemetry
from overrides import EnforceOverrides, override
from graphlib import TopologicalSorter
import inspect

logger = logging.getLogger(__name__)

_legacy_config_values = {
    "duckdb": "chromadb.db.duckdb.DuckDB",
    "duckdb+parquet": "chromadb.db.duckdb.PersistentDuckDB",
    "clickhouse": "chromadb.db.clickhouse.Clickhouse",
    "rest": "chromadb.api.fastapi.FastAPI",
    "local": "chromadb.api.local.LocalAPI",
}

_abstract_type_keys: dict[type, str] = {
    chromadb.db.DB: "chroma_db_impl",
    chromadb.api.API: "chroma_api_impl",
    chromadb.telemetry.Telemetry: "chroma_telemetry_impl",
}


class Settings(BaseSettings):
    environment: str = ""

    chroma_db_impl: str = "chromadb.db.duckdb.DuckDB"
    chroma_api_impl: str = "chromadb.api.local.LocalAPI"
    chroma_telemetry_impl: str = "chromadb.telemetry.posthog.Posthog"

    clickhouse_host: Optional[str] = None
    clickhouse_port: Optional[str] = None

    persist_directory: str = ".chroma"

    chroma_server_host: Optional[str] = None
    chroma_server_http_port: Optional[str] = None
    chroma_server_ssl_enabled: Optional[bool] = False
    chroma_server_grpc_port: Optional[str] = None
    chroma_server_cors_allow_origins: List[str] = []  # eg ["http://localhost:3000"]

    anonymized_telemetry: bool = True

    allow_reset: bool = False

    sqlite_database: Optional[str] = ":memory:"
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
        # Backwards compatibility with short names instead of full class names
        if val in _legacy_config_values:
            newval = _legacy_config_values[val]
            val = newval
        return val

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


T = TypeVar("T", bound="Component")


class Component(ABC, EnforceOverrides):
    _dependencies: Set["Component"]
    _system: "System"

    def __init__(self, system: "System"):
        self._dependencies = set()
        self._system = system

    def require(self, type: type[T]) -> T:
        """Get a Component instance of the given type, and register as a dependency of
        that instance."""
        inst = self._system.instance(type)
        self._dependencies.add(inst)
        return inst

    def dependencies(self) -> Set["Component"]:
        """Return the full set of components this component depends on."""
        return self._dependencies

    @abstractmethod
    def stop(self) -> None:
        """Idempotently stop this component's execution and free all associated
        resources."""
        pass

    @abstractmethod
    def start(self) -> None:
        """Idempotently start this component's execution"""
        pass


class System(Component):
    settings: Settings

    _instances: Dict[type, Component]

    def __init__(self, settings: Settings):
        self.settings = settings
        self._instances = {}

    def instance(self, type: type[T]) -> T:
        """Return an instance of the component type specified."""

        if inspect.isabstract(type):
            if type not in _abstract_type_keys:
                raise ValueError(f"Cannot instantiate abstract type: {type}")
            key = _abstract_type_keys[type]
            fqn = self.settings.require(key)
            type = get_class(fqn, type)

        if type not in self._instances:
            impl = type(self)
            self._instances[type] = impl

        inst = self._instances[type]
        assert isinstance(inst, type)
        return inst

    def components(self) -> Iterable[Component]:
        """Return the full set of all components and their dependencies in dependency
        order."""
        sorter: TopologicalSorter[Component] = TopologicalSorter()
        for component in self._instances.values():
            sorter.add(component, *component.dependencies())

        return sorter.static_order()

    @override
    def start(self) -> None:
        for component in self.components():
            component.start()

    @override
    def stop(self) -> None:
        for component in reversed(list(self.components())):
            component.stop()


def get_class(fqn: str, type: Type[T]) -> Type[T]:
    """Given a fully qualifed class name, import the module and return the class"""
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    assert issubclass(cls, type)
    return cast(Type[T], cls)
