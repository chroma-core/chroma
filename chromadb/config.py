from pydantic import BaseSettings
from typing import Optional, List, Any, Dict, TypeVar
from typing_extensions import Literal
import importlib
import logging
import chromadb.db
import chromadb.db.system
import chromadb.db.metadata
import chromadb.api
import chromadb.telemetry


logger = logging.getLogger(__name__)

_legacy_config_values = {
    "duckdb": "chromadb.db.duckdb.DuckDB",
    "duckdb+parquet": "chromadb.db.duckdb.PersistentDuckDB",
    "clickhouse": "chromadb.db.clickhouse.Clickhouse",
    "rest": "chromadb.api.fastapi.FastAPI",
    "local": "chromadb.api.local.LocalAPI",
}


class Settings(BaseSettings):
    environment: str = ""

    chroma_db_impl: str = "chromadb.db.duckdb.DuckDB"
    chroma_api_impl: str = "chromadb.api.local.LocalAPI"
    chroma_telemetry_impl: str = "chromadb.telemetry.posthog.Posthog"
    chroma_sysdb_impl: str = "chromadb.db.impl.sqlite.SqliteDB"
    chroma_metadb_impl: str = "chromadb.db.impl.sqlite.SqliteDB"

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


T = TypeVar("T")


class System:
    settings: Settings

    _instances: Dict[str, Any] = {}

    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = None
        self.api = None
        self.telemetry = None

    def _instance(self, key: str) -> Any:
        assert self.settings[key], f"Setting '{key}' is required."
        fqn = self.settings[key]
        if fqn not in self._instances:
            module_name, class_name = fqn.rsplit(".", 1)
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            impl = cls(self)
            self._instances[fqn] = impl
        instance = self._instances[fqn]
        return instance

    def get_db(self) -> chromadb.db.DB:
        inst = self._instance("chroma_db_impl")
        assert isinstance(inst, chromadb.db.DB)
        return inst

    def get_api(self) -> chromadb.api.API:
        inst = self._instance("chroma_api_impl")
        assert isinstance(inst, chromadb.api.API)
        return inst

    def get_telemetry(self) -> chromadb.telemetry.Telemetry:
        inst = self._instance("chroma_telemetry_impl")
        assert isinstance(inst, chromadb.telemetry.Telemetry)
        return inst

    def get_system_db(self) -> chromadb.db.system.SysDB:
        inst = self._instance("chroma_sysdb_impl")
        assert isinstance(inst, chromadb.db.system.SysDB)
        return inst

    def get_metadata_db(self) -> chromadb.db.metadata.MetadataDB:
        inst = self._instance("chroma_metadb_impl")
        assert isinstance(inst, chromadb.db.metadata.MetadataDB)
        return inst
