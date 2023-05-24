from pydantic import BaseSettings
from typing import Optional, List, Any
from typing_extensions import Literal
import importlib
import logging
import chromadb.db
import chromadb.api
import chromadb.telemetry

# The thin client will have a flag to control which implementations to use
is_thin_client = False
try:
    from chromadb.is_thin_client import is_thin_client  # type: ignore
except ImportError:
    is_thin_client = False


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


class System:
    settings: Settings

    db: Optional[chromadb.db.DB]
    api: Optional[chromadb.api.API]
    telemetry: Optional[chromadb.telemetry.Telemetry]

    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = None
        self.api = None
        self.telemetry = None

    def _instantiate(self, key: str) -> Any:
        assert self.settings[key], f"Setting '{key}' is required."
        fqn = self.settings[key]
        module_name, class_name = fqn.rsplit(".", 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        impl = cls(self)
        return impl

    def get_db(self) -> chromadb.db.DB:
        if is_thin_client:
            raise RuntimeError(
                "Chroma is running in thin client mode, and cannot directly access the database. \
                See https://docs.trychroma.com/usage-guide?lang=py#using-the-python-http-only-client for more information."
            )

        if self.db is None:
            self.db = self._instantiate("chroma_db_impl")
        return self.db

    def get_api(self) -> chromadb.api.API:
        if (
            is_thin_client
            and self.settings["chroma_api_impl"] != "chromadb.api.fastapi.FastAPI"
        ):
            # TODO: show example and link to docs
            raise RuntimeError(
                "Chroma is running in thin client mode, and can only be run with 'chromadb.api.fastapi.FastAPI' or 'rest' as the chroma_api_impl. \
                see https://docs.trychroma.com/usage-guide?lang=py#using-the-python-http-only-client for more information."
            )

        if self.api is None:
            self.api = self._instantiate("chroma_api_impl")
        return self.api

    def get_telemetry(self) -> chromadb.telemetry.Telemetry:
        if self.telemetry is None:
            self.telemetry = self._instantiate("chroma_telemetry_impl")
        return self.telemetry
