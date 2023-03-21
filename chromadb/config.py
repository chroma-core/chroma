from pydantic import BaseSettings, Field
from typing import Optional
import importlib
import logging

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

    clickhouse_host: Optional[str] = None
    clickhouse_port: Optional[str] = None

    duckdb_database: Optional[str] = None

    persist_directory: str = ".chroma"

    chroma_server_host: Optional[str] = None
    chroma_server_http_port: Optional[str] = None
    chroma_server_ssl_enabled: bool = False
    chroma_server_grpc_port: Optional[str] = None

    def validate(self, item):
        val = self[item]
        if val is None:
            raise ValueError(f"Missing required config value '{item}'")
        return val


    def __getitem__(self, item):
        val = getattr(self, item)
        # Backwards compatibility with short names instead of full class names
        if val in _legacy_config_values:
            newval = _legacy_config_values[val]
            logging.warning(f"Setting '{val}' for '{item}' is deprecated, use '{newval}' instead")
            val = newval
        return val

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


_impls = {}


def get_component(settings, key):
    """Retrieve a component instance, constructing it if necessary."""

    assert settings[key], f"Setting '{key}' is required."

    fqn = settings[key]

    if fqn not in _impls:
        module_name, class_name = fqn.rsplit(".", 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        _impls[fqn] = cls(settings)

    logger.info(f"Using {fqn} for {key}")
    return _impls[fqn]
