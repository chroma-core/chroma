import chromadb.config
import logging


__settings = chromadb.config.Settings()


def configure(**kwargs):
    """Override Chroma's default settings, environment variables or .env files"""
    __settings = chromadb.config.Settings(**kwargs)


def get_settings():
    return __settings


def get_db(settings=__settings):
    """Return a chroma.DB instance based on the provided or environmental settings."""

    setting = settings.chroma_db_impl.lower()

    def require(key):
        assert settings[key], f"Setting '{key}' is required when chroma_db_impl={setting}"

    if setting == "clickhouse":
        require("clickhouse_host")
        require("clickhouse_port")
        require("persist_directory")
        print("Using Clickhouse for database")
        import chromadb.db.clickhouse

        return chromadb.db.clickhouse.Clickhouse(settings)
    elif setting == "duckdb+parquet":
        require("persist_directory")
        import chromadb.db.duckdb

        return chromadb.db.duckdb.PersistentDuckDB(settings)
    elif setting == "duckdb":
        require("persist_directory")
        print("Using DuckDB in-memory for database. Data will be transient.")
        import chromadb.db.duckdb

        return chromadb.db.duckdb.DuckDB(settings)
    else:
        raise Exception(f"Unknown value '{setting} for chroma_db_impl")


def Client(settings=__settings):
    """Return a chroma.API instance based on the provided or environmental
    settings, optionally overriding the DB instance."""

    setting = settings.chroma_api_impl.lower()

    def require(key):
        assert settings[key], f"Setting '{key}' is required when chroma_api_impl={setting}"

    if setting == "rest":
        require("chroma_server_host")
        require("chroma_server_http_port")
        print("Running Chroma in client mode using REST to connect to remote server")
        import chromadb.api.fastapi

        return chromadb.api.fastapi.FastAPI(settings)
    elif setting == "local":
        print("Running Chroma using direct local API.")
        import chromadb.api.local

        return chromadb.api.local.LocalAPI(settings, get_db(settings))
    else:
        raise Exception(f"Unknown value '{setting} for chroma_api_impl")
