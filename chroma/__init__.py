import chroma.config
import logging


__settings = chroma.config.Settings()


def configure(**kwags):
    """Override Chroma's default settings, environment variables or .env files"""
    __settings = chroma.config.Settings(**kwargs)


def get_settings():
    return __settings


def get_db(settings=__settings):
    """Return a chroma.DB instance based on the provided or environmental settings."""

    setting = settings.chroma_db_impl.lower()

    def require(key):
        assert settings[key], f"Setting '{key}' is required when chroma_db_impl={setting}"

    if setting == "clickhouse":
        require('clickhouse_host')
        require('clickhouse_port')
        require('chroma_cache_dir')
        print("Using Clickhouse for database")
        import chroma.db.clickhouse
        return chroma.db.clickhouse.Clickhouse(settings)
    elif setting  == "duckdb+parquet":
        require('chroma_cache_dir')
        import chroma.db.duckdb
        return chroma.db.duckdb.PersistentDuckDB(settings)
    elif setting == "duckdb":
        require('chroma_cache_dir')
        print("Using DuckDB in-memory for database. Data will be transient.")
        import chroma.db.duckdb
        return chroma.db.duckdb.DuckDB(settings)
    else:
        raise Exception(f"Unknown value '{setting} for chroma_db_impl")


def init(settings=__settings):
    """Return a chroma.API instance based on the provided or environmental
       settings, optionally overriding the DB instance."""

    setting = settings.chroma_api_impl.lower()

    def require(key):
        assert settings[key], f"Setting '{key}' is required when chroma_api_impl={setting}"

    if setting == "arrowflight":
        require('chroma_server_host')
        require('chroma_server_grpc_port')
        print("Running Chroma in client mode using ArrowFlight to connect to remote server")
        import chroma.api.arrowflight
        return chroma.api.arrowflight.ArrowFlightAPI(settings)
    elif setting == "rest":
        require('chroma_server_host')
        require('chroma_server_http_port')
        print("Running Chroma in client mode using REST to connect to remote server")
        import chroma.api.fastapi
        return chroma.api.fastapi.FastAPI(settings)
    elif setting == "celery":
        require('celery_broker_url')
        require('celery_result_backend')
        print("Running Chroma in server mode with Celery jobs enabled.")
        import chroma.api.celery
        return chroma.api.celery.CeleryAPI(settings, get_db(settings))
    elif setting == "local":
        print("Running Chroma using direct local API.")
        import chroma.api.local
        return chroma.api.local.LocalAPI(settings, get_db(settings))
    else:
        raise Exception(f"Unknown value '{setting} for chroma_api_impl")
