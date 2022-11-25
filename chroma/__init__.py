import chroma.config


__settings = chroma.config.Settings()


def configure(**kwags):
    """Override Chroma's default settings, environment variables or .env files"""
    __settings = chroma.config.Settings(**kwargs)


def get_settings():
    return __settings


def get_db(settings=__settings):
    """Return a chroma.DB instance based on the provided or environmental settings."""

    if settings.clickhouse_host:
        print("Using Clickhouse for database")
        import chroma.db.clickhouse
        return chroma.db.clickhouse.Clickhouse(settings)
    elif settings.chroma_cache_dir:
        print("Using DuckDB with local filesystem persistence for database")
        import chroma.db.duckdb
        return chroma.db.duckdb.PersistentDuckDB(settings)
    else:
        print("Using DuckDB in-memory for database. Data will be transient.")
        import chroma.db.duckdb
        return chroma.db.duckdb.DuckDB(settings)


def get_api(settings=__settings):
    """Return a chroma.API instance based on the provided or environmental
       settings, optionally overriding the DB instance."""

    if settings.chroma_server_host and settings.chroma_server_grpc_port:
        print("Running Chroma in client/server mode using ArrowFlight protocol.")
        import chroma.api.arrowflight
        return chroma.api.arrowflight.ArrowFlightAPI(settings)
    elif settings.chroma_server_host and settings.chroma_server_http_port:
        print("Running Chroma in client/server mode using REST protocol.")
        import chroma.api.fastapi
        return chroma.api.fastapi.FastAPI(settings)
    elif settings.celery_broker_url:
        print("Running Chroma in server mode with Celery jobs enabled.")
        import chroma.api.celery
        return chroma.api.celery.CeleryAPI(settings, get_db(settings))
    else:
        print("Running Chroma using direct local API.")
        import chroma.api.local
        return chroma.api.local.LocalAPI(settings, get_db(settings))
