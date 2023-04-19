from chromadb.config import Settings
from chromadb import Client
import hypothesis
import tempfile
import os


hypothesis.settings.register_profile(
    "dev", deadline=10000, suppress_health_check=[hypothesis.HealthCheck.data_too_large]
)
hypothesis.settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "dev"))


def duckdb():
    yield Client(
       Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb",
            persist_directory=tempfile.gettempdir(),
        )
    )


def duckdb_parquet():
    yield Client(
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb+parquet",
            persist_directory=tempfile.gettempdir() + "/tests",
        )
    )


def fixtures():
    return [duckdb, duckdb_parquet]


def persist_configurations():
    return [
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb+parquet",
            persist_directory=tempfile.gettempdir() + "/tests",
        )
    ]