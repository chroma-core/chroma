from chromadb.config import Settings
import hypothesis
import tempfile
import os


hypothesis.settings.register_profile("dev", deadline=10000)
hypothesis.settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "dev"))


def configurations():
    """Based on the environment, return a list of API configurations to test."""
    return [
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb",
            persist_directory=tempfile.gettempdir(),
        ),
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb+parquet",
            persist_directory=tempfile.gettempdir() + "/tests",
        ),
    ]


def persist_configurations():
    """Only returns configurations that persist to disk."""
    return [
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb+parquet",
            persist_directory=tempfile.gettempdir() + "/tests",
        ),
    ]

