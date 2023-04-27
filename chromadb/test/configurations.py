from typing import List, Tuple
from chromadb.config import Settings
import hypothesis
import tempfile
import os


hypothesis.settings.register_profile(
    "dev",
    deadline=10000,
    suppress_health_check=[hypothesis.HealthCheck.data_too_large,
                           hypothesis.HealthCheck.large_base_example]
)
hypothesis.settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "dev"))


def configurations(persist_only: bool = False):
    """Based on the environment, return a list of API configurations to test."""
    configurations = [
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
    if persist_only:
        return configurations[1:]
    else:
        return configurations


def persist_old_version_configurations(
    versions: List[str],
) -> List[Tuple[str, Settings]]:
    """
    Only returns configurations that persist to disk at a given path for a version.
    """

    return [
        (
            version,
            Settings(
                chroma_api_impl="local",
                chroma_db_impl="duckdb+parquet",
                persist_directory=tempfile.gettempdir() + "/tests/" + version + "/",
            ),
        )
        for version in versions
    ]
