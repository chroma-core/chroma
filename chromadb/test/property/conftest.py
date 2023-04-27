from typing import Generator
import pytest
from chromadb import Client
from chromadb.api import API
from chromadb.config import Settings
from chromadb.test.configurations import configurations
import os
import shutil


# https://docs.pytest.org/en/latest/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
@pytest.fixture(scope="module", params=configurations())
def api(request) -> Generator[API, None, None]:
    configuration = request.param
    yield Client(configuration)
    if configuration.chroma_db_impl == "duckdb+parquet":
        if os.path.exists(configuration.persist_directory):
            shutil.rmtree(configuration.persist_directory)


@pytest.fixture(scope="module", params=configurations(True))
def settings(request) -> Generator[Settings, None, None]:
    configuration = request.param
    yield configuration
    save_path = configuration.persist_directory
    # Remove if it exists
    if os.path.exists(save_path):
        shutil.rmtree(save_path)
