from typing import Generator
import pytest
from chromadb import Client
from chromadb.api import API
from chromadb.test.configurations import configurations
import os
import shutil


# https://docs.pytest.org/en/latest/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
@pytest.fixture(scope="module", params=configurations())
def api(request) -> Generator[API, None, None]:
    configuration = request.param
    yield Client(configuration)
    if configuration.persist_directory is not None:
        if os.path.exists(configuration.persist_directory):
            shutil.rmtree(configuration.persist_directory)
