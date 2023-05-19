import os
import shutil
import tempfile
from typing import Generator

import pytest
from chromadb.db.index.hnswlib import Hnswlib
from chromadb.config import Settings
import uuid
import numpy as np


@pytest.fixture(scope="module")
def settings() -> Generator[Settings, None, None]:
    save_path = tempfile.gettempdir() + "/tests/hnswlib/"
    yield Settings(persist_directory=save_path)
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def test_count_tracking(settings: Settings) -> None:
    hnswlib = Hnswlib("test", settings, {}, 2)
    hnswlib._init_index(2)
    assert hnswlib._index_metadata["curr_elements"] == 0
    assert hnswlib._index_metadata["total_elements_added"] == 0
    idA, idB = uuid.uuid4(), uuid.uuid4()

    embeddingA = np.random.rand(1, 2)
    hnswlib.add([idA], embeddingA.tolist())
    assert (
        hnswlib._index_metadata["curr_elements"]
        == hnswlib._index_metadata["total_elements_added"]
        == 1
    )
    embeddingB = np.random.rand(1, 2)
    hnswlib.add([idB], embeddingB.tolist())
    assert (
        hnswlib._index_metadata["curr_elements"]
        == hnswlib._index_metadata["total_elements_added"]
        == 2
    )
    hnswlib.delete_from_index(ids=[idA])
    assert hnswlib._index_metadata["curr_elements"] == 1
    assert hnswlib._index_metadata["total_elements_added"] == 2
    hnswlib.delete_from_index(ids=[idB])
    assert hnswlib._index_metadata["curr_elements"] == 0
    assert hnswlib._index_metadata["total_elements_added"] == 2


def test_add_delete_large_amount(settings: Settings) -> None:
    # Test adding a large number of records
    N = 2000
    D = 512
    large_records = np.random.rand(N, D).astype(np.float32).tolist()
    ids = [uuid.uuid4() for _ in range(N)]
    hnswlib = Hnswlib("test", settings, {}, N)
    hnswlib._init_index(D)
    hnswlib.add(ids, large_records)
    assert hnswlib._index_metadata["curr_elements"] == N
    assert hnswlib._index_metadata["total_elements_added"] == N

    # Test deleting a large number of records by getting a random subset of the ids
    ids_to_delete = np.random.choice(np.array(ids), size=100, replace=False).tolist()
    hnswlib.delete_from_index(ids_to_delete)

    assert hnswlib._index_metadata["curr_elements"] == N - 100
    assert hnswlib._index_metadata["total_elements_added"] == N
