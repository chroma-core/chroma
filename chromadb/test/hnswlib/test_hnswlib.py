from chromadb.db.index.hnswlib import Hnswlib
from chromadb.config import Settings
import uuid
import numpy as np


def test_count_tracking() -> None:
    hnswlib = Hnswlib("test", Settings(), {})
    hnswlib._init_index(2)
    assert hnswlib._index_metadata["elements"] == 0
    idA, idB = uuid.uuid4(), uuid.uuid4()

    embeddingA = np.random.rand(1, 2)
    hnswlib.add([idA], embeddingA.tolist())
    assert hnswlib._index_metadata["elements"] == 1

    embeddingB = np.random.rand(1, 2)
    hnswlib.add([idB], embeddingB.tolist())
    assert hnswlib._index_metadata["elements"] == 2
    hnswlib.delete_from_index(ids=[idA])
    assert hnswlib._index_metadata["elements"] == 1
    hnswlib.delete_from_index(ids=[idB])
    assert hnswlib._index_metadata["elements"] == 0
