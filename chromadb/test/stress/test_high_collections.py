from typing import List
import numpy as np

from chromadb.api import API
from chromadb.api.models.Collection import Collection


def test_high_collections(api: API) -> None:
    """Test that we can create a large number of collections and that the system
    # remains responsive."""
    api.reset()

    metadata = None
    if api.get_settings().is_persistent:
        metadata = {"hnsw:batch_size": 3, "hnsw:sync_threshold": 3}
    else:
        return  # FOR NOW

    num_collections = 10000
    collections: List[Collection] = []
    for i in range(num_collections):
        new_collection = api.create_collection(
            f"test_collection_{i}",
            metadata=metadata,
        )
        collections.append(new_collection)
        if i % 100 == 0:
            print(f"Created {i} collections")

    # Add a few embeddings to each collection
    N = 10
    D = 10
    data = np.random.rand(N, D).tolist()
    ids = [f"test_id_{i}" for i in range(N)]
    for i in range(num_collections):
        collections[i].add(ids, data)
        print(f"Added embeddings to {i} collections")
