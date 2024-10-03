from chromadb.api import CollectionConfiguration
from chromadb.api.client import Client
from chromadb.api.configuration import HNSWConfiguration
from chromadb.config import System
from chromadb.test.property import invariants


def test_log_purge(sqlite_persistent: System) -> None:
    client = Client.from_system(sqlite_persistent)

    first_collection = client.create_collection(
        "first_collection",
        configuration=CollectionConfiguration(
            hnsw_configuration=HNSWConfiguration(sync_threshold=10, batch_size=10)
        ),
    )
    second_collection = client.create_collection(
        "second_collection",
        configuration=CollectionConfiguration(
            hnsw_configuration=HNSWConfiguration(sync_threshold=10, batch_size=10)
        ),
    )
    collections = [first_collection, second_collection]

    # (Does not trigger a purge)
    for i in range(5):
        first_collection.add(ids=str(i), embeddings=[i, i])

    # (Should trigger a purge)
    for i in range(100):
        second_collection.add(ids=str(i), embeddings=[i, i])

    # The purge of the second collection should not be blocked by the first
    invariants.log_size_below_max(client._system, collections, True)
