from uuid import uuid4

from chromadb.api.collection_configuration import CollectionConfiguration
from chromadb.types import Collection


def test_setitem_updates_collection_configuration() -> None:
    collection = Collection(
        id=uuid4(),
        name="test",
        configuration_json={},
        serialized_schema=None,
        metadata=None,
        dimension=None,
        tenant="tenant",
        database="database",
    )
    configuration = CollectionConfiguration(
        hnsw=None,
        spann=None,
        embedding_function=None,
    )

    collection["configuration"] = configuration

    assert collection.configuration_json == {
        "hnsw": None,
        "spann": None,
        "embedding_function": {"type": "legacy"},
    }
