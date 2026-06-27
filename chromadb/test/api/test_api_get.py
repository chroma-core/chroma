# type: ignore
import pytest

from chromadb.errors import NotFoundError
from chromadb.api.types import QueryResult

from .utils import approx_equal

batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}

records = {
    "embeddings": [[0, 0, 0], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
    "documents": ["this document is first", "this document is second"],
}


def test_get_from_db(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    includes = ["embeddings", "documents", "metadatas"]
    records = collection.get(include=includes)
    for key in records.keys():
        if (key in includes) or (key == "ids"):
            assert len(records[key]) == 2
        elif key == "included":
            assert set(records[key]) == set(includes)
        else:
            assert records[key] is None


def test_get_include(client):
    client.reset()
    collection = client.create_collection("test_get_include")
    collection.add(**records)
    include = ["metadatas", "documents"]
    items = collection.get(include=include, where={"int_value": 1})
    assert items["embeddings"] is None
    assert items["ids"][0] == "id1"
    assert items["metadatas"][0]["int_value"] == 1
    assert items["documents"][0] == "this document is first"
    assert set(items["included"]) == set(include)
    include = ["embeddings", "documents"]
    items = collection.get(include=include)
    assert items["metadatas"] is None
    assert items["ids"][0] == "id1"
    assert approx_equal(items["embeddings"][1][0], 1.2)
    assert set(items["included"]) == set(include)
    items = collection.get(include=[])
    assert items["documents"] is None
    assert items["metadatas"] is None
    assert items["embeddings"] is None
    assert items["ids"][0] == "id1"
    assert items["included"] == []
    with pytest.raises(ValueError, match="include"):
        items = collection.get(include=["metadatas", "undefined"])
    with pytest.raises(ValueError, match="include"):
        items = collection.get(include=None)


def test_get_version(client):
    import re

    client.reset()
    version = client.get_version()
    assert re.match(r"\d+\.\d+\.\d+", version)


def test_collection_get_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")
    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.get()


def test_get_collection_by_id(client):
    import uuid

    client.reset()
    collection = client.create_collection("testspace", metadata={"key": "value"})
    collection_id = collection.id
    retrieved = client.get_collection_by_id(collection_id)
    assert retrieved.name == "testspace"
    assert retrieved.id == collection_id
    assert retrieved.metadata == {"key": "value"}
    with pytest.raises(NotFoundError):
        client.get_collection_by_id(uuid.uuid4())


def test_list_collections(client):
    client.reset()
    client.create_collection("testspace")
    client.create_collection("testspace2")
    collections = client.list_collections()
    assert len(collections) == 2
