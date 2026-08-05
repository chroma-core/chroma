# type: ignore
import pytest

from chromadb.errors import NotFoundError

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


def test_get_or_create(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    with pytest.raises(Exception):
        collection = client.create_collection("testspace")
    collection = client.get_or_create_collection("testspace")
    assert collection.count() == 2


def test_add_a_collection(client):
    client.reset()
    client.create_collection("testspace")
    collection = client.get_collection("testspace")
    assert collection.name == "testspace"
    with pytest.raises(Exception):
        collection = client.get_collection("testspace2")


def test_delete_collection(client):
    client.reset()
    collection = client.create_collection("test_delete_collection")
    collection.add(**records)
    assert len(client.list_collections()) == 1
    client.delete_collection("test_delete_collection")
    assert len(client.list_collections()) == 0


def test_count(client):
    client.reset()
    collection = client.create_collection("testspace")
    assert collection.count() == 0
    collection.add(**batch_records)
    assert collection.count() == 2


def test_collection_count_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")
    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.count()


def test_peek(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    peek = collection.peek()
    for key in peek.keys():
        if key in ["embeddings", "documents", "metadatas"] or key == "ids":
            assert len(peek[key]) == 2
        elif key == "included":
            assert set(peek[key]) == set(["embeddings", "metadatas", "documents"])
        else:
            assert peek[key] is None


def test_collection_peek_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")
    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.peek()


def test_reset(client):
    client.reset()
    client.create_collection("testspace")
    client.create_collection("testspace2")
    collections = client.list_collections()
    assert len(collections) == 2
    client.reset()
    collections = client.list_collections()
    assert len(collections) == 0


def test_reset_db(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    client.reset()
    assert len(client.list_collections()) == 0
