# type: ignore
import pytest

from chromadb.errors import NotFoundError

batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


def test_delete(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    with pytest.raises(Exception):
        collection.delete()


def test_delete_returns_delete_result(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    result = collection.delete(ids=batch_records["ids"])
    assert isinstance(result, dict)
    assert "deleted" in result
    assert result["deleted"] >= 0


def test_delete_with_limit(client):
    client.reset()
    collection = client.create_collection(
        "testspace",
        metadata={"hnsw:space": "l2"},
    )
    collection.add(
        ids=["id1", "id2", "id3", "id4", "id5"],
        embeddings=[[1, 0, 0], [0, 1, 0], [0, 0, 1], [1, 1, 0], [0, 1, 1]],
        metadatas=[
            {"category": "a"},
            {"category": "a"},
            {"category": "a"},
            {"category": "b"},
            {"category": "b"},
        ],
    )
    assert collection.count() == 5
    result = collection.delete(where={"category": "a"}, limit=2)
    assert result["deleted"] == 2
    assert collection.count() == 3


def test_delete_with_limit_zero_is_noop(client):
    client.reset()
    collection = client.create_collection(
        "testspace",
        metadata={"hnsw:space": "l2"},
    )
    collection.add(
        ids=["id1", "id2"],
        embeddings=[[1, 0, 0], [0, 1, 0]],
        metadatas=[{"category": "a"}, {"category": "a"}],
    )
    assert collection.count() == 2
    result = collection.delete(where={"category": "a"}, limit=0)
    assert result["deleted"] == 0
    assert collection.count() == 2


def test_delete_with_limit_requires_where(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    with pytest.raises(ValueError, match="limit can only be specified"):
        collection.delete(ids=["id1"], limit=5)


def test_delete_with_index(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1)


def test_collection_delete_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")
    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.delete(ids=["id1"])
