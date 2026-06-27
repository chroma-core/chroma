# type: ignore
import numpy as np
import pytest

from chromadb.errors import NotFoundError

batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}

minimal_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


def test_add(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2


def test_add_minimal(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**minimal_records)
    assert collection.count() == 2


def test_add_large(client):
    client.reset()
    collection = client.create_collection("testspace")
    large_records = np.random.rand(2000, 512).astype(np.float32).tolist()
    collection.add(
        embeddings=large_records,
        ids=[f"http://example.com/{i}" for i in range(len(large_records))],
    )
    assert collection.count() == len(large_records)


def test_collection_add_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")
    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.add(**batch_records)


def test_increment_index_on(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


def test_multiple_collections(client):
    embeddings1 = np.random.rand(10, 512).astype(np.float32).tolist()
    embeddings2 = np.random.rand(10, 512).astype(np.float32).tolist()
    ids1 = [f"http://example.com/1/{i}" for i in range(len(embeddings1))]
    ids2 = [f"http://example.com/2/{i}" for i in range(len(embeddings2))]
    client.reset()
    coll1 = client.create_collection("coll1")
    coll1.add(embeddings=embeddings1, ids=ids1)
    coll2 = client.create_collection("coll2")
    coll2.add(embeddings=embeddings2, ids=ids2)
    assert len(client.list_collections()) == 2
    assert coll1.count() == len(embeddings1)
    assert coll2.count() == len(embeddings2)
    results1 = coll1.query(query_embeddings=embeddings1[0], n_results=1)
    results2 = coll2.query(query_embeddings=embeddings2[0], n_results=1)
    assert len(results1["ids"]) > 0
    assert len(results2["ids"]) > 0
    assert len(results1["ids"][0]) > 0
    assert len(results2["ids"][0]) > 0
    assert results1["ids"][0][0] == ids1[0]
    assert results2["ids"][0][0] == ids2[0]
