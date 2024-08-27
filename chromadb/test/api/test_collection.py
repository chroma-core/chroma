import pytest
import numpy as np

from chromadb.test.api.utils import local_persist_api, batch_records,records
from chromadb.errors import InvalidCollectionException

@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist(api_fixture, request):
    client = request.getfixturevalue(api_fixture.__name__)

    client.reset()

    collection = client.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2

    client = request.getfixturevalue(api_fixture.__name__)
    collection = client.get_collection("testspace")
    assert collection.count() == 2

    client.delete_collection("testspace")

    client = request.getfixturevalue(api_fixture.__name__)
    assert client.list_collections() == []

def test_get_or_create(client):
    client.reset()

    collection = client.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2

    with pytest.raises(Exception):
        collection = client.create_collection("testspace")

    collection = client.get_or_create_collection("testspace")

    assert collection.count() == 2
    
# test delete_collection
def test_delete_collection(client):
    client.reset()
    collection = client.create_collection("test_delete_collection")
    collection.add(**records)

    assert len(client.list_collections()) == 1
    client.delete_collection("test_delete_collection")
    assert len(client.list_collections()) == 0
    
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

    assert results1["ids"][0][0] == ids1[0]
    assert results2["ids"][0][0] == ids2[0]
    
def test_collection_peek_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.peek()