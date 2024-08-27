import pytest
import numpy as np

from chromadb.test.api.utils import local_persist_api, records
import tempfile
from chromadb.api.types import EmbeddingFunction, Document

persist_dir = tempfile.mkdtemp()


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist_index_loading(api_fixture, request):
    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection("test")
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection("test")

    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist_index_loading_embedding_function(api_fixture, request):
    class TestEF(EmbeddingFunction[Document]):
        def __call__(self, input):
            return [[1, 2, 3] for _ in range(len(input))]

    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection("test", embedding_function=TestEF())
    collection.add(ids="id1", documents="hello")

    client2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = client2.get_collection("test", embedding_function=TestEF())

    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_texts="hello",
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

@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist_index_get_or_create_embedding_function(api_fixture, request):
    class TestEF(EmbeddingFunction[Document]):
        def __call__(self, input):
            return [[1, 2, 3] for _ in range(len(input))]

    api = request.getfixturevalue("local_persist_api")
    api.reset()
    collection = api.get_or_create_collection("test", embedding_function=TestEF())
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_or_create_collection("test", embedding_function=TestEF())

    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_texts="hello",
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

    assert nn["ids"] == [["id1"]]
    assert nn["embeddings"] == [[[1, 2, 3]]]
    assert nn["documents"] == [["hello"]]
    assert nn["distances"] == [[0]]
    
def test_index_params(client):
    EPS = 1e-12
    # first standard add
    client.reset()
    collection = client.create_collection(name="test_index_params")
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] > 4

    # cosine
    client.reset()
    collection = client.create_collection(
        name="test_index_params",
        metadata={"hnsw:space": "cosine", "hnsw:construction_ef": 20, "hnsw:M": 5},
    )
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] > 0 - EPS
    assert items["distances"][0][0] < 1 + EPS

    # ip
    client.reset()
    collection = client.create_collection(
        name="test_index_params", metadata={"hnsw:space": "ip"}
    )
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] < -5
    
def test_invalid_index_params(client):
    client.reset()

    with pytest.raises(Exception):
        collection = client.create_collection(
            name="test_index_params", metadata={"hnsw:foobar": "blarg"}
        )
        collection.add(**records)

    with pytest.raises(Exception):
        collection = client.create_collection(
            name="test_index_params", metadata={"hnsw:space": "foobar"}
        )
        collection.add(**records)
        
def test_persist_index_loading_params(client, request):
    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection(
        "test",
        metadata={"hnsw:space": "ip"},
    )
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection(
        "test",
    )

    assert collection.metadata["hnsw:space"] == "ip"
    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_texts="hello",
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